namespace QoSKit
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// The abstract base for every queue discipline. It owns depth limits and overflow, thread
    /// safety, asynchronous wakeups, statistics, events, and disposal. A concrete discipline supplies
    /// only its classification and ordering logic.
    /// </summary>
    /// <remarks>
    /// Thread safety: all public members are safe for concurrent producers and consumers. User
    /// delegates (classifiers, cost selectors, event handlers) are invoked with no internal lock
    /// held, so a throwing delegate can never leak a lock or stall servicing.
    /// </remarks>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public abstract class QoSQueueBase<T> : IQoSQueue<T>, IQoSGaugeSource
    {
        // The class label used when a discipline sets no traffic class (FIFO, LIFO), matching the
        // reserved default-class name used by class-based disciplines.
        private const string DefaultClassLabel = "class-default";

        // Bounds on the re-check delay an async consumer arms when items are resident but none is
        // currently eligible (an LLQ priority class throttled by its policer). The floor keeps a
        // computed delay of zero from spinning; the ceiling is a safety-net poll, so a consumer
        // re-checks at least this often even if a discipline cannot predict eligibility.
        private const long MinimumEligibilityDelayMilliseconds = 1;
        private const long MaximumEligibilityDelayMilliseconds = 1000;

        private static readonly TimerCallback _EligibilityTimerCallback = OnEligibilityTimer;

        private static int _NameCounter;

        private readonly object _Lock = new object();
        private readonly string _TypeLabel;
        private string _Name;
        private int _MaxDepth;
        private OverflowPolicy _OverflowPolicy;
        private readonly IQoSTimeProvider _TimeProvider;
        private readonly bool _EnableMetrics;
        private readonly bool _EnablePerClassMetrics;
        private readonly bool _EnableTracing;
        private readonly LinkedList<QoSWaiter> _ItemWaiters = new LinkedList<QoSWaiter>();
        private readonly LinkedList<QoSWaiter> _SpaceWaiters = new LinkedList<QoSWaiter>();

        private long _SequenceCounter;
        private int _Count;
        private int _PeakDepth;
        private long _Enqueued;
        private long _Dequeued;
        private long _Dropped;
        private long _Rejected;
        private long _ResidentCost;
        private long _GaugeId;
        private double _TotalWaitMilliseconds;
        private int _Disposed;

        /// <summary>
        /// The cost function used by size-fair disciplines. Null means every item costs 1. A
        /// discipline sets this from its constructor; the base validates and applies it.
        /// </summary>
        private Func<T, int>? _CostSelector;

        /// <summary>The optional durability journal. Null when persistence is not enabled.</summary>
        private IQoSStore<T>? _Store;

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSQueueBase{T}"/> class.
        /// </summary>
        /// <param name="options">Cross-cutting options; when null, defaults are used.</param>
        /// <param name="typeLabel">A short discipline label used to auto-generate a name when none is set.</param>
        protected QoSQueueBase(QoSQueueOptions? options, string typeLabel)
        {
            options ??= new QoSQueueOptions();
            _TypeLabel = typeLabel;
            _MaxDepth = options.MaxDepth;
            _OverflowPolicy = options.OverflowPolicy;
            _TimeProvider = options.TimeProvider;
            _EnableMetrics = options.EnableMetrics;
            _EnablePerClassMetrics = options.EnablePerClassMetrics;
            _EnableTracing = options.EnableTracing;
            _Name = options.Name ?? typeLabel + "-" + Interlocked.Increment(ref _NameCounter).ToString();

            if (_EnableMetrics)
                _GaugeId = QoSMetrics.RegisterGaugeSource(this);
        }

        /// <inheritdoc/>
        public string Name
        {
            get { return _Name; }
        }

        /// <inheritdoc/>
        public int MaxDepth
        {
            get { return _MaxDepth; }
        }

        /// <inheritdoc/>
        public OverflowPolicy OverflowPolicy
        {
            get { return _OverflowPolicy; }
        }

        /// <summary>
        /// Gets a value indicating whether this queue publishes metrics to the QoSKit meter.
        /// </summary>
        public bool MetricsEnabled
        {
            get { return _EnableMetrics; }
        }

        /// <inheritdoc/>
        public int Count
        {
            get { lock (_Lock) { return _Count; } }
        }

        /// <inheritdoc/>
        public bool IsEmpty
        {
            get { lock (_Lock) { return _Count == 0; } }
        }

        /// <inheritdoc/>
        public bool IsSynchronized
        {
            get { return true; }
        }

        /// <inheritdoc/>
        public object SyncRoot
        {
            get { return _Lock; }
        }

        /// <inheritdoc/>
        public QoSQueueStatistics Statistics
        {
            get
            {
                lock (_Lock)
                {
                    double average = _Dequeued > 0 ? _TotalWaitMilliseconds / _Dequeued : 0.0;
                    return new QoSQueueStatistics(_Enqueued, _Dequeued, _Dropped, _Rejected, _Count, _PeakDepth, average);
                }
            }
        }

        /// <inheritdoc/>
        public event EventHandler<QoSItemEventArgs<T>>? ItemEnqueued;

        /// <inheritdoc/>
        public event EventHandler<QoSItemEventArgs<T>>? ItemDequeued;

        /// <inheritdoc/>
        public event EventHandler<QoSDropEventArgs<T>>? ItemDropped;

        // Internal diagnostics for the test harness (see [InternalsVisibleTo]).
        internal int PendingItemWaiterCount
        {
            get { lock (_Lock) { return _ItemWaiters.Count; } }
        }

        internal int PendingSpaceWaiterCount
        {
            get { lock (_Lock) { return _SpaceWaiters.Count; } }
        }

        // Pull-gauge surface consumed by the QoSKit meter. Reads are cheap and off the hot path.
        string IQoSGaugeSource.GaugeQueueName
        {
            get { return _Name; }
        }

        string IQoSGaugeSource.GaugeQueueType
        {
            get { return _TypeLabel; }
        }

        long IQoSGaugeSource.GaugeCapacity
        {
            get { lock (_Lock) { return _MaxDepth; } }
        }

        long IQoSGaugeSource.GaugePeakDepth
        {
            get { lock (_Lock) { return _PeakDepth; } }
        }

        long IQoSGaugeSource.GaugeResidentBytes
        {
            get { lock (_Lock) { return _ResidentCost; } }
        }

        // Maps an entry's traffic class to the metric/span tag value, honoring the per-class switch.
        // Returns null when per-class breakdown is disabled, which drops the class tag entirely.
        private string? PerClassLabel(string? key)
        {
            if (!_EnablePerClassMetrics)
                return null;
            return key ?? DefaultClassLabel;
        }

        // Emits a policer conform decision (a class policer had tokens and the item was served).
        // Called by a discipline under the queue lock; constant-time and allocation-free.
        private protected void ReportPolicerConformed(string className)
        {
            if (_EnableMetrics)
                QoSMetrics.PolicerConformed(_Name, _TypeLabel, PerClassLabel(className));
        }

        // Emits a policer exceed decision (a class policer had no tokens and the class was throttled).
        private protected void ReportPolicerExceeded(string className)
        {
            if (_EnableMetrics)
                QoSMetrics.PolicerExceeded(_Name, _TypeLabel, PerClassLabel(className));
        }

        /// <summary>
        /// Sets the cost selector used by size-fair disciplines. Call from a derived constructor.
        /// </summary>
        /// <param name="costSelector">The cost function, or null for unit cost.</param>
        protected void SetCostSelector(Func<T, int>? costSelector)
        {
            _CostSelector = costSelector;
        }

        /// <summary>Sets the queue name (fluent configuration).</summary>
        /// <param name="name">The new name; must not be null, empty, or whitespace.</param>
        /// <exception cref="ArgumentException">The name is null, empty, or whitespace.</exception>
        protected void SetName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Name must not be null, empty, or whitespace.", nameof(name));
            lock (_Lock)
            {
                _Name = name;
            }
        }

        /// <summary>Sets the maximum depth (fluent configuration). Lowering below the current count does not evict.</summary>
        /// <param name="maxDepth">Zero for unbounded, or a positive bound.</param>
        /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
        protected void SetMaxDepth(int maxDepth)
        {
            if (maxDepth < 0)
                throw new ArgumentOutOfRangeException(nameof(maxDepth), "MaxDepth must be zero (unbounded) or greater.");
            lock (_Lock)
            {
                _MaxDepth = maxDepth;
            }

            // A raised limit frees capacity that no dequeue will announce; let blocked producers retry.
            WakeAllWaiters(_SpaceWaiters);
        }

        /// <summary>Sets the overflow policy (fluent configuration).</summary>
        /// <param name="policy">The overflow policy.</param>
        protected void SetOverflowPolicy(OverflowPolicy policy)
        {
            lock (_Lock)
            {
                _OverflowPolicy = policy;
            }
        }

        /// <summary>
        /// Gets the time provider supplied at construction.
        /// </summary>
        protected IQoSTimeProvider TimeProvider
        {
            get { return _TimeProvider; }
        }

        /// <summary>
        /// Enables durable persistence backed by the given store, replaying any persisted backlog into
        /// the queue. Must be called before any items are enqueued.
        /// </summary>
        /// <param name="store">The durability journal.</param>
        /// <exception cref="ArgumentNullException"><paramref name="store"/> is null.</exception>
        /// <exception cref="ObjectDisposedException">The queue has been disposed.</exception>
        /// <exception cref="InvalidOperationException">Persistence is already enabled, or the queue is not empty.</exception>
        public void EnablePersistence(IQoSStore<T> store)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));
            ThrowIfDisposed();
            lock (_Lock)
            {
                if (_Store != null)
                    throw new InvalidOperationException("Persistence is already enabled on this queue.");
                if (_Count > 0)
                    throw new InvalidOperationException("Enable persistence before enqueuing any items.");
                _Store = store;
            }

            IReadOnlyList<QoSPersistedItem<T>> recovered = store.Recover();
            foreach (QoSPersistedItem<T> persisted in recovered)
                RecoverEnqueue(persisted.Item, persisted.Sequence);
        }

        private void RecoverEnqueue(T item, long sequence)
        {
            if (!typeof(T).IsValueType && item is null)
                return;

            QoSEntry<T> entry = new QoSEntry<T>(item);
            entry.Cost = ComputeCost(item);
            ClassifyOutsideLock(entry);

            QoSWaiter? waiter = null;
            lock (_Lock)
            {
                if (!ResolveClassification(entry))
                    return;
                entry.Sequence = sequence;
                if (sequence > _SequenceCounter)
                    _SequenceCounter = sequence;
                entry.EnqueuedMilliseconds = _TimeProvider.MonotonicMilliseconds;
                StoreAdd(entry);
                _Count++;
                _ResidentCost += entry.Cost;
                if (_Count > _PeakDepth)
                    _PeakDepth = _Count;
                _Enqueued++;
                waiter = DequeueOneWaiter(_ItemWaiters);
            }

            SignalWaiter(waiter);
        }

        #region Discipline contract

        /// <summary>
        /// Computes an entry's classification by calling the discipline's user-supplied selector.
        /// Invoked outside the lock; must not touch shared mutable state. Sets the entry's Key or Band.
        /// </summary>
        /// <param name="entry">The entry to classify.</param>
        private protected abstract void ClassifyOutsideLock(QoSEntry<T> entry);

        /// <summary>
        /// Resolves a classified entry against shared state under the lock, creating a dynamic flow or
        /// routing to a default where the policy calls for it.
        /// </summary>
        /// <param name="entry">The classified entry.</param>
        /// <returns><c>true</c> to proceed; <c>false</c> to reject as an unknown classification.</returns>
        /// <exception cref="UnknownClassificationException">The key is unknown under a throw policy.</exception>
        private protected virtual bool ResolveClassification(QoSEntry<T> entry)
        {
            return true;
        }

        /// <summary>Adds a classified entry to the discipline structure. Called under the lock.</summary>
        /// <param name="entry">The entry to add.</param>
        private protected abstract void StoreAdd(QoSEntry<T> entry);

        /// <summary>Removes and returns the next entry to service. Called under the lock.</summary>
        /// <param name="entry">The removed entry when the method returns <c>true</c>.</param>
        /// <returns><c>true</c> if an entry was removed; otherwise <c>false</c>.</returns>
        private protected abstract bool StoreTryTake(out QoSEntry<T> entry);

        /// <summary>Returns the next entry without removing it. Called under the lock.</summary>
        /// <param name="entry">The next entry when the method returns <c>true</c>.</param>
        /// <returns><c>true</c> if an entry is available; otherwise <c>false</c>.</returns>
        private protected abstract bool StoreTryPeek(out QoSEntry<T> entry);

        /// <summary>Removes and returns the longest-waiting entry (for DropOldest). Called under the lock.</summary>
        /// <param name="entry">The removed entry when the method returns <c>true</c>.</param>
        /// <returns><c>true</c> if an entry was removed; otherwise <c>false</c>.</returns>
        private protected abstract bool StoreTryTakeOldest(out QoSEntry<T> entry);

        /// <summary>Removes all entries from the discipline structure. Called under the lock.</summary>
        private protected abstract void StoreClear();

        /// <summary>
        /// Removes a specific resident entry, applying the discipline's service accounting as if it had
        /// been scheduled. Used by chain pumps so the item removed from a source is exactly the item
        /// already forwarded downstream, even if the scheduling decision changed in between (a
        /// concurrent enqueue, a policer refill, or aging). Called under the lock. The default takes
        /// through <see cref="StoreTryTake"/> when the entry is still next in service order, and
        /// otherwise unlinks it through <see cref="StoreRemoveEntry"/>.
        /// </summary>
        /// <param name="entry">The entry to remove.</param>
        /// <returns><c>true</c> if the entry was resident and was removed; otherwise <c>false</c>.</returns>
        private protected virtual bool StoreTryTakeEntry(QoSEntry<T> entry)
        {
            if (entry.Node == null || entry.Node.List == null)
                return false;

            if (StoreTryPeek(out QoSEntry<T> next) && ReferenceEquals(next, entry))
            {
                bool took = StoreTryTake(out QoSEntry<T> taken);
                Debug.Assert(took && ReferenceEquals(taken, entry), "A discipline's peek and take must agree under the lock.");
                return took;
            }

            return StoreRemoveEntry(entry);
        }

        /// <summary>
        /// Unlinks a specific resident entry that is not next in service order, charging any
        /// discipline state (virtual time, deficit) for it. Called under the lock. The default only
        /// unlinks the entry from its list.
        /// </summary>
        /// <param name="entry">The entry to remove.</param>
        /// <returns><c>true</c> if the entry was resident and was removed; otherwise <c>false</c>.</returns>
        private protected virtual bool StoreRemoveEntry(QoSEntry<T> entry)
        {
            LinkedListNode<QoSEntry<T>>? node = entry.Node;
            LinkedList<QoSEntry<T>>? list = node?.List;
            if (node == null || list == null)
                return false;
            list.Remove(node);
            return true;
        }

        /// <summary>
        /// Reports how long until the earliest resident-but-ineligible entry can become eligible, for a
        /// discipline whose <see cref="StoreTryTake"/> can decline while entries are resident (an LLQ
        /// priority class throttled by its token-bucket policer). Called under the lock, only after a
        /// take has failed with entries resident. The default reports nothing, in which case an async
        /// consumer falls back to a bounded re-check poll.
        /// </summary>
        /// <param name="delayMilliseconds">The delay in milliseconds, zero or greater, when the method returns <c>true</c>.</param>
        /// <returns><c>true</c> if a delay was computed; otherwise <c>false</c>.</returns>
        private protected virtual bool StoreTryGetNextEligibleDelay(out long delayMilliseconds)
        {
            delayMilliseconds = 0;
            return false;
        }

        /// <summary>Appends resident items to the destination in a stable order. Called under the lock.</summary>
        /// <param name="destination">The list to append to.</param>
        private protected abstract void StoreSnapshot(List<T> destination);

        #endregion

        #region Enqueue

        /// <inheritdoc/>
        public void Enqueue(T item)
        {
            EnqueueInternal(item, null, throwOnFailure: true);
        }

        /// <inheritdoc/>
        public bool TryEnqueue(T item)
        {
            return EnqueueInternal(item, null, throwOnFailure: false);
        }

        /// <summary>
        /// Enqueues an item with a per-item classification override, bypassing the discipline's selector.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        /// <param name="classificationOverride">The override value (a band index or key), interpreted by the discipline.</param>
        /// <param name="throwOnFailure">When <c>true</c>, failures throw; when <c>false</c>, they return <c>false</c>.</param>
        /// <returns><c>true</c> if the item was admitted; otherwise <c>false</c>.</returns>
        protected bool EnqueueInternal(T item, object? classificationOverride, bool throwOnFailure)
        {
            return EnqueueCore(item, classificationOverride, throwOnFailure, throwOnFailure, out bool _);
        }

        // Admits an item. throwWhenFull governs only the full-queue rejection, which is also reported
        // through full, so an async producer under the Block policy waits for space only when the queue
        // is actually full and never mistakes another rejection (an unknown class) for a full queue.
        private bool EnqueueCore(T item, object? classificationOverride, bool throwOnFailure, bool throwWhenFull, out bool full)
        {
            full = false;
            ThrowIfDisposed();
            if (!typeof(T).IsValueType && item is null)
                throw new ArgumentNullException(nameof(item));

            QoSEntry<T> entry = new QoSEntry<T>(item);
            entry.Override = classificationOverride;
            entry.Cost = ComputeCost(item);

            // Classification's user-delegate call runs outside the lock.
            ClassifyOutsideLock(entry);

            // Serialize ahead of admission so a serializer failure aborts before the item is stored.
            object? prepared = _Store?.Prepare(item);

            QoSWaiter? waiterToSignal = null;
            bool unknownReject = false;
            bool droppedNewest = false;
            bool added = false;
            T evicted = default!;
            bool didEvict = false;
            long evictedSequence = 0;
            int evictedCost = 0;

            lock (_Lock)
            {
                ThrowIfDisposed();

                // Resolve classification against shared state (may create a dynamic flow, or throw
                // under a throw policy). Done before any depth eviction so an unknown key never
                // evicts a resident item.
                if (!ResolveClassification(entry))
                {
                    unknownReject = true;
                    _Dropped++;
                }
                else if (_MaxDepth > 0 && _Count >= _MaxDepth)
                {
                    switch (_OverflowPolicy)
                    {
                        case OverflowPolicy.DropNewest:
                            _Dropped++;
                            droppedNewest = true;
                            break;

                        case OverflowPolicy.DropOldest:
                            if (StoreTryTakeOldest(out QoSEntry<T> old))
                            {
                                evicted = old.Item;
                                evictedSequence = old.Sequence;
                                evictedCost = old.Cost;
                                didEvict = true;
                                _Count--;
                                _ResidentCost -= old.Cost;
                                _Dropped++;
                            }

                            AddEntryLocked(entry);
                            added = true;
                            waiterToSignal = DequeueOneWaiter(_ItemWaiters);
                            break;

                        default:
                            full = true;
                            _Rejected++;
                            break;
                    }
                }
                else
                {
                    AddEntryLocked(entry);
                    added = true;
                    waiterToSignal = DequeueOneWaiter(_ItemWaiters);
                }
            }

            string? classLabel = PerClassLabel(entry.Key);

            if (didEvict)
            {
                _Store?.Remove(evictedSequence);
                if (_EnableMetrics)
                    QoSMetrics.Dropped(_Name, _TypeLabel, classLabel, DropReason.Oldest, evictedCost, wasResident: true);
                RaiseDropped(evicted, DropReason.Oldest);
            }

            if (droppedNewest)
            {
                if (_EnableMetrics)
                    QoSMetrics.Dropped(_Name, _TypeLabel, classLabel, DropReason.Newest, entry.Cost, wasResident: false);
                TraceEnqueueOutcome(entry, classLabel, "dropped.newest", DropReason.Newest);
                RaiseDropped(item, DropReason.Newest);
                return false;
            }

            if (unknownReject)
            {
                if (_EnableMetrics)
                    QoSMetrics.Dropped(_Name, _TypeLabel, classLabel, DropReason.UnknownClass, entry.Cost, wasResident: false);
                TraceEnqueueOutcome(entry, classLabel, "dropped.unknown_class", DropReason.UnknownClass);
                RaiseDropped(item, DropReason.UnknownClass);
                if (throwOnFailure)
                    throw new UnknownClassificationException(_Name, entry.Key);
                return false;
            }

            if (full)
            {
                if (_EnableMetrics)
                    QoSMetrics.Rejected(_Name, _TypeLabel, classLabel, entry.Cost);
                TraceEnqueueOutcome(entry, classLabel, "rejected", null);
                if (throwWhenFull)
                    throw new QueueFullException(_Name, _MaxDepth, _MaxDepth);
                return false;
            }

            if (added)
            {
                _Store?.Commit(entry.Sequence, prepared);
                if (_EnableMetrics)
                    QoSMetrics.Enqueued(_Name, _TypeLabel, classLabel, entry.Cost);
                TraceEnqueueOutcome(entry, classLabel, "admitted", null);
                SignalWaiter(waiterToSignal);
                RaiseEnqueued(item);
                return true;
            }

            return false;
        }

        // Opens a short-lived enqueue span recording the admission outcome. Null-cost when no trace
        // listener is subscribed or tracing is disabled for this queue.
        private void TraceEnqueueOutcome(QoSEntry<T> entry, string? classLabel, string outcome, DropReason? reason)
        {
            if (!_EnableTracing || !QoSTracing.HasListeners)
                return;

            using (Activity? activity = QoSTracing.Source.StartActivity(QoSTracing.EnqueueSpanName, ActivityKind.Producer))
            {
                if (activity == null)
                    return;
                activity.SetTag(QoSMetrics.TagQueueName, _Name);
                activity.SetTag(QoSMetrics.TagQueueType, _TypeLabel);
                if (classLabel != null)
                    activity.SetTag(QoSMetrics.TagQueueClass, classLabel);
                activity.SetTag("qoskit.cost", entry.Cost);
                activity.SetTag("qoskit.outcome", outcome);
                if (reason.HasValue)
                    activity.SetTag(QoSMetrics.TagDropReason, reason.Value.ToString());
            }
        }

        // Opens a short-lived dequeue span recording the class, cost, and time the item waited.
        // Null-cost when no trace listener is subscribed or tracing is disabled for this queue.
        private void TraceDequeue(string? classLabel, int cost, double waitMilliseconds)
        {
            if (!_EnableTracing || !QoSTracing.HasListeners)
                return;

            using (Activity? activity = QoSTracing.Source.StartActivity(QoSTracing.DequeueSpanName, ActivityKind.Consumer))
            {
                if (activity == null)
                    return;
                activity.SetTag(QoSMetrics.TagQueueName, _Name);
                activity.SetTag(QoSMetrics.TagQueueType, _TypeLabel);
                if (classLabel != null)
                    activity.SetTag(QoSMetrics.TagQueueClass, classLabel);
                activity.SetTag("qoskit.cost", cost);
                activity.SetTag("qoskit.wait_ms", waitMilliseconds);
            }
        }

        /// <summary>
        /// Asynchronously enqueues an item. Under <see cref="OverflowPolicy.Block"/> this waits for
        /// capacity rather than rejecting.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        /// <param name="cancellationToken">A token to cancel the wait.</param>
        /// <returns>A task that completes when the item is admitted.</returns>
        /// <exception cref="ArgumentNullException">The item is null (reference type).</exception>
        /// <exception cref="OperationCanceledException">The token was cancelled while waiting, or the queue was disposed while waiting.</exception>
        /// <exception cref="ObjectDisposedException">The queue has been disposed.</exception>
        /// <exception cref="UnknownClassificationException">The item's class is unknown and the discipline rejects unknown classes.</exception>
        public async ValueTask EnqueueAsync(T item, CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();
            if (!typeof(T).IsValueType && item is null)
                throw new ArgumentNullException(nameof(item));

            bool completed = false;
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (_OverflowPolicy != OverflowPolicy.Block)
                    {
                        Enqueue(item);
                        completed = true;
                        return;
                    }

                    // Only a full queue waits; any other rejection throws exactly as Enqueue does.
                    if (EnqueueCore(item, null, true, false, out bool full) || !full)
                    {
                        completed = true;
                        return;
                    }

                    // Block policy and full: wait for space.
                    QoSWaiter waiter = RegisterWaiter(_SpaceWaiters, cancellationToken);
                    try
                    {
                        if (EnqueueCore(item, null, true, false, out bool stillFull) || !stillFull)
                        {
                            completed = true;
                            return;
                        }

                        await waiter.Completion.Task.ConfigureAwait(false);
                    }
                    finally
                    {
                        RemoveWaiter(_SpaceWaiters, waiter);
                    }
                }
            }
            finally
            {
                // A space signal this producer absorbed but did not use (it was cancelled or faulted
                // after being woken) passes to the next blocked producer, so no free slot goes unannounced.
                if (!completed)
                    ForwardWakeup(_SpaceWaiters);
            }
        }

        #endregion

        #region Dequeue

        /// <inheritdoc/>
        public bool TryDequeue(out T item)
        {
            return TryDequeueCore(null, false, out item, out long _);
        }

        // Takes the next entry (or, for a chain pump, the specific expected entry) with full dequeue
        // accounting. When the take fails with entries still resident and computeRetryDelay is set,
        // retryDelayMilliseconds reports when to re-check, clamped to the eligibility bounds; it is
        // zero otherwise (nothing resident: only an enqueue can help).
        private bool TryDequeueCore(QoSEntry<T>? expected, bool computeRetryDelay, out T item, out long retryDelayMilliseconds)
        {
            ThrowIfDisposed();
            QoSWaiter? spaceWaiter = null;
            bool took;
            T taken = default!;
            long takenSequence = 0;
            double takenWait = 0;
            int takenCost = 1;
            string? takenKey = null;
            retryDelayMilliseconds = 0;

            lock (_Lock)
            {
                QoSEntry<T> entry;
                if (expected == null)
                {
                    took = StoreTryTake(out entry);
                }
                else
                {
                    took = StoreTryTakeEntry(expected);
                    entry = expected;
                }

                if (!took && computeRetryDelay && _Count > 0)
                {
                    // Entries are resident but none is eligible now, and no enqueue may ever arrive to
                    // wake a waiter, so learn when the earliest one can become eligible.
                    long delay;
                    if (!StoreTryGetNextEligibleDelay(out delay) || delay > MaximumEligibilityDelayMilliseconds)
                        delay = MaximumEligibilityDelayMilliseconds;
                    if (delay < MinimumEligibilityDelayMilliseconds)
                        delay = MinimumEligibilityDelayMilliseconds;
                    retryDelayMilliseconds = delay;
                }

                if (took)
                {
                    taken = entry.Item;
                    takenSequence = entry.Sequence;
                    takenCost = entry.Cost;
                    takenKey = entry.Key;
                    _Count--;
                    _ResidentCost -= entry.Cost;
                    _Dequeued++;
                    double wait = _TimeProvider.MonotonicMilliseconds - entry.EnqueuedMilliseconds;
                    if (wait < 0)
                        wait = 0;
                    takenWait = wait;
                    _TotalWaitMilliseconds += wait;
                    spaceWaiter = DequeueOneWaiter(_SpaceWaiters);
                }
            }

            if (took)
            {
                string? classLabel = PerClassLabel(takenKey);
                _Store?.Remove(takenSequence);
                if (_EnableMetrics)
                    QoSMetrics.Dequeued(_Name, _TypeLabel, classLabel, takenCost, takenWait);
                TraceDequeue(classLabel, takenCost, takenWait);
                SignalWaiter(spaceWaiter);
                RaiseDequeued(taken);
                item = taken;
                return true;
            }

            item = default!;
            return false;
        }

        /// <inheritdoc/>
        public bool TryPeek(out T item)
        {
            ThrowIfDisposed();
            lock (_Lock)
            {
                if (StoreTryPeek(out QoSEntry<T> entry))
                {
                    item = entry.Item;
                    return true;
                }
            }

            item = default!;
            return false;
        }

        // Chain-pump support: peeks the next entry itself, so the pump can later remove exactly that
        // entry rather than whatever the scheduler would pick at removal time.
        internal bool TryPeekEntry(out QoSEntry<T> entry)
        {
            ThrowIfDisposed();
            lock (_Lock)
            {
                return StoreTryPeek(out entry);
            }
        }

        // Chain-pump support: removes a previously peeked entry with full dequeue accounting. Returns
        // false if the entry already left the queue (taken by another consumer, evicted, or cleared).
        internal bool TryDequeueEntry(QoSEntry<T> entry)
        {
            return TryDequeueCore(entry, false, out T _, out long _);
        }

        /// <inheritdoc/>
        public T Dequeue()
        {
            if (TryDequeue(out T item))
                return item;
            throw new QueueEmptyException(_Name);
        }

        /// <inheritdoc/>
        /// <remarks>
        /// Completes when an item is enqueued or, if items are resident but none is currently eligible
        /// (a low-latency priority class throttled by its policer), no later than when the earliest of
        /// them can become eligible; no further enqueue is needed to release them. Cancellation and
        /// disposal end the wait promptly and always deregister the waiter.
        /// </remarks>
        public async ValueTask<T> DequeueAsync(CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            bool completed = false;
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (TryDequeue(out T item))
                    {
                        completed = true;
                        return item;
                    }

                    QoSWaiter waiter = RegisterWaiter(_ItemWaiters, cancellationToken);
                    try
                    {
                        // Re-check after registering to close the enqueue/register race; if items are
                        // resident but ineligible, also learn when to re-check without any enqueue.
                        if (TryDequeueCore(null, true, out T raced, out long retryDelay))
                        {
                            completed = true;
                            return raced;
                        }

                        if (retryDelay > 0)
                            waiter.Timer = new Timer(_EligibilityTimerCallback, waiter, retryDelay, Timeout.Infinite);

                        await waiter.Completion.Task.ConfigureAwait(false);
                    }
                    finally
                    {
                        RemoveWaiter(_ItemWaiters, waiter);
                    }

                    if (Volatile.Read(ref _Disposed) != 0)
                        throw new OperationCanceledException();
                }
            }
            finally
            {
                // An item signal this consumer absorbed but did not use (it was cancelled or faulted
                // after being woken) passes to the next waiting consumer, so no item is stranded.
                if (!completed)
                    ForwardWakeup(_ItemWaiters);
            }
        }

        /// <inheritdoc/>
        public async IAsyncEnumerable<T> ConsumeAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (true)
            {
                T item;
                try
                {
                    item = await DequeueAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    yield break;
                }

                yield return item;
            }
        }

        #endregion

        #region Collection surface

        /// <inheritdoc/>
        public void Clear()
        {
            ThrowIfDisposed();
            lock (_Lock)
            {
                StoreClear();
                _Count = 0;
                _ResidentCost = 0;
            }

            _Store?.Clear();

            // Clearing frees capacity that no dequeue will announce; let blocked producers retry.
            WakeAllWaiters(_SpaceWaiters);
        }

        /// <inheritdoc/>
        public bool TryAdd(T item)
        {
            return TryEnqueue(item);
        }

        /// <inheritdoc/>
        public bool TryTake(out T item)
        {
            return TryDequeue(out item);
        }

        /// <inheritdoc/>
        public T[] ToArray()
        {
            List<T> list = SnapshotList();
            return list.ToArray();
        }

        /// <inheritdoc/>
        public void CopyTo(T[] array, int index)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            List<T> list = SnapshotList();
            if (index + list.Count > array.Length)
                throw new ArgumentException("The destination array is not large enough for the copy.", nameof(array));
            list.CopyTo(array, index);
        }

        /// <inheritdoc/>
        public void CopyTo(Array array, int index)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index));
            List<T> list = SnapshotList();
            if (index + list.Count > array.Length)
                throw new ArgumentException("The destination array is not large enough for the copy.", nameof(array));
            for (int i = 0; i < list.Count; i++)
                array.SetValue(list[i], index + i);
        }

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator()
        {
            return SnapshotList().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return SnapshotList().GetEnumerator();
        }

        private List<T> SnapshotList()
        {
            List<T> list = new List<T>();
            lock (_Lock)
            {
                StoreSnapshot(list);
            }

            return list;
        }

        #endregion

        #region Helpers

        private void AddEntryLocked(QoSEntry<T> entry)
        {
            entry.Sequence = ++_SequenceCounter;
            entry.EnqueuedMilliseconds = _TimeProvider.MonotonicMilliseconds;
            StoreAdd(entry);
            _Count++;
            _ResidentCost += entry.Cost;
            if (_Count > _PeakDepth)
                _PeakDepth = _Count;
            _Enqueued++;
        }

        private int ComputeCost(T item)
        {
            if (_CostSelector == null)
                return 1;
            int cost = _CostSelector(item);
            if (cost < 0)
                throw new ArgumentOutOfRangeException(nameof(item), "The cost selector returned a negative value; cost must be zero or greater.");
            return cost == 0 ? 1 : cost;
        }

        private QoSWaiter RegisterWaiter(LinkedList<QoSWaiter> list, CancellationToken cancellationToken)
        {
            QoSWaiter waiter = new QoSWaiter();

            // Register for cancellation before publishing the waiter, so the registration is written
            // only by the owning task and never read by a signalling thread mid-write.
            if (cancellationToken.CanBeCanceled)
            {
                // On cancellation, remove the waiter from the registry immediately so it never
                // lingers — the registry heals itself rather than waiting for a later sweep. The owner
                // disposes the registration when it deregisters.
                waiter.Registration = cancellationToken.Register(() =>
                {
                    lock (_Lock)
                    {
                        if (waiter.Node != null && waiter.Node.List != null)
                        {
                            waiter.Node.List.Remove(waiter.Node);
                            waiter.Node = null;
                        }
                    }

                    waiter.Completion.TrySetCanceled();
                });
            }

            lock (_Lock)
            {
                if (Volatile.Read(ref _Disposed) != 0)
                    waiter.Completion.TrySetCanceled();
                else if (!waiter.Completion.Task.IsCompleted)
                    waiter.Node = list.AddLast(waiter);
            }

            return waiter;
        }

        // Deregisters a waiter on every exit path of its owner: unlinks it if still registered and
        // releases its cancellation registration and eligibility timer. Idempotent.
        private void RemoveWaiter(LinkedList<QoSWaiter> list, QoSWaiter waiter)
        {
            lock (_Lock)
            {
                if (waiter.Node != null && waiter.Node.List == list)
                    list.Remove(waiter.Node);
                waiter.Node = null;
            }

            Timer? timer = waiter.Timer;
            if (timer != null)
            {
                waiter.Timer = null;
                timer.Dispose();
            }

            waiter.Registration.Dispose();
        }

        // Passes a wakeup to the next waiter while its condition still holds (items resident, or free
        // capacity) after a woken waiter exits without using its signal. A spurious wakeup is harmless:
        // the woken waiter re-checks and re-registers.
        private void ForwardWakeup(LinkedList<QoSWaiter> list)
        {
            if (Volatile.Read(ref _Disposed) != 0)
                return;

            QoSWaiter? waiter;
            lock (_Lock)
            {
                bool available = ReferenceEquals(list, _ItemWaiters)
                    ? _Count > 0
                    : _MaxDepth == 0 || _Count < _MaxDepth;
                if (!available)
                    return;
                waiter = DequeueOneWaiter(list);
            }

            SignalWaiter(waiter);
        }

        private void WakeAllWaiters(LinkedList<QoSWaiter> list)
        {
            List<QoSWaiter>? toWake = null;
            lock (_Lock)
            {
                QoSWaiter? waiter;
                while ((waiter = DequeueOneWaiter(list)) != null)
                {
                    toWake ??= new List<QoSWaiter>();
                    toWake.Add(waiter);
                }
            }

            if (toWake == null)
                return;
            foreach (QoSWaiter waiter in toWake)
                SignalWaiter(waiter);
        }

        private QoSWaiter? DequeueOneWaiter(LinkedList<QoSWaiter> list)
        {
            LinkedListNode<QoSWaiter>? node = list.First;
            while (node != null)
            {
                LinkedListNode<QoSWaiter>? next = node.Next;
                QoSWaiter waiter = node.Value;
                list.Remove(node);
                waiter.Node = null;
                if (!waiter.Completion.Task.IsCompleted)
                    return waiter;
                node = next;
            }

            return null;
        }

        private static void SignalWaiter(QoSWaiter? waiter)
        {
            if (waiter == null)
                return;
            waiter.Completion.TrySetResult(true);
        }

        private static void OnEligibilityTimer(object? state)
        {
            ((QoSWaiter)state!).Completion.TrySetResult(true);
        }

        private void RaiseEnqueued(T item)
        {
            EventHandler<QoSItemEventArgs<T>>? handler = ItemEnqueued;
            handler?.Invoke(this, new QoSItemEventArgs<T>(item, _Name));
        }

        private void RaiseDequeued(T item)
        {
            EventHandler<QoSItemEventArgs<T>>? handler = ItemDequeued;
            handler?.Invoke(this, new QoSItemEventArgs<T>(item, _Name));
        }

        private void RaiseDropped(T item, DropReason reason)
        {
            EventHandler<QoSDropEventArgs<T>>? handler = ItemDropped;
            handler?.Invoke(this, new QoSDropEventArgs<T>(item, reason, _Name));
        }

        private void ThrowIfDisposed()
        {
            if (Volatile.Read(ref _Disposed) != 0)
                throw new ObjectDisposedException(_Name);
        }

        #endregion

        #region Disposal

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return default;
        }

        /// <summary>
        /// Releases resources held by the queue and completes any pending waiters.
        /// </summary>
        /// <param name="disposing"><c>true</c> when called from <see cref="Dispose()"/>.</param>
        protected virtual void Dispose(bool disposing)
        {
            if (Interlocked.Exchange(ref _Disposed, 1) != 0)
                return;

            if (disposing)
            {
                if (_EnableMetrics)
                    QoSMetrics.UnregisterGaugeSource(_GaugeId);

                List<QoSWaiter> toCancel = new List<QoSWaiter>();
                lock (_Lock)
                {
                    foreach (QoSWaiter waiter in _ItemWaiters)
                        toCancel.Add(waiter);
                    foreach (QoSWaiter waiter in _SpaceWaiters)
                        toCancel.Add(waiter);
                    _ItemWaiters.Clear();
                    _SpaceWaiters.Clear();
                    StoreClear();
                    _Count = 0;
                    _ResidentCost = 0;
                }

                // Each owner releases its own registration and timer as its wait unwinds.
                foreach (QoSWaiter waiter in toCancel)
                    waiter.Completion.TrySetCanceled();

                IQoSStore<T>? store = _Store;
                if (store != null)
                {
                    store.Flush();
                    store.Dispose();
                }
            }
        }

        #endregion
    }
}
