namespace QoSKit
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A weighted round robin queue (WRR/DWRR). Sub-queues are served in proportion to weight using a
    /// deficit round-robin scheduler; with unit costs this reduces to classic WRR, and with a cost
    /// selector it becomes byte-fair DWRR. Two ingress modes: balancer (no selector, items are spread
    /// across sub-queues by weight) and classifier (a selector routes items to named sub-queues).
    /// Thread-safe.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class WeightedRoundRobinQoSQueue<T> : QoSQueueBase<T>
    {
        private readonly List<WrrSubState<T>> _Subs = new List<WrrSubState<T>>();
        private readonly Dictionary<string, int> _NameToIndex;
        private readonly Func<T, string>? _Selector;
        private readonly UnknownKeyPolicy _UnknownKeyPolicy;
        private readonly string? _DefaultKey;
        private int _Pointer;

        /// <summary>
        /// Initializes a new instance of the <see cref="WeightedRoundRobinQoSQueue{T}"/> class.
        /// </summary>
        /// <param name="subQueues">The weighted sub-queues.</param>
        /// <param name="options">Cross-cutting options; when null, defaults are used.</param>
        /// <param name="subQueueSelector">Classifier mode: maps an item to a sub-queue name. Null enables balancer mode.</param>
        /// <param name="costSelector">Maps an item to a cost (default 1). A cost selector makes scheduling byte-fair (DWRR).</param>
        /// <param name="unknownKeyPolicy">Classifier mode: how to handle an unknown name. Default <see cref="UnknownKeyPolicy.Throw"/>.</param>
        /// <param name="defaultKey">Classifier mode: the sub-queue used under <see cref="UnknownKeyPolicy.RouteToDefault"/>.</param>
        /// <param name="keyComparer">Comparer for sub-queue names. Default is <see cref="StringComparer.OrdinalIgnoreCase"/>.</param>
        /// <exception cref="ArgumentNullException"><paramref name="subQueues"/> is null.</exception>
        /// <exception cref="ArgumentException">Empty sub-queue set, a duplicate name, an unsupported policy, or a missing default.</exception>
        public WeightedRoundRobinQoSQueue(
            IEnumerable<WeightedSubQueue> subQueues,
            QoSQueueOptions? options = null,
            Func<T, string>? subQueueSelector = null,
            Func<T, int>? costSelector = null,
            UnknownKeyPolicy unknownKeyPolicy = UnknownKeyPolicy.Throw,
            string? defaultKey = null,
            StringComparer? keyComparer = null)
            : base(options, "wrr")
        {
            if (subQueues == null)
                throw new ArgumentNullException(nameof(subQueues));

            StringComparer comparer = keyComparer ?? StringComparer.OrdinalIgnoreCase;
            _NameToIndex = new Dictionary<string, int>(comparer);
            foreach (WeightedSubQueue sq in subQueues)
            {
                if (_NameToIndex.ContainsKey(sq.Name))
                    throw new ArgumentException($"Duplicate sub-queue name '{sq.Name}'.", nameof(subQueues));
                _NameToIndex[sq.Name] = _Subs.Count;
                _Subs.Add(new WrrSubState<T>(sq.Name, sq.Weight));
            }

            if (_Subs.Count == 0)
                throw new ArgumentException("At least one sub-queue is required.", nameof(subQueues));

            _Selector = subQueueSelector;
            _UnknownKeyPolicy = unknownKeyPolicy;
            _DefaultKey = defaultKey;

            if (subQueueSelector != null)
            {
                if (unknownKeyPolicy == UnknownKeyPolicy.CreateDynamic)
                    throw new ArgumentException("WRR sub-queues form a closed set; UnknownKeyPolicy.CreateDynamic is not supported.", nameof(unknownKeyPolicy));
                if (unknownKeyPolicy == UnknownKeyPolicy.RouteToDefault && (defaultKey == null || !_NameToIndex.ContainsKey(defaultKey)))
                    throw new ArgumentException("UnknownKeyPolicy.RouteToDefault requires a defaultKey that names a defined sub-queue.", nameof(defaultKey));
            }

            SetCostSelector(costSelector);
        }

        /// <summary>Enqueues an item into a specific sub-queue, bypassing the selector (classifier mode).</summary>
        /// <param name="item">The item to enqueue.</param>
        /// <param name="subQueueName">The sub-queue name.</param>
        public void Enqueue(T item, string subQueueName)
        {
            EnqueueInternal(item, subQueueName, throwOnFailure: true);
        }

        /// <summary>Attempts to enqueue an item into a specific sub-queue, bypassing the selector.</summary>
        /// <param name="item">The item to enqueue.</param>
        /// <param name="subQueueName">The sub-queue name.</param>
        /// <returns><c>true</c> if admitted; otherwise <c>false</c>.</returns>
        public bool TryEnqueue(T item, string subQueueName)
        {
            return EnqueueInternal(item, subQueueName, throwOnFailure: false);
        }

        /// <inheritdoc/>
        private protected override void ClassifyOutsideLock(QoSEntry<T> entry)
        {
            if (_Selector != null)
                entry.Key = entry.Override is string key ? key : _Selector(entry.Item);
        }

        /// <inheritdoc/>
        private protected override bool ResolveClassification(QoSEntry<T> entry)
        {
            if (_Selector == null && entry.Override is not string)
            {
                // Balancer mode: choose the sub-queue furthest below its weighted share.
                int chosen = 0;
                double bestRatio = double.MaxValue;
                for (int i = 0; i < _Subs.Count; i++)
                {
                    double ratio = (_Subs[i].AssignedCount + 1.0) / _Subs[i].Weight;
                    if (ratio < bestRatio)
                    {
                        bestRatio = ratio;
                        chosen = i;
                    }
                }

                _Subs[chosen].AssignedCount++;
                entry.Band = chosen;
                return true;
            }

            string? key = entry.Key;
            if (key != null && _NameToIndex.TryGetValue(key, out int idx))
            {
                entry.Band = idx;
                return true;
            }

            switch (_UnknownKeyPolicy)
            {
                case UnknownKeyPolicy.RouteToDefault:
                    entry.Band = _NameToIndex[_DefaultKey!];
                    return true;

                case UnknownKeyPolicy.Reject:
                    return false;

                default:
                    throw new UnknownClassificationException(Name, key);
            }
        }

        /// <inheritdoc/>
        private protected override void StoreAdd(QoSEntry<T> entry)
        {
            entry.Node = _Subs[entry.Band].Queue.AddLast(entry);
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTake(out QoSEntry<T> entry)
        {
            int n = _Subs.Count;
            bool anyItems = false;
            for (int i = 0; i < n; i++)
            {
                if (_Subs[i].Queue.First != null)
                {
                    anyItems = true;
                    break;
                }
            }

            if (!anyItems)
            {
                entry = null!;
                return false;
            }

            while (true)
            {
                WrrSubState<T> sub = _Subs[_Pointer];
                if (!sub.RoundStarted)
                {
                    sub.Deficit += sub.Weight;
                    sub.RoundStarted = true;
                }

                if (sub.Queue.First != null && sub.Queue.First.Value.Cost <= sub.Deficit)
                {
                    entry = sub.Queue.First.Value;
                    sub.Queue.RemoveFirst();
                    sub.Deficit -= entry.Cost;
                    return true;
                }

                if (sub.Queue.First == null)
                    sub.Deficit = 0;
                sub.RoundStarted = false;
                _Pointer = (_Pointer + 1) % n;
            }
        }

        /// <inheritdoc/>
        private protected override bool StoreTryPeek(out QoSEntry<T> entry)
        {
            // Peek does not disturb the deficit scheduler; it predicts the entry the next take will serve.
            WrrSubState<T>? next = SelectNextSub();
            if (next != null)
            {
                entry = next.Queue.First!.Value;
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override bool StoreRemoveEntry(QoSEntry<T> entry)
        {
            if (!base.StoreRemoveEntry(entry))
                return false;

            // Charge the sub-queue for the service it received out of turn; a resulting debt is repaid
            // by the weight it accrues on later visits, keeping the long-run shares intact.
            _Subs[entry.Band].Deficit -= entry.Cost;
            return true;
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTakeOldest(out QoSEntry<T> entry)
        {
            WrrSubState<T>? oldest = null;
            long oldestSeq = long.MaxValue;
            foreach (WrrSubState<T> sub in _Subs)
            {
                if (sub.Queue.First != null && sub.Queue.First.Value.Sequence < oldestSeq)
                {
                    oldestSeq = sub.Queue.First.Value.Sequence;
                    oldest = sub;
                }
            }

            if (oldest != null)
            {
                entry = oldest.Queue.First!.Value;
                oldest.Queue.RemoveFirst();
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override void StoreClear()
        {
            foreach (WrrSubState<T> sub in _Subs)
            {
                sub.Queue.Clear();
                sub.Deficit = 0;
                sub.RoundStarted = false;
            }

            _Pointer = 0;
        }

        /// <inheritdoc/>
        private protected override void StoreSnapshot(List<T> destination)
        {
            foreach (WrrSubState<T> sub in _Subs)
            {
                for (LinkedListNode<QoSEntry<T>>? node = sub.Queue.First; node != null; node = node.Next)
                    destination.Add(node.Value.Item);
            }
        }

        // Predicts, without mutating any state, the sub-queue StoreTryTake will serve next. Starting at
        // the pointer, the take loop visits each sub once per round and adds its weight at the start of
        // each visit (except a round already started at the pointer); a non-empty sub is served on the
        // first visit whose deficit covers its head's cost. The earliest such visit across subs wins.
        private WrrSubState<T>? SelectNextSub()
        {
            int n = _Subs.Count;
            WrrSubState<T>? best = null;
            long bestStep = long.MaxValue;
            for (int k = 0; k < n; k++)
            {
                WrrSubState<T> sub = _Subs[(_Pointer + k) % n];
                if (sub.Queue.First == null)
                    continue;

                long available = sub.Deficit + (sub.RoundStarted ? 0 : sub.Weight);
                long shortfall = sub.Queue.First.Value.Cost - available;
                long extraVisits = shortfall > 0 ? (shortfall + sub.Weight - 1) / sub.Weight : 0;
                long step = k + (extraVisits * n);
                if (step < bestStep)
                {
                    bestStep = step;
                    best = sub;
                }
            }

            return best;
        }
    }
}
