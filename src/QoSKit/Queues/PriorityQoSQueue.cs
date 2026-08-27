namespace QoSKit
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A strict-priority queue over a fixed number of bands. Lower selector values are higher
    /// priority. Optional aging promotes long-waiting items to prevent starvation. Thread-safe.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class PriorityQoSQueue<T> : QoSQueueBase<T>
    {
        private readonly int _Levels;
        private readonly Func<T, int> _PrioritySelector;
        private readonly LinkedList<QoSEntry<T>>[] _Bands;
        private long _AgingThresholdMilliseconds;

        /// <summary>
        /// Initializes a new instance of the <see cref="PriorityQoSQueue{T}"/> class.
        /// </summary>
        /// <param name="levels">The number of priority bands; must be greater than zero. Band 0 is highest priority.</param>
        /// <param name="prioritySelector">Maps an item to a band index. Out-of-range values are clamped.</param>
        /// <param name="options">Cross-cutting options; when null, defaults are used.</param>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="levels"/> is not positive.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="prioritySelector"/> is null.</exception>
        public PriorityQoSQueue(int levels, Func<T, int> prioritySelector, QoSQueueOptions? options = null)
            : base(options, "priority")
        {
            if (levels <= 0)
                throw new ArgumentOutOfRangeException(nameof(levels), "The number of priority levels must be greater than zero.");
            _Levels = levels;
            _PrioritySelector = prioritySelector ?? throw new ArgumentNullException(nameof(prioritySelector));
            _Bands = new LinkedList<QoSEntry<T>>[levels];
            for (int i = 0; i < levels; i++)
                _Bands[i] = new LinkedList<QoSEntry<T>>();
        }

        /// <summary>
        /// The wait threshold, in milliseconds, after which a waiting item is promoted ahead of its
        /// band. Zero (the default) disables aging.
        /// </summary>
        public long AgingThresholdMilliseconds
        {
            get { return _AgingThresholdMilliseconds; }
        }

        /// <summary>The number of priority bands.</summary>
        public int Levels
        {
            get { return _Levels; }
        }

        /// <summary>Enables aging with a wait threshold and returns this instance for chaining.</summary>
        /// <param name="thresholdMilliseconds">The wait threshold; zero disables aging.</param>
        /// <returns>This instance.</returns>
        /// <exception cref="ArgumentOutOfRangeException">The threshold is negative.</exception>
        public PriorityQoSQueue<T> WithAging(long thresholdMilliseconds)
        {
            if (thresholdMilliseconds < 0)
                throw new ArgumentOutOfRangeException(nameof(thresholdMilliseconds), "The aging threshold must be zero or greater.");
            _AgingThresholdMilliseconds = thresholdMilliseconds;
            return this;
        }

        /// <summary>Sets the queue name and returns this instance for chaining.</summary>
        /// <param name="name">The queue name.</param>
        /// <returns>This instance.</returns>
        public PriorityQoSQueue<T> WithName(string name)
        {
            SetName(name);
            return this;
        }

        /// <summary>Sets the maximum depth and returns this instance for chaining.</summary>
        /// <param name="maxDepth">Zero for unbounded, or a positive bound.</param>
        /// <returns>This instance.</returns>
        public PriorityQoSQueue<T> WithMaxDepth(int maxDepth)
        {
            SetMaxDepth(maxDepth);
            return this;
        }

        /// <summary>Sets the overflow policy and returns this instance for chaining.</summary>
        /// <param name="policy">The overflow policy.</param>
        /// <returns>This instance.</returns>
        public PriorityQoSQueue<T> WithOverflowPolicy(OverflowPolicy policy)
        {
            SetOverflowPolicy(policy);
            return this;
        }

        /// <summary>Enqueues an item into a specific band, bypassing the selector.</summary>
        /// <param name="item">The item to enqueue.</param>
        /// <param name="priority">The band index; clamped to the valid range.</param>
        /// <exception cref="QueueFullException">The queue is full under a reject policy.</exception>
        public void Enqueue(T item, int priority)
        {
            EnqueueInternal(item, priority, throwOnFailure: true);
        }

        /// <summary>Attempts to enqueue an item into a specific band, bypassing the selector.</summary>
        /// <param name="item">The item to enqueue.</param>
        /// <param name="priority">The band index; clamped to the valid range.</param>
        /// <returns><c>true</c> if admitted; otherwise <c>false</c>.</returns>
        public bool TryEnqueue(T item, int priority)
        {
            return EnqueueInternal(item, priority, throwOnFailure: false);
        }

        /// <inheritdoc/>
        private protected override void ClassifyOutsideLock(QoSEntry<T> entry)
        {
            int band = entry.Override is int explicitBand ? explicitBand : _PrioritySelector(entry.Item);
            if (band < 0)
                band = 0;
            else if (band >= _Levels)
                band = _Levels - 1;
            entry.Band = band;
        }

        /// <inheritdoc/>
        private protected override void StoreAdd(QoSEntry<T> entry)
        {
            _Bands[entry.Band].AddLast(entry);
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTake(out QoSEntry<T> entry)
        {
            LinkedList<QoSEntry<T>>? band = SelectServeBand();
            if (band != null && band.First != null)
            {
                entry = band.First.Value;
                band.RemoveFirst();
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override bool StoreTryPeek(out QoSEntry<T> entry)
        {
            LinkedList<QoSEntry<T>>? band = SelectServeBand();
            if (band != null && band.First != null)
            {
                entry = band.First.Value;
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTakeOldest(out QoSEntry<T> entry)
        {
            LinkedList<QoSEntry<T>>? oldestBand = null;
            long oldestSeq = long.MaxValue;
            for (int i = 0; i < _Levels; i++)
            {
                if (_Bands[i].First != null && _Bands[i].First!.Value.Sequence < oldestSeq)
                {
                    oldestSeq = _Bands[i].First!.Value.Sequence;
                    oldestBand = _Bands[i];
                }
            }

            if (oldestBand != null)
            {
                entry = oldestBand.First!.Value;
                oldestBand.RemoveFirst();
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override void StoreClear()
        {
            for (int i = 0; i < _Levels; i++)
                _Bands[i].Clear();
        }

        /// <inheritdoc/>
        private protected override void StoreSnapshot(List<T> destination)
        {
            for (int i = 0; i < _Levels; i++)
            {
                for (LinkedListNode<QoSEntry<T>>? node = _Bands[i].First; node != null; node = node.Next)
                    destination.Add(node.Value.Item);
            }
        }

        private LinkedList<QoSEntry<T>>? SelectServeBand()
        {
            if (_AgingThresholdMilliseconds > 0)
            {
                long now = TimeProvider.MonotonicMilliseconds;
                LinkedList<QoSEntry<T>>? agedBand = null;
                long bestWait = -1;
                for (int i = 0; i < _Levels; i++)
                {
                    if (_Bands[i].First != null)
                    {
                        long wait = now - _Bands[i].First!.Value.EnqueuedMilliseconds;
                        if (wait >= _AgingThresholdMilliseconds && wait > bestWait)
                        {
                            bestWait = wait;
                            agedBand = _Bands[i];
                        }
                    }
                }

                if (agedBand != null)
                    return agedBand;
            }

            for (int i = 0; i < _Levels; i++)
            {
                if (_Bands[i].First != null)
                    return _Bands[i];
            }

            return null;
        }
    }
}
