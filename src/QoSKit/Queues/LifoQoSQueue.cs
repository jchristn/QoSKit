namespace QoSKit
{
    using System.Collections.Generic;

    /// <summary>
    /// A last-in, first-out queue (stack semantics). The most recently enqueued item is serviced
    /// first. Thread-safe.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class LifoQoSQueue<T> : QoSQueueBase<T>
    {
        private readonly LinkedList<QoSEntry<T>> _Items = new LinkedList<QoSEntry<T>>();

        /// <summary>
        /// Initializes a new instance of the <see cref="LifoQoSQueue{T}"/> class with default options.
        /// </summary>
        public LifoQoSQueue()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LifoQoSQueue{T}"/> class.
        /// </summary>
        /// <param name="options">Cross-cutting options; when null, defaults are used.</param>
        public LifoQoSQueue(QoSQueueOptions? options)
            : base(options, "lifo")
        {
        }

        /// <summary>Sets the queue name and returns this instance for chaining.</summary>
        /// <param name="name">The queue name.</param>
        /// <returns>This instance.</returns>
        public LifoQoSQueue<T> WithName(string name)
        {
            SetName(name);
            return this;
        }

        /// <summary>Sets the maximum depth and returns this instance for chaining.</summary>
        /// <param name="maxDepth">Zero for unbounded, or a positive bound.</param>
        /// <returns>This instance.</returns>
        public LifoQoSQueue<T> WithMaxDepth(int maxDepth)
        {
            SetMaxDepth(maxDepth);
            return this;
        }

        /// <summary>Sets the overflow policy and returns this instance for chaining.</summary>
        /// <param name="policy">The overflow policy.</param>
        /// <returns>This instance.</returns>
        public LifoQoSQueue<T> WithOverflowPolicy(OverflowPolicy policy)
        {
            SetOverflowPolicy(policy);
            return this;
        }

        /// <inheritdoc/>
        private protected override void ClassifyOutsideLock(QoSEntry<T> entry)
        {
        }

        /// <inheritdoc/>
        private protected override void StoreAdd(QoSEntry<T> entry)
        {
            _Items.AddLast(entry);
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTake(out QoSEntry<T> entry)
        {
            // Newest item is at the tail.
            if (_Items.Last != null)
            {
                entry = _Items.Last.Value;
                _Items.RemoveLast();
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override bool StoreTryPeek(out QoSEntry<T> entry)
        {
            if (_Items.Last != null)
            {
                entry = _Items.Last.Value;
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTakeOldest(out QoSEntry<T> entry)
        {
            // Oldest item is at the head.
            if (_Items.First != null)
            {
                entry = _Items.First.Value;
                _Items.RemoveFirst();
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override void StoreClear()
        {
            _Items.Clear();
        }

        /// <inheritdoc/>
        private protected override void StoreSnapshot(List<T> destination)
        {
            // Service order is newest-first.
            for (LinkedListNode<QoSEntry<T>>? node = _Items.Last; node != null; node = node.Previous)
                destination.Add(node.Value.Item);
        }
    }
}
