namespace QoSKit
{
    using System.Collections.Generic;

    /// <summary>
    /// A first-in, first-out queue. Items are serviced in the order they were enqueued. Thread-safe.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class FifoQoSQueue<T> : QoSQueueBase<T>
    {
        private readonly LinkedList<QoSEntry<T>> _Items = new LinkedList<QoSEntry<T>>();

        /// <summary>
        /// Initializes a new instance of the <see cref="FifoQoSQueue{T}"/> class with default options.
        /// </summary>
        public FifoQoSQueue()
            : this(null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FifoQoSQueue{T}"/> class.
        /// </summary>
        /// <param name="options">Cross-cutting options; when null, defaults are used.</param>
        public FifoQoSQueue(QoSQueueOptions? options)
            : base(options, "fifo")
        {
        }

        /// <summary>Sets the queue name and returns this instance for chaining.</summary>
        /// <param name="name">The queue name.</param>
        /// <returns>This instance.</returns>
        public FifoQoSQueue<T> WithName(string name)
        {
            SetName(name);
            return this;
        }

        /// <summary>Sets the maximum depth and returns this instance for chaining.</summary>
        /// <param name="maxDepth">Zero for unbounded, or a positive bound.</param>
        /// <returns>This instance.</returns>
        public FifoQoSQueue<T> WithMaxDepth(int maxDepth)
        {
            SetMaxDepth(maxDepth);
            return this;
        }

        /// <summary>Sets the overflow policy and returns this instance for chaining.</summary>
        /// <param name="policy">The overflow policy.</param>
        /// <returns>This instance.</returns>
        public FifoQoSQueue<T> WithOverflowPolicy(OverflowPolicy policy)
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
        private protected override bool StoreTryPeek(out QoSEntry<T> entry)
        {
            if (_Items.First != null)
            {
                entry = _Items.First.Value;
                return true;
            }

            entry = null!;
            return false;
        }

        /// <inheritdoc/>
        private protected override bool StoreTryTakeOldest(out QoSEntry<T> entry)
        {
            return StoreTryTake(out entry);
        }

        /// <inheritdoc/>
        private protected override void StoreClear()
        {
            _Items.Clear();
        }

        /// <inheritdoc/>
        private protected override void StoreSnapshot(List<T> destination)
        {
            for (LinkedListNode<QoSEntry<T>>? node = _Items.First; node != null; node = node.Next)
                destination.Add(node.Value.Item);
        }
    }
}
