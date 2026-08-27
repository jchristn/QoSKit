namespace QoSKit
{
    /// <summary>
    /// A recovered item and the sequence number it was persisted under. Used during recovery to
    /// replay a queue's backlog in its original order.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class QoSPersistedItem<T>
    {
        /// <summary>The monotonic sequence number the item was enqueued under.</summary>
        public long Sequence { get; }

        /// <summary>The recovered item.</summary>
        public T Item { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSPersistedItem{T}"/> class.
        /// </summary>
        /// <param name="sequence">The sequence number.</param>
        /// <param name="item">The item.</param>
        public QoSPersistedItem(long sequence, T item)
        {
            Sequence = sequence;
            Item = item;
        }
    }
}
