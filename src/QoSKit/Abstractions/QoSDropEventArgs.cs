namespace QoSKit
{
    using System;

    /// <summary>
    /// Event data for the <c>ItemDropped</c> event.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class QoSDropEventArgs<T> : EventArgs
    {
        /// <summary>
        /// The item that was dropped.
        /// </summary>
        public T Item { get; }

        /// <summary>
        /// The reason the item was dropped.
        /// </summary>
        public DropReason Reason { get; }

        /// <summary>
        /// The name of the queue that dropped the item. Never null.
        /// </summary>
        public string QueueName { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSDropEventArgs{T}"/> class.
        /// </summary>
        /// <param name="item">The dropped item.</param>
        /// <param name="reason">The reason for the drop.</param>
        /// <param name="queueName">The name of the raising queue.</param>
        public QoSDropEventArgs(T item, DropReason reason, string queueName)
        {
            Item = item;
            Reason = reason;
            QueueName = queueName;
        }
    }
}
