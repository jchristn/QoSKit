namespace QoSKit
{
    using System;

    /// <summary>
    /// Event data for the <c>ItemEnqueued</c> and <c>ItemDequeued</c> events.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class QoSItemEventArgs<T> : EventArgs
    {
        /// <summary>
        /// The item that was enqueued or dequeued.
        /// </summary>
        public T Item { get; }

        /// <summary>
        /// The name of the queue that raised the event. Never null.
        /// </summary>
        public string QueueName { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSItemEventArgs{T}"/> class.
        /// </summary>
        /// <param name="item">The item involved in the event.</param>
        /// <param name="queueName">The name of the raising queue.</param>
        public QoSItemEventArgs(T item, string queueName)
        {
            Item = item;
            QueueName = queueName;
        }
    }
}
