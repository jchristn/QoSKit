namespace QoSKit
{
    /// <summary>
    /// Thrown by <c>Dequeue</c> or <c>Peek</c> when a queue is empty. The non-throwing
    /// <c>TryDequeue</c> and <c>TryPeek</c> return <c>false</c> instead.
    /// </summary>
    public sealed class QueueEmptyException : QoSException
    {
        /// <summary>
        /// The name of the queue that was empty. Never null.
        /// </summary>
        public string QueueName { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueEmptyException"/> class.
        /// </summary>
        /// <param name="queueName">The name of the empty queue.</param>
        public QueueEmptyException(string queueName)
            : base($"Queue '{queueName}' is empty; there is no item to dequeue or peek.")
        {
            QueueName = queueName;
        }
    }
}
