namespace QoSKit
{
    /// <summary>
    /// Thrown by <c>Enqueue</c> when a queue is at its maximum depth and the overflow policy is
    /// <see cref="OverflowPolicy.Reject"/>. The non-throwing <c>TryEnqueue</c> returns <c>false</c> instead.
    /// </summary>
    public sealed class QueueFullException : QoSException
    {
        /// <summary>
        /// The name of the queue that rejected the item. Never null.
        /// </summary>
        public string QueueName { get; }

        /// <summary>
        /// The configured maximum depth of the queue.
        /// </summary>
        public int MaxDepth { get; }

        /// <summary>
        /// The number of items resident in the queue at the time of rejection.
        /// </summary>
        public int Count { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueueFullException"/> class.
        /// </summary>
        /// <param name="queueName">The name of the queue that rejected the item.</param>
        /// <param name="maxDepth">The configured maximum depth.</param>
        /// <param name="count">The resident item count at rejection.</param>
        public QueueFullException(string queueName, int maxDepth, int count)
            : base($"Queue '{queueName}' is full (count {count} of maximum {maxDepth}); the item was rejected under OverflowPolicy.Reject.")
        {
            QueueName = queueName;
            MaxDepth = maxDepth;
            Count = count;
        }
    }
}
