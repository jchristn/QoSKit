namespace QoSKit
{
    /// <summary>
    /// Thrown when a classification key (from a selector delegate or an explicit override) is not in
    /// the queue's defined set of keys and the configured <see cref="UnknownKeyPolicy"/> is
    /// <see cref="UnknownKeyPolicy.Throw"/>, or is <see cref="UnknownKeyPolicy.Reject"/> and the caller
    /// used the throwing <c>Enqueue</c> path.
    /// </summary>
    public sealed class UnknownClassificationException : QoSException
    {
        /// <summary>
        /// The name of the queue that could not classify the item. Never null.
        /// </summary>
        public string QueueName { get; }

        /// <summary>
        /// The unrecognized classification key. May be null or empty if the selector produced such a value.
        /// </summary>
        public string? Key { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="UnknownClassificationException"/> class.
        /// </summary>
        /// <param name="queueName">The name of the queue.</param>
        /// <param name="key">The unrecognized classification key.</param>
        public UnknownClassificationException(string queueName, string? key)
            : base($"Queue '{queueName}' does not recognize classification key '{key ?? "(null)"}' and its UnknownKeyPolicy does not admit it.")
        {
            QueueName = queueName;
            Key = key;
        }
    }
}
