namespace QoSKit
{
    /// <summary>
    /// Determines what a queue does when an enqueue would exceed its configured maximum depth.
    /// </summary>
    public enum OverflowPolicy
    {
        /// <summary>
        /// Reject the incoming item. <c>TryEnqueue</c> returns <c>false</c> and <c>Enqueue</c> throws
        /// <see cref="QueueFullException"/>. This is the default.
        /// </summary>
        Reject = 0,

        /// <summary>
        /// Discard the incoming (newest) item silently, recording a drop. Existing items are untouched.
        /// </summary>
        DropNewest = 1,

        /// <summary>
        /// Evict the oldest item currently in the queue to make room, then admit the incoming item.
        /// </summary>
        DropOldest = 2,

        /// <summary>
        /// Block the producer until capacity is available. Honored only on the asynchronous and
        /// <see cref="System.Collections.Concurrent.BlockingCollection{T}"/> paths; synchronous
        /// <c>TryEnqueue</c> treats this as <see cref="Reject"/>.
        /// </summary>
        Block = 3
    }
}
