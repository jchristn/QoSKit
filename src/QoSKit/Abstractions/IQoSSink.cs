namespace QoSKit
{
    /// <summary>
    /// The producer half of a queue: a destination items can be placed into. Chaining code targets
    /// this interface when it only needs "somewhere to put work."
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public interface IQoSSink<T>
    {
        /// <summary>
        /// Attempts to enqueue an item without throwing when the queue is full.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        /// <returns><c>true</c> if the item was admitted; <c>false</c> if it was rejected or dropped.</returns>
        bool TryEnqueue(T item);

        /// <summary>
        /// Enqueues an item, throwing if it cannot be admitted.
        /// </summary>
        /// <param name="item">The item to enqueue.</param>
        void Enqueue(T item);
    }
}
