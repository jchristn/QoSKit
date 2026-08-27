namespace QoSKit
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// The consumer half of a queue: a source items can be taken from. Chaining code targets this
    /// interface when it only needs "somewhere to take work from."
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public interface IQoSSource<T>
    {
        /// <summary>
        /// Attempts to remove and return the next item without throwing when the queue is empty.
        /// </summary>
        /// <param name="item">When this method returns <c>true</c>, the dequeued item.</param>
        /// <returns><c>true</c> if an item was dequeued; otherwise <c>false</c>.</returns>
        bool TryDequeue(out T item);

        /// <summary>
        /// Attempts to return the next item without removing it and without throwing when empty.
        /// </summary>
        /// <param name="item">When this method returns <c>true</c>, the next item.</param>
        /// <returns><c>true</c> if an item is available; otherwise <c>false</c>.</returns>
        bool TryPeek(out T item);

        /// <summary>
        /// Removes and returns the next item, throwing <see cref="QueueEmptyException"/> when empty.
        /// </summary>
        /// <returns>The next item.</returns>
        /// <exception cref="QueueEmptyException">The queue is empty.</exception>
        T Dequeue();

        /// <summary>
        /// Asynchronously removes and returns the next item, completing when one becomes available.
        /// </summary>
        /// <param name="cancellationToken">A token to cancel the wait.</param>
        /// <returns>A task that completes with the next item.</returns>
        /// <exception cref="System.OperationCanceledException">The token was cancelled while waiting.</exception>
        ValueTask<T> DequeueAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Asynchronously yields items as they become available until the token is cancelled.
        /// </summary>
        /// <param name="cancellationToken">A token to stop the stream.</param>
        /// <returns>An asynchronous sequence of items.</returns>
        IAsyncEnumerable<T> ConsumeAsync(CancellationToken cancellationToken = default);
    }
}
