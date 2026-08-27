namespace QoSKit
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A durability journal for a queue. A queue records admitted items and their removals here; on
    /// startup the queue replays <see cref="Recover"/> to rebuild its backlog. Implementations must
    /// be safe to call from a single queue's enqueue and dequeue paths (the queue never calls a store
    /// method while holding its internal lock).
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public interface IQoSStore<T> : IDisposable
    {
        /// <summary>
        /// Serializes an item ahead of admission, returning an opaque token passed to <see cref="Commit"/>.
        /// May throw on a serialization failure; must have no side effects, so a failure aborts the
        /// enqueue before the item is admitted.
        /// </summary>
        /// <param name="item">The item to prepare.</param>
        /// <returns>An opaque token for <see cref="Commit"/>.</returns>
        object? Prepare(T item);

        /// <summary>Records a prepared item under a sequence number.</summary>
        /// <param name="sequence">The item's sequence number.</param>
        /// <param name="prepared">The token returned by <see cref="Prepare"/>.</param>
        void Commit(long sequence, object? prepared);

        /// <summary>Records that the item with a sequence number was removed (dequeued or dropped).</summary>
        /// <param name="sequence">The item's sequence number.</param>
        void Remove(long sequence);

        /// <summary>Records that all items were removed.</summary>
        void Clear();

        /// <summary>Ensures all buffered writes are durable.</summary>
        void Flush();

        /// <summary>Returns the persisted backlog in ascending sequence order.</summary>
        /// <returns>The recovered items.</returns>
        IReadOnlyList<QoSPersistedItem<T>> Recover();
    }
}
