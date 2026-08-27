namespace QoSKit
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;

    /// <summary>
    /// A quality-of-service queue: both a sink and a source, and a first-class .NET collection.
    /// Implements <see cref="IProducerConsumerCollection{T}"/> so it can back a
    /// <see cref="BlockingCollection{T}"/>, and <see cref="IReadOnlyCollection{T}"/> for enumeration.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public interface IQoSQueue<T> :
        IQoSSink<T>,
        IQoSSource<T>,
        IProducerConsumerCollection<T>,
        IReadOnlyCollection<T>,
        IDisposable,
        IAsyncDisposable
    {
        /// <summary>
        /// The name of the queue, used in diagnostics, exceptions, and metric tags. Never null.
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The maximum number of items the queue will hold. Zero means unbounded (the default).
        /// </summary>
        int MaxDepth { get; }

        /// <summary>
        /// The policy applied when an enqueue would exceed <see cref="MaxDepth"/>.
        /// </summary>
        OverflowPolicy OverflowPolicy { get; }

        /// <summary>
        /// The number of items currently resident in the queue. Re-declared to resolve the ambiguity
        /// between the inherited collection interfaces.
        /// </summary>
        new int Count { get; }

        /// <summary>
        /// Gets a value indicating whether the queue currently holds no items.
        /// </summary>
        bool IsEmpty { get; }

        /// <summary>
        /// Captures a point-in-time snapshot of the queue's counters.
        /// </summary>
        QoSQueueStatistics Statistics { get; }

        /// <summary>
        /// Raised after an item is admitted. Not raised when there are no subscribers.
        /// </summary>
        event EventHandler<QoSItemEventArgs<T>>? ItemEnqueued;

        /// <summary>
        /// Raised after an item is dequeued. Not raised when there are no subscribers.
        /// </summary>
        event EventHandler<QoSItemEventArgs<T>>? ItemDequeued;

        /// <summary>
        /// Raised after an item is dropped. Not raised when there are no subscribers.
        /// </summary>
        event EventHandler<QoSDropEventArgs<T>>? ItemDropped;

        /// <summary>
        /// Removes all items from the queue without raising dequeue events.
        /// </summary>
        void Clear();
    }
}
