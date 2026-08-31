namespace QoSKit
{
    using System.Diagnostics;

    /// <summary>
    /// The QoSKit tracing surface: an <see cref="System.Diagnostics.ActivitySource"/> named
    /// <c>QoSKit</c> that opens spans for item admission (<c>queue.enqueue</c>), item service
    /// (<c>queue.dequeue</c>), and chain hops (<c>link.move</c>). Spans carry the queue name, type,
    /// traffic class, cost, and outcome so a trace resolves end-to-end latency across a pipeline to
    /// the exact hop and class that dominated it.
    /// </summary>
    /// <remarks>
    /// Cost: <see cref="System.Diagnostics.ActivitySource.StartActivity(string, ActivityKind)"/>
    /// returns <c>null</c> and does essentially nothing while no listener is subscribed, so tracing is
    /// free until an operator attaches a collector. Once attached, span volume tracks item volume;
    /// sampling is the collector's responsibility. Tracing can be turned off per queue with
    /// <see cref="QoSQueueOptions.EnableTracing"/> and per link with
    /// <see cref="QoSLinkOptions.EnableTracing"/>.
    /// </remarks>
    public static class QoSTracing
    {
        /// <summary>The name of the activity source, <c>QoSKit</c>.</summary>
        public const string ActivitySourceName = "QoSKit";

        /// <summary>The span name for an item admission.</summary>
        public const string EnqueueSpanName = "queue.enqueue";

        /// <summary>The span name for an item service (dequeue).</summary>
        public const string DequeueSpanName = "queue.dequeue";

        /// <summary>The span name for a chain hop moving an item downstream.</summary>
        public const string MoveSpanName = "link.move";

        private static readonly ActivitySource _Source =
            new ActivitySource(ActivitySourceName, typeof(QoSTracing).Assembly.GetName().Version?.ToString() ?? "0.0.0");

        /// <summary>Gets the underlying activity source.</summary>
        public static ActivitySource Source
        {
            get { return _Source; }
        }

        /// <summary>
        /// Gets a value indicating whether any listener is currently subscribed. Callers can use this
        /// to skip building span attributes when tracing would produce no span.
        /// </summary>
        public static bool HasListeners
        {
            get { return _Source.HasListeners(); }
        }
    }
}
