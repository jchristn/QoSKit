namespace QoSKit
{
    using System.Diagnostics.Metrics;

    /// <summary>
    /// The QoSKit metrics surface: a <see cref="System.Diagnostics.Metrics.Meter"/> named
    /// <c>QoSKit</c> exposing OpenTelemetry-shaped instruments. Emission allocates nothing until a
    /// listener subscribes, and every measurement is tagged with the queue name and type.
    /// </summary>
    public static class QoSMetrics
    {
        /// <summary>The name of the meter, <c>QoSKit</c>.</summary>
        public const string MeterName = "QoSKit";

        private static readonly Meter _Meter = new Meter(MeterName, "0.1.0");

        private static readonly Counter<long> _Enqueued =
            _Meter.CreateCounter<long>("qoskit.queue.enqueued", "items", "Items admitted to a queue.");

        private static readonly Counter<long> _Dequeued =
            _Meter.CreateCounter<long>("qoskit.queue.dequeued", "items", "Items dequeued from a queue.");

        private static readonly Counter<long> _Dropped =
            _Meter.CreateCounter<long>("qoskit.queue.dropped", "items", "Items dropped by a queue.");

        private static readonly Counter<long> _Rejected =
            _Meter.CreateCounter<long>("qoskit.queue.rejected", "items", "Items rejected by a full queue.");

        private static readonly UpDownCounter<long> _Depth =
            _Meter.CreateUpDownCounter<long>("qoskit.queue.depth", "items", "Current resident depth of a queue.");

        private static readonly Histogram<double> _Wait =
            _Meter.CreateHistogram<double>("qoskit.queue.wait.duration", "ms", "Time items waited before being dequeued.");

        /// <summary>Gets the underlying meter.</summary>
        public static Meter Meter
        {
            get { return _Meter; }
        }

        internal static void Enqueued(string name, string type)
        {
            _Enqueued.Add(1, Tag("queue.name", name), Tag("queue.type", type));
            _Depth.Add(1, Tag("queue.name", name), Tag("queue.type", type));
        }

        internal static void Dequeued(string name, string type, double waitMilliseconds)
        {
            _Dequeued.Add(1, Tag("queue.name", name), Tag("queue.type", type));
            _Depth.Add(-1, Tag("queue.name", name), Tag("queue.type", type));
            _Wait.Record(waitMilliseconds, Tag("queue.name", name), Tag("queue.type", type));
        }

        internal static void Dropped(string name, string type, bool wasResident)
        {
            _Dropped.Add(1, Tag("queue.name", name), Tag("queue.type", type));
            if (wasResident)
                _Depth.Add(-1, Tag("queue.name", name), Tag("queue.type", type));
        }

        internal static void Rejected(string name, string type)
        {
            _Rejected.Add(1, Tag("queue.name", name), Tag("queue.type", type));
        }

        private static System.Collections.Generic.KeyValuePair<string, object?> Tag(string key, string value)
        {
            return new System.Collections.Generic.KeyValuePair<string, object?>(key, value);
        }
    }
}
