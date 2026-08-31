namespace QoSKit
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.Metrics;
    using System.Threading;

    /// <summary>
    /// The QoSKit metrics surface: a <see cref="System.Diagnostics.Metrics.Meter"/> named
    /// <c>QoSKit</c> exposing OpenTelemetry-shaped instruments. Emission allocates nothing until a
    /// listener subscribes, and every measurement is tagged with the queue name, the queue type, and
    /// (unless per-class metrics are disabled) the traffic class. Drops additionally carry a
    /// <c>drop.reason</c> tag, and cost-bearing operations emit a parallel byte-equivalent counter so
    /// bandwidth, not just packet count, is visible.
    /// </summary>
    /// <remarks>
    /// Cardinality: the <c>queue.class</c> tag is bounded for closed-set disciplines (priority bands,
    /// CBWFQ/LLQ/WRR classes) but open-ended for a weighted-fair queue with dynamically created flows.
    /// Set <see cref="QoSQueueOptions.EnablePerClassMetrics"/> to <c>false</c> on such a queue to drop
    /// the class tag and keep series count bounded.
    /// </remarks>
    public static class QoSMetrics
    {
        /// <summary>The name of the meter, <c>QoSKit</c>.</summary>
        public const string MeterName = "QoSKit";

        /// <summary>The tag key carrying the queue name.</summary>
        public const string TagQueueName = "queue.name";

        /// <summary>The tag key carrying the discipline type.</summary>
        public const string TagQueueType = "queue.type";

        /// <summary>The tag key carrying the traffic class or flow.</summary>
        public const string TagQueueClass = "queue.class";

        /// <summary>The tag key carrying the reason an item was dropped.</summary>
        public const string TagDropReason = "drop.reason";

        private static readonly Meter _Meter =
            new Meter(MeterName, typeof(QoSMetrics).Assembly.GetName().Version?.ToString() ?? "0.0.0");

        // Gauge sources are held weakly and keyed by id so a queue that is never disposed does not leak
        // through the meter; dead references are pruned lazily during observation.
        private static readonly ConcurrentDictionary<long, WeakReference<IQoSGaugeSource>> _GaugeSources =
            new ConcurrentDictionary<long, WeakReference<IQoSGaugeSource>>();

        private static long _GaugeIdCounter;

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

        private static readonly Counter<long> _EnqueuedBytes =
            _Meter.CreateCounter<long>("qoskit.queue.enqueued.bytes", "By", "Cost (bytes-equivalent) admitted to a queue.");

        private static readonly Counter<long> _DequeuedBytes =
            _Meter.CreateCounter<long>("qoskit.queue.dequeued.bytes", "By", "Cost (bytes-equivalent) dequeued from a queue.");

        private static readonly Counter<long> _DroppedBytes =
            _Meter.CreateCounter<long>("qoskit.queue.dropped.bytes", "By", "Cost (bytes-equivalent) dropped by a queue.");

        private static readonly Counter<long> _RejectedBytes =
            _Meter.CreateCounter<long>("qoskit.queue.rejected.bytes", "By", "Cost (bytes-equivalent) rejected by a full queue.");

        private static readonly Counter<long> _PolicerConformed =
            _Meter.CreateCounter<long>("qoskit.policer.conformed", "items", "Items that conformed to a class policer and were served.");

        private static readonly Counter<long> _PolicerExceeded =
            _Meter.CreateCounter<long>("qoskit.policer.exceeded", "items", "Items throttled because a class policer had no tokens.");

        // Observable gauges are pull-based: their callbacks run only when a listener collects, so they
        // cost nothing on the enqueue/dequeue hot path. The Meter roots the instruments once created,
        // so the return handles are intentionally discarded rather than stored.
        static QoSMetrics()
        {
            _Meter.CreateObservableGauge<long>("qoskit.queue.capacity", ObserveCapacity, "items", "Configured maximum depth of a queue; zero means unbounded.");
            _Meter.CreateObservableGauge<long>("qoskit.queue.peak.depth", ObservePeakDepth, "items", "High-water mark of resident depth over the life of a queue.");
            _Meter.CreateObservableGauge<long>("qoskit.queue.resident.bytes", ObserveResidentBytes, "By", "Current resident cost (bytes-equivalent) of a queue.");
        }

        /// <summary>Gets the underlying meter.</summary>
        public static Meter Meter
        {
            get { return _Meter; }
        }

        // Registers a queue so its pull gauges are observable, returning a token used to unregister it.
        internal static long RegisterGaugeSource(IQoSGaugeSource source)
        {
            long id = Interlocked.Increment(ref _GaugeIdCounter);
            _GaugeSources[id] = new WeakReference<IQoSGaugeSource>(source);
            return id;
        }

        // Removes a queue from gauge observation (on disposal).
        internal static void UnregisterGaugeSource(long id)
        {
            _GaugeSources.TryRemove(id, out WeakReference<IQoSGaugeSource>? _);
        }

        internal static void Enqueued(string name, string type, string? className, int cost)
        {
            TagList tags = Tags(name, type, className);
            _Enqueued.Add(1, tags);
            _EnqueuedBytes.Add(cost, tags);
            _Depth.Add(1, tags);
        }

        internal static void Dequeued(string name, string type, string? className, int cost, double waitMilliseconds)
        {
            TagList tags = Tags(name, type, className);
            _Dequeued.Add(1, tags);
            _DequeuedBytes.Add(cost, tags);
            _Depth.Add(-1, tags);
            _Wait.Record(waitMilliseconds, tags);
        }

        internal static void Dropped(string name, string type, string? className, DropReason reason, int cost, bool wasResident)
        {
            TagList tags = Tags(name, type, className);
            tags.Add(TagDropReason, ReasonLabel(reason));
            _Dropped.Add(1, tags);
            _DroppedBytes.Add(cost, tags);
            if (wasResident)
                _Depth.Add(-1, tags);
        }

        internal static void Rejected(string name, string type, string? className, int cost)
        {
            TagList tags = Tags(name, type, className);
            _Rejected.Add(1, tags);
            _RejectedBytes.Add(cost, tags);
        }

        internal static void PolicerConformed(string name, string type, string? className)
        {
            _PolicerConformed.Add(1, Tags(name, type, className));
        }

        internal static void PolicerExceeded(string name, string type, string? className)
        {
            _PolicerExceeded.Add(1, Tags(name, type, className));
        }

        // Builds the common tag set. TagList is a stack-allocated struct with inline storage for up to
        // eight tags, so this allocates nothing on the heap. The class tag is omitted when null.
        private static TagList Tags(string name, string type, string? className)
        {
            TagList tags = new TagList();
            tags.Add(TagQueueName, name);
            tags.Add(TagQueueType, type);
            if (className != null)
                tags.Add(TagQueueClass, className);
            return tags;
        }

        private static string ReasonLabel(DropReason reason)
        {
            switch (reason)
            {
                case DropReason.Newest:
                    return "newest";
                case DropReason.Oldest:
                    return "oldest";
                case DropReason.UnknownClass:
                    return "unknown_class";
                case DropReason.Unroutable:
                    return "unroutable";
                default:
                    return "unknown";
            }
        }

        private static IEnumerable<Measurement<long>> ObserveCapacity()
        {
            List<Measurement<long>> measurements = new List<Measurement<long>>(_GaugeSources.Count);
            foreach (IQoSGaugeSource source in LiveSources())
                measurements.Add(new Measurement<long>(source.GaugeCapacity, GaugeTags(source)));
            return measurements;
        }

        private static IEnumerable<Measurement<long>> ObservePeakDepth()
        {
            List<Measurement<long>> measurements = new List<Measurement<long>>(_GaugeSources.Count);
            foreach (IQoSGaugeSource source in LiveSources())
                measurements.Add(new Measurement<long>(source.GaugePeakDepth, GaugeTags(source)));
            return measurements;
        }

        private static IEnumerable<Measurement<long>> ObserveResidentBytes()
        {
            List<Measurement<long>> measurements = new List<Measurement<long>>(_GaugeSources.Count);
            foreach (IQoSGaugeSource source in LiveSources())
                measurements.Add(new Measurement<long>(source.GaugeResidentBytes, GaugeTags(source)));
            return measurements;
        }

        // Snapshots the live gauge sources, pruning any whose queue has been collected without disposal.
        private static List<IQoSGaugeSource> LiveSources()
        {
            List<IQoSGaugeSource> live = new List<IQoSGaugeSource>(_GaugeSources.Count);
            foreach (KeyValuePair<long, WeakReference<IQoSGaugeSource>> pair in _GaugeSources)
            {
                if (pair.Value.TryGetTarget(out IQoSGaugeSource? source))
                    live.Add(source);
                else
                    _GaugeSources.TryRemove(pair.Key, out WeakReference<IQoSGaugeSource>? _);
            }

            return live;
        }

        private static KeyValuePair<string, object?>[] GaugeTags(IQoSGaugeSource source)
        {
            return new KeyValuePair<string, object?>[]
            {
                new KeyValuePair<string, object?>(TagQueueName, source.GaugeQueueName),
                new KeyValuePair<string, object?>(TagQueueType, source.GaugeQueueType)
            };
        }
    }
}
