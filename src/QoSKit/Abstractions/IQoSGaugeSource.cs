namespace QoSKit
{
    // Internal contract a queue implements so the QoSKit meter can pull point-in-time gauge values
    // (capacity, peak depth, resident cost) on demand when a listener collects. Reads are cheap and
    // happen off the enqueue/dequeue hot path. Not part of the public surface.
    internal interface IQoSGaugeSource
    {
        // The queue name, read live so a rename after registration is reflected.
        string GaugeQueueName { get; }

        // The discipline type label.
        string GaugeQueueType { get; }

        // The configured maximum depth in items; zero means unbounded.
        long GaugeCapacity { get; }

        // The high-water mark of resident depth in items observed over the life of the queue.
        long GaugePeakDepth { get; }

        // The current resident cost (bytes-equivalent) summed across resident items.
        long GaugeResidentBytes { get; }
    }
}
