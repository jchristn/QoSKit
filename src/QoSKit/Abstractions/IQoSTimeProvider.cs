namespace QoSKit
{
    using System;

    /// <summary>
    /// Supplies the current time to queues whose behavior depends on it (priority aging and the
    /// low-latency-queue policer). Injecting this abstraction keeps time-dependent code paths
    /// deterministically testable.
    /// </summary>
    public interface IQoSTimeProvider
    {
        /// <summary>
        /// Gets the current UTC time.
        /// </summary>
        DateTime UtcNow { get; }

        /// <summary>
        /// Gets a monotonically increasing tick count, in milliseconds, suitable for measuring
        /// elapsed durations. Not related to any wall-clock epoch.
        /// </summary>
        long MonotonicMilliseconds { get; }
    }
}
