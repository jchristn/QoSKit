namespace QoSKit
{
    using System;
    using System.Diagnostics;

    /// <summary>
    /// The default <see cref="IQoSTimeProvider"/> implementation, backed by the system clock and a
    /// process-wide <see cref="Stopwatch"/> for monotonic timing. Thread-safe.
    /// </summary>
    public sealed class SystemQoSTimeProvider : IQoSTimeProvider
    {
        private static readonly Stopwatch _Stopwatch = Stopwatch.StartNew();

        /// <summary>
        /// Gets the shared default instance.
        /// </summary>
        public static SystemQoSTimeProvider Instance { get; } = new SystemQoSTimeProvider();

        /// <summary>
        /// Initializes a new instance of the <see cref="SystemQoSTimeProvider"/> class.
        /// </summary>
        public SystemQoSTimeProvider()
        {
        }

        /// <inheritdoc/>
        public DateTime UtcNow
        {
            get { return DateTime.UtcNow; }
        }

        /// <inheritdoc/>
        public long MonotonicMilliseconds
        {
            get { return _Stopwatch.ElapsedMilliseconds; }
        }
    }
}
