namespace Test.Shared
{
    using System;
    using System.Threading;
    using QoSKit;

    /// <summary>
    /// A controllable <see cref="IQoSTimeProvider"/> that advances only when told to, so aging and
    /// policing are tested deterministically.
    /// </summary>
    public sealed class ManualTimeProvider : IQoSTimeProvider
    {
        private long _Milliseconds;

        /// <summary>Initializes a new instance of the <see cref="ManualTimeProvider"/> class.</summary>
        public ManualTimeProvider()
        {
        }

        /// <inheritdoc/>
        public DateTime UtcNow
        {
            get { return new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(Interlocked.Read(ref _Milliseconds)); }
        }

        /// <inheritdoc/>
        public long MonotonicMilliseconds
        {
            get { return Interlocked.Read(ref _Milliseconds); }
        }

        /// <summary>Advances the clock by a number of milliseconds.</summary>
        /// <param name="milliseconds">The amount to advance.</param>
        public void Advance(long milliseconds)
        {
            Interlocked.Add(ref _Milliseconds, milliseconds);
        }
    }
}
