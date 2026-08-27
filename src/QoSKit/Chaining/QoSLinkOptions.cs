namespace QoSKit
{
    using System;

    /// <summary>
    /// Options controlling a <see cref="QoSLink{T}"/> background pump.
    /// </summary>
    public sealed class QoSLinkOptions
    {
        private int _BatchSize = 64;
        private TimeSpan _PollInterval = TimeSpan.FromMilliseconds(1);

        /// <summary>
        /// The maximum number of items moved from a single source per pump pass. Default 64, minimum 1.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">The value is less than 1.</exception>
        public int BatchSize
        {
            get { return _BatchSize; }
            set
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException(nameof(value), "BatchSize must be at least 1.");
                _BatchSize = value;
            }
        }

        /// <summary>
        /// How long the pump waits before re-checking when a pass moved nothing. Default 1 ms.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
        public TimeSpan PollInterval
        {
            get { return _PollInterval; }
            set
            {
                if (value < TimeSpan.Zero)
                    throw new ArgumentOutOfRangeException(nameof(value), "PollInterval must be zero or greater.");
                _PollInterval = value;
            }
        }

        /// <summary>Initializes a new instance of the <see cref="QoSLinkOptions"/> class.</summary>
        public QoSLinkOptions()
        {
        }
    }
}
