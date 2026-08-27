namespace QoSKit
{
    using System;

    /// <summary>
    /// Cross-cutting configuration shared by every queue discipline. All properties have sensible
    /// defaults; construct, set what you need, and pass to a queue constructor.
    /// </summary>
    public sealed class QoSQueueOptions
    {
        private string? _Name;
        private int _MaxDepth;
        private IQoSTimeProvider _TimeProvider = SystemQoSTimeProvider.Instance;

        /// <summary>
        /// The queue name used in diagnostics, exceptions, and metric tags. When null, the queue
        /// generates a name. Setting an empty or whitespace value throws.
        /// </summary>
        /// <exception cref="ArgumentException">The value is empty or whitespace.</exception>
        public string? Name
        {
            get { return _Name; }
            set
            {
                if (value != null && string.IsNullOrWhiteSpace(value))
                    throw new ArgumentException("Name may be null (auto-generated) but not empty or whitespace.", nameof(value));
                _Name = value;
            }
        }

        /// <summary>
        /// The maximum number of items the queue will hold. Zero means unbounded. Default is 0.
        /// Minimum is 0.
        /// </summary>
        /// <exception cref="ArgumentOutOfRangeException">The value is negative.</exception>
        public int MaxDepth
        {
            get { return _MaxDepth; }
            set
            {
                if (value < 0)
                    throw new ArgumentOutOfRangeException(nameof(value), "MaxDepth must be zero (unbounded) or greater.");
                _MaxDepth = value;
            }
        }

        /// <summary>
        /// The policy applied when an enqueue would exceed <see cref="MaxDepth"/>. Default is
        /// <see cref="OverflowPolicy.Reject"/>.
        /// </summary>
        public OverflowPolicy OverflowPolicy { get; set; } = OverflowPolicy.Reject;

        /// <summary>
        /// Whether the queue publishes metrics to the QoSKit meter. Default is <c>true</c>; emission
        /// allocates nothing until a listener subscribes.
        /// </summary>
        public bool EnableMetrics { get; set; } = true;

        /// <summary>
        /// The time source for aging and policing. Default is <see cref="SystemQoSTimeProvider.Instance"/>.
        /// </summary>
        /// <exception cref="ArgumentNullException">The value is null.</exception>
        public IQoSTimeProvider TimeProvider
        {
            get { return _TimeProvider; }
            set { _TimeProvider = value ?? throw new ArgumentNullException(nameof(value)); }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSQueueOptions"/> class with default values.
        /// </summary>
        public QoSQueueOptions()
        {
        }
    }
}
