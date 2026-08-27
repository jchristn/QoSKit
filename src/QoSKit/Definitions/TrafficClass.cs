namespace QoSKit
{
    using System;

    /// <summary>
    /// Defines a named traffic class: a predicate that matches items, a scheduling weight, and an
    /// optional rate limit (used only for low-latency priority classes).
    /// </summary>
    /// <typeparam name="T">The payload type carried by the queue.</typeparam>
    public sealed class TrafficClass<T>
    {
        /// <summary>The class name. Never null or empty.</summary>
        public string Name { get; }

        /// <summary>The predicate that matches items to this class. Never null.</summary>
        public Func<T, bool> Matcher { get; }

        /// <summary>The scheduling weight for weighted-fair service among classes. At least 1.</summary>
        public int Weight { get; }

        /// <summary>
        /// The rate limit applied when this class is used as a low-latency priority class. Null means
        /// unpoliced strict priority, which can starve fair classes.
        /// </summary>
        public TokenBucket? RateLimit { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="TrafficClass{T}"/> class.
        /// </summary>
        /// <param name="name">The class name; must not be null, empty, or whitespace.</param>
        /// <param name="matcher">The predicate matching items to this class.</param>
        /// <param name="weight">The scheduling weight; at least 1. Default 1.</param>
        /// <param name="rateLimit">An optional rate limit for a low-latency priority class.</param>
        /// <exception cref="ArgumentException"><paramref name="name"/> is null, empty, or whitespace.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="matcher"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="weight"/> is less than 1.</exception>
        public TrafficClass(string name, Func<T, bool> matcher, int weight = 1, TokenBucket? rateLimit = null)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Class name must not be null, empty, or whitespace.", nameof(name));
            if (weight < 1)
                throw new ArgumentOutOfRangeException(nameof(weight), "Class weight must be at least 1.");
            Name = name;
            Matcher = matcher ?? throw new ArgumentNullException(nameof(matcher));
            Weight = weight;
            RateLimit = rateLimit;
        }
    }
}
