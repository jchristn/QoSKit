namespace QoSKit
{
    using System;

    /// <summary>
    /// Defines a named sub-queue with a weight for weighted round robin.
    /// </summary>
    public sealed class WeightedSubQueue
    {
        /// <summary>The sub-queue name. Never null or empty.</summary>
        public string Name { get; }

        /// <summary>The weight. At least 1; larger weights receive proportionally more service.</summary>
        public int Weight { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="WeightedSubQueue"/> class.
        /// </summary>
        /// <param name="name">The sub-queue name; must not be null, empty, or whitespace.</param>
        /// <param name="weight">The weight; must be at least 1.</param>
        /// <exception cref="ArgumentException"><paramref name="name"/> is null, empty, or whitespace.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="weight"/> is less than 1.</exception>
        public WeightedSubQueue(string name, int weight)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Sub-queue name must not be null, empty, or whitespace.", nameof(name));
            if (weight < 1)
                throw new ArgumentOutOfRangeException(nameof(weight), "Sub-queue weight must be at least 1.");
            Name = name;
            Weight = weight;
        }
    }
}
