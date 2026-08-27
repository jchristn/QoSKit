namespace QoSKit
{
    using System;

    /// <summary>
    /// Defines a named flow with a scheduling weight for weighted fair queuing.
    /// </summary>
    public sealed class WeightedFlow
    {
        /// <summary>The flow key. Never null or empty.</summary>
        public string Name { get; }

        /// <summary>The scheduling weight. At least 1; larger weights receive proportionally more service.</summary>
        public int Weight { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="WeightedFlow"/> class.
        /// </summary>
        /// <param name="name">The flow key; must not be null, empty, or whitespace.</param>
        /// <param name="weight">The scheduling weight; must be at least 1.</param>
        /// <exception cref="ArgumentException"><paramref name="name"/> is null, empty, or whitespace.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="weight"/> is less than 1.</exception>
        public WeightedFlow(string name, int weight)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Flow name must not be null, empty, or whitespace.", nameof(name));
            if (weight < 1)
                throw new ArgumentOutOfRangeException(nameof(weight), "Flow weight must be at least 1.");
            Name = name;
            Weight = weight;
        }
    }
}
