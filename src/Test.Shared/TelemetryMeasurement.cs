namespace Test.Shared
{
    using System.Collections.Generic;

    /// <summary>
    /// A captured telemetry signal: either a metric measurement (with a numeric value) or a span
    /// (value zero), together with the tag set that accompanied it. Used by the telemetry suites to
    /// assert per-class breakdown, drop reasons, cost accounting, and span emission.
    /// </summary>
    public sealed class TelemetryMeasurement
    {
        /// <summary>The instrument name for a metric, or the operation name for a span.</summary>
        public string Name { get; }

        /// <summary>The measured value; zero for a span.</summary>
        public double Value { get; }

        /// <summary>The tags that accompanied the measurement or span, values rendered as strings.</summary>
        public IReadOnlyDictionary<string, string?> Tags { get; }

        /// <summary>Initializes a new instance of the <see cref="TelemetryMeasurement"/> class.</summary>
        /// <param name="name">The instrument or span name.</param>
        /// <param name="value">The measured value; zero for a span.</param>
        /// <param name="tags">The accompanying tag set.</param>
        public TelemetryMeasurement(string name, double value, IReadOnlyDictionary<string, string?> tags)
        {
            Name = name;
            Value = value;
            Tags = tags;
        }

        /// <summary>Returns the value of a tag, or null when the tag is absent.</summary>
        /// <param name="key">The tag key.</param>
        /// <returns>The tag value, or null.</returns>
        public string? Tag(string key)
        {
            return Tags.TryGetValue(key, out string? value) ? value : null;
        }

        /// <summary>Returns whether a tag is present.</summary>
        /// <param name="key">The tag key.</param>
        /// <returns><c>true</c> if the tag is present; otherwise <c>false</c>.</returns>
        public bool HasTag(string key)
        {
            return Tags.ContainsKey(key);
        }
    }
}
