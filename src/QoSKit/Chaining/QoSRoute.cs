namespace QoSKit
{
    using System;

    /// <summary>
    /// A single route in a <see cref="QoSRouter{T}"/>: a predicate and the sink matching items go to.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    public sealed class QoSRoute<T>
    {
        /// <summary>The predicate that matches items to this route. Never null.</summary>
        public Func<T, bool> Predicate { get; }

        /// <summary>The sink matching items are forwarded to. Never null.</summary>
        public IQoSSink<T> Sink { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSRoute{T}"/> class.
        /// </summary>
        /// <param name="predicate">The matching predicate.</param>
        /// <param name="sink">The destination sink.</param>
        /// <exception cref="ArgumentNullException">Either argument is null.</exception>
        public QoSRoute(Func<T, bool> predicate, IQoSSink<T> sink)
        {
            Predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
            Sink = sink ?? throw new ArgumentNullException(nameof(sink));
        }
    }
}
