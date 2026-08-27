namespace QoSKit
{
    using System;
    using System.Collections.Generic;

    // Per-class runtime state for class-based and low-latency queuing: the class name and matcher,
    // its queued entries, its weight, and the virtual finish tag of its most recent entry.
    internal sealed class WfqClassState<T>
    {
        internal readonly string Name;

        internal readonly Func<T, bool> Matcher;

        internal readonly LinkedList<QoSEntry<T>> Queue = new LinkedList<QoSEntry<T>>();

        internal double Weight;

        internal double LastFinish;

        internal WfqClassState(string name, Func<T, bool> matcher, double weight)
        {
            Name = name;
            Matcher = matcher;
            Weight = weight;
        }
    }
}
