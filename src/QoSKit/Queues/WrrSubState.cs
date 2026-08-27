namespace QoSKit
{
    using System.Collections.Generic;

    // Per-sub-queue runtime state for weighted round robin. Deficit and RoundStarted drive the
    // deficit round-robin scheduler; AssignedCount drives weighted distribution in balancer mode.
    internal sealed class WrrSubState<T>
    {
        internal readonly string Name;

        internal readonly int Weight;

        internal readonly LinkedList<QoSEntry<T>> Queue = new LinkedList<QoSEntry<T>>();

        internal long Deficit;

        internal bool RoundStarted;

        internal long AssignedCount;

        internal WrrSubState(string name, int weight)
        {
            Name = name;
            Weight = weight;
        }
    }
}
