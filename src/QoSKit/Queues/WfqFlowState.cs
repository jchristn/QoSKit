namespace QoSKit
{
    using System.Collections.Generic;

    // Per-flow runtime state for weighted fair queuing: the flow's queued entries, its weight, and
    // the virtual finish tag of its most recently enqueued entry. Not part of the public surface.
    internal sealed class WfqFlowState<T>
    {
        internal readonly LinkedList<QoSEntry<T>> Queue = new LinkedList<QoSEntry<T>>();

        internal double Weight;

        internal double LastFinish;

        internal WfqFlowState(double weight)
        {
            Weight = weight;
        }
    }
}
