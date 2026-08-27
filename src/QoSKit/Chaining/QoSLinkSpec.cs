namespace QoSKit
{
    // A single link in a chain: one or more upstream queues feeding one downstream queue. Used to
    // build links and to validate the pipeline graph. Not part of the public surface.
    internal sealed class QoSLinkSpec<T>
    {
        internal readonly IQoSQueue<T>[] Upstreams;

        internal readonly IQoSQueue<T> Downstream;

        internal QoSLinkSpec(IQoSQueue<T>[] upstreams, IQoSQueue<T> downstream)
        {
            Upstreams = upstreams;
            Downstream = downstream;
        }
    }
}
