namespace QoSKit
{
    using System;
    using System.Collections.Generic;

    // A strict-priority class in a low-latency queue: its matcher, its queued entries, and an
    // optional token-bucket policer. A null policer means unpoliced strict priority.
    internal sealed class LlqPriorityState<T>
    {
        internal readonly string Name;

        internal readonly Func<T, bool> Matcher;

        internal readonly TokenBucket? RateLimit;

        internal readonly LinkedList<QoSEntry<T>> Queue = new LinkedList<QoSEntry<T>>();

        internal LlqPriorityState(string name, Func<T, bool> matcher, TokenBucket? rateLimit)
        {
            Name = name;
            Matcher = matcher;
            RateLimit = rateLimit;
        }
    }
}
