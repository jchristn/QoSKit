namespace QoSKit
{
    // Internal envelope carrying an item plus the metadata a discipline needs to order and account
    // for it. Classification fields are populated outside the queue lock (from user delegates);
    // scheduler math fields are populated under the lock. Not part of the public surface.
    internal sealed class QoSEntry<T>
    {
        internal long Sequence;

        internal T Item = default!;

        internal long EnqueuedMilliseconds;

        internal int Cost = 1;

        // Optional per-enqueue classification override (an int band for priority, a string key for
        // keyed disciplines). Null when the discipline's selector should be used.
        internal object? Override;

        // Scheduler scratch, interpreted per discipline.
        internal string? Key;

        internal int Band;

        internal double VirtualFinish;

        internal QoSEntry()
        {
        }

        internal QoSEntry(T item)
        {
            Item = item;
        }
    }
}
