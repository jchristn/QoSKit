namespace QoSKit
{
    using System.Collections.Generic;

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

        // The node holding this entry in its discipline list, captured from AddLast (no extra
        // allocation). Lets a chain pump remove the exact entry it forwarded; its List is null once
        // the entry has left the queue.
        internal LinkedListNode<QoSEntry<T>>? Node;

        internal QoSEntry()
        {
        }

        internal QoSEntry(T item)
        {
            Item = item;
        }
    }
}
