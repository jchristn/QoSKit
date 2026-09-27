namespace QoSKit
{
    // Peek-then-take support for code that moves items between queues (DrainTo, QoSLink). A mover
    // must remove from the source exactly the item it forwarded to the sink. For a QoSKit queue the
    // peeked entry itself is remembered and removed, so a concurrent enqueue, a policer refill, or
    // aging between the peek and the take can never make the mover discard a different item (loss)
    // while the forwarded one stays behind to be forwarded again (duplication). Any other source
    // falls back to its public TryPeek/TryDequeue pair. Allocation-free.
    internal static class QoSTransfer
    {
        // Peeks the next item. transferToken is the peeked entry for a QoSKit queue, otherwise null;
        // pass it back to Take.
        internal static bool TryPeek<T>(IQoSSource<T> source, out T item, out object? transferToken)
        {
            if (source is QoSQueueBase<T> queue)
            {
                if (queue.TryPeekEntry(out QoSEntry<T> entry))
                {
                    item = entry.Item;
                    transferToken = entry;
                    return true;
                }

                item = default!;
                transferToken = null;
                return false;
            }

            transferToken = null;
            return source.TryPeek(out item);
        }

        // Removes the item previously returned by TryPeek.
        internal static void Take<T>(IQoSSource<T> source, object? transferToken)
        {
            if (transferToken is QoSEntry<T> entry && source is QoSQueueBase<T> queue)
            {
                queue.TryDequeueEntry(entry);
                return;
            }

            source.TryDequeue(out T _);
        }
    }
}
