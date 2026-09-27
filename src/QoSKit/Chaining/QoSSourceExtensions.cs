namespace QoSKit
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Extension methods for moving items between queues and for building fluent chains.
    /// </summary>
    public static class QoSSourceExtensions
    {
        /// <summary>
        /// Moves up to <paramref name="max"/> items from a source into a sink, stopping early if the
        /// sink rejects an item. Intended for a single drainer per source.
        /// </summary>
        /// <typeparam name="T">The payload type.</typeparam>
        /// <param name="source">The source to drain.</param>
        /// <param name="sink">The sink to fill.</param>
        /// <param name="max">The maximum number of items to move.</param>
        /// <returns>The number of items actually moved.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sink"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="max"/> is negative.</exception>
        public static int DrainTo<T>(this IQoSSource<T> source, IQoSSink<T> sink, int max)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (sink == null)
                throw new ArgumentNullException(nameof(sink));
            if (max < 0)
                throw new ArgumentOutOfRangeException(nameof(max), "max must be zero or greater.");

            int moved = 0;
            while (moved < max && QoSTransfer.TryPeek(source, out T item, out object? transferToken))
            {
                if (!sink.TryEnqueue(item))
                    break;
                QoSTransfer.Take(source, transferToken);
                moved++;
            }

            return moved;
        }

        /// <summary>
        /// Asynchronously moves up to <paramref name="max"/> items from a source into a sink.
        /// </summary>
        /// <typeparam name="T">The payload type.</typeparam>
        /// <param name="source">The source to drain.</param>
        /// <param name="sink">The sink to fill.</param>
        /// <param name="max">The maximum number of items to move.</param>
        /// <param name="cancellationToken">A token to cancel the drain.</param>
        /// <returns>The number of items actually moved before completion or cancellation.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sink"/> is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="max"/> is negative.</exception>
        public static async ValueTask<int> DrainToAsync<T>(this IQoSSource<T> source, IQoSSink<T> sink, int max, CancellationToken cancellationToken = default)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));
            if (sink == null)
                throw new ArgumentNullException(nameof(sink));
            if (max < 0)
                throw new ArgumentOutOfRangeException(nameof(max), "max must be zero or greater.");

            int moved = 0;
            while (moved < max)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!QoSTransfer.TryPeek(source, out T item, out object? transferToken))
                    break;
                if (!sink.TryEnqueue(item))
                    break;
                QoSTransfer.Take(source, transferToken);
                moved++;
                await Task.Yield();
            }

            return moved;
        }

        /// <summary>Begins a fluent chain from this queue into the next.</summary>
        /// <typeparam name="T">The payload type.</typeparam>
        /// <param name="from">The upstream queue.</param>
        /// <param name="to">The downstream queue.</param>
        /// <returns>A chain builder whose tail is <paramref name="to"/>.</returns>
        /// <exception cref="ArgumentNullException">Either argument is null.</exception>
        public static QoSChain<T> ChainTo<T>(this IQoSQueue<T> from, IQoSQueue<T> to)
        {
            if (from == null)
                throw new ArgumentNullException(nameof(from));
            if (to == null)
                throw new ArgumentNullException(nameof(to));
            QoSChain<T> chain = new QoSChain<T>();
            chain.AddInitial(from, to);
            return chain;
        }

        /// <summary>Begins a fluent chain that merges several upstream queues.</summary>
        /// <typeparam name="T">The payload type.</typeparam>
        /// <param name="first">The first upstream queue.</param>
        /// <param name="others">Additional upstream queues.</param>
        /// <returns>A chain builder with the merged upstreams pending a downstream.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="first"/> or an element of <paramref name="others"/> is null.</exception>
        public static QoSChain<T> Merge<T>(this IQoSQueue<T> first, params IQoSQueue<T>[] others)
        {
            if (first == null)
                throw new ArgumentNullException(nameof(first));
            if (others == null)
                throw new ArgumentNullException(nameof(others));
            QoSChain<T> chain = new QoSChain<T>();
            chain.AddPendingUpstream(first);
            foreach (IQoSQueue<T> other in others)
            {
                if (other == null)
                    throw new ArgumentNullException(nameof(others));
                chain.AddPendingUpstream(other);
            }

            return chain;
        }
    }
}
