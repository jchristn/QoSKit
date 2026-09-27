namespace Test.Shared
{
    using System;
    using QoSKit;

    /// <summary>
    /// A test sink that forwards to an inner sink and runs a callback after each accepted item, so a
    /// suite can inject a concurrent change (an enqueue, a clock advance) between a mover's peek and
    /// its take. Not thread-safe; intended for single-threaded deterministic tests.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    public sealed class InterceptingSink<T> : IQoSSink<T>
    {
        private readonly IQoSSink<T> _Inner;
        private readonly Action<int, T> _AfterAccept;
        private int _Accepted;

        /// <summary>Initializes a new instance of the <see cref="InterceptingSink{T}"/> class.</summary>
        /// <param name="inner">The sink that receives forwarded items.</param>
        /// <param name="afterAccept">Invoked with the zero-based accept index and the item after each accepted item.</param>
        /// <exception cref="ArgumentNullException"><paramref name="inner"/> or <paramref name="afterAccept"/> is null.</exception>
        public InterceptingSink(IQoSSink<T> inner, Action<int, T> afterAccept)
        {
            _Inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _AfterAccept = afterAccept ?? throw new ArgumentNullException(nameof(afterAccept));
        }

        /// <inheritdoc/>
        public bool TryEnqueue(T item)
        {
            if (!_Inner.TryEnqueue(item))
                return false;
            _AfterAccept(_Accepted++, item);
            return true;
        }

        /// <inheritdoc/>
        public void Enqueue(T item)
        {
            _Inner.Enqueue(item);
            _AfterAccept(_Accepted++, item);
        }
    }
}
