namespace QoSKit
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A fan-out sink that forwards each item to the first route whose predicate matches, or to a
    /// default sink. With no default route, an unmatched item is rejected. Thread-safe for concurrent
    /// producers to the extent the destination sinks are.
    /// </summary>
    /// <typeparam name="T">The payload type.</typeparam>
    public sealed class QoSRouter<T> : IQoSSink<T>
    {
        private readonly List<QoSRoute<T>> _Routes;
        private readonly IQoSSink<T>? _DefaultSink;
        private readonly string _Name;

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSRouter{T}"/> class.
        /// </summary>
        /// <param name="routes">The ordered routes, evaluated first-match-wins.</param>
        /// <param name="defaultSink">An optional sink for items matching no route.</param>
        /// <param name="name">The router name used in diagnostics and exceptions.</param>
        /// <exception cref="ArgumentNullException"><paramref name="routes"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="routes"/> is empty or contains null.</exception>
        public QoSRouter(IEnumerable<QoSRoute<T>> routes, IQoSSink<T>? defaultSink = null, string name = "router")
        {
            if (routes == null)
                throw new ArgumentNullException(nameof(routes));
            _Routes = new List<QoSRoute<T>>();
            foreach (QoSRoute<T> route in routes)
            {
                if (route == null)
                    throw new ArgumentException("Routes must not contain null.", nameof(routes));
                _Routes.Add(route);
            }

            if (_Routes.Count == 0)
                throw new ArgumentException("At least one route is required.", nameof(routes));
            _DefaultSink = defaultSink;
            _Name = string.IsNullOrWhiteSpace(name) ? "router" : name;
        }

        /// <summary>Raised when an item cannot be routed and there is no default sink.</summary>
        public event EventHandler<QoSDropEventArgs<T>>? ItemDropped;

        /// <summary>Gets the router name.</summary>
        public string Name
        {
            get { return _Name; }
        }

        /// <inheritdoc/>
        public bool TryEnqueue(T item)
        {
            if (!typeof(T).IsValueType && item is null)
                throw new ArgumentNullException(nameof(item));

            IQoSSink<T>? target = SelectTarget(item);
            if (target != null)
                return target.TryEnqueue(item);

            RaiseDropped(item);
            return false;
        }

        /// <inheritdoc/>
        public void Enqueue(T item)
        {
            if (!typeof(T).IsValueType && item is null)
                throw new ArgumentNullException(nameof(item));

            IQoSSink<T>? target = SelectTarget(item);
            if (target != null)
            {
                target.Enqueue(item);
                return;
            }

            RaiseDropped(item);
            throw new UnroutableItemException(_Name);
        }

        private IQoSSink<T>? SelectTarget(T item)
        {
            for (int i = 0; i < _Routes.Count; i++)
            {
                if (_Routes[i].Predicate(item))
                    return _Routes[i].Sink;
            }

            return _DefaultSink;
        }

        private void RaiseDropped(T item)
        {
            EventHandler<QoSDropEventArgs<T>>? handler = ItemDropped;
            handler?.Invoke(this, new QoSDropEventArgs<T>(item, DropReason.Unroutable, _Name));
        }
    }
}
