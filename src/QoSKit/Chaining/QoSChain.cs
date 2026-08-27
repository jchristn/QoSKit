namespace QoSKit
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// A fluent builder for a chain of queues. Begin with <see cref="QoSSourceExtensions.ChainTo{T}"/>
    /// or <see cref="QoSSourceExtensions.Merge{T}"/>, continue with <see cref="ChainTo"/>, and finalize
    /// with <see cref="AsPipeline"/>.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the chain.</typeparam>
    public sealed class QoSChain<T>
    {
        private readonly List<QoSLinkSpec<T>> _Specs = new List<QoSLinkSpec<T>>();
        private readonly List<IQoSQueue<T>> _PendingUpstreams = new List<IQoSQueue<T>>();
        private IQoSQueue<T>? _Tail;

        internal QoSChain()
        {
        }

        /// <summary>Gets the current tail queue (what a consumer dequeues from), or null before any link.</summary>
        public IQoSQueue<T>? Tail
        {
            get { return _Tail; }
        }

        internal void AddInitial(IQoSQueue<T> from, IQoSQueue<T> to)
        {
            _Specs.Add(new QoSLinkSpec<T>(new[] { from }, to));
            _Tail = to;
        }

        internal void AddPendingUpstream(IQoSQueue<T> queue)
        {
            _PendingUpstreams.Add(queue);
        }

        /// <summary>Chains the current tail (or pending merged upstreams) into the next queue.</summary>
        /// <param name="to">The downstream queue.</param>
        /// <returns>This builder, with its tail advanced to <paramref name="to"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="to"/> is null.</exception>
        /// <exception cref="InvalidOperationException">There is no upstream to chain from.</exception>
        public QoSChain<T> ChainTo(IQoSQueue<T> to)
        {
            if (to == null)
                throw new ArgumentNullException(nameof(to));

            IQoSQueue<T>[] upstreams;
            if (_PendingUpstreams.Count > 0)
            {
                upstreams = _PendingUpstreams.ToArray();
                _PendingUpstreams.Clear();
            }
            else if (_Tail != null)
            {
                upstreams = new[] { _Tail };
            }
            else
            {
                throw new InvalidOperationException("There is no upstream queue to chain from.");
            }

            _Specs.Add(new QoSLinkSpec<T>(upstreams, to));
            _Tail = to;
            return this;
        }

        /// <summary>Finalizes the chain into a startable pipeline.</summary>
        /// <param name="name">The pipeline name; must not be null, empty, or whitespace.</param>
        /// <returns>A pipeline over the chain's links.</returns>
        /// <exception cref="ArgumentException"><paramref name="name"/> is null, empty, or whitespace.</exception>
        /// <exception cref="InvalidOperationException">The chain has no links.</exception>
        public QoSPipeline<T> AsPipeline(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Pipeline name must not be null, empty, or whitespace.", nameof(name));
            if (_Specs.Count == 0)
                throw new InvalidOperationException("The chain has no links; call ChainTo before AsPipeline.");

            List<QoSLink<T>> links = new List<QoSLink<T>>();
            foreach (QoSLinkSpec<T> spec in _Specs)
            {
                IQoSSource<T>[] sources = new IQoSSource<T>[spec.Upstreams.Length];
                for (int i = 0; i < spec.Upstreams.Length; i++)
                    sources[i] = spec.Upstreams[i];
                links.Add(new QoSLink<T>(sources, spec.Downstream));
            }

            return new QoSPipeline<T>(name, links, _Specs);
        }
    }
}
