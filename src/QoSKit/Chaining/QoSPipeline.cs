namespace QoSKit
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A named, startable composition of links. Validates the link graph for cycles, starts and stops
    /// all pumps together, and is disposed as a unit.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the pipeline.</typeparam>
    public sealed class QoSPipeline<T> : IAsyncDisposable
    {
        private readonly string _Name;
        private readonly List<QoSLink<T>> _Links;
        private readonly List<QoSLinkSpec<T>> _Specs;
        private int _Started;
        private int _Disposed;

        internal QoSPipeline(string name, List<QoSLink<T>> links, List<QoSLinkSpec<T>> specs)
        {
            _Name = name;
            _Links = links;
            _Specs = specs;
        }

        /// <summary>Gets the pipeline name.</summary>
        public string Name
        {
            get { return _Name; }
        }

        /// <summary>Gets the total number of items moved across all links.</summary>
        public long Moved
        {
            get
            {
                long total = 0;
                foreach (QoSLink<T> link in _Links)
                    total += link.Moved;
                return total;
            }
        }

        /// <summary>
        /// Validates the graph and starts all links.
        /// </summary>
        /// <param name="cancellationToken">A token that also stops the pipeline when cancelled.</param>
        /// <returns>This pipeline, running.</returns>
        /// <exception cref="InvalidOperationException">The pipeline is already started.</exception>
        /// <exception cref="PipelineCycleException">The link graph contains a cycle or self-loop.</exception>
        /// <exception cref="ObjectDisposedException">The pipeline has been disposed.</exception>
        public async ValueTask<QoSPipeline<T>> StartAsync(CancellationToken cancellationToken = default)
        {
            if (Volatile.Read(ref _Disposed) != 0)
                throw new ObjectDisposedException(_Name);
            if (Interlocked.Exchange(ref _Started, 1) == 1)
                throw new InvalidOperationException("The pipeline is already started.");

            ValidateNoCycle();

            foreach (QoSLink<T> link in _Links)
                await link.StartAsync(cancellationToken).ConfigureAwait(false);
            return this;
        }

        /// <summary>
        /// Stops all links. Safe to call when the pipeline was never started.
        /// </summary>
        /// <param name="cancellationToken">A token to cancel the stop.</param>
        /// <returns>A task that completes when all links have stopped.</returns>
        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            foreach (QoSLink<T> link in _Links)
                await link.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _Disposed, 1) != 0)
                return;
            foreach (QoSLink<T> link in _Links)
                await link.DisposeAsync().ConfigureAwait(false);
        }

        private void ValidateNoCycle()
        {
            List<IQoSQueue<T>> nodes = new List<IQoSQueue<T>>();
            foreach (QoSLinkSpec<T> spec in _Specs)
            {
                AddNode(nodes, spec.Downstream);
                foreach (IQoSQueue<T> up in spec.Upstreams)
                    AddNode(nodes, up);
            }

            int n = nodes.Count;
            List<int>[] adjacency = new List<int>[n];
            for (int i = 0; i < n; i++)
                adjacency[i] = new List<int>();

            foreach (QoSLinkSpec<T> spec in _Specs)
            {
                int to = IndexOf(nodes, spec.Downstream);
                foreach (IQoSQueue<T> up in spec.Upstreams)
                {
                    int from = IndexOf(nodes, up);
                    adjacency[from].Add(to);
                    if (from == to)
                        throw new PipelineCycleException(new List<string> { nodes[from].Name });
                }
            }

            int[] state = new int[n]; // 0 = unvisited, 1 = in-progress, 2 = done
            for (int i = 0; i < n; i++)
            {
                if (state[i] == 0)
                {
                    List<int> path = new List<int>();
                    if (HasCycle(i, adjacency, state, path))
                    {
                        List<string> names = new List<string>();
                        foreach (int idx in path)
                            names.Add(nodes[idx].Name);
                        throw new PipelineCycleException(names);
                    }
                }
            }
        }

        private static bool HasCycle(int node, List<int>[] adjacency, int[] state, List<int> path)
        {
            state[node] = 1;
            path.Add(node);
            foreach (int next in adjacency[node])
            {
                if (state[next] == 1)
                {
                    path.Add(next);
                    return true;
                }

                if (state[next] == 0 && HasCycle(next, adjacency, state, path))
                    return true;
            }

            state[node] = 2;
            path.RemoveAt(path.Count - 1);
            return false;
        }

        private static void AddNode(List<IQoSQueue<T>> nodes, IQoSQueue<T> node)
        {
            if (IndexOf(nodes, node) < 0)
                nodes.Add(node);
        }

        private static int IndexOf(List<IQoSQueue<T>> nodes, IQoSQueue<T> node)
        {
            for (int i = 0; i < nodes.Count; i++)
            {
                if (ReferenceEquals(nodes[i], node))
                    return i;
            }

            return -1;
        }
    }
}
