namespace QoSKit
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// A background pump that continuously drains one or more upstream sources into a downstream
    /// sink, honoring the sink's backpressure. The pump holds its own cancellation source, so its
    /// asynchronous methods do not each take a token.
    /// </summary>
    /// <typeparam name="T">The payload type carried by the link.</typeparam>
    public sealed class QoSLink<T> : IAsyncDisposable
    {
        private readonly IReadOnlyList<IQoSSource<T>> _Sources;
        private readonly IQoSSink<T> _Sink;
        private readonly QoSLinkOptions _Options;
        private CancellationTokenSource? _Cts;
        private Task? _Pump;
        private int _Running;
        private long _Moved;

        /// <summary>
        /// Initializes a new instance of the <see cref="QoSLink{T}"/> class.
        /// </summary>
        /// <param name="sources">The upstream sources to drain.</param>
        /// <param name="sink">The downstream sink to fill.</param>
        /// <param name="options">Pump options; when null, defaults are used.</param>
        /// <exception cref="ArgumentNullException"><paramref name="sources"/> or <paramref name="sink"/> is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="sources"/> is empty or contains null.</exception>
        public QoSLink(IEnumerable<IQoSSource<T>> sources, IQoSSink<T> sink, QoSLinkOptions? options = null)
        {
            if (sources == null)
                throw new ArgumentNullException(nameof(sources));
            _Sink = sink ?? throw new ArgumentNullException(nameof(sink));
            List<IQoSSource<T>> list = new List<IQoSSource<T>>();
            foreach (IQoSSource<T> source in sources)
            {
                if (source == null)
                    throw new ArgumentException("Sources must not contain null.", nameof(sources));
                list.Add(source);
            }

            if (list.Count == 0)
                throw new ArgumentException("At least one source is required.", nameof(sources));
            _Sources = list;
            _Options = options ?? new QoSLinkOptions();
        }

        /// <summary>Gets the total number of items this link has moved.</summary>
        public long Moved
        {
            get { return Interlocked.Read(ref _Moved); }
        }

        /// <summary>
        /// Starts the pump.
        /// </summary>
        /// <param name="cancellationToken">A token that also stops the pump when cancelled.</param>
        /// <returns>A completed task.</returns>
        /// <exception cref="InvalidOperationException">The link is already running.</exception>
        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            if (Interlocked.Exchange(ref _Running, 1) == 1)
                throw new InvalidOperationException("The link is already running.");
            _Cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            CancellationToken token = _Cts.Token;
            _Pump = Task.Run(() => PumpAsync(token), CancellationToken.None);
            return Task.CompletedTask;
        }

        /// <summary>
        /// Stops the pump. Safe to call when the link was never started.
        /// </summary>
        /// <param name="cancellationToken">Unused; present for symmetry.</param>
        /// <returns>A task that completes when the pump has stopped.</returns>
        public async Task StopAsync(CancellationToken cancellationToken = default)
        {
            CancellationTokenSource? cts = _Cts;
            if (cts != null)
            {
                try
                {
                    cts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }
            }

            Task? pump = _Pump;
            if (pump != null)
            {
                try
                {
                    await pump.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            await StopAsync().ConfigureAwait(false);
            _Cts?.Dispose();
        }

        private async Task PumpAsync(CancellationToken token)
        {
            try
            {
                while (!token.IsCancellationRequested)
                {
                    int moved = 0;
                    for (int i = 0; i < _Sources.Count; i++)
                        moved += DrainSource(_Sources[i]);

                    if (moved > 0)
                    {
                        Interlocked.Add(ref _Moved, moved);
                    }
                    else
                    {
                        await Task.Delay(_Options.PollInterval, token).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        // Moves up to BatchSize items from a source to the sink, tolerating a throwing sink: an item
        // the sink rejects by throwing (for example a classifier fault) is discarded from the source
        // so it cannot wedge the pump, and the pump keeps running.
        private int DrainSource(IQoSSource<T> source)
        {
            int moved = 0;
            for (int k = 0; k < _Options.BatchSize; k++)
            {
                if (!source.TryPeek(out T item))
                    break;

                bool admitted;
                try
                {
                    admitted = _Sink.TryEnqueue(item);
                }
                catch (Exception)
                {
                    // Poison item: discard it from the source and continue.
                    source.TryDequeue(out T _);
                    continue;
                }

                if (!admitted)
                    break; // backpressure: the sink is full under a reject/block policy

                source.TryDequeue(out T _);
                moved++;
            }

            return moved;
        }
    }
}

