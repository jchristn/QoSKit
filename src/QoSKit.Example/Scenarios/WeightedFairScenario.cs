namespace QoSKit.Example.Scenarios
{
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using QoSKit.Example.Harness;

    /// <summary>
    /// Scenario 2: a weighted fair queue. Three tenants weighted 5:3:1 are kept backlogged; over the
    /// window the serviced counts converge to the configured weights.
    /// </summary>
    public sealed class WeightedFairScenario : IDemoScenario
    {
        private static readonly string[] _Tenants = { "tenant-a", "tenant-b", "tenant-c" };

        /// <inheritdoc/>
        public string Name
        {
            get { return "Scenario 2 - Weighted Fair Queue"; }
        }

        /// <inheritdoc/>
        public string Description
        {
            get { return "Three tenants weighted 5:3:1 share a backlogged fair queue; service converges to the weights."; }
        }

        /// <inheritdoc/>
        public async Task RunAsync(IDemoReporter reporter, CancellationToken cancellationToken)
        {
            WeightedFairQoSQueue<WorkItem> queue = new WeightedFairQoSQueue<WorkItem>(
                w => w.Tenant,
                new[] { new WeightedFlow("tenant-a", 5), new WeightedFlow("tenant-b", 3), new WeightedFlow("tenant-c", 1) });
            reporter.Note("WFQ over three tenants, weights 5:3:1. All kept backlogged so the scheduler is what you see.");

            Task producer = Task.Run(() => ProduceAsync(queue, reporter, cancellationToken), CancellationToken.None);
            await ConsumeAsync(queue, reporter, cancellationToken).ConfigureAwait(false);
            await producer.ConfigureAwait(false);
            queue.Dispose();
        }

        private static async Task ProduceAsync(WeightedFairQoSQueue<WorkItem> queue, IDemoReporter reporter, CancellationToken cancellationToken)
        {
            int id = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                if (queue.Count > 60000)
                {
                    await Task.Delay(1).ConfigureAwait(false);
                    continue;
                }

                for (int t = 0; t < 3; t++)
                {
                    WorkItem item = new WorkItem(id++) { Tenant = _Tenants[t] };
                    queue.TryEnqueue(item);
                    reporter.Enqueued(_Tenants[t]);
                }
            }
        }

        private static async Task ConsumeAsync(WeightedFairQoSQueue<WorkItem> queue, IDemoReporter reporter, CancellationToken cancellationToken)
        {
            int serviced = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                if (queue.TryDequeue(out WorkItem item))
                {
                    reporter.Serviced(item.Tenant);
                    serviced++;
                    if ((serviced & 15) == 0)
                        await Task.Delay(1).ConfigureAwait(false);
                }
                else
                {
                    await Task.Delay(1).ConfigureAwait(false);
                }
            }
        }
    }
}
