namespace QoSKit.Example.Scenarios
{
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using QoSKit.Example.Harness;

    /// <summary>
    /// Scenario 3: chained queues. Two level-one queues - a low-latency queue for interactive work and
    /// a weighted-fair queue for batch tenants - feed a level-two priority queue through a pipeline.
    /// The consumer dequeues only from the level-two tail.
    /// </summary>
    public sealed class ChainedScenario : IDemoScenario
    {
        /// <inheritdoc/>
        public string Name
        {
            get { return "Scenario 3 - Chained Queues"; }
        }

        /// <inheritdoc/>
        public string Description
        {
            get { return "Interactive LLQ + batch WFQ feed a level-2 priority queue; service the tail."; }
        }

        /// <inheritdoc/>
        public async Task RunAsync(IDemoReporter reporter, CancellationToken cancellationToken)
        {
            LowLatencyQoSQueue<WorkItem> interactive = new LowLatencyQoSQueue<WorkItem>(
                new[] { new TrafficClass<WorkItem>("interactive", w => true) },
                new TrafficClass<WorkItem>[0]);
            WeightedFairQoSQueue<WorkItem> batch = new WeightedFairQoSQueue<WorkItem>(
                w => w.Tenant,
                new[] { new WeightedFlow("tenant-a", 2), new WeightedFlow("tenant-b", 1) });
            PriorityQoSQueue<WorkItem> level2 = new PriorityQoSQueue<WorkItem>(2, w => w.Interactive ? 0 : 1);

            reporter.Note("L1: interactive (LLQ) + batch (WFQ).  L2: priority queue.  Consumer services the L2 tail.");

            await using QoSPipeline<WorkItem> pipeline = await interactive
                .Merge(batch)
                .ChainTo(level2)
                .AsPipeline("dispatch")
                .StartAsync(cancellationToken)
                .ConfigureAwait(false);

            Task producer = Task.Run(() => ProduceAsync(interactive, batch, level2, reporter, cancellationToken), CancellationToken.None);
            await ConsumeAsync(level2, reporter, cancellationToken).ConfigureAwait(false);
            await producer.ConfigureAwait(false);
            await pipeline.StopAsync().ConfigureAwait(false);
        }

        private static async Task ProduceAsync(LowLatencyQoSQueue<WorkItem> interactive, WeightedFairQoSQueue<WorkItem> batch, PriorityQoSQueue<WorkItem> level2, IDemoReporter reporter, CancellationToken cancellationToken)
        {
            int id = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                if (level2.Count + interactive.Count + batch.Count > 60000)
                {
                    await Task.Delay(1).ConfigureAwait(false);
                    continue;
                }

                interactive.TryEnqueue(new WorkItem(id++) { Interactive = true });
                reporter.Enqueued("interactive");

                for (int i = 0; i < 4; i++)
                {
                    string tenant = (id % 2 == 0) ? "tenant-a" : "tenant-b";
                    batch.TryEnqueue(new WorkItem(id++) { Tenant = tenant });
                    reporter.Enqueued(tenant);
                }
            }
        }

        private static async Task ConsumeAsync(PriorityQoSQueue<WorkItem> level2, IDemoReporter reporter, CancellationToken cancellationToken)
        {
            int serviced = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                if (level2.TryDequeue(out WorkItem item))
                {
                    reporter.Serviced(item.Interactive ? "interactive" : item.Tenant);
                    serviced++;
                    if ((serviced & 255) == 0)
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
