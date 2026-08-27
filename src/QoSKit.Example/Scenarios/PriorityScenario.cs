namespace QoSKit.Example.Scenarios
{
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using QoSKit.Example.Harness;

    /// <summary>
    /// Scenario 1: a strict-priority queue. A producer offers high, normal, and low priority work in
    /// equal measure; a slower consumer services in priority order, so under a persistent backlog the
    /// high band dominates what actually gets serviced.
    /// </summary>
    public sealed class PriorityScenario : IDemoScenario
    {
        private static readonly string[] _Labels = { "high", "normal", "low" };

        /// <inheritdoc/>
        public string Name
        {
            get { return "Scenario 1 - Priority Queue"; }
        }

        /// <inheritdoc/>
        public string Description
        {
            get { return "Equal high/normal/low offered; a backlogged priority queue services high first."; }
        }

        /// <inheritdoc/>
        public async Task RunAsync(IDemoReporter reporter, CancellationToken cancellationToken)
        {
            PriorityQoSQueue<WorkItem> queue = new PriorityQoSQueue<WorkItem>(3, w => w.Priority);
            reporter.Note("A 3-band priority queue. The producer outpaces the consumer, so under a persistent backlog high is serviced far more than normal, and normal more than low.");

            Task producer = Task.Run(() => ProduceAsync(queue, reporter, cancellationToken), CancellationToken.None);
            await ConsumeAsync(queue, reporter, cancellationToken).ConfigureAwait(false);
            await producer.ConfigureAwait(false);
            queue.Dispose();
        }

        private static async Task ProduceAsync(PriorityQoSQueue<WorkItem> queue, IDemoReporter reporter, CancellationToken cancellationToken)
        {
            int id = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                if (queue.Count > 50000)
                {
                    await Task.Delay(1).ConfigureAwait(false);
                    continue;
                }

                for (int band = 0; band < 3; band++)
                {
                    WorkItem item = new WorkItem(id++) { Priority = band };
                    queue.TryEnqueue(item);
                    reporter.Enqueued(_Labels[band]);
                }
            }
        }

        private static async Task ConsumeAsync(PriorityQoSQueue<WorkItem> queue, IDemoReporter reporter, CancellationToken cancellationToken)
        {
            int serviced = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                if (queue.TryDequeue(out WorkItem item))
                {
                    reporter.Serviced(_Labels[item.Priority]);
                    serviced++;
                    if ((serviced & 127) == 0)
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
