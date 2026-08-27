namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using Touchstone.Core;

    /// <summary>
    /// Long-running endurance suites (plan Suites S10/S14), duration-scaled by <see cref="SoakConfig"/>.
    /// A short window runs on every commit; a nightly job runs them long-form.
    /// </summary>
    public static class SoakSuites
    {
        /// <summary>The soak suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor Suite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(new TestCaseDescriptor(
                suiteId: "Soak",
                caseId: "PipelineFaultEndurance",
                displayName: $"Faulted pipeline keeps flowing for {SoakConfig.Seconds}s",
                executeAsync: ct => PipelineEnduranceAsync()));

            cases.Add(new TestCaseDescriptor(
                suiteId: "Soak",
                caseId: "AsyncChurnNoDrift",
                displayName: $"Async churn holds steady for {SoakConfig.Seconds}s",
                executeAsync: ct => AsyncChurnAsync()));

            return new TestSuiteDescriptor("Soak", "Endurance", cases);
        }

        private static async Task PipelineEnduranceAsync()
        {
            FifoQoSQueue<int> head = new FifoQoSQueue<int>();
            // A poison classifier throws on multiples of 100 — the pump must discard and keep going.
            PriorityQoSQueue<int> middle = new PriorityQoSQueue<int>(3, v =>
            {
                if (v % 100 == 0)
                    throw new InvalidOperationException("poison");
                return v % 3;
            });
            FifoQoSQueue<int> tail = new FifoQoSQueue<int>();

            await using QoSPipeline<int> pipeline = await head
                .ChainTo(middle)
                .ChainTo(tail)
                .AsPipeline("soak")
                .StartAsync(CancellationToken.None)
                .ConfigureAwait(false);

            using CancellationTokenSource cts = new CancellationTokenSource();
            long consumed = 0;
            long midpoint = 0;

            Task producer = Task.Run(() =>
            {
                int v = 0;
                while (!cts.IsCancellationRequested)
                {
                    if (head.Count + middle.Count + tail.Count < 50000)
                        head.Enqueue(v++);
                    else
                        Thread.Sleep(1);
                }
            });

            Task consumer = Task.Run(() =>
            {
                while (!cts.IsCancellationRequested)
                {
                    if (tail.TryDequeue(out int _))
                        Interlocked.Increment(ref consumed);
                    else
                        Thread.Sleep(1);
                }
            });

            // Let it run for the window, capturing a midpoint reading to confirm work continued in
            // both halves (progress kept happening, not a single early burst then a stall).
            await Task.Delay(TimeSpan.FromSeconds(SoakConfig.Seconds) / 2).ConfigureAwait(false);
            midpoint = Interlocked.Read(ref consumed);
            await Task.Delay(TimeSpan.FromSeconds(SoakConfig.Seconds) / 2).ConfigureAwait(false);

            cts.Cancel();
            await producer.ConfigureAwait(false);
            await consumer.ConfigureAwait(false);
            await pipeline.StopAsync().ConfigureAwait(false);

            long total = Interlocked.Read(ref consumed);
            // A wedged or poison-stalled pipeline would service almost nothing; sustained flow proves
            // the fault-tolerant pump kept going. Work in the second half proves no mid-run stall.
            Check.True(total > 200, $"faulted pipeline serviced sustained work ({total} items)");
            Check.True(total > midpoint, $"throughput continued into the second half (midpoint {midpoint}, total {total})");
        }

        private static async Task AsyncChurnAsync()
        {
            FifoQoSQueue<int> queue = new FifoQoSQueue<int>();
            using CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(SoakConfig.Seconds));
            long cycles = 0;

            while (!cts.IsCancellationRequested)
            {
                ValueTask<int> pending = queue.DequeueAsync(CancellationToken.None);
                int value = unchecked((int)cycles);
                queue.Enqueue(value);
                int got = await pending.ConfigureAwait(false);
                Check.Equal(value, got, "round-trip value");
                cycles++;
            }

            Check.True(cycles > 200, $"sustained many cycles ({cycles})");
            Check.Equal(0, queue.PendingItemWaiterCount, "no waiter drift after sustained churn");
            Check.True(queue.IsEmpty, "queue empty at end");
        }
    }
}
