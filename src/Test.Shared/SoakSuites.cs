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

            using CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(SoakConfig.Seconds));
            long consumed = 0;

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

            // Sample throughput; it must keep rising (no stall to zero) across the run.
            List<long> samples = new List<long>();
            while (!cts.IsCancellationRequested)
            {
                samples.Add(Interlocked.Read(ref consumed));
                try
                {
                    await Task.Delay(500, cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                }
            }

            await producer.ConfigureAwait(false);
            await consumer.ConfigureAwait(false);
            await pipeline.StopAsync().ConfigureAwait(false);

            long total = Interlocked.Read(ref consumed);
            Check.True(total > 1000, $"pipeline serviced meaningful work ({total} items)");

            int rises = 0;
            for (int i = 1; i < samples.Count; i++)
            {
                if (samples[i] > samples[i - 1])
                    rises++;
            }

            Check.True(rises >= samples.Count / 2, $"throughput kept rising ({rises}/{samples.Count - 1} intervals) — no stall");
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

            Check.True(cycles > 1000, $"sustained many cycles ({cycles})");
            Check.Equal(0, queue.PendingItemWaiterCount, "no waiter drift after sustained churn");
            Check.True(queue.IsEmpty, "queue empty at end");
        }
    }
}
