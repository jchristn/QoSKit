namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using Touchstone.Core;

    /// <summary>
    /// Chaining suites: DrainTo, manual and fluent chains, pipelines, cycle detection, and routing.
    /// </summary>
    public static class ChainingSuites
    {
        /// <summary>The chaining suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor Suite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Chain", "DrainToMovesMin", "DrainTo moves min(max, available)", _ =>
            {
                FifoQoSQueue<int> a = new FifoQoSQueue<int>();
                FifoQoSQueue<int> b = new FifoQoSQueue<int>();
                for (int i = 0; i < 10; i++)
                    a.Enqueue(i);
                int moved = a.DrainTo(b, 4);
                Check.Equal(4, moved, "moved 4");
                Check.Equal(6, a.Count, "6 left in a");
                Check.Equal(4, b.Count, "4 in b");
                Check.Equal(0, b.Dequeue(), "order preserved");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Chain", "DrainToEmpty", "DrainTo from empty returns 0", _ =>
            {
                FifoQoSQueue<int> a = new FifoQoSQueue<int>();
                FifoQoSQueue<int> b = new FifoQoSQueue<int>();
                Check.Equal(0, a.DrainTo(b, 100), "nothing moved");
                Check.Throws<ArgumentOutOfRangeException>(() => a.DrainTo(b, -1), "negative max throws");
                Check.Throws<ArgumentNullException>(() => a.DrainTo(null!, 1), "null sink throws");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Chain", "ManualTwoLevel", "Manual drain of two L1 into L2 priority", _ =>
            {
                LowLatencyQoSQueue<DemoItem> realtime = new LowLatencyQoSQueue<DemoItem>(
                    new[] { new TrafficClass<DemoItem>("rt", i => true) },
                    new TrafficClass<DemoItem>[0]);
                WeightedFairQoSQueue<DemoItem> fair = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow, new[] { new WeightedFlow("x", 1) });
                PriorityQoSQueue<DemoItem> level2 = new PriorityQoSQueue<DemoItem>(2, i => i.Priority);

                realtime.Enqueue(new DemoItem(1) { Priority = 1 });
                fair.Enqueue(new DemoItem(2) { Flow = "x", Priority = 0 });
                realtime.DrainTo(level2, 10);
                fair.DrainTo(level2, 10);

                Check.Equal(2, level2.Count, "both drained into L2");
                Check.Equal(2, level2.Dequeue().Id, "priority 0 served first");
                Check.Equal(1, level2.Dequeue().Id, "priority 1 next");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Chain", "FluentPipeline", "Fluent chain pumps head to tail", async ct =>
            {
                FifoQoSQueue<int> ingress = new FifoQoSQueue<int>();
                PriorityQoSQueue<int> tail = new PriorityQoSQueue<int>(1, i => 0);
                await using QoSPipeline<int> pipeline = await ingress.ChainTo(tail).AsPipeline("test").StartAsync(ct).ConfigureAwait(false);

                for (int i = 0; i < 100; i++)
                    ingress.Enqueue(i);

                using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(TimeSpan.FromSeconds(5));
                HashSet<int> received = new HashSet<int>();
                for (int i = 0; i < 100; i++)
                    received.Add(await tail.DequeueAsync(cts.Token).ConfigureAwait(false));

                Check.Equal(100, received.Count, "all 100 reached the tail");
            }));

            cases.Add(Case("Chain", "MergeManyToOne", "Merge drains two sources into one tail", async ct =>
            {
                FifoQoSQueue<int> a = new FifoQoSQueue<int>();
                FifoQoSQueue<int> b = new FifoQoSQueue<int>();
                FifoQoSQueue<int> tail = new FifoQoSQueue<int>();
                await using QoSPipeline<int> pipeline = await a.Merge(b).ChainTo(tail).AsPipeline("merge").StartAsync(ct).ConfigureAwait(false);

                for (int i = 0; i < 50; i++)
                {
                    a.Enqueue(i);
                    b.Enqueue(1000 + i);
                }

                using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(TimeSpan.FromSeconds(5));
                HashSet<int> received = new HashSet<int>();
                for (int i = 0; i < 100; i++)
                    received.Add(await tail.DequeueAsync(cts.Token).ConfigureAwait(false));

                Check.Equal(100, received.Count, "all from both sources reached tail");
            }));

            cases.Add(Case("Chain", "CycleDetected", "A cyclic pipeline throws PipelineCycleException", async ct =>
            {
                FifoQoSQueue<int> a = new FifoQoSQueue<int>();
                FifoQoSQueue<int> b = new FifoQoSQueue<int>();
                QoSPipeline<int> pipeline = a.ChainTo(b).ChainTo(a).AsPipeline("cyclic");
                await Check.ThrowsAsync<PipelineCycleException>(async () => await pipeline.StartAsync(ct).ConfigureAwait(false), "cycle detected").ConfigureAwait(false);
                await pipeline.DisposeAsync().ConfigureAwait(false);
            }));

            cases.Add(Case("Chain", "SelfLoopDetected", "A self-loop throws PipelineCycleException", async ct =>
            {
                FifoQoSQueue<int> a = new FifoQoSQueue<int>();
                QoSPipeline<int> pipeline = a.ChainTo(a).AsPipeline("self");
                await Check.ThrowsAsync<PipelineCycleException>(async () => await pipeline.StartAsync(ct).ConfigureAwait(false), "self-loop detected").ConfigureAwait(false);
                await pipeline.DisposeAsync().ConfigureAwait(false);
            }));

            cases.Add(Case("Chain", "RouterFanOut", "Router partitions items across sinks", _ =>
            {
                FifoQoSQueue<int> evens = new FifoQoSQueue<int>();
                FifoQoSQueue<int> odds = new FifoQoSQueue<int>();
                QoSRouter<int> router = new QoSRouter<int>(new[]
                {
                    new QoSRoute<int>(x => x % 2 == 0, evens),
                    new QoSRoute<int>(x => x % 2 == 1, odds)
                });
                for (int i = 0; i < 100; i++)
                    router.Enqueue(i);
                Check.Equal(50, evens.Count, "50 evens");
                Check.Equal(50, odds.Count, "50 odds");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Chain", "RouterUnroutable", "No default route rejects unmatched", _ =>
            {
                FifoQoSQueue<int> evens = new FifoQoSQueue<int>();
                QoSRouter<int> router = new QoSRouter<int>(new[] { new QoSRoute<int>(x => x % 2 == 0, evens) });
                DropReason? reason = null;
                router.ItemDropped += (s, e) => reason = e.Reason;
                Check.False(router.TryEnqueue(3), "odd rejected");
                Check.Equal(DropReason.Unroutable, reason!.Value, "unroutable reason");
                Check.Throws<UnroutableItemException>(() => router.Enqueue(5), "enqueue throws");
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Chain", "Chaining", cases);
        }

        private static TestCaseDescriptor Case(string suite, string id, string name, Func<CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(suiteId: suite, caseId: id, displayName: name, executeAsync: body);
        }
    }
}
