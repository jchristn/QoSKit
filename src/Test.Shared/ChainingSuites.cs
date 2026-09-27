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

            cases.Add(Case("Chain", "DrainToWrrExactItems", "DrainTo from WRR moves each item exactly once", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> source = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("a", 1), new WeightedSubQueue("b", 1) },
                    subQueueSelector: i => i.Flow);
                source.Enqueue(new DemoItem(1) { Flow = "a" });
                source.Enqueue(new DemoItem(2) { Flow = "a" });
                source.Enqueue(new DemoItem(3) { Flow = "b" });
                source.Enqueue(new DemoItem(4) { Flow = "b" });
                Check.Equal(1, source.Dequeue().Id, "a spends its deficit on the first item");

                FifoQoSQueue<DemoItem> sink = new FifoQoSQueue<DemoItem>();
                Check.Equal(3, source.DrainTo(sink, 10), "three moved");
                Check.True(source.IsEmpty, "source drained");
                HashSet<int> ids = new HashSet<int>();
                while (sink.TryDequeue(out DemoItem moved))
                    Check.True(ids.Add(moved.Id), "item " + moved.Id + " moved once");
                Check.Equal(3, ids.Count, "no item lost");
                Check.True(ids.Contains(2) && ids.Contains(3) && ids.Contains(4), "exactly the resident items moved");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Chain", "DrainToLifoConcurrentEnqueue", "An enqueue between peek and take never loses or duplicates", _ =>
            {
                LifoQoSQueue<int> source = new LifoQoSQueue<int>();
                source.Enqueue(1);
                source.Enqueue(2);
                source.Enqueue(3);
                FifoQoSQueue<int> sink = new FifoQoSQueue<int>();

                // After the first item is forwarded, a producer pushes a new top-of-stack item.
                InterceptingSink<int> intercept = new InterceptingSink<int>(sink, (index, item) =>
                {
                    if (index == 0)
                        source.Enqueue(99);
                });

                Check.Equal(4, source.DrainTo(intercept, 10), "four moved");
                Check.True(source.IsEmpty, "source drained");
                List<int> moved = new List<int>(sink.ToArray());
                Check.Equal(4, new HashSet<int>(moved).Count, "no duplicates");
                Check.True(moved.Contains(99) && moved.Contains(1) && moved.Contains(2) && moved.Contains(3), "no item lost");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Chain", "DrainToLlqRefillBetweenPeekAndTake", "A policer refill between peek and take moves the peeked item", _ =>
            {
                ManualTimeProvider time = new ManualTimeProvider();
                LowLatencyQoSQueue<DemoItem> source = new LowLatencyQoSQueue<DemoItem>(
                    new[] { new TrafficClass<DemoItem>("rt", i => i.Tier == "rt", 1, new TokenBucket(ratePerSecond: 1, burst: 1)) },
                    new[] { new TrafficClass<DemoItem>("bulk", i => true) },
                    new QoSQueueOptions { TimeProvider = time });
                source.Enqueue(new DemoItem(1) { Tier = "rt" });
                source.Enqueue(new DemoItem(2) { Tier = "rt" });
                source.Enqueue(new DemoItem(50) { Tier = "bulk" });
                Check.Equal(1, source.Dequeue().Id, "first rt spends the burst");

                FifoQoSQueue<DemoItem> sink = new FifoQoSQueue<DemoItem>();
                InterceptingSink<DemoItem> intercept = new InterceptingSink<DemoItem>(sink, (index, item) => time.Advance(2000));

                Check.Equal(1, source.DrainTo(intercept, 1), "one moved");
                Check.Equal(50, sink.Dequeue().Id, "the peeked bulk item was forwarded");
                Check.Equal(1, source.Count, "one item left in the source");
                Check.Equal(2, source.Dequeue().Id, "the refilled rt item was not discarded");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Chain", "PipelineWrrConserves", "A pipeline pump drains WRR with no loss or duplication", async ct =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> ingress = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("a", 1), new WeightedSubQueue("b", 2) },
                    subQueueSelector: i => i.Flow);
                FifoQoSQueue<DemoItem> tail = new FifoQoSQueue<DemoItem>();
                await using QoSPipeline<DemoItem> pipeline = await ingress.ChainTo(tail).AsPipeline("wrr").StartAsync(ct).ConfigureAwait(false);

                for (int i = 0; i < 300; i++)
                    ingress.Enqueue(new DemoItem(i) { Flow = i % 3 == 0 ? "a" : "b" });

                using CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(TimeSpan.FromSeconds(10));
                HashSet<int> received = new HashSet<int>();
                for (int i = 0; i < 300; i++)
                    Check.True(received.Add((await tail.DequeueAsync(cts.Token).ConfigureAwait(false)).Id), "no duplicate reached the tail");

                Check.Equal(300, received.Count, "all 300 reached the tail");
                Check.True(ingress.IsEmpty, "ingress drained");
            }));

            cases.Add(Case("Chain", "PumpStopsOnDisposedSink", "A disposed sink stops the pump instead of discarding the source", async ct =>
            {
                FifoQoSQueue<int> source = new FifoQoSQueue<int>();
                FifoQoSQueue<int> sink = new FifoQoSQueue<int>();
                for (int i = 0; i < 5; i++)
                    source.Enqueue(i);
                sink.Dispose();

                QoSLink<int> link = new QoSLink<int>(new[] { source }, sink);
                await link.StartAsync(ct).ConfigureAwait(false);
                await Task.Delay(200).ConfigureAwait(false);
                await link.DisposeAsync().ConfigureAwait(false);

                Check.Equal(5, source.Count, "no item was discarded as poison into a disposed sink");
                Check.Equal(0L, link.Moved, "nothing moved");
            }));

            return new TestSuiteDescriptor("Chain", "Chaining", cases);
        }

        private static TestCaseDescriptor Case(string suite, string id, string name, Func<CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(suiteId: suite, caseId: id, displayName: name, executeAsync: body);
        }
    }
}
