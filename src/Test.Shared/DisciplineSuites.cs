namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using Touchstone.Core;

    /// <summary>
    /// Per-discipline suites: priority, WFQ, CBWFQ, LLQ, and WRR/DWRR.
    /// </summary>
    public static class DisciplineSuites
    {
        private const double RatioTolerance = 0.06;

        /// <summary>Priority discipline suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor PrioritySuite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Priority", "BandOrder", "Higher band served first", _ =>
            {
                PriorityQoSQueue<DemoItem> q = new PriorityQoSQueue<DemoItem>(3, i => i.Priority);
                q.Enqueue(new DemoItem(1) { Priority = 2 });
                q.Enqueue(new DemoItem(2) { Priority = 0 });
                q.Enqueue(new DemoItem(3) { Priority = 1 });
                Check.Equal(2, Dequeue(q).Id, "band 0 first");
                Check.Equal(3, Dequeue(q).Id, "band 1 next");
                Check.Equal(1, Dequeue(q).Id, "band 2 last");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Priority", "WithinBandFifo", "Within a band, FIFO", _ =>
            {
                PriorityQoSQueue<DemoItem> q = new PriorityQoSQueue<DemoItem>(2, i => i.Priority);
                for (int i = 0; i < 5; i++)
                    q.Enqueue(new DemoItem(i) { Priority = 0 });
                for (int i = 0; i < 5; i++)
                    Check.Equal(i, Dequeue(q).Id, "fifo within band at " + i);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Priority", "ExplicitOverride", "Explicit priority overrides selector", _ =>
            {
                PriorityQoSQueue<DemoItem> q = new PriorityQoSQueue<DemoItem>(3, i => 2);
                q.Enqueue(new DemoItem(1));
                q.Enqueue(new DemoItem(2), 0);
                Check.Equal(2, Dequeue(q).Id, "override served first");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Priority", "OutOfRangeClamps", "Out-of-range band clamps", _ =>
            {
                PriorityQoSQueue<DemoItem> q = new PriorityQoSQueue<DemoItem>(2, i => i.Priority);
                q.Enqueue(new DemoItem(1) { Priority = 99 });
                q.Enqueue(new DemoItem(2) { Priority = -5 });
                Check.Equal(2, Dequeue(q).Id, "negative clamps to band 0");
                Check.Equal(1, Dequeue(q).Id, "large clamps to last band");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Priority", "BadCtor", "Invalid construction throws", _ =>
            {
                Check.Throws<ArgumentOutOfRangeException>(() => new PriorityQoSQueue<DemoItem>(0, i => 0), "levels <= 0");
                Check.Throws<ArgumentNullException>(() => new PriorityQoSQueue<DemoItem>(2, null!), "null selector");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Priority", "AgingOffByDefault", "Aging off by default: low band starves", _ =>
            {
                ManualTimeProvider time = new ManualTimeProvider();
                QoSQueueOptions opts = new QoSQueueOptions { TimeProvider = time };
                PriorityQoSQueue<DemoItem> q = new PriorityQoSQueue<DemoItem>(2, i => i.Priority, opts);
                q.Enqueue(new DemoItem(1) { Priority = 1 });
                time.Advance(1_000_000);
                q.Enqueue(new DemoItem(2) { Priority = 0 });
                Check.Equal(2, Dequeue(q).Id, "high still first without aging");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Priority", "AgingPromotes", "Aging promotes a starved item", _ =>
            {
                ManualTimeProvider time = new ManualTimeProvider();
                QoSQueueOptions opts = new QoSQueueOptions { TimeProvider = time };
                PriorityQoSQueue<DemoItem> q = new PriorityQoSQueue<DemoItem>(2, i => i.Priority, opts).WithAging(1000);
                q.Enqueue(new DemoItem(1) { Priority = 1 });
                time.Advance(2000);
                q.Enqueue(new DemoItem(2) { Priority = 0 });
                Check.Equal(1, Dequeue(q).Id, "aged low-priority item promoted");
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Priority", "Priority Queue", cases);
        }

        /// <summary>WFQ discipline suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor WeightedFairSuite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Wfq", "RatioConverges", "Service ratio matches 3:1 weights", _ =>
            {
                WeightedFairQoSQueue<DemoItem> q = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow,
                    new[] { new WeightedFlow("a", 3), new WeightedFlow("b", 1) });
                for (int i = 0; i < 3000; i++)
                {
                    q.Enqueue(new DemoItem(i) { Flow = "a" });
                    q.Enqueue(new DemoItem(i) { Flow = "b" });
                }

                Dictionary<string, int> counts = CountByFlowFirstN(q, 2000);
                double share = counts["a"] / 2000.0;
                Check.True(Math.Abs(share - 0.75) < RatioTolerance, $"a share {share:F3} near 0.75");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Wfq", "SingleFlowFifo", "Single flow behaves FIFO", _ =>
            {
                WeightedFairQoSQueue<DemoItem> q = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow, new[] { new WeightedFlow("a", 1) });
                for (int i = 0; i < 20; i++)
                    q.Enqueue(new DemoItem(i) { Flow = "a" });
                for (int i = 0; i < 20; i++)
                    Check.Equal(i, Dequeue(q).Id, "fifo at " + i);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Wfq", "CostAffectsShare", "Higher cost reduces per-item share", _ =>
            {
                WeightedFairQoSQueue<DemoItem> q = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow,
                    new[] { new WeightedFlow("a", 1), new WeightedFlow("b", 1) },
                    costSelector: i => i.Cost);
                for (int i = 0; i < 3000; i++)
                {
                    q.Enqueue(new DemoItem(i) { Flow = "a", Cost = 1 });
                    q.Enqueue(new DemoItem(i) { Flow = "b", Cost = 2 });
                }

                Dictionary<string, int> counts = CountByFlowFirstN(q, 2000);
                // Equal weight, but b costs 2x, so a should get ~2x the dequeues of b.
                double ratio = counts["a"] / (double)counts["b"];
                Check.True(ratio > 1.7 && ratio < 2.3, $"a:b count ratio {ratio:F2} near 2.0");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Wfq", "BadCtor", "Invalid construction throws", _ =>
            {
                Check.Throws<ArgumentNullException>(() => new WeightedFairQoSQueue<DemoItem>(null!, new[] { new WeightedFlow("a", 1) }), "null selector");
                Check.Throws<ArgumentException>(() => new WeightedFairQoSQueue<DemoItem>(i => i.Flow, new[] { new WeightedFlow("a", 1), new WeightedFlow("A", 1) }), "duplicate flow (case-insensitive)");
                Check.Throws<ArgumentOutOfRangeException>(() => new WeightedFlow("a", 0), "weight < 1");
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Wfq", "Weighted Fair Queuing", cases);
        }

        /// <summary>CBWFQ discipline suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor ClassBasedSuite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Cbwfq", "RoutingAndDefault", "Items route to classes; unmatched to default", _ =>
            {
                ClassBasedWeightedFairQoSQueue<DemoItem> q = new ClassBasedWeightedFairQoSQueue<DemoItem>(
                    new[]
                    {
                        new TrafficClass<DemoItem>("gold", i => i.Tier == "gold", 5),
                        new TrafficClass<DemoItem>("silver", i => i.Tier == "silver", 3)
                    });
                q.Enqueue(new DemoItem(1) { Tier = "gold" });
                q.Enqueue(new DemoItem(2) { Tier = "bronze" }); // -> default
                Check.Equal(2, q.Count, "two enqueued");
                List<int> ids = new List<int> { Dequeue(q).Id, Dequeue(q).Id };
                Check.True(ids.Contains(1) && ids.Contains(2), "both served, default caught bronze");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Cbwfq", "FirstMatchWins", "First matching class captures the item", _ =>
            {
                ClassBasedWeightedFairQoSQueue<DemoItem> q = new ClassBasedWeightedFairQoSQueue<DemoItem>(
                    new[]
                    {
                        new TrafficClass<DemoItem>("first", i => true, 1),
                        new TrafficClass<DemoItem>("second", i => true, 1)
                    });
                q.Enqueue(new DemoItem(1));
                Check.Equal(1, Dequeue(q).Id, "served from first class");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Cbwfq", "WeightedShare", "Classes converge to weights", _ =>
            {
                ClassBasedWeightedFairQoSQueue<DemoItem> q = new ClassBasedWeightedFairQoSQueue<DemoItem>(
                    new[]
                    {
                        new TrafficClass<DemoItem>("hi", i => i.Tier == "hi", 3),
                        new TrafficClass<DemoItem>("lo", i => i.Tier == "lo", 1)
                    });
                for (int i = 0; i < 3000; i++)
                {
                    q.Enqueue(new DemoItem(i) { Tier = "hi" });
                    q.Enqueue(new DemoItem(i) { Tier = "lo" });
                }

                Dictionary<string, int> counts = CountByTierFirstN(q, 2000);
                double share = counts["hi"] / 2000.0;
                Check.True(Math.Abs(share - 0.75) < RatioTolerance, $"hi share {share:F3} near 0.75");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Cbwfq", "DuplicateNameThrows", "Duplicate class name throws", _ =>
            {
                Check.Throws<ArgumentException>(() => new ClassBasedWeightedFairQoSQueue<DemoItem>(
                    new[]
                    {
                        new TrafficClass<DemoItem>("x", i => true),
                        new TrafficClass<DemoItem>("X", i => true)
                    }), "case-insensitive duplicate");
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Cbwfq", "Class-Based WFQ", cases);
        }

        /// <summary>LLQ discipline suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor LowLatencySuite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Llq", "PriorityJumpsLine", "Priority class served ahead of fair", _ =>
            {
                LowLatencyQoSQueue<DemoItem> q = new LowLatencyQoSQueue<DemoItem>(
                    new[] { new TrafficClass<DemoItem>("rt", i => i.Tier == "rt") },
                    new[] { new TrafficClass<DemoItem>("bulk", i => true) });
                for (int i = 0; i < 5; i++)
                    q.Enqueue(new DemoItem(i) { Tier = "bulk" });
                q.Enqueue(new DemoItem(100) { Tier = "rt" });
                Check.Equal(100, Dequeue(q).Id, "rt jumps ahead of bulk");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Llq", "PolicerDemotes", "Over-rate priority yields to fair", _ =>
            {
                ManualTimeProvider time = new ManualTimeProvider();
                QoSQueueOptions opts = new QoSQueueOptions { TimeProvider = time };
                TokenBucket bucket = new TokenBucket(ratePerSecond: 1, burst: 1);
                LowLatencyQoSQueue<DemoItem> q = new LowLatencyQoSQueue<DemoItem>(
                    new[] { new TrafficClass<DemoItem>("rt", i => i.Tier == "rt", 1, bucket) },
                    new[] { new TrafficClass<DemoItem>("bulk", i => true) },
                    opts);
                q.Enqueue(new DemoItem(1) { Tier = "rt" });
                q.Enqueue(new DemoItem(2) { Tier = "rt" });
                q.Enqueue(new DemoItem(50) { Tier = "bulk" });
                // First rt within burst is served; second rt is over-rate so bulk is served next.
                Check.Equal(1, Dequeue(q).Id, "first rt within burst");
                Check.Equal(50, Dequeue(q).Id, "bulk served while rt policed");
                time.Advance(2000); // refill tokens
                Check.Equal(2, Dequeue(q).Id, "second rt after refill");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Llq", "OversizeItemRateLimited", "An item costing more than the burst is rate-limited, not stranded", _ =>
            {
                ManualTimeProvider time = new ManualTimeProvider();
                QoSQueueOptions opts = new QoSQueueOptions { TimeProvider = time };
                TokenBucket bucket = new TokenBucket(ratePerSecond: 10, burst: 1);
                LowLatencyQoSQueue<DemoItem> q = new LowLatencyQoSQueue<DemoItem>(
                    new[] { new TrafficClass<DemoItem>("rt", i => i.Tier == "rt", 1, bucket) },
                    new TrafficClass<DemoItem>[0],
                    opts,
                    costSelector: i => i.Cost);
                q.Enqueue(new DemoItem(1) { Tier = "rt", Cost = 5 });
                q.Enqueue(new DemoItem(2) { Tier = "rt", Cost = 5 });

                Check.True(q.TryDequeue(out DemoItem first), "an oversize item conforms against a full bucket");
                Check.Equal(1, first.Id, "first oversize item served");
                Check.False(q.TryDequeue(out DemoItem _), "the debt it left holds the next one back");
                time.Advance(400); // the 4-token debt plus the 1-token burst take 500 ms at 10/s
                Check.False(q.TryDequeue(out DemoItem _), "still repaying the debt");
                time.Advance(100);
                Check.True(q.TryDequeue(out DemoItem second), "served once the bucket is full again");
                Check.Equal(2, second.Id, "second oversize item served");
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Llq", "Low-Latency Queuing", cases);
        }

        /// <summary>WRR/DWRR discipline suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor WeightedRoundRobinSuite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Wrr", "WeightedService", "Classifier WRR converges to 2:1", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> q = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("a", 2), new WeightedSubQueue("b", 1) },
                    subQueueSelector: i => i.Flow);
                for (int i = 0; i < 3000; i++)
                {
                    q.Enqueue(new DemoItem(i) { Flow = "a" });
                    q.Enqueue(new DemoItem(i) { Flow = "b" });
                }

                Dictionary<string, int> counts = CountByFlowFirstN(q, 3000);
                double share = counts["a"] / 3000.0;
                Check.True(Math.Abs(share - 0.6667) < RatioTolerance, $"a share {share:F3} near 0.667");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Wrr", "DeficitByteFair", "DWRR is byte-fair with mixed sizes", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> q = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("a", 1), new WeightedSubQueue("b", 1) },
                    subQueueSelector: i => i.Flow,
                    costSelector: i => i.Cost);
                for (int i = 0; i < 3000; i++)
                {
                    q.Enqueue(new DemoItem(i) { Flow = "a", Cost = 1 });
                    q.Enqueue(new DemoItem(i) { Flow = "b", Cost = 2 });
                }

                Dictionary<string, int> counts = CountByFlowFirstN(q, 1500);
                // Equal weight, byte-fair: 'a' (cost 1) served about twice as often as 'b' (cost 2).
                double ratio = counts["a"] / (double)counts["b"];
                Check.True(ratio > 1.7 && ratio < 2.3, $"a:b ratio {ratio:F2} near 2.0");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Wrr", "BalancerSpreads", "Balancer mode spreads by weight", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> q = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("a", 3), new WeightedSubQueue("b", 1) });
                for (int i = 0; i < 4000; i++)
                    q.Enqueue(new DemoItem(i));
                int total = 0;
                while (q.TryDequeue(out DemoItem _))
                    total++;
                Check.Equal(4000, total, "all served, conserved");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Wrr", "BadCtor", "Invalid construction throws", _ =>
            {
                Check.Throws<ArgumentException>(() => new WeightedRoundRobinQoSQueue<DemoItem>(new WeightedSubQueue[0]), "empty set");
                Check.Throws<ArgumentOutOfRangeException>(() => new WeightedSubQueue("a", 0), "weight < 1");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Wrr", "PeekMatchesDequeue", "TryPeek always reports the item TryDequeue serves next", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> q = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("a", 2), new WeightedSubQueue("b", 1), new WeightedSubQueue("c", 3) },
                    subQueueSelector: i => i.Flow,
                    costSelector: i => i.Cost);
                string[] flows = { "a", "b", "c" };
                for (int i = 0; i < 600; i++)
                    q.Enqueue(new DemoItem(i) { Flow = flows[i % 3], Cost = 1 + (i % 4) });

                int served = 0;
                while (q.TryPeek(out DemoItem peeked))
                {
                    Check.True(q.TryDequeue(out DemoItem taken), "peek implies an item to take");
                    Check.Equal(peeked.Id, taken.Id, "peek agrees with dequeue at step " + served);
                    served++;
                }

                Check.Equal(600, served, "all served");
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Wrr", "Weighted Round Robin", cases);
        }

        private static DemoItem Dequeue(IQoSQueue<DemoItem> q)
        {
            return q.Dequeue();
        }

        private static Dictionary<string, int> CountByFlowFirstN(IQoSQueue<DemoItem> q, int n)
        {
            Dictionary<string, int> counts = new Dictionary<string, int>();
            for (int i = 0; i < n; i++)
            {
                DemoItem item = q.Dequeue();
                counts.TryGetValue(item.Flow, out int c);
                counts[item.Flow] = c + 1;
            }

            return counts;
        }

        private static Dictionary<string, int> CountByTierFirstN(IQoSQueue<DemoItem> q, int n)
        {
            Dictionary<string, int> counts = new Dictionary<string, int>();
            for (int i = 0; i < n; i++)
            {
                DemoItem item = q.Dequeue();
                counts.TryGetValue(item.Tier, out int c);
                counts[item.Tier] = c + 1;
            }

            return counts;
        }

        private static TestCaseDescriptor Case(string suite, string id, string name, Func<CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(suiteId: suite, caseId: id, displayName: name, executeAsync: body);
        }
    }
}
