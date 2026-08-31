namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using Touchstone.Core;

    /// <summary>
    /// Telemetry suite: verifies the QoSKit meter and activity source emit the granular, per-class,
    /// cost-aware, policer-aware signals an operator needs, and honor every switch that turns them off.
    /// </summary>
    public static class TelemetrySuites
    {
        /// <summary>The telemetry suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor Suite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            // ---- Metrics: counters and cost accounting (positive) ----

            cases.Add(Case("Telemetry", "CountersMatchStats", "Meter counters match the statistics snapshot", _ =>
            {
                string name = Unique("meter");
                FifoQoSQueue<int> queue = new FifoQoSQueue<int>(new QoSQueueOptions { Name = name });
                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    for (int i = 0; i < 100; i++)
                        queue.Enqueue(i);
                    for (int i = 0; i < 60; i++)
                        queue.Dequeue();
                });

                QoSQueueStatistics stats = queue.Statistics;
                Check.Equal((double)stats.Enqueued, Sum(m, "qoskit.queue.enqueued"), "enqueued counter matches stats");
                Check.Equal((double)stats.Dequeued, Sum(m, "qoskit.queue.dequeued"), "dequeued counter matches stats");
                Check.Equal(100.0, Sum(m, "qoskit.queue.enqueued"), "100 enqueued observed");
                Check.Equal(60.0, Sum(m, "qoskit.queue.dequeued"), "60 dequeued observed");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "ByteCountersMatchCost", "Byte counters accumulate the cost weight, not item count", _ =>
            {
                string name = Unique("bytes");
                WeightedFairQoSQueue<DemoItem> queue = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow,
                    new[] { new WeightedFlow("a", 1), new WeightedFlow("b", 1) },
                    new QoSQueueOptions { Name = name },
                    costSelector: i => i.Cost);

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    queue.Enqueue(new DemoItem(1) { Flow = "a", Cost = 10 });
                    queue.Enqueue(new DemoItem(2) { Flow = "a", Cost = 5 });
                    queue.Enqueue(new DemoItem(3) { Flow = "b", Cost = 100 });
                    while (queue.TryDequeue(out DemoItem _)) { }
                });

                Check.Equal(3.0, Sum(m, "qoskit.queue.enqueued"), "three items enqueued");
                Check.Equal(115.0, Sum(m, "qoskit.queue.enqueued.bytes"), "enqueued bytes sum the cost");
                Check.Equal(115.0, Sum(m, "qoskit.queue.dequeued.bytes"), "dequeued bytes sum the cost");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            // ---- Metrics: per-class breakdown (positive + negative) ----

            cases.Add(Case("Telemetry", "PerClassFlowTags", "WFQ measurements carry the per-flow class tag", _ =>
            {
                string name = Unique("wfqclass");
                WeightedFairQoSQueue<DemoItem> queue = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow,
                    new[] { new WeightedFlow("a", 1), new WeightedFlow("b", 1) },
                    new QoSQueueOptions { Name = name });

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    for (int i = 0; i < 3; i++)
                        queue.Enqueue(new DemoItem(i) { Flow = "a" });
                    for (int i = 0; i < 2; i++)
                        queue.Enqueue(new DemoItem(i) { Flow = "b" });
                });

                Check.Equal(3.0, SumClass(m, "qoskit.queue.enqueued", "a"), "flow a enqueued count tagged");
                Check.Equal(2.0, SumClass(m, "qoskit.queue.enqueued", "b"), "flow b enqueued count tagged");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "PerClassBandTags", "Priority measurements carry a per-band class tag", _ =>
            {
                string name = Unique("prioclass");
                PriorityQoSQueue<int> queue = new PriorityQoSQueue<int>(3, i => i % 3, new QoSQueueOptions { Name = name });

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    queue.Enqueue(0); // band 0
                    queue.Enqueue(3); // band 0
                    queue.Enqueue(1); // band 1
                });

                Check.Equal(2.0, SumClass(m, "qoskit.queue.enqueued", "band-0"), "band-0 tagged twice");
                Check.Equal(1.0, SumClass(m, "qoskit.queue.enqueued", "band-1"), "band-1 tagged once");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "PerClassDisabledOmitsTag", "EnablePerClassMetrics=false drops the class tag", _ =>
            {
                string name = Unique("noclass");
                WeightedFairQoSQueue<DemoItem> queue = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow,
                    new[] { new WeightedFlow("a", 1) },
                    new QoSQueueOptions { Name = name, EnablePerClassMetrics = false });

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    queue.Enqueue(new DemoItem(1) { Flow = "a" });
                });

                Check.True(m.Count > 0, "measurements were emitted");
                foreach (TelemetryMeasurement measurement in m)
                    Check.False(measurement.HasTag(QoSMetrics.TagQueueClass), "no measurement carries a class tag");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            // ---- Metrics: drops and rejections with reasons (positive) ----

            cases.Add(Case("Telemetry", "DropReasonNewest", "DropNewest overflow tags drop.reason=newest", _ =>
            {
                string name = Unique("dropnew");
                FifoQoSQueue<int> queue = new FifoQoSQueue<int>(new QoSQueueOptions
                {
                    Name = name,
                    MaxDepth = 2,
                    OverflowPolicy = OverflowPolicy.DropNewest
                });

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    queue.Enqueue(1);
                    queue.Enqueue(2);
                    queue.TryEnqueue(3); // dropped: newest
                });

                Check.Equal(1.0, Sum(m, "qoskit.queue.dropped"), "one drop counted");
                TelemetryMeasurement? drop = First(m, "qoskit.queue.dropped");
                Check.True(drop != null, "drop measurement present");
                Check.Equal("newest", drop!.Tag(QoSMetrics.TagDropReason), "drop.reason is newest");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "DropReasonOldest", "DropOldest overflow tags drop.reason=oldest", _ =>
            {
                string name = Unique("dropold");
                FifoQoSQueue<int> queue = new FifoQoSQueue<int>(new QoSQueueOptions
                {
                    Name = name,
                    MaxDepth = 2,
                    OverflowPolicy = OverflowPolicy.DropOldest
                });

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    queue.Enqueue(1);
                    queue.Enqueue(2);
                    queue.Enqueue(3); // evicts oldest
                });

                TelemetryMeasurement? drop = First(m, "qoskit.queue.dropped");
                Check.True(drop != null, "drop measurement present");
                Check.Equal("oldest", drop!.Tag(QoSMetrics.TagDropReason), "drop.reason is oldest");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "DropReasonUnknownClass", "Unknown-class rejection tags drop.reason=unknown_class", _ =>
            {
                string name = Unique("unknown");
                WeightedRoundRobinQoSQueue<DemoItem> queue = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("a", 1) },
                    subQueueSelector: i => i.Flow,
                    unknownKeyPolicy: UnknownKeyPolicy.Reject,
                    options: new QoSQueueOptions { Name = name });

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    queue.TryEnqueue(new DemoItem(1) { Flow = "zzz" });
                });

                TelemetryMeasurement? drop = First(m, "qoskit.queue.dropped");
                Check.True(drop != null, "drop measurement present");
                Check.Equal("unknown_class", drop!.Tag(QoSMetrics.TagDropReason), "drop.reason is unknown_class");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "RejectedCounted", "A full Reject queue emits rejected item and byte counters", _ =>
            {
                string name = Unique("reject");
                WeightedFairQoSQueue<DemoItem> queue = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow,
                    new[] { new WeightedFlow("a", 1) },
                    new QoSQueueOptions { Name = name, MaxDepth = 1, OverflowPolicy = OverflowPolicy.Reject },
                    costSelector: i => i.Cost);

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    queue.Enqueue(new DemoItem(1) { Flow = "a", Cost = 7 });
                    queue.TryEnqueue(new DemoItem(2) { Flow = "a", Cost = 9 }); // rejected
                });

                Check.Equal(1.0, Sum(m, "qoskit.queue.rejected"), "one rejection counted");
                Check.Equal(9.0, Sum(m, "qoskit.queue.rejected.bytes"), "rejected bytes reflect the rejected item cost");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            // ---- Metrics: LLQ policer accounting (positive + negative) ----

            cases.Add(Case("Telemetry", "PolicerConformAndExceed", "LLQ policer emits conform and exceed per class", _ =>
            {
                string name = Unique("policer");
                ManualTimeProvider time = new ManualTimeProvider();
                TokenBucket bucket = new TokenBucket(ratePerSecond: 1, burst: 1);
                LowLatencyQoSQueue<DemoItem> queue = new LowLatencyQoSQueue<DemoItem>(
                    new[] { new TrafficClass<DemoItem>("rt", i => i.Tier == "rt", 1, bucket) },
                    new[] { new TrafficClass<DemoItem>("bulk", i => true) },
                    new QoSQueueOptions { Name = name, TimeProvider = time });

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    queue.Enqueue(new DemoItem(1) { Tier = "rt" });
                    queue.Enqueue(new DemoItem(2) { Tier = "rt" });
                    queue.Enqueue(new DemoItem(50) { Tier = "bulk" });
                    queue.Dequeue(); // rt #1 conforms (burst)
                    queue.Dequeue(); // rt #2 exceeds -> bulk served
                });

                Check.True(Sum(m, "qoskit.policer.conformed") >= 1.0, "at least one conform");
                Check.True(Sum(m, "qoskit.policer.exceeded") >= 1.0, "at least one exceed");
                TelemetryMeasurement? conform = First(m, "qoskit.policer.conformed");
                Check.Equal("rt", conform!.Tag(QoSMetrics.TagQueueClass), "conform tagged with the rt class");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "UnpolicedPriorityNoPolicerMetrics", "An unpoliced priority class emits no policer counters", _ =>
            {
                string name = Unique("nopolicer");
                LowLatencyQoSQueue<DemoItem> queue = new LowLatencyQoSQueue<DemoItem>(
                    new[] { new TrafficClass<DemoItem>("rt", i => i.Tier == "rt") }, // null rate limit
                    new[] { new TrafficClass<DemoItem>("bulk", i => true) },
                    new QoSQueueOptions { Name = name });

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    queue.Enqueue(new DemoItem(1) { Tier = "rt" });
                    queue.Enqueue(new DemoItem(2) { Tier = "rt" });
                    queue.Dequeue();
                    queue.Dequeue();
                });

                Check.Equal(0.0, Sum(m, "qoskit.policer.conformed"), "no conform without a policer");
                Check.Equal(0.0, Sum(m, "qoskit.policer.exceeded"), "no exceed without a policer");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            // ---- Metrics: observable gauges (positive + negative) ----

            cases.Add(Case("Telemetry", "GaugesReportCapacityPeakBytes", "Pull gauges report capacity, peak depth, and resident bytes", _ =>
            {
                string name = Unique("gauge");
                WeightedFairQoSQueue<DemoItem> queue = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow,
                    new[] { new WeightedFlow("a", 1) },
                    new QoSQueueOptions { Name = name, MaxDepth = 10 },
                    costSelector: i => i.Cost);

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    for (int i = 0; i < 5; i++)
                        queue.Enqueue(new DemoItem(i) { Flow = "a", Cost = 3 });
                }, recordObservable: true);

                TelemetryMeasurement? capacity = First(m, "qoskit.queue.capacity");
                TelemetryMeasurement? peak = First(m, "qoskit.queue.peak.depth");
                TelemetryMeasurement? resident = First(m, "qoskit.queue.resident.bytes");
                Check.Equal(10.0, capacity!.Value, "capacity gauge reports MaxDepth");
                Check.Equal(5.0, peak!.Value, "peak depth gauge reports the high-water mark");
                Check.Equal(15.0, resident!.Value, "resident bytes gauge reports 5 items x cost 3");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "GaugeUnregisteredAfterDispose", "A disposed queue no longer reports gauges", _ =>
            {
                string name = Unique("gaugedispose");
                WeightedFairQoSQueue<DemoItem> queue = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow,
                    new[] { new WeightedFlow("a", 1) },
                    new QoSQueueOptions { Name = name, MaxDepth = 4 });
                queue.Enqueue(new DemoItem(1) { Flow = "a" });
                queue.Dispose();

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () => { }, recordObservable: true);
                Check.Equal(0, CountName(m, "qoskit.queue.capacity"), "no capacity gauge after dispose");
                return Task.CompletedTask;
            }));

            // ---- Metrics: master switch (negative) ----

            cases.Add(Case("Telemetry", "DisabledEmitsNothing", "Metrics disabled emits no measurements", _ =>
            {
                string name = Unique("nometer");
                FifoQoSQueue<int> queue = new FifoQoSQueue<int>(new QoSQueueOptions { Name = name, EnableMetrics = false });

                List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                {
                    for (int i = 0; i < 50; i++)
                        queue.Enqueue(i);
                    queue.Dequeue();
                }, recordObservable: true);

                Check.Equal(0, m.Count, "no measurements when metrics disabled");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            // ---- Traces (positive + negative) ----

            cases.Add(Case("Telemetry", "EnqueueDequeueSpans", "Enqueue and dequeue open spans with class, cost, and outcome", _ =>
            {
                string name = Unique("span");
                FifoQoSQueue<int> queue = new FifoQoSQueue<int>(new QoSQueueOptions { Name = name });

                List<TelemetryMeasurement> spans = TelemetryCapture.CaptureSpans(() =>
                {
                    queue.Enqueue(1);
                    queue.Dequeue();
                });

                TelemetryMeasurement? enqueue = FirstSpanFor(spans, QoSTracing.EnqueueSpanName, name);
                TelemetryMeasurement? dequeue = FirstSpanFor(spans, QoSTracing.DequeueSpanName, name);
                Check.True(enqueue != null, "enqueue span emitted");
                Check.Equal("admitted", enqueue!.Tag("qoskit.outcome"), "enqueue outcome is admitted");
                Check.True(dequeue != null, "dequeue span emitted");
                Check.True(dequeue!.HasTag("qoskit.wait_ms"), "dequeue span carries wait_ms");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "DropSpanCarriesReason", "A dropped enqueue span records the outcome and reason", _ =>
            {
                string name = Unique("dropspan");
                FifoQoSQueue<int> queue = new FifoQoSQueue<int>(new QoSQueueOptions
                {
                    Name = name,
                    MaxDepth = 1,
                    OverflowPolicy = OverflowPolicy.DropNewest
                });

                List<TelemetryMeasurement> spans = TelemetryCapture.CaptureSpans(() =>
                {
                    queue.Enqueue(1);
                    queue.TryEnqueue(2); // dropped newest
                });

                bool sawDrop = false;
                foreach (TelemetryMeasurement span in spans)
                {
                    if (span.Name == QoSTracing.EnqueueSpanName && span.Tag(QoSMetrics.TagQueueName) == name
                        && span.Tag("qoskit.outcome") == "dropped.newest")
                        sawDrop = true;
                }

                Check.True(sawDrop, "a dropped.newest enqueue span was emitted");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            cases.Add(Case("Telemetry", "LinkMoveSpanEmitted", "A pipeline hop opens a link.move span", async ct =>
            {
                string ingressName = Unique("ingress");
                string tailName = Unique("tail");
                FifoQoSQueue<int> ingress = new FifoQoSQueue<int>(new QoSQueueOptions { Name = ingressName });
                FifoQoSQueue<int> tail = new FifoQoSQueue<int>(new QoSQueueOptions { Name = tailName });

                List<TelemetryMeasurement> spans = TelemetryCapture.CaptureSpans(() =>
                {
                    MoveThroughPipeline(ingress, tail, ct).GetAwaiter().GetResult();
                });

                bool sawMove = false;
                foreach (TelemetryMeasurement span in spans)
                {
                    if (span.Name == QoSTracing.MoveSpanName && span.Tag("qoskit.sink") == tailName
                        && span.Tag("qoskit.outcome") == "moved")
                        sawMove = true;
                }

                Check.True(sawMove, "a moved link.move span for the tail was emitted");
                GC.KeepAlive(ingress);
                GC.KeepAlive(tail);
                await Task.CompletedTask.ConfigureAwait(false);
            }));

            cases.Add(Case("Telemetry", "TracingDisabledNoSpans", "EnableTracing=false emits no spans even with a listener", _ =>
            {
                string name = Unique("notrace");
                FifoQoSQueue<int> queue = new FifoQoSQueue<int>(new QoSQueueOptions { Name = name, EnableTracing = false });

                List<TelemetryMeasurement> spans = TelemetryCapture.CaptureSpans(() =>
                {
                    queue.Enqueue(1);
                    queue.Dequeue();
                });

                foreach (TelemetryMeasurement span in spans)
                    Check.False(span.Tag(QoSMetrics.TagQueueName) == name, "no span for a tracing-disabled queue");
                GC.KeepAlive(queue);
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Telemetry", "Telemetry", cases);
        }

        private static async Task MoveThroughPipeline(FifoQoSQueue<int> ingress, FifoQoSQueue<int> tail, CancellationToken ct)
        {
            await using (QoSPipeline<int> pipeline = await ingress.ChainTo(tail).AsPipeline("telemetry-move").StartAsync(ct).ConfigureAwait(false))
            {
                for (int i = 0; i < 10; i++)
                    ingress.Enqueue(i);

                using (CancellationTokenSource cts = CancellationTokenSource.CreateLinkedTokenSource(ct))
                {
                    cts.CancelAfter(TimeSpan.FromSeconds(5));
                    for (int i = 0; i < 10; i++)
                        await tail.DequeueAsync(cts.Token).ConfigureAwait(false);
                }

                await pipeline.StopAsync(ct).ConfigureAwait(false);
            }
        }

        private static double Sum(List<TelemetryMeasurement> measurements, string instrument)
        {
            double total = 0;
            foreach (TelemetryMeasurement measurement in measurements)
            {
                if (measurement.Name == instrument)
                    total += measurement.Value;
            }

            return total;
        }

        private static double SumClass(List<TelemetryMeasurement> measurements, string instrument, string className)
        {
            double total = 0;
            foreach (TelemetryMeasurement measurement in measurements)
            {
                if (measurement.Name == instrument && measurement.Tag(QoSMetrics.TagQueueClass) == className)
                    total += measurement.Value;
            }

            return total;
        }

        private static int CountName(List<TelemetryMeasurement> measurements, string name)
        {
            int count = 0;
            foreach (TelemetryMeasurement measurement in measurements)
            {
                if (measurement.Name == name)
                    count++;
            }

            return count;
        }

        private static TelemetryMeasurement? First(List<TelemetryMeasurement> measurements, string name)
        {
            foreach (TelemetryMeasurement measurement in measurements)
            {
                if (measurement.Name == name)
                    return measurement;
            }

            return null;
        }

        private static TelemetryMeasurement? FirstSpanFor(List<TelemetryMeasurement> spans, string spanName, string queueName)
        {
            foreach (TelemetryMeasurement span in spans)
            {
                if (span.Name == spanName && span.Tag(QoSMetrics.TagQueueName) == queueName)
                    return span;
            }

            return null;
        }

        private static string Unique(string prefix)
        {
            return prefix + "-" + Guid.NewGuid().ToString("N");
        }

        private static TestCaseDescriptor Case(string suite, string id, string name, Func<CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(suiteId: suite, caseId: id, displayName: name, executeAsync: body);
        }
    }
}
