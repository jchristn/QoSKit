namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using Touchstone.Core;

    /// <summary>
    /// Classification semantics: unknown-key policies, case sensitivity, and overrides.
    /// </summary>
    public static class ClassificationSuites
    {
        /// <summary>The classification suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor Suite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Classify", "WfqCreateDynamic", "WFQ admits unknown key as a new flow", _ =>
            {
                WeightedFairQoSQueue<DemoItem> q = new WeightedFairQoSQueue<DemoItem>(
                    i => i.Flow, new[] { new WeightedFlow("known", 1) });
                Check.True(q.TryEnqueue(new DemoItem(1) { Flow = "brand-new" }), "unknown admitted dynamically");
                Check.Equal(1, q.Count, "admitted");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Classify", "WrrThrowUnknown", "WRR Throw policy throws on unknown key", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> q = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("gpu-a", 1), new WeightedSubQueue("gpu-b", 1) },
                    subQueueSelector: i => i.Flow);
                Check.Throws<UnknownClassificationException>(() => q.Enqueue(new DemoItem(1) { Flow = "gpu-c" }), "enqueue unknown throws");
                Check.Throws<UnknownClassificationException>(() => q.TryEnqueue(new DemoItem(2) { Flow = "gpu-c" }), "try-enqueue unknown throws under Throw");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Classify", "WrrRouteToDefault", "WRR RouteToDefault sends unknown to default", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> q = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("gpu-a", 1), new WeightedSubQueue("cpu", 1) },
                    subQueueSelector: i => i.Flow,
                    unknownKeyPolicy: UnknownKeyPolicy.RouteToDefault,
                    defaultKey: "cpu");
                Check.True(q.TryEnqueue(new DemoItem(1) { Flow = "gpu-c" }), "unknown routed to default");
                Check.Equal(1, q.Count, "admitted to default");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Classify", "WrrRejectUnknown", "WRR Reject drops unknown with event", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> q = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("gpu-a", 1) },
                    subQueueSelector: i => i.Flow,
                    unknownKeyPolicy: UnknownKeyPolicy.Reject);
                DropReason? reason = null;
                q.ItemDropped += (s, e) => reason = e.Reason;
                Check.False(q.TryEnqueue(new DemoItem(1) { Flow = "gpu-c" }), "try-enqueue false under Reject");
                Check.Equal(DropReason.UnknownClass, reason!.Value, "drop reason");
                Check.Throws<UnknownClassificationException>(() => q.Enqueue(new DemoItem(2) { Flow = "gpu-c" }), "enqueue throws under Reject");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Classify", "CaseInsensitiveDefault", "Keys match case-insensitively by default", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> q = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("gpu-a", 1) },
                    subQueueSelector: i => i.Flow);
                Check.True(q.TryEnqueue(new DemoItem(1) { Flow = "GPU-A" }), "case-mismatched key matches");
                Check.Equal(1, q.Count, "admitted");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Classify", "OrdinalComparer", "Ordinal comparer makes case distinct", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> q = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("gpu-a", 1) },
                    subQueueSelector: i => i.Flow,
                    keyComparer: StringComparer.Ordinal);
                Check.Throws<UnknownClassificationException>(() => q.Enqueue(new DemoItem(1) { Flow = "GPU-A" }), "case mismatch is unknown under ordinal");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Classify", "ExplicitOverride", "Explicit key override routes and validates", _ =>
            {
                WeightedRoundRobinQoSQueue<DemoItem> q = new WeightedRoundRobinQoSQueue<DemoItem>(
                    new[] { new WeightedSubQueue("gpu-a", 1) },
                    subQueueSelector: i => i.Flow);
                Check.True(q.TryEnqueue(new DemoItem(1), "gpu-a"), "known override admitted");
                Check.Throws<UnknownClassificationException>(() => q.Enqueue(new DemoItem(2), "gpu-c"), "unknown override throws");
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Classify", "Classification Semantics", cases);
        }

        private static TestCaseDescriptor Case(string suite, string id, string name, Func<CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(suiteId: suite, caseId: id, displayName: name, executeAsync: body);
        }
    }
}
