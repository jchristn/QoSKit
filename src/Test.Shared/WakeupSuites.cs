namespace Test.Shared
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using Touchstone.Core;

    /// <summary>
    /// Wakeup and eligibility suites: an async consumer must wake when resident items become eligible
    /// (an LLQ priority class throttled by its policer refilling) with no further enqueue, must end
    /// promptly on cancellation or disposal without leaking a waiter, must not spin, and must never
    /// lose a wakeup to a cancellation race. Blocked producers must wake when capacity is freed by
    /// any means. Each case runs under a watchdog so a lost wakeup fails fast rather than hanging.
    /// </summary>
    public static class WakeupSuites
    {
        private const int WatchdogMs = 10000;

        /// <summary>The wakeup suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor Suite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Wakeup", "LlqThrottledDequeueAsyncManualClock", "DequeueAsync on a throttled LLQ completes after refill with no enqueue", ct =>
                Watchdog(async () =>
                {
                    ManualTimeProvider time = new ManualTimeProvider();
                    LowLatencyQoSQueue<DemoItem> q = PolicedLlq(time, new TokenBucket(ratePerSecond: 100, burst: 1));
                    q.Enqueue(new DemoItem(1) { Tier = "rt" });
                    q.Enqueue(new DemoItem(2) { Tier = "rt" });
                    Check.Equal(1, q.Dequeue().Id, "first rt within burst");

                    Task<DemoItem> pending = q.DequeueAsync(CancellationToken.None).AsTask();
                    await Task.Delay(50).ConfigureAwait(false);
                    Check.False(pending.IsCompleted, "second rt is held back while the policer is empty");
                    Check.Equal(1, q.Count, "throttled item still resident");

                    time.Advance(10); // exactly one token at 100/s; no enqueue follows
                    DemoItem got = await pending.ConfigureAwait(false);
                    Check.Equal(2, got.Id, "throttled item released by refill alone");
                    Check.Equal(0, q.PendingItemWaiterCount, "no waiter remains registered");
                    q.Dispose();
                })));

            cases.Add(Case("Wakeup", "LlqThrottledDequeueAsyncRealClock", "DequeueAsync wakes at the computed refill time on the system clock", ct =>
                Watchdog(async () =>
                {
                    LowLatencyQoSQueue<DemoItem> q = PolicedLlq(SystemQoSTimeProvider.Instance, new TokenBucket(ratePerSecond: 20, burst: 1));
                    q.Enqueue(new DemoItem(1) { Tier = "rt" });
                    q.Enqueue(new DemoItem(2) { Tier = "rt" });
                    Check.Equal(1, q.Dequeue().Id, "first rt within burst");

                    Stopwatch sw = Stopwatch.StartNew();
                    DemoItem got = await q.DequeueAsync(CancellationToken.None).ConfigureAwait(false);
                    sw.Stop();
                    Check.Equal(2, got.Id, "throttled item released by refill alone");
                    Check.True(sw.ElapsedMilliseconds >= 30, "the policer still held the item until refill (" + sw.ElapsedMilliseconds + " ms)");
                    Check.True(sw.ElapsedMilliseconds < 2000, "woken near the computed refill time (" + sw.ElapsedMilliseconds + " ms)");
                    Check.Equal(0, q.PendingItemWaiterCount, "no waiter remains registered");
                    q.Dispose();
                })));

            cases.Add(Case("Wakeup", "LlqThrottledConsumeAsync", "ConsumeAsync drains a throttled LLQ as the policer refills", ct =>
                Watchdog(async () =>
                {
                    ManualTimeProvider time = new ManualTimeProvider();
                    LowLatencyQoSQueue<DemoItem> q = PolicedLlq(time, new TokenBucket(ratePerSecond: 100, burst: 1));
                    for (int i = 1; i <= 3; i++)
                        q.Enqueue(new DemoItem(i) { Tier = "rt" });

                    using CancellationTokenSource cts = new CancellationTokenSource();
                    List<int> received = new List<int>();
                    Task consumer = Task.Run(async () =>
                    {
                        await foreach (DemoItem item in q.ConsumeAsync(cts.Token).ConfigureAwait(false))
                        {
                            lock (received)
                                received.Add(item.Id);
                            if (item.Id == 3)
                                break;
                        }
                    });

                    // Only the clock moves; nothing is ever enqueued after the consumer starts.
                    while (!consumer.IsCompleted)
                    {
                        time.Advance(10);
                        await Task.Delay(20).ConfigureAwait(false);
                    }

                    await consumer.ConfigureAwait(false);
                    Check.Equal(3, received.Count, "all three throttled items consumed");
                    Check.Equal(1, received[0], "service order kept (1)");
                    Check.Equal(3, received[2], "service order kept (3)");
                    Check.Equal(0, q.PendingItemWaiterCount, "no waiter remains registered");
                    q.Dispose();
                })));

            cases.Add(Case("Wakeup", "LlqThrottledWaitCancels", "Cancelling a throttled wait ends it promptly and deregisters", ct =>
                Watchdog(async () =>
                {
                    ManualTimeProvider time = new ManualTimeProvider();
                    LowLatencyQoSQueue<DemoItem> q = PolicedLlq(time, new TokenBucket(ratePerSecond: 0.001, burst: 1));
                    q.Enqueue(new DemoItem(1) { Tier = "rt" });
                    q.Enqueue(new DemoItem(2) { Tier = "rt" });
                    q.Dequeue();

                    using CancellationTokenSource cts = new CancellationTokenSource();
                    Task<DemoItem> pending = q.DequeueAsync(cts.Token).AsTask();
                    await Task.Delay(50).ConfigureAwait(false);
                    Check.False(pending.IsCompleted, "waiting on a throttled class");

                    Stopwatch sw = Stopwatch.StartNew();
                    cts.Cancel();
                    await Check.ThrowsAsync<OperationCanceledException>(() => pending, "cancellation surfaces").ConfigureAwait(false);
                    Check.True(sw.ElapsedMilliseconds < 500, "cancellation is prompt, not deferred to the re-check timer (" + sw.ElapsedMilliseconds + " ms)");
                    Check.Equal(0, q.PendingItemWaiterCount, "cancelled waiter deregistered");
                    Check.Equal(1, q.Count, "the throttled item is still resident");
                    q.Dispose();
                })));

            cases.Add(Case("Wakeup", "LlqThrottledWaitDisposal", "Disposing during a throttled wait ends it", ct =>
                Watchdog(async () =>
                {
                    ManualTimeProvider time = new ManualTimeProvider();
                    LowLatencyQoSQueue<DemoItem> q = PolicedLlq(time, new TokenBucket(ratePerSecond: 0.001, burst: 1));
                    q.Enqueue(new DemoItem(1) { Tier = "rt" });
                    q.Enqueue(new DemoItem(2) { Tier = "rt" });
                    q.Dequeue();

                    Task<DemoItem> pending = q.DequeueAsync(CancellationToken.None).AsTask();
                    await Task.Delay(50).ConfigureAwait(false);
                    Check.False(pending.IsCompleted, "waiting on a throttled class");

                    Stopwatch sw = Stopwatch.StartNew();
                    q.Dispose();
                    await Check.ThrowsAsync<OperationCanceledException>(() => pending, "disposal ends the wait").ConfigureAwait(false);
                    Check.True(sw.ElapsedMilliseconds < 500, "disposal is prompt (" + sw.ElapsedMilliseconds + " ms)");
                    Check.Equal(0, q.PendingItemWaiterCount, "no waiter remains after disposal");
                })));

            cases.Add(Case("Wakeup", "LlqThrottledWaitEnqueueWakes", "An enqueue still wakes a consumer waiting on a throttled class", ct =>
                Watchdog(async () =>
                {
                    ManualTimeProvider time = new ManualTimeProvider();
                    LowLatencyQoSQueue<DemoItem> q = PolicedLlq(time, new TokenBucket(ratePerSecond: 0.001, burst: 1));
                    q.Enqueue(new DemoItem(1) { Tier = "rt" });
                    q.Enqueue(new DemoItem(2) { Tier = "rt" });
                    q.Dequeue();

                    Task<DemoItem> pending = q.DequeueAsync(CancellationToken.None).AsTask();
                    await Task.Delay(50).ConfigureAwait(false);
                    Check.False(pending.IsCompleted, "waiting on a throttled class");

                    Stopwatch sw = Stopwatch.StartNew();
                    q.Enqueue(new DemoItem(50) { Tier = "bulk" });
                    DemoItem got = await pending.ConfigureAwait(false);
                    Check.Equal(50, got.Id, "the fair item is served while rt is policed");
                    Check.True(sw.ElapsedMilliseconds < 500, "woken by the enqueue, not the re-check timer (" + sw.ElapsedMilliseconds + " ms)");
                    Check.Equal(0, q.PendingItemWaiterCount, "no waiter remains registered");
                    q.Dispose();
                })));

            cases.Add(Case("Wakeup", "LlqThrottledWaitDoesNotSpin", "A throttled wait re-checks on a timer, not in a hot loop", ct =>
                Watchdog(async () =>
                {
                    string name = "nospin-" + Guid.NewGuid().ToString("N");
                    ManualTimeProvider time = new ManualTimeProvider();
                    QoSQueueOptions opts = new QoSQueueOptions { Name = name, TimeProvider = time };

                    // A huge rate makes the computed delay round to the 1 ms floor while the frozen
                    // clock keeps the item ineligible forever: the worst case for spinning.
                    LowLatencyQoSQueue<DemoItem> q = new LowLatencyQoSQueue<DemoItem>(
                        new[] { new TrafficClass<DemoItem>("rt", i => i.Tier == "rt", 1, new TokenBucket(ratePerSecond: 1000000, burst: 1)) },
                        new[] { new TrafficClass<DemoItem>("bulk", i => true) },
                        opts);
                    q.Enqueue(new DemoItem(1) { Tier = "rt" });
                    q.Enqueue(new DemoItem(2) { Tier = "rt" });
                    q.Dequeue();

                    List<TelemetryMeasurement> m = TelemetryCapture.CaptureMetrics(name, () =>
                    {
                        using CancellationTokenSource cts = new CancellationTokenSource();
                        Task<DemoItem> pending = q.DequeueAsync(cts.Token).AsTask();
                        Thread.Sleep(300);
                        cts.Cancel();
                        try
                        {
                            pending.GetAwaiter().GetResult();
                        }
                        catch (OperationCanceledException)
                        {
                        }
                    });

                    double passes = 0;
                    foreach (TelemetryMeasurement measurement in m)
                    {
                        if (measurement.Name == "qoskit.policer.exceeded")
                            passes += measurement.Value;
                    }

                    Check.True(passes >= 1, "the consumer did re-check the throttled class");
                    Check.True(passes < 2000, "no hot loop: " + passes + " scheduling passes in 300 ms");
                    Check.Equal(0, q.PendingItemWaiterCount, "no waiter remains registered");
                    q.Dispose();
                    await Task.CompletedTask.ConfigureAwait(false);
                })));

            cases.Add(Case("Wakeup", "BlockedProducerWokenByClear", "Clear wakes a producer blocked on a full queue", ct =>
                Watchdog(async () =>
                {
                    FifoQoSQueue<int> q = new FifoQoSQueue<int>(new QoSQueueOptions { MaxDepth = 1, OverflowPolicy = OverflowPolicy.Block });
                    q.Enqueue(1);
                    Task pending = q.EnqueueAsync(2, CancellationToken.None).AsTask();
                    await Task.Delay(50).ConfigureAwait(false);
                    Check.False(pending.IsCompleted, "producer blocked while full");

                    q.Clear(); // frees capacity without any dequeue
                    await pending.ConfigureAwait(false);
                    Check.Equal(1, q.Count, "blocked item admitted after clear");
                    Check.Equal(2, q.Dequeue(), "the blocked item is the one resident");
                    Check.Equal(0, q.PendingSpaceWaiterCount, "no space waiter remains registered");
                    q.Dispose();
                })));

            cases.Add(Case("Wakeup", "BlockedEnqueueUnknownClassThrows", "EnqueueAsync under Block rejects an unknown class instead of waiting forever", ct =>
                Watchdog(async () =>
                {
                    WeightedFairQoSQueue<DemoItem> q = new WeightedFairQoSQueue<DemoItem>(
                        i => i.Flow,
                        new[] { new WeightedFlow("known", 1) },
                        new QoSQueueOptions { MaxDepth = 4, OverflowPolicy = OverflowPolicy.Block },
                        unknownKeyPolicy: UnknownKeyPolicy.Reject);

                    await Check.ThrowsAsync<UnknownClassificationException>(
                        async () => await q.EnqueueAsync(new DemoItem(1) { Flow = "stranger" }, CancellationToken.None).ConfigureAwait(false),
                        "an unknown class is rejected, not treated as a full queue").ConfigureAwait(false);
                    Check.Equal(0, q.PendingSpaceWaiterCount, "no space waiter was left behind");

                    await q.EnqueueAsync(new DemoItem(2) { Flow = "known" }, CancellationToken.None).ConfigureAwait(false);
                    Check.Equal(1, q.Count, "a known class is still admitted");
                    q.Dispose();
                })));

            cases.Add(Case("Wakeup", "CancelledWakeupIsForwarded", "A wakeup absorbed by a cancelled consumer passes to the next consumer", ct =>
                Watchdog(async () =>
                {
                    for (int trial = 0; trial < 300; trial++)
                    {
                        FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                        using CancellationTokenSource cancelA = new CancellationTokenSource();
                        using CancellationTokenSource cancelB = new CancellationTokenSource();
                        Task<int> a = q.DequeueAsync(cancelA.Token).AsTask();
                        Task<int> b = q.DequeueAsync(cancelB.Token).AsTask();
                        await WaitUntil(() => q.PendingItemWaiterCount == 2).ConfigureAwait(false);

                        // Race the first waiter's cancellation against the enqueue that signals it.
                        int value = trial;
                        Task cancel = Task.Run(() => cancelA.Cancel());
                        Task produce = Task.Run(() => q.Enqueue(value));
                        await Task.WhenAll(cancel, produce).ConfigureAwait(false);

                        bool aGot = false;
                        try
                        {
                            Check.Equal(value, await a.ConfigureAwait(false), "A received the item");
                            aGot = true;
                        }
                        catch (OperationCanceledException)
                        {
                        }

                        if (aGot)
                        {
                            cancelB.Cancel();
                            await Check.ThrowsAsync<OperationCanceledException>(() => b, "B cancelled cleanly").ConfigureAwait(false);
                        }
                        else
                        {
                            Task finished = await Task.WhenAny(b, Task.Delay(2000)).ConfigureAwait(false);
                            Check.True(finished == b, "trial " + trial + ": the item was stranded while B slept");
                            Check.Equal(value, await b.ConfigureAwait(false), "B received the item");
                        }

                        Check.Equal(0, q.PendingItemWaiterCount, "trial " + trial + ": no waiter remains registered");
                        Check.True(q.IsEmpty, "trial " + trial + ": the item was consumed exactly once");
                        q.Dispose();
                    }
                })));

            return new TestSuiteDescriptor("Wakeup", "Wakeup and Eligibility", cases);
        }

        // An LLQ with one policed priority class ("rt") and one fair class ("bulk").
        private static LowLatencyQoSQueue<DemoItem> PolicedLlq(IQoSTimeProvider time, TokenBucket bucket)
        {
            return new LowLatencyQoSQueue<DemoItem>(
                new[] { new TrafficClass<DemoItem>("rt", i => i.Tier == "rt", 1, bucket) },
                new[] { new TrafficClass<DemoItem>("bulk", i => true) },
                new QoSQueueOptions { TimeProvider = time });
        }

        private static async Task WaitUntil(Func<bool> condition)
        {
            Stopwatch sw = Stopwatch.StartNew();
            while (!condition())
            {
                if (sw.ElapsedMilliseconds > 5000)
                    throw new InvalidOperationException("Condition not reached within 5 s.");
                await Task.Delay(1).ConfigureAwait(false);
            }
        }

        private static async Task Watchdog(Func<Task> body)
        {
            Task work = body();
            Task finished = await Task.WhenAny(work, Task.Delay(WatchdogMs)).ConfigureAwait(false);
            if (finished != work)
                throw new InvalidOperationException("Watchdog timeout — a waiter was never woken.");
            await work.ConfigureAwait(false);
        }

        private static TestCaseDescriptor Case(string suite, string id, string name, Func<CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(suiteId: suite, caseId: id, displayName: name, executeAsync: body);
        }
    }
}
