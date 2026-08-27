namespace Test.Shared
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using Touchstone.Core;

    /// <summary>
    /// Concurrency and liveness suites (subset of plan Suites R and S): stress conservation, fault
    /// injection with post-fault liveness, cancellation storms, and waiter-registry integrity. Each
    /// case runs under a watchdog so a deadlock fails fast rather than hanging.
    /// </summary>
    public static class LivenessSuites
    {
        private const int WatchdogMs = 30000;

        /// <summary>The liveness suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor Suite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Liveness", "ConcurrencyFifo", "8x8 FIFO conserves under load", ct =>
                Watchdog(() => Conserve(() => new FifoQoSQueue<int>()))));

            cases.Add(Case("Liveness", "ConcurrencyPriority", "8x8 priority conserves under load", ct =>
                Watchdog(() => Conserve(() => new PriorityQoSQueue<int>(4, i => i % 4)))));

            cases.Add(Case("Liveness", "ConcurrencyWfq", "8x8 WFQ conserves under load", ct =>
                Watchdog(() => Conserve(() => new WeightedFairQoSQueue<int>(
                    i => (i % 3).ToString(),
                    new[] { new WeightedFlow("0", 1), new WeightedFlow("1", 2), new WeightedFlow("2", 3) })))));

            cases.Add(Case("Liveness", "ThrowingClassifier", "A throwing classifier under load keeps servicing", ct =>
                Watchdog(async () =>
                {
                    PriorityQoSQueue<int> queue = new PriorityQoSQueue<int>(3, i =>
                    {
                        if (i % 50 == 0)
                            throw new InvalidOperationException("poison");
                        return i % 3;
                    });

                    int admitted = 0;
                    Task[] producers = new Task[4];
                    for (int p = 0; p < 4; p++)
                    {
                        int b = p * 5000;
                        producers[p] = Task.Run(() =>
                        {
                            for (int i = 0; i < 5000; i++)
                            {
                                try
                                {
                                    queue.Enqueue(b + i);
                                    Interlocked.Increment(ref admitted);
                                }
                                catch (InvalidOperationException)
                                {
                                }
                            }
                        });
                    }

                    await Task.WhenAll(producers).ConfigureAwait(false);
                    int drained = 0;
                    while (queue.TryDequeue(out int _))
                        drained++;
                    Check.Equal(admitted, drained, "every admitted item serviced despite poison items");

                    // Post-fault liveness: the queue still works.
                    queue.Enqueue(7);
                    Check.Equal(7, queue.Dequeue(), "queue services after the fault storm");
                })));

            cases.Add(Case("Liveness", "ThrowingEventHandler", "A throwing event handler does not wedge the queue", ct =>
                Watchdog(async () =>
                {
                    FifoQoSQueue<int> queue = new FifoQoSQueue<int>();
                    queue.ItemDequeued += (s, e) =>
                    {
                        if (e.Item % 100 == 0)
                            throw new InvalidOperationException("handler boom");
                    };

                    for (int i = 0; i < 1000; i++)
                        queue.Enqueue(i);

                    int drained = 0;
                    while (drained < 1000)
                    {
                        try
                        {
                            queue.Dequeue();
                            drained++;
                        }
                        catch (InvalidOperationException)
                        {
                            drained++; // the item was still dequeued; the handler threw afterward
                        }
                    }

                    Check.True(queue.IsEmpty, "all serviced despite throwing handler");
                    queue.Enqueue(1);
                    Check.Equal(1, queue.Dequeue(), "queue services after handler faults");
                    await Task.CompletedTask.ConfigureAwait(false);
                })));

            cases.Add(Case("Liveness", "CancellationStorm", "Cancellation storm leaves no lingering waiters", ct =>
                Watchdog(async () =>
                {
                    FifoQoSQueue<int> queue = new FifoQoSQueue<int>();
                    using CancellationTokenSource cts = new CancellationTokenSource();
                    Task[] waiters = new Task[200];
                    for (int i = 0; i < waiters.Length; i++)
                        waiters[i] = AwaitCancelled(queue, cts.Token);

                    cts.Cancel();
                    await Task.WhenAll(waiters).ConfigureAwait(false);

                    Check.Equal(0, queue.PendingItemWaiterCount, "no waiter nodes linger after cancellation");
                    queue.Enqueue(42);
                    Check.Equal(42, queue.Dequeue(), "queue drainable after the storm");
                })));

            cases.Add(Case("Liveness", "DisposeWithWaiters", "Dispose completes pending waiters under load", ct =>
                Watchdog(async () =>
                {
                    FifoQoSQueue<int> queue = new FifoQoSQueue<int>();
                    Task[] waiters = new Task[50];
                    for (int i = 0; i < waiters.Length; i++)
                        waiters[i] = AwaitCancelled(queue, CancellationToken.None);

                    queue.Dispose();
                    await Task.WhenAll(waiters).ConfigureAwait(false); // all resolve, none hang
                })));

            cases.Add(Case("Liveness", "AsyncNoWaiterDrift", "Async churn leaves the waiter registry empty", ct =>
                Watchdog(async () =>
                {
                    FifoQoSQueue<int> queue = new FifoQoSQueue<int>();
                    for (int i = 0; i < 2000; i++)
                    {
                        ValueTask<int> pending = queue.DequeueAsync(CancellationToken.None);
                        queue.Enqueue(i);
                        int got = await pending.ConfigureAwait(false);
                        Check.Equal(i, got, "round-trip " + i);
                    }

                    Check.Equal(0, queue.PendingItemWaiterCount, "no waiters remain after churn");
                })));

            return new TestSuiteDescriptor("Liveness", "Concurrency and Liveness", cases);
        }

        private static async Task Conserve(Func<IQoSQueue<int>> factory)
        {
            IQoSQueue<int> queue = factory();
            int perProducer = 20000;
            int producers = 8;
            int total = producers * perProducer;
            ConcurrentBag<int> consumed = new ConcurrentBag<int>();

            Task[] prod = new Task[producers];
            for (int p = 0; p < producers; p++)
            {
                int b = p * perProducer;
                prod[p] = Task.Run(() =>
                {
                    for (int i = 0; i < perProducer; i++)
                        queue.Enqueue(b + i);
                });
            }

            int remaining = total;
            Task[] cons = new Task[8];
            for (int c = 0; c < cons.Length; c++)
            {
                cons[c] = Task.Run(() =>
                {
                    while (Volatile.Read(ref remaining) > 0)
                    {
                        if (queue.TryDequeue(out int v))
                        {
                            consumed.Add(v);
                            Interlocked.Decrement(ref remaining);
                        }
                    }
                });
            }

            await Task.WhenAll(prod).ConfigureAwait(false);
            await Task.WhenAll(cons).ConfigureAwait(false);

            Check.Equal(total, consumed.Count, "all consumed");
            Check.Equal(total, new HashSet<int>(consumed).Count, "no duplicates");
            Check.True(queue.IsEmpty, "queue drained");
            queue.Dispose();
        }

        private static async Task AwaitCancelled(IQoSQueue<int> queue, CancellationToken token)
        {
            try
            {
                await queue.DequeueAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        private static async Task Watchdog(Func<Task> body)
        {
            Task work = body();
            Task finished = await Task.WhenAny(work, Task.Delay(WatchdogMs)).ConfigureAwait(false);
            if (finished != work)
                throw new InvalidOperationException("Watchdog timeout — possible deadlock.");
            await work.ConfigureAwait(false);
        }

        private static TestCaseDescriptor Case(string suite, string id, string name, Func<CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(suiteId: suite, caseId: id, displayName: name, executeAsync: body);
        }
    }
}
