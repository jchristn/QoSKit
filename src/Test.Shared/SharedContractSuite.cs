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
    /// The base queue contract (plan Suite A) run against every discipline via a factory list, so the
    /// shared behavior — peek, count, overflow, async, disposal, interop, and concurrency — is verified
    /// for all seven, not just FIFO.
    /// </summary>
    public static class SharedContractSuite
    {
        private static readonly List<QueueFactory> _Factories = BuildFactories();

        /// <summary>The shared-contract suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor Suite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();
            foreach (QueueFactory factory in _Factories)
            {
                cases.Add(Case(factory, "Empty", "new queue is empty", (q, ct) =>
                {
                    Check.True(q.IsEmpty, "empty");
                    Check.Equal(0, q.Count, "count 0");
                    Check.False(q.TryDequeue(out int _), "dequeue empty false");
                    Check.False(q.TryPeek(out int _), "peek empty false");
                    Check.Throws<QueueEmptyException>(() => q.Dequeue(), "dequeue throws when empty");
                    return Task.CompletedTask;
                }));

                cases.Add(Case(factory, "RoundTripSet", "enqueue N, dequeue N (as a set), conserve", (q, ct) =>
                {
                    HashSet<int> input = new HashSet<int>();
                    for (int i = 0; i < 300; i++)
                    {
                        q.Enqueue(i);
                        input.Add(i);
                    }

                    Check.Equal(300, q.Count, "count after enqueue");
                    HashSet<int> output = new HashSet<int>();
                    while (q.TryDequeue(out int v))
                        output.Add(v);
                    Check.True(input.SetEquals(output), "every item comes out exactly once");
                    Check.True(q.IsEmpty, "empty at end");
                    Check.Equal(300L, q.Statistics.Dequeued, "stat dequeued");
                    return Task.CompletedTask;
                }));

                cases.Add(Case(factory, "PeekNoRemove", "peek does not remove", (q, ct) =>
                {
                    q.Enqueue(1);
                    q.Enqueue(2);
                    Check.True(q.TryPeek(out int _), "peek");
                    Check.Equal(2, q.Count, "count unchanged after peek");
                    return Task.CompletedTask;
                }));

                cases.Add(Case(factory, "Clear", "clear empties", (q, ct) =>
                {
                    for (int i = 0; i < 20; i++)
                        q.Enqueue(i);
                    q.Clear();
                    Check.True(q.IsEmpty, "empty after clear");
                    return Task.CompletedTask;
                }));

                cases.Add(CaseBounded(factory, "RejectAtCapacity", "reject at capacity", (q, ct) =>
                {
                    for (int i = 0; i < 5; i++)
                        Check.True(q.TryEnqueue(i), "admit " + i);
                    Check.False(q.TryEnqueue(99), "reject at capacity");
                    Check.Throws<QueueFullException>(() => q.Enqueue(100), "enqueue throws when full");
                    Check.Equal(5, q.Count, "depth held at 5");
                    return Task.CompletedTask;
                }));

                cases.Add(Case(factory, "DequeueAsync", "async dequeue completes on enqueue", async (q, ct) =>
                {
                    Task<int> pending = q.DequeueAsync(ct).AsTask();
                    q.Enqueue(77);
                    int v = await pending.ConfigureAwait(false);
                    Check.Equal(77, v, "async got the item");
                }));

                cases.Add(Case(factory, "Disposed", "operations after dispose throw", (q, ct) =>
                {
                    q.Dispose();
                    Check.Throws<ObjectDisposedException>(() => q.Enqueue(1), "enqueue after dispose");
                    Check.Throws<ObjectDisposedException>(() => q.TryDequeue(out int _), "dequeue after dispose");
                    return Task.CompletedTask;
                }));

                cases.Add(Case(factory, "BlockingCollection", "backs a BlockingCollection", (q, ct) =>
                {
                    using System.Collections.Concurrent.BlockingCollection<int> bc = new System.Collections.Concurrent.BlockingCollection<int>(q);
                    bc.Add(5);
                    Check.Equal(5, bc.Take(), "take via BCL");
                    return Task.CompletedTask;
                }));

                cases.Add(Case(factory, "Concurrency", "concurrent producers/consumers conserve", async (q, ct) =>
                {
                    int producers = 4;
                    int per = 2500;
                    int total = producers * per;
                    ConcurrentBag<int> consumed = new ConcurrentBag<int>();
                    Task[] prod = new Task[producers];
                    for (int p = 0; p < producers; p++)
                    {
                        int b = p * per;
                        prod[p] = Task.Run(() =>
                        {
                            for (int i = 0; i < per; i++)
                                q.Enqueue(b + i);
                        });
                    }

                    int remaining = total;
                    Task[] cons = new Task[4];
                    for (int c = 0; c < cons.Length; c++)
                    {
                        cons[c] = Task.Run(() =>
                        {
                            while (Volatile.Read(ref remaining) > 0)
                            {
                                if (q.TryDequeue(out int v))
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
                    Check.True(q.IsEmpty, "drained");
                }));
            }

            return new TestSuiteDescriptor("Contract-All", "Shared Queue Contract (all disciplines)", cases);
        }

        private static TestCaseDescriptor Case(QueueFactory factory, string id, string name, Func<IQoSQueue<int>, CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(
                suiteId: "Contract-All",
                caseId: factory.Name + "." + id,
                displayName: $"{factory.Name}: {name}",
                executeAsync: async ct =>
                {
                    IQoSQueue<int> queue = factory.Create(null);
                    await body(queue, ct).ConfigureAwait(false);
                });
        }

        private static TestCaseDescriptor CaseBounded(QueueFactory factory, string id, string name, Func<IQoSQueue<int>, CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(
                suiteId: "Contract-All",
                caseId: factory.Name + "." + id,
                displayName: $"{factory.Name}: {name}",
                executeAsync: async ct =>
                {
                    QoSQueueOptions options = new QoSQueueOptions { MaxDepth = 5, OverflowPolicy = OverflowPolicy.Reject };
                    IQoSQueue<int> queue = factory.Create(options);
                    await body(queue, ct).ConfigureAwait(false);
                });
        }

        private static List<QueueFactory> BuildFactories()
        {
            return new List<QueueFactory>
            {
                new QueueFactory("FIFO", o => new FifoQoSQueue<int>(o)),
                new QueueFactory("LIFO", o => new LifoQoSQueue<int>(o)),
                new QueueFactory("Priority", o => new PriorityQoSQueue<int>(4, i => i % 4, o)),
                new QueueFactory("WFQ", o => new WeightedFairQoSQueue<int>(
                    i => (i % 3).ToString(),
                    new[] { new WeightedFlow("0", 1), new WeightedFlow("1", 2), new WeightedFlow("2", 3) },
                    o)),
                new QueueFactory("CBWFQ", o => new ClassBasedWeightedFairQoSQueue<int>(
                    new[] { new TrafficClass<int>("even", i => i % 2 == 0, 2) },
                    o)),
                new QueueFactory("LLQ", o => new LowLatencyQoSQueue<int>(
                    new[] { new TrafficClass<int>("rt", i => i % 5 == 0) },
                    new[] { new TrafficClass<int>("bulk", i => true) },
                    o)),
                new QueueFactory("WRR", o => new WeightedRoundRobinQoSQueue<int>(
                    new[] { new WeightedSubQueue("a", 2), new WeightedSubQueue("b", 1) },
                    o))
            };
        }
    }
}
