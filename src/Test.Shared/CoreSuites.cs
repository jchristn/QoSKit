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
    /// Base-contract, FIFO, and LIFO suites (a subset of plan Suites A, B, C).
    /// </summary>
    public static class CoreSuites
    {
        /// <summary>The base queue contract, exercised against FIFO.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor ContractSuite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Contract", "NewIsEmpty", "New queue is empty", _ =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                Check.Equal(0, q.Count, "count");
                Check.True(q.IsEmpty, "empty");
                Check.False(q.TryPeek(out int _), "peek empty");
                Check.False(q.TryDequeue(out int _), "dequeue empty");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "EnqueuePeek", "Enqueue then peek does not remove", _ =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                q.Enqueue(7);
                Check.Equal(1, q.Count, "count");
                Check.True(q.TryPeek(out int v), "peek");
                Check.Equal(7, v, "peeked value");
                Check.Equal(1, q.Count, "count unchanged after peek");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "DequeueEmptyThrows", "Dequeue on empty throws QueueEmptyException", _ =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                Check.Throws<QueueEmptyException>(() => q.Dequeue(), "dequeue empty throws");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "EnqueueDequeueN", "Enqueue N, dequeue N, conserve", _ =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                for (int i = 0; i < 100; i++)
                    q.Enqueue(i);
                int seen = 0;
                while (q.TryDequeue(out int _))
                    seen++;
                Check.Equal(100, seen, "dequeued count");
                Check.True(q.IsEmpty, "empty at end");
                QoSQueueStatistics stats = q.Statistics;
                Check.Equal(100L, stats.Enqueued, "stat enqueued");
                Check.Equal(100L, stats.Dequeued, "stat dequeued");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "PeakDepth", "Peak depth records high-water mark", _ =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                for (int i = 0; i < 10; i++)
                    q.Enqueue(i);
                for (int i = 0; i < 10; i++)
                    q.TryDequeue(out int _);
                Check.Equal(10, q.Statistics.PeakDepth, "peak");
                Check.Equal(0, q.Statistics.CurrentDepth, "current");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "ClearEmpties", "Clear empties the queue", _ =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                for (int i = 0; i < 5; i++)
                    q.Enqueue(i);
                q.Clear();
                Check.True(q.IsEmpty, "empty after clear");
                Check.Equal(0, q.Count, "count 0");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "NullItemThrows", "Enqueue null reference throws ArgumentNullException", _ =>
            {
                FifoQoSQueue<string> q = new FifoQoSQueue<string>();
                Check.Throws<ArgumentNullException>(() => q.Enqueue(null!), "null enqueue");
                Check.Equal(0, q.Count, "unchanged");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "RejectPolicy", "Reject at capacity throws/returns false", _ =>
            {
                QoSQueueOptions opts = new QoSQueueOptions { MaxDepth = 2, OverflowPolicy = OverflowPolicy.Reject };
                FifoQoSQueue<int> q = new FifoQoSQueue<int>(opts);
                Check.True(q.TryEnqueue(1), "1");
                Check.True(q.TryEnqueue(2), "2");
                Check.False(q.TryEnqueue(3), "3 rejected");
                Check.Throws<QueueFullException>(() => q.Enqueue(4), "enqueue throws when full");
                Check.Equal(2, q.Count, "depth held");
                Check.Equal(2L, q.Statistics.Rejected, "rejected count");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "DropNewest", "DropNewest discards incoming at capacity", _ =>
            {
                QoSQueueOptions opts = new QoSQueueOptions { MaxDepth = 2, OverflowPolicy = OverflowPolicy.DropNewest };
                FifoQoSQueue<int> q = new FifoQoSQueue<int>(opts);
                q.Enqueue(1);
                q.Enqueue(2);
                DropReason? reason = null;
                q.ItemDropped += (s, e) => reason = e.Reason;
                Check.False(q.TryEnqueue(3), "3 dropped");
                Check.Equal(DropReason.Newest, reason!.Value, "reason newest");
                Check.True(q.TryDequeue(out int a) && a == 1, "head still 1");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "DropOldest", "DropOldest evicts oldest and admits new", _ =>
            {
                QoSQueueOptions opts = new QoSQueueOptions { MaxDepth = 2, OverflowPolicy = OverflowPolicy.DropOldest };
                FifoQoSQueue<int> q = new FifoQoSQueue<int>(opts);
                q.Enqueue(1);
                q.Enqueue(2);
                int dropped = -1;
                q.ItemDropped += (s, e) => dropped = e.Item;
                Check.True(q.TryEnqueue(3), "3 admitted");
                Check.Equal(1, dropped, "1 evicted");
                Check.Equal(2, q.Count, "depth held");
                Check.True(q.TryDequeue(out int a) && a == 2, "now head is 2");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "DequeueAsyncCompletes", "DequeueAsync completes when item arrives", async ct =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                Task<int> pending = q.DequeueAsync(ct).AsTask();
                Check.False(pending.IsCompleted, "not completed yet");
                q.Enqueue(42);
                int v = await pending.ConfigureAwait(false);
                Check.Equal(42, v, "got item");
            }));

            cases.Add(Case("Contract", "DequeueAsyncCancelled", "DequeueAsync throws on cancellation", async ct =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                using CancellationTokenSource cts = new CancellationTokenSource();
                Task<int> pending = q.DequeueAsync(cts.Token).AsTask();
                cts.Cancel();
                await Check.ThrowsAsync<OperationCanceledException>(async () => await pending.ConfigureAwait(false), "cancelled").ConfigureAwait(false);
                Check.True(q.IsEmpty, "still empty");
                await Task.CompletedTask.ConfigureAwait(false);
            }));

            cases.Add(Case("Contract", "TwoWaitersTwoItems", "Two async waiters each get one item", async ct =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                Task<int> w1 = q.DequeueAsync(ct).AsTask();
                Task<int> w2 = q.DequeueAsync(ct).AsTask();
                q.Enqueue(1);
                q.Enqueue(2);
                int[] got = await Task.WhenAll(w1, w2).ConfigureAwait(false);
                Check.True((got[0] == 1 && got[1] == 2) || (got[0] == 2 && got[1] == 1), "both delivered distinctly");
                Check.True(q.IsEmpty, "empty");
            }));

            cases.Add(Case("Contract", "BlockingCollectionInterop", "Works as a BlockingCollection backing store", async ct =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                using System.Collections.Concurrent.BlockingCollection<int> bc = new System.Collections.Concurrent.BlockingCollection<int>(q);
                bc.Add(5);
                int taken = bc.Take();
                Check.Equal(5, taken, "take");
                await Task.CompletedTask.ConfigureAwait(false);
            }));

            cases.Add(Case("Contract", "DisposedThrows", "Operations after dispose throw ObjectDisposedException", _ =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                q.Dispose();
                q.Dispose(); // idempotent
                Check.Throws<ObjectDisposedException>(() => q.Enqueue(1), "enqueue after dispose");
                Check.Throws<ObjectDisposedException>(() => q.TryDequeue(out int _), "dequeue after dispose");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "DisposeCompletesWaiters", "Dispose completes pending waiters", async ct =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                Task<int> pending = q.DequeueAsync(ct).AsTask();
                q.Dispose();
                await Check.ThrowsAsync<OperationCanceledException>(async () => await pending.ConfigureAwait(false), "waiter cancelled on dispose").ConfigureAwait(false);
            }));

            cases.Add(Case("Contract", "ClassifierThrowsLeavesStateIntact", "A throwing selector aborts enqueue cleanly", _ =>
            {
                PriorityQoSQueue<int> q = new PriorityQoSQueue<int>(3, x => throw new InvalidOperationException("boom"));
                Check.Throws<InvalidOperationException>(() => q.Enqueue(1), "selector throw propagates");
                Check.Equal(0, q.Count, "state intact");
                return Task.CompletedTask;
            }));

            cases.Add(Case("Contract", "ConcurrentProducersConsumers", "Concurrent producers/consumers conserve", async ct =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                int perProducer = 5000;
                int producers = 4;
                ConcurrentBag<int> consumed = new ConcurrentBag<int>();
                Task[] prod = new Task[producers];
                for (int p = 0; p < producers; p++)
                {
                    int baseId = p * perProducer;
                    prod[p] = Task.Run(() =>
                    {
                        for (int i = 0; i < perProducer; i++)
                            q.Enqueue(baseId + i);
                    });
                }

                int total = producers * perProducer;
                Task[] cons = new Task[4];
                int remaining = total;
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
                HashSet<int> unique = new HashSet<int>(consumed);
                Check.Equal(total, unique.Count, "no duplicates");
                Check.True(q.IsEmpty, "queue drained");
            }));

            return new TestSuiteDescriptor("Contract", "Base Queue Contract", cases);
        }

        /// <summary>FIFO ordering suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor FifoSuite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Fifo", "InsertionOrder", "Dequeues in insertion order", _ =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                for (int i = 0; i < 50; i++)
                    q.Enqueue(i);
                for (int i = 0; i < 50; i++)
                {
                    q.TryDequeue(out int v);
                    Check.Equal(i, v, "order at " + i);
                }

                return Task.CompletedTask;
            }));

            cases.Add(Case("Fifo", "SnapshotFrontToBack", "Enumeration is front-to-back", _ =>
            {
                FifoQoSQueue<int> q = new FifoQoSQueue<int>();
                q.Enqueue(1);
                q.Enqueue(2);
                q.Enqueue(3);
                List<int> snap = new List<int>(q);
                Check.Equal(3, snap.Count, "snapshot size");
                Check.Equal(1, snap[0], "first");
                Check.Equal(3, snap[2], "last");
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Fifo", "FIFO Ordering", cases);
        }

        /// <summary>LIFO ordering suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor LifoSuite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Lifo", "ReverseOrder", "Dequeues in reverse order", _ =>
            {
                LifoQoSQueue<int> q = new LifoQoSQueue<int>();
                for (int i = 0; i < 10; i++)
                    q.Enqueue(i);
                for (int i = 9; i >= 0; i--)
                {
                    q.TryDequeue(out int v);
                    Check.Equal(i, v, "reverse order at " + i);
                }

                return Task.CompletedTask;
            }));

            cases.Add(Case("Lifo", "Interleaved", "Interleaved stack semantics", _ =>
            {
                LifoQoSQueue<int> q = new LifoQoSQueue<int>();
                q.Enqueue(1);
                q.Enqueue(2);
                q.Enqueue(3);
                q.TryDequeue(out int a);
                Check.Equal(3, a, "pop 3");
                q.Enqueue(4);
                q.TryDequeue(out int b);
                Check.Equal(4, b, "pop 4");
                q.TryDequeue(out int c);
                Check.Equal(2, c, "pop 2");
                return Task.CompletedTask;
            }));

            return new TestSuiteDescriptor("Lifo", "LIFO Ordering", cases);
        }

        private static TestCaseDescriptor Case(string suite, string id, string name, Func<CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(
                suiteId: suite,
                caseId: id,
                displayName: name,
                executeAsync: body);
        }
    }
}
