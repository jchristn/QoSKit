namespace Test.Shared
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;
    using QoSKit.Persistence.Sqlite;
    using Touchstone.Core;

    /// <summary>
    /// SQLite persistence and recovery suite (plan Suite T). Uses a temp-file database per case.
    /// </summary>
    public static class PersistenceSuites
    {
        /// <summary>The persistence suite.</summary>
        /// <returns>The suite descriptor.</returns>
        public static TestSuiteDescriptor Suite()
        {
            List<TestCaseDescriptor> cases = new List<TestCaseDescriptor>();

            cases.Add(Case("Persist", "RoundTripOrder", "FIFO backlog recovers in order after restart", _ =>
            {
                string path = NewPath();
                try
                {
                    FifoQoSQueue<DemoItem> q1 = new FifoQoSQueue<DemoItem>();
                    q1.EnablePersistence(new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.Fast(path)));
                    for (int i = 0; i < 200; i++)
                        q1.Enqueue(new DemoItem(i));
                    q1.Dispose();

                    FifoQoSQueue<DemoItem> q2 = new FifoQoSQueue<DemoItem>();
                    q2.EnablePersistence(new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.Fast(path)));
                    Check.Equal(200, q2.Count, "recovered count");
                    for (int i = 0; i < 200; i++)
                        Check.Equal(i, q2.Dequeue().Id, "order at " + i);
                    q2.Dispose();
                }
                finally
                {
                    Cleanup(path);
                }

                return Task.CompletedTask;
            }));

            cases.Add(Case("Persist", "PriorityOrderSurvives", "Priority order survives restart", _ =>
            {
                string path = NewPath();
                try
                {
                    PriorityQoSQueue<DemoItem> q1 = new PriorityQoSQueue<DemoItem>(3, d => d.Priority);
                    q1.EnablePersistence(new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.Fast(path)));
                    q1.Enqueue(new DemoItem(1) { Priority = 2 });
                    q1.Enqueue(new DemoItem(2) { Priority = 0 });
                    q1.Enqueue(new DemoItem(3) { Priority = 1 });
                    q1.Dispose();

                    PriorityQoSQueue<DemoItem> q2 = new PriorityQoSQueue<DemoItem>(3, d => d.Priority);
                    q2.EnablePersistence(new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.Fast(path)));
                    Check.Equal(2, q2.Dequeue().Id, "band 0 first after recovery");
                    Check.Equal(3, q2.Dequeue().Id, "band 1 next");
                    Check.Equal(1, q2.Dequeue().Id, "band 2 last");
                    q2.Dispose();
                }
                finally
                {
                    Cleanup(path);
                }

                return Task.CompletedTask;
            }));

            cases.Add(Case("Persist", "DurableCommitsImmediately", "Durable items are visible without a flush", _ =>
            {
                string path = NewPath();
                try
                {
                    FifoQoSQueue<DemoItem> q1 = new FifoQoSQueue<DemoItem>();
                    q1.EnablePersistence(new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.GuaranteedDelivery(path)));
                    for (int i = 0; i < 100; i++)
                        q1.Enqueue(new DemoItem(i));

                    // A separate reader sees every committed item, without disposing q1.
                    using SqliteQoSStore<DemoItem> reader = new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.GuaranteedDelivery(path));
                    Check.Equal(100, reader.Recover().Count, "all durable items committed");
                    q1.Dispose();
                }
                finally
                {
                    Cleanup(path);
                }

                return Task.CompletedTask;
            }));

            cases.Add(Case("Persist", "WriteBehindLossWindow", "Write-behind items are not durable until flush", _ =>
            {
                string path = NewPath();
                try
                {
                    FifoQoSQueue<DemoItem> q1 = new FifoQoSQueue<DemoItem>();
                    // Large batch size so nothing auto-commits during the test.
                    QoSPersistenceOptions<DemoItem> opts = QoSPersistenceOptions<DemoItem>.Fast(path);
                    opts.BatchSize = 100000;
                    q1.EnablePersistence(new SqliteQoSStore<DemoItem>(opts));
                    for (int i = 0; i < 50; i++)
                        q1.Enqueue(new DemoItem(i));

                    using (SqliteQoSStore<DemoItem> reader = new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.Fast(path)))
                        Check.Equal(0, reader.Recover().Count, "uncommitted items not yet durable");

                    q1.Dispose(); // flushes

                    using (SqliteQoSStore<DemoItem> reader = new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.Fast(path)))
                        Check.Equal(50, reader.Recover().Count, "flushed on dispose");
                }
                finally
                {
                    Cleanup(path);
                }

                return Task.CompletedTask;
            }));

            cases.Add(Case("Persist", "DequeueRemovesRow", "Dequeue removes the persisted row", _ =>
            {
                string path = NewPath();
                try
                {
                    FifoQoSQueue<DemoItem> q1 = new FifoQoSQueue<DemoItem>();
                    q1.EnablePersistence(new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.GuaranteedDelivery(path)));
                    for (int i = 0; i < 10; i++)
                        q1.Enqueue(new DemoItem(i));
                    for (int i = 0; i < 4; i++)
                        q1.Dequeue();
                    q1.Dispose();

                    FifoQoSQueue<DemoItem> q2 = new FifoQoSQueue<DemoItem>();
                    q2.EnablePersistence(new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.Fast(path)));
                    Check.Equal(6, q2.Count, "dequeued rows removed");
                    Check.Equal(4, q2.Dequeue().Id, "first survivor is id 4");
                    q2.Dispose();
                }
                finally
                {
                    Cleanup(path);
                }

                return Task.CompletedTask;
            }));

            cases.Add(Case("Persist", "SerializerErrorNotAdmitted", "A serializer failure aborts enqueue", _ =>
            {
                string path = NewPath();
                try
                {
                    FifoQoSQueue<DemoItem> q1 = new FifoQoSQueue<DemoItem>();
                    q1.EnablePersistence(new SqliteQoSStore<DemoItem>(new QoSPersistenceOptions<DemoItem>(path, new ThrowingSerializer())));
                    Check.Throws<InvalidOperationException>(() => q1.Enqueue(new DemoItem(1) { Id = 13 }), "poison item throws");
                    q1.Dispose();

                    FifoQoSQueue<DemoItem> q2 = new FifoQoSQueue<DemoItem>();
                    q2.EnablePersistence(new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.Fast(path)));
                    Check.Equal(0, q2.Count, "poison item not persisted");
                    q2.Dispose();
                }
                finally
                {
                    Cleanup(path);
                }

                return Task.CompletedTask;
            }));

            cases.Add(Case("Persist", "ConservationAcrossRestart", "Concurrent load conserves across restart", async ct =>
            {
                string path = NewPath();
                try
                {
                    FifoQoSQueue<DemoItem> q1 = new FifoQoSQueue<DemoItem>();
                    q1.EnablePersistence(new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.Fast(path)));

                    ConcurrentBag<int> dequeued = new ConcurrentBag<int>();
                    Task[] producers = new Task[4];
                    for (int p = 0; p < 4; p++)
                    {
                        int b = p * 1000;
                        producers[p] = Task.Run(() =>
                        {
                            for (int i = 0; i < 1000; i++)
                                q1.Enqueue(new DemoItem(b + i));
                        });
                    }

                    await Task.WhenAll(producers).ConfigureAwait(false);

                    // Drain half concurrently, then restart with the rest still resident.
                    Task[] consumers = new Task[2];
                    int drain = 2000;
                    for (int c = 0; c < 2; c++)
                    {
                        consumers[c] = Task.Run(() =>
                        {
                            while (Interlocked.Decrement(ref drain) >= 0)
                            {
                                if (q1.TryDequeue(out DemoItem item))
                                    dequeued.Add(item.Id);
                                else
                                    Interlocked.Increment(ref drain);
                            }
                        });
                    }

                    await Task.WhenAll(consumers).ConfigureAwait(false);
                    int residentBefore = q1.Count;
                    q1.Dispose();

                    FifoQoSQueue<DemoItem> q2 = new FifoQoSQueue<DemoItem>();
                    q2.EnablePersistence(new SqliteQoSStore<DemoItem>(QoSPersistenceOptions<DemoItem>.Fast(path)));
                    int recovered = q2.Count;
                    Check.Equal(residentBefore, recovered, "recovered equals what was resident");
                    Check.Equal(4000, dequeued.Count + recovered, "dequeued + recovered == produced");
                    q2.Dispose();
                }
                finally
                {
                    Cleanup(path);
                }
            }));

            return new TestSuiteDescriptor("Persist", "SQLite Persistence", cases);
        }

        private static string NewPath()
        {
            return Path.Combine(Path.GetTempPath(), "qoskit-test-" + Guid.NewGuid().ToString("N") + ".db");
        }

        private static void Cleanup(string path)
        {
            TryDelete(path);
            TryDelete(path + "-wal");
            TryDelete(path + "-shm");
        }

        private static void TryDelete(string path)
        {
            try
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
            catch (IOException)
            {
            }
        }

        private static TestCaseDescriptor Case(string suite, string id, string name, Func<CancellationToken, Task> body)
        {
            return new TestCaseDescriptor(suiteId: suite, caseId: id, displayName: name, executeAsync: body);
        }
    }
}
