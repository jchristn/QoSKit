using System;
using System.Threading;
using QoSKit;
using QoSKit.Persistence.Sqlite;

// Durably enqueues integers to a SQLite-backed queue, printing a line after each committed item so a
// parent process can hard-kill this process mid-flight and then verify that every item this process
// reported as committed survived. Args: <dbPath>.
if (args.Length < 1)
{
    Console.Error.WriteLine("usage: Test.CrashHarness <dbPath>");
    return 2;
}

string dbPath = args[0];

FifoQoSQueue<int> queue = new FifoQoSQueue<int>();
queue.EnablePersistence(new SqliteQoSStore<int>(QoSPersistenceOptions<int>.GuaranteedDelivery(dbPath)));

int i = 0;
while (true)
{
    queue.Enqueue(i);
    // Durable mode committed the row before Enqueue returned, so this count is on disk.
    Console.WriteLine("COMMITTED " + (i + 1).ToString());
    Console.Out.Flush();
    i++;

    // Yield briefly so the parent can observe progress and choose a kill point.
    if ((i % 25) == 0)
        Thread.Sleep(1);
}
