# Getting Started

This walks from an empty project to a running two-level QoS pipeline.

## 1. Reference the package

```
dotnet add package QoSKit
```

## 2. A single queue

Start with the plain case. Every queue supports synchronous, non-blocking, and awaitable consumption.

```csharp
using QoSKit;

PriorityQoSQueue<string> queue = new PriorityQoSQueue<string>(
    levels: 3,
    prioritySelector: s => s.StartsWith("URGENT") ? 0 : 2);

queue.Enqueue("normal work");
queue.Enqueue("URGENT: page on-call");

string first = queue.Dequeue();   // "URGENT: page on-call" — band 0 wins
```

## 3. Bound it and watch it

```csharp
QoSQueueOptions options = new QoSQueueOptions
{
    Name = "ingress",
    MaxDepth = 1000,
    OverflowPolicy = OverflowPolicy.DropOldest
};

FifoQoSQueue<string> bounded = new FifoQoSQueue<string>(options);
bounded.ItemDropped += (sender, e) => Console.WriteLine($"dropped: {e.Reason}");

QoSQueueStatistics stats = bounded.Statistics;   // enqueued, dequeued, dropped, depth, wait time
```

## 4. Chain two levels

Two level-one queues feeding a level-two priority queue, run as a pipeline. You dequeue from the tail.

```csharp
LowLatencyQoSQueue<Job> realtime = new LowLatencyQoSQueue<Job>(
    priorityClasses: new[] { new TrafficClass<Job>("interactive", j => j.Interactive) },
    fairClasses: new TrafficClass<Job>[0]);

WeightedFairQoSQueue<Job> batch = new WeightedFairQoSQueue<Job>(
    flowSelector: j => j.Tenant,
    flows: new[] { new WeightedFlow("a", 2), new WeightedFlow("b", 1) });

PriorityQoSQueue<Job> level2 = new PriorityQoSQueue<Job>(
    levels: 2,
    prioritySelector: j => j.Interactive ? 0 : 1);

await using QoSPipeline<Job> pipeline = await realtime
    .Merge(batch)
    .ChainTo(level2)
    .AsPipeline("dispatch")
    .StartAsync(cancellationToken);

// Producers enqueue into the level-one queues; a worker services the tail.
realtime.Enqueue(new Job { Interactive = true });
batch.Enqueue(new Job { Tenant = "a" });

Job serviced = await level2.DequeueAsync(cancellationToken);
```

## 5. Many workers

Because queues are thread-safe, point as many workers as you like at one queue with no external locking:

```csharp
await Parallel.ForEachAsync(Enumerable.Range(0, 8), async (_, ct) =>
{
    await foreach (Job job in level2.ConsumeAsync(ct))
        await DispatchAsync(job, ct);
});
```

For more, run `dotnet run --project src/QoSKit.Example` to see the three scenarios in action, and read `QOSKIT_PLAN.md` for the full design.
