<p align="center">
  <img src="https://raw.githubusercontent.com/jchristn/QoSKit/main/assets/logo.png" alt="QoSKit" width="128" height="128" />
</p>

<h1 align="center">QoSKit</h1>

<p align="center">
  <strong>v0.1.0 &mdash; alpha</strong>
</p>

> **Alpha release.** This is pre-release software. The public API surface is still evolving and may change without notice between `0.x` versions. Pin to an exact version and review the [CHANGELOG](CHANGELOG.md) before upgrading.

QoSKit builds Quality-of-Service systems out of queues you compose in code. It ships the scheduling disciplines that networking gear has used for decades — FIFO, LIFO, strict priority, weighted fair queuing, class-based weighted fair queuing, low-latency queuing, and weighted round robin — as ordinary .NET collections. You construct a queue, enqueue work, dequeue work, and chain queues into hierarchies as deep as you need.

QoSKit enables you to apply control over use of potentially congested resources in a way that aligns with your objectives nad your business.

## Install

```
dotnet add package QoSKit
```

## The thirty-second version

```csharp
using QoSKit;

FifoQoSQueue<string> queue = new FifoQoSQueue<string>();
queue.Enqueue("first");
queue.Enqueue("second");
string next = queue.Dequeue();          // "first"

if (queue.TryDequeue(out string item))  // non-blocking, no exception when empty
{
    // handle 'item'
}

string awaited = await queue.DequeueAsync(cancellationToken);  // completes when work arrives
```

Every queue is thread-safe, implements `IProducerConsumerCollection<T>` (so it backs a `BlockingCollection<T>`), and offers a synchronous, a non-blocking, and an awaitable way to take work.

## The queue catalog

Each discipline is a drop-in `IQoSQueue<T>`. The scheduling ones take small classifier delegates at construction, so your payload type stays clean.

**`FifoQoSQueue<T>` / `LifoQoSQueue<T>`** — first-in-first-out and last-in-first-out. The baseline building blocks.

**`PriorityQoSQueue<T>`** — strict priority across a fixed number of bands (lower value = higher priority). Optional aging promotes long-waiting items so a busy high band cannot starve the low ones forever.

```csharp
PriorityQoSQueue<Job> queue = new PriorityQoSQueue<Job>(levels: 3, prioritySelector: j => j.Urgency);
queue.Enqueue(job);            // classified by the selector
queue.Enqueue(urgent, 0);      // explicit band for one item
```

**`WeightedFairQoSQueue<T>`** — approximates generalized processor sharing across flows using a virtual-time finish tag. Flows are weighted; heavier flows get proportionally more service. Flow keys are an open namespace — an unseen key becomes a new flow by default.

```csharp
WeightedFairQoSQueue<Job> queue = new WeightedFairQoSQueue<Job>(
    flowSelector: j => j.TenantId,
    flows: new[] { new WeightedFlow("gold", 5), new WeightedFlow("silver", 3), new WeightedFlow("bronze", 1) });
```

**`ClassBasedWeightedFairQoSQueue<T>`** — weighted fair queuing over named classes matched by predicate (first match wins). An implicit, non-removable `class-default` catches everything else, so an item is never unclassifiable.

**`LowLatencyQoSQueue<T>`** — class-based weighted fair queuing plus strict-priority classes served first, optionally policed by a token bucket. Within its rate, priority work jumps the line; above the rate it yields so fair classes are not starved. A priority class with no rate limit is unpoliced strict priority.

**`WeightedRoundRobinQoSQueue<T>`** — sub-queues served in proportion to weight using deficit round robin. With unit costs this is classic WRR; give it a cost selector and it becomes byte-fair DWRR. Two ingress modes: a balancer that spreads incoming work across sub-queues by weight, or a classifier that routes by a name selector.

## Chaining

The whole reason the disciplines share one interface is so you can wire them together into queuing hierarchies. Chaining comes in three flavors.

**By hand** — because every queue is an `IQoSSource<T>` and `IQoSSink<T>`, you just dequeue from one and enqueue into another:

```csharp
realtime.DrainTo(level2, max: 256);
fair.DrainTo(level2, max: 256);
Job serviced = level2.Dequeue();
```

**Fluently** — build a running pipeline as one expression and dequeue from its tail:

```csharp
await using QoSPipeline<Job> pipeline = await realtime
    .Merge(fair)             // two level-one queues...
    .ChainTo(level2)         // ...into a level-two priority queue (the tail)
    .AsPipeline("servicing")
    .StartAsync(cancellationToken);

Job serviced = await level2.DequeueAsync(cancellationToken);
```

The pipeline runs background pumps that move work downstream and honor backpressure, validates the graph (a cycle throws `PipelineCycleException`), and starts and stops as a unit. A `QoSRouter<T>` fans one stream out to many sinks by predicate. There is no limit to how deep a chain goes.

## Classification

The scheduling queues classify with delegates you supply, not attributes or wrappers on your payload. Priority takes a `Func<T,int>`, WFQ a `Func<T,string>` flow selector, class-based disciplines a `Func<T,bool>` matcher per class, WRR an optional sub-queue selector. Each also accepts a per-item override — `queue.Enqueue(item, "gpu-a")` — that bypasses the selector.

String keys and class names compare case-insensitively by default (`StringComparer.OrdinalIgnoreCase`), because a case mismatch is almost always a typo rather than a distinct class; pass `StringComparer.Ordinal` when case is significant. An unrecognized key is governed by `UnknownKeyPolicy`: WFQ admits it as a new flow (`CreateDynamic`), while a closed set like WRR throws by default, routes to a designated default, or rejects with an `ItemDropped` event — your choice, and nothing is ever silently discarded.

## Bounds, overflow, and observability

Any queue can be bounded with `MaxDepth` and an `OverflowPolicy` of `Reject` (the default), `DropNewest`, `DropOldest`, or `Block`. Drops and rejections raise events and are counted. Every queue exposes a `Statistics` snapshot — enqueued, dequeued, dropped, rejected, current and peak depth, and average wait time — so you can watch behavior without any external stack.

For hosts that run OpenTelemetry, the same measurements flow through a `System.Diagnostics.Metrics.Meter` named `QoSKit` with OTel-shaped instruments (`qoskit.queue.enqueued`, `qoskit.queue.dequeued`, `qoskit.queue.dropped`, `qoskit.queue.rejected`, `qoskit.queue.depth`, `qoskit.queue.wait.duration`), each tagged with the queue name and type. Emission is on by default and allocates nothing until a listener subscribes; set `EnableMetrics = false` to turn it off. The library writes nothing to the console.

## Persistence (optional)

By default a queue lives entirely in memory with no dependencies. When losing enqueued work across a restart is unacceptable, add the `QoSKit.Persistence.Sqlite` package and give a queue a durability journal — the queue records every admitted item and its removal, and replays the backlog on startup.

```
dotnet add package QoSKit.Persistence.Sqlite
```

```csharp
using QoSKit;
using QoSKit.Persistence.Sqlite;

FifoQoSQueue<Job> queue = new FifoQoSQueue<Job>();
queue.EnablePersistence(new SqliteQoSStore<Job>(
    QoSPersistenceOptions<Job>.GuaranteedDelivery("data/jobs.db")));

// ... enqueue and dequeue as usual. On a fresh process, EnablePersistence recovers the backlog.
```

Two presets bracket the trade-off. `Fast(path)` is write-behind: near in-memory throughput, with a small window of the most recent writes at risk on a hard crash. `GuaranteedDelivery(path)` is durable: every write is committed with `WAL` + `synchronous=FULL` before the call returns, so **no acknowledged enqueue is lost across a restart**. Because the payload type is opaque to the library, persistence needs an `IQoSSerializer<T>`; a JSON serializer is the default, and a serialization failure aborts the enqueue before the item is admitted, so a poison item never lands in memory or on disk. `EnablePersistence` must be called on an empty queue, before enqueuing.

### Running under Docker with WAL

In WAL mode SQLite keeps a queue's data across **three** files — `jobs.db`, `jobs.db-wal`, and `jobs.db-shm` — and the `-wal` file holds committed transactions that have not yet been checkpointed into the main database. If your container persists only `jobs.db`, you **lose the most recently committed data on restart**, silently defeating the durability guarantee.

The rules:

- Mount a **volume at the directory** that holds the database, not a bind-mount of the single `.db` file. A single-file bind-mount also breaks SQLite's create-and-rename of the sidecar files.
- Keep the database on the mounted volume, not the container's overlay layer, and give the container user write permission to that directory (watch the UID).
- Prefer a local volume; a networked filesystem (NFS, SMB) can break WAL locking and memory-mapping.
- On a clean shutdown, disposing the store runs `wal_checkpoint(TRUNCATE)` to fold the WAL back into the main file. Do not rely on a clean shutdown, though — the sidecar files must be on the persistent volume for the crash case.

```yaml
services:
  app:
    image: your-app:latest
    volumes:
      - qos-data:/data          # the whole directory, so .db, .db-wal, and .db-shm all persist
    environment:
      - QOS_DB_PATH=/data/jobs.db
volumes:
  qos-data:
```

The honest limits: `GuaranteedDelivery` protects an acknowledged enqueue across process crash, OS crash, and power loss as far as the disk honors `fsync` — hardware that lies about flushing its cache is beyond any user-space library's reach. Durability across a chain is composable but not automatic: to make a whole pipeline lossless, keep its queues in one database and coordinate the hand-offs. The current release ships per-queue durability and recovery, not exactly-once across a multi-stage pipeline or hierarchy.

## The example app

`src/QoSKit.Example` is a console demo that runs three scenarios back to back, ten seconds each: a priority queue, a weighted fair queue, and a chained pipeline. Run it and watch the summaries:

```
dotnet run --project src/QoSKit.Example
```

The code is split deliberately: `Scenarios/` holds the queuing logic and contains no console calls, while `Harness/` holds all the formatting and the ten-second runner. Open a scenario file to see exactly how a discipline is used, without wading through presentation code.

## Building and testing

```
dotnet build QoSKit.slnx
dotnet run --project src/Test.Automated          # console runner, colored output
dotnet test src/Test.Xunit                       # same suites through xUnit
dotnet test src/Test.Nunit                       # same suites through NUnit
```

Tests use [Touchstone](https://github.com/jchristn/touchstone): the suites are written once in `Test.Shared` and executed through the console runner, xUnit, and NUnit. They cover each discipline's ordering and fairness, the classification policies, overflow and disposal, asynchronous consumption, concurrency, persistence, multi-threading, and the chaining layer.

## What is intentionally absent

QoSKit is an in-process library, so there is no Docker image, REST or MCP surface, health endpoint, or bundled observability stack here — those requirements apply to services, not a collection. The core `QoSKit` package depends on nothing beyond the .NET base libraries; SQLite durability lives in the separate, opt-in `QoSKit.Persistence.Sqlite` package, so you only take the native SQLite dependency when you ask for persistence.

## License

MIT. See [LICENSE.md](LICENSE.md).
