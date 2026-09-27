# QoSKit Telemetry Guide

QoSKit is instrumented for observability the way networking gear is: it does not just count what went in and out of an interface, it accounts for **every traffic class** — what was admitted, served, dropped (and *why*), how long it waited, how deep the backlog is, and whether a priority class hit its policer. This guide walks through what QoSKit emits, how to turn it on and collect it, how to configure it, and how to read it.

The design rule throughout: **QoSKit emits, your host collects.** The library takes no dependency on any exporter, opens no connection to any backend, and writes nothing to the console. Emission is allocation-free on the hot path and costs effectively nothing until a listener subscribes.

---

## Table of contents

1. [The three surfaces](#the-three-surfaces)
2. [Metrics — the `QoSKit` meter](#metrics--the-qoskit-meter)
3. [Traces — the `QoSKit` activity source](#traces--the-qoskit-activity-source)
4. [The pull-based statistics snapshot](#the-pull-based-statistics-snapshot)
5. [Configuration](#configuration)
6. [Collecting the signals](#collecting-the-signals)
7. [Cardinality and the per-class tag](#cardinality-and-the-per-class-tag)
8. [Performance and cost](#performance-and-cost)
9. [Recipes](#recipes)
10. [Mapping to `show policy-map interface`](#mapping-to-show-policy-map-interface)

---

## The three surfaces

QoSKit exposes observability three ways, from cheapest/coarsest to richest:

| Surface | Type | Answers | Cost |
| --- | --- | --- | --- |
| `QoSQueueStatistics` | Pull snapshot, no dependencies | "What are this queue's lifetime totals right now?" | A lock and a struct copy. |
| `QoSMetrics` | OpenTelemetry `Meter` named `QoSKit` | "How is the system behaving in aggregate, per class, and is that changing?" | Allocation-free; nothing until a listener subscribes. |
| `QoSTracing` | OpenTelemetry `ActivitySource` named `QoSKit` | "Why was *this* item slow, and which hop/class caused it?" | Null-cost until a trace listener subscribes. |

Metrics and traces answer different questions — keep the split clean. Metrics are cheap, always-on, bounded-cardinality aggregates; traces are per-item span waterfalls you sample. Do not try to reconstruct aggregates from traces or chase individual items through metrics.

---

## Metrics — the `QoSKit` meter

All instruments live on a single `System.Diagnostics.Metrics.Meter` named **`QoSKit`** (constant `QoSMetrics.MeterName`). Every measurement is tagged with:

| Tag key | Constant | Value |
| --- | --- | --- |
| `queue.name` | `QoSMetrics.TagQueueName` | The queue's name (from `QoSQueueOptions.Name`, or auto-generated e.g. `fifo-3`). |
| `queue.type` | `QoSMetrics.TagQueueType` | The discipline: `fifo`, `lifo`, `priority`, `wfq`, `cbwfq`, `llq`, `wrr`. |
| `queue.class` | `QoSMetrics.TagQueueClass` | The traffic class (see [per-class](#cardinality-and-the-per-class-tag)); omitted when `EnablePerClassMetrics = false`. |
| `drop.reason` | `QoSMetrics.TagDropReason` | On drops only: `newest`, `oldest`, `unknown_class`, `unroutable`. |

### Instruments

| Instrument | Kind | Unit | Meaning |
| --- | --- | --- | --- |
| `qoskit.queue.enqueued` | Counter | items | Items admitted. |
| `qoskit.queue.enqueued.bytes` | Counter | By | Cost (bytes-equivalent) admitted. |
| `qoskit.queue.dequeued` | Counter | items | Items serviced. |
| `qoskit.queue.dequeued.bytes` | Counter | By | Cost serviced. |
| `qoskit.queue.dropped` | Counter | items | Items dropped; carries `drop.reason`. |
| `qoskit.queue.dropped.bytes` | Counter | By | Cost dropped; carries `drop.reason`. |
| `qoskit.queue.rejected` | Counter | items | Items rejected by a full queue under `Reject`. |
| `qoskit.queue.rejected.bytes` | Counter | By | Cost rejected. |
| `qoskit.queue.depth` | UpDownCounter | items | Current resident depth (pushed on every enqueue/dequeue/resident-drop). |
| `qoskit.queue.wait.duration` | Histogram | ms | Time each item waited between enqueue and dequeue; **per class**. |
| `qoskit.policer.conformed` | Counter | items | LLQ priority-class items that had a token and were served. |
| `qoskit.policer.exceeded` | Counter | items | Scheduling passes in which an LLQ priority class had work but its policer had no tokens, so the class was held back. A throttled item can count more than once, as consumers re-check it until the bucket refills. |
| `qoskit.queue.capacity` | ObservableGauge | items | Configured `MaxDepth` (0 = unbounded); pull-based. |
| `qoskit.queue.peak.depth` | ObservableGauge | items | High-water mark of resident depth; pull-based. |
| `qoskit.queue.resident.bytes` | ObservableGauge | By | Current resident cost; pull-based. |

### Why cost/byte counters matter

Every item carries a **cost** — 1 by default, or whatever a discipline's `costSelector` returns (analogous to a packet's size in bytes). Bandwidth guarantees are expressed in cost, not item count, so a class moving a few large items can dominate a class moving many small ones while the item counters look identical. The `.bytes` counters and `resident.bytes` gauge expose that. If you never set a `costSelector`, cost is 1 per item and the byte counters simply track the item counters.

### Why `drop.reason` matters

A drop is not a drop. `newest` is tail-drop of the arriving item (`DropNewest`); `oldest` is head-drop of a resident item to make room (`DropOldest`); `unknown_class` is a classification rejection (a closed-set discipline saw a key it does not know under `UnknownKeyPolicy.Reject`); `unroutable` is a router with no matching route and no default. Splitting them tells you whether you are shedding load, misclassifying, or misrouting — three very different problems.

### Deriving quantiles

`qoskit.queue.wait.duration` emits raw measurements. Compute p50/p95/p99 **downstream** in your collector/Grafana from the histogram buckets — do not precompute quantiles in-process. Because the histogram is tagged `queue.class`, you can prove the SLA per class: the LLQ priority class stays low while a fair class absorbs the delay.

---

## Traces — the `QoSKit` activity source

A `System.Diagnostics.ActivitySource` named **`QoSTracing.ActivitySourceName`** (`"QoSKit"`) opens three spans:

| Span (`OperationName`) | Constant | Opened by | Key attributes |
| --- | --- | --- | --- |
| `queue.enqueue` | `QoSTracing.EnqueueSpanName` | every enqueue | `queue.name`, `queue.type`, `queue.class`, `qoskit.cost`, `qoskit.outcome` (`admitted` / `rejected` / `dropped.newest` / `dropped.unknown_class`), `drop.reason` when dropped |
| `queue.dequeue` | `QoSTracing.DequeueSpanName` | every dequeue | `queue.name`, `queue.type`, `queue.class`, `qoskit.cost`, `qoskit.wait_ms` |
| `link.move` | `QoSTracing.MoveSpanName` | each chain-pump hop | `qoskit.source`, `qoskit.sink`, `qoskit.outcome` (`moved` / `backpressure` / `poison`) |

The star of the show is **`link.move`**. When you chain queues into a `QoSPipeline`, an item traverses multiple queues; the `link.move` spans decompose end-to-end latency **hop by hop** — which queue added the delay, which branch a `QoSRouter` took, and where in the pipeline an item met backpressure or was discarded as poison. This is the in-process analogue of per-hop latency along a network path, and it is where tracing earns its keep.

`ActivitySource.StartActivity` returns `null` and does essentially nothing while no listener is subscribed, so all three spans are free until you attach a collector. Once attached, **span volume tracks item volume** — sampling is the collector's responsibility (tail-based "keep if dropped or slow" is a good policy for a queue).

---

## The pull-based statistics snapshot

You do not need OpenTelemetry at all to watch a queue. Every queue exposes an immutable, point-in-time snapshot:

```csharp
QoSQueueStatistics s = queue.Statistics;
// s.Enqueued, s.Dequeued, s.Dropped, s.Rejected,
// s.CurrentDepth, s.PeakDepth, s.AverageWaitMilliseconds
```

This is lifetime totals for the whole queue (not per-class) and a mean wait (not a histogram). Use it for a quick health check, a log line, or a custom dashboard with no external stack. For per-class breakdown, quantiles, byte accounting, policer counters, and traces, use the meter and activity source.

---

## Configuration

Everything is off-by-default-free and on-by-default-emitting. Four switches, all simple booleans:

| Option | Default | Effect |
| --- | --- | --- |
| `QoSQueueOptions.EnableMetrics` | `true` | Master switch for this queue's meter emission and its pull gauges. `false` = the queue is invisible to the meter entirely. |
| `QoSQueueOptions.EnablePerClassMetrics` | `true` | Whether measurements carry the `queue.class` tag. Set `false` on a WFQ queue with unbounded dynamic flows (see [cardinality](#cardinality-and-the-per-class-tag)). |
| `QoSQueueOptions.EnableTracing` | `true` | Whether the queue opens `queue.enqueue` / `queue.dequeue` spans. A kill switch for a hot queue even when a trace listener is attached. |
| `QoSLinkOptions.EnableTracing` | `true` | Whether a chain pump opens `link.move` spans. |

```csharp
QoSQueueOptions options = new QoSQueueOptions
{
    Name = "ingress",
    MaxDepth = 10_000,
    OverflowPolicy = OverflowPolicy.DropOldest,
    EnableMetrics = true,          // emit to the QoSKit meter (default)
    EnablePerClassMetrics = true,  // include the queue.class tag (default)
    EnableTracing = true           // open enqueue/dequeue spans (default)
};

WeightedFairQoSQueue<Packet> q = new WeightedFairQoSQueue<Packet>(
    p => p.Flow,
    new[] { new WeightedFlow("gold", 3), new WeightedFlow("silver", 1) },
    options,
    costSelector: p => p.SizeBytes);   // makes the .bytes counters meaningful
```

---

## Collecting the signals

QoSKit follows the standard OTel-shaped contract: subscribe to the two names and export wherever you like. The meter and activity-source names are the entire contract — treat them as public API once a dashboard depends on them.

### With a collector (Radiant / OpenTelemetry SDK)

```csharp
// Subscribe both names alongside anything else you collect.
settings.Sources.AddMeter("QoSKit");
settings.Sources.AddActivitySource("QoSKit");
```

A Prometheus exporter rewrites the dotted names to snake_case with the usual suffixes, e.g. `qoskit_queue_wait_duration_ms_bucket`, `qoskit_queue_depth`, `qoskit_policer_exceeded_total`.

### In-process with no backend (metrics)

```csharp
using System.Diagnostics.Metrics;

using MeterListener listener = new MeterListener();
listener.InstrumentPublished = (instrument, l) =>
{
    if (instrument.Meter.Name == QoSMetrics.MeterName)
        l.EnableMeasurementEvents(instrument);
};
listener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
{
    // inspect instrument.Name and tags (queue.name, queue.type, queue.class, drop.reason)
});
listener.SetMeasurementEventCallback<double>((instrument, value, tags, state) => { /* histogram */ });
listener.Start();

// ... run work ...
listener.RecordObservableInstruments();   // pull capacity / peak.depth / resident.bytes
```

### In-process with no backend (traces)

```csharp
using System.Diagnostics;

ActivityListener listener = new ActivityListener
{
    ShouldListenTo = src => src.Name == QoSTracing.ActivitySourceName,
    Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
    ActivityStopped = a =>
    {
        // a.OperationName is queue.enqueue / queue.dequeue / link.move
        // a.TagObjects carries queue.name, queue.class, qoskit.outcome, qoskit.wait_ms, ...
    }
};
ActivitySource.AddActivityListener(listener);
```

> The observable gauges (`capacity`, `peak.depth`, `resident.bytes`) are **pull-based**: their callbacks run only when a collector scrapes (`RecordObservableInstruments()` in the in-process case). A real collector polls them on its scrape interval automatically.

---

## Cardinality and the per-class tag

Per-class breakdown is the whole point of `queue.class`, but cardinality is a real cost — a tag that can take thousands of values multiplies every series and can overwhelm a scrape endpoint. How bounded the class tag is depends on the discipline:

| Discipline | `queue.class` value | Cardinality |
| --- | --- | --- |
| FIFO, LIFO | `class-default` | 1 (bounded) |
| Priority | `band-0` … `band-(N-1)` | N bands (bounded) |
| CBWFQ, LLQ, WRR | the class / sub-queue name | closed set (bounded) |
| WFQ with static flows | the flow key | bounded to declared flows |
| **WFQ with `CreateDynamic`** | the flow key of each new flow | **open-ended** |

The only risk case is a WFQ queue that mints a new flow per key (e.g. per-user, per-connection). There, set `EnablePerClassMetrics = false` on that queue: measurements still emit with `queue.name`/`queue.type`, just without the unbounded class tag. Every other discipline keeps class cardinality naturally bounded, so leave it on. QoSKit never puts item ids, payloads, or free-form input in a tag.

---

## Performance and cost

QoSKit is engineered so that turning telemetry on does not add measurable latency:

- **Zero heap allocation on the hot path.** Metric tags are built with a stack-allocated `TagList`; there is no per-measurement array or dictionary allocation.
- **Free until observed.** Counters/histograms cost a few nanoseconds and allocate nothing until a `MeterListener` subscribes. Spans return `null` from `StartActivity` until an `ActivityListener` subscribes — the enqueue/dequeue/move paths pay only a `HasListeners` check.
- **Pull gauges are pull-only.** `capacity`, `peak.depth`, and `resident.bytes` run their callbacks solely when a collector scrapes, never on the enqueue/dequeue path.
- **Policer counters are constant-time.** LLQ conform/exceed counts are emitted inline under the queue lock with no allocation.
- **No leaks.** The gauge registry holds queues by **weak reference**, keyed by id and unregistered on `Dispose`, so a queue you forget to dispose can still be garbage-collected instead of being pinned alive by the static meter.

If you need to shave the last cost off an extremely hot queue while a listener is attached, use the switches: `EnableTracing = false` to drop spans, `EnablePerClassMetrics = false` to shrink the tag set, or `EnableMetrics = false` to go dark entirely.

---

## Recipes

**Per-class drop rate** — `rate(qoskit_queue_dropped_total{queue_name="ingress"}[5m])` grouped by `queue_class` and `drop_reason`. A spike in `drop_reason="oldest"` on one class means that class is overrunning its share.

**Priority-class policer health (LLQ)** — plot `qoskit_policer_exceeded_total` vs `qoskit_policer_conformed_total` per `queue_class`. A rising exceed ratio means the priority class is hitting its configured rate and being held back — exactly what the policer exists to do, and the signal that upstream is over-sending.

**Backlog / fill ratio** — `qoskit_queue_depth / qoskit_queue_capacity` (skip where capacity is 0 = unbounded) shows how close a bounded queue is to tail-drop. Pair with `qoskit_queue_resident_bytes` when items vary in size.

**Per-class latency SLA** — p99 of `qoskit_queue_wait_duration_ms` filtered to the priority `queue_class` proves the low-latency contract; the same query on a bulk class shows where the delay was absorbed.

**Hop-by-hop pipeline latency** — open a trace and read the `link.move` span durations across the pipeline; the longest span is the hop that dominated. `qoskit.outcome="backpressure"` on a hop points at the downstream queue that is full.

---

## Mapping to `show policy-map interface`

For operators coming from Cisco QoS, the correspondence is close:

| `show policy-map interface` | QoSKit |
| --- | --- |
| per-class packets/bytes offered | `qoskit.queue.enqueued` / `.enqueued.bytes` by `queue.class` |
| per-class packets/bytes transmitted | `qoskit.queue.dequeued` / `.dequeued.bytes` by `queue.class` |
| per-class drops (tail / no-buffer) | `qoskit.queue.dropped` / `.dropped.bytes` by `drop.reason` |
| current / max queue depth | `qoskit.queue.depth` / `qoskit.queue.capacity` |
| per-class queue latency | `qoskit.queue.wait.duration` by `queue.class` |
| policer conformed / exceeded | `qoskit.policer.conformed` / `.exceeded` |

The gap QoSKit does not fill on its own is the hosting stack — Prometheus, Grafana, Tempo. QoSKit emits the signals; standing up a collector and dashboards for them is the host application's job, by design.
