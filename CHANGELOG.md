# Changelog

All notable changes to QoSKit are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2026-10-03

### Changed
- **Dependency refresh.** No behavior or public API changes.
  - `QoSKit` (netstandard2.0 only): `Microsoft.Bcl.AsyncInterfaces` 8.0.0 → 10.0.12,
    `System.Diagnostics.DiagnosticSource` 8.0.1 → 10.0.12, `System.Threading.Tasks.Extensions`
    4.5.4 → 4.6.3. The net8.0 and net10.0 targets remain dependency-free.
  - `QoSKit.Persistence.Sqlite`: `Microsoft.Data.Sqlite` 9.0.9 → 10.0.12 (store format unchanged).
  - Tests: `Touchstone.*` 0.1.12 → 0.2.0, `Microsoft.NET.Test.Sdk` 17.14.1 → 18.10.1,
    `coverlet.collector` 6.0.4 → 10.1.0, `NUnit` 4.3.2 → 5.0.0, `NUnit.Analyzers` 4.7.0 → 4.15.0,
    `NUnit3TestAdapter` 5.0.0 → 6.3.0, `xunit.runner.visualstudio` 3.1.4 → 4.0.0.

## [0.2.1] - 2026-09-27

### Fixed
- **Throttled LLQ work no longer stalls async consumers.** When the only resident items were in a
  low-latency priority class whose token-bucket policer was empty, `DequeueAsync` (and so
  `ConsumeAsync`) parked until the next *enqueue*, even though the bucket refilled milliseconds
  later; with no new traffic the items sat indefinitely. A waiter that finds items resident but
  none eligible now arms a timer for the moment the earliest can conform (clamped to 1 ms–1 s, so it
  never spins and always re-checks), and re-checks when it fires. Enqueue wakeups work as before;
  cancellation and disposal still end the wait promptly.
- **Waiters are always deregistered.** Every exit from `DequeueAsync`/`EnqueueAsync` (success,
  cancellation, disposal, or a throwing event handler) now unlinks the waiter and releases its
  cancellation registration and timer. Previously a registration could outlive its wait on a
  long-lived token, and a waiter could be left registered if the post-registration re-check threw.
- **No lost wakeups on cancellation races.** A consumer woken for an item that is then cancelled
  (or faults) passes the wakeup to the next waiting consumer instead of stranding the item; blocked
  producers do the same for freed capacity.
- **Blocked producers wake whenever capacity is freed.** `Clear()` and raising `MaxDepth` now wake
  producers blocked in `EnqueueAsync` under `OverflowPolicy.Block`; before, only a dequeue did.
- **`EnqueueAsync` under `Block` no longer waits forever on an unknown class.** A rejection for any
  reason other than a full queue (for example `UnknownKeyPolicy.Reject`) now throws
  `UnknownClassificationException`, as `Enqueue` does, instead of being mistaken for a full queue.
- **Chain pumps and `DrainTo` move exactly the item they peeked.** The mover peeked one item,
  forwarded it, then dequeued whatever the scheduler picked *at that moment*; a concurrent enqueue
  (LIFO, priority, WFQ), a policer refill (LLQ), aging (priority), or WRR deficit state could make
  that a different item, silently discarding it and forwarding the peeked one again later. The
  exact forwarded entry is now removed, with the discipline's service accounting applied. A poison
  item is likewise discarded exactly.
- **WRR `TryPeek` reports the item `TryDequeue` serves next.** It previously returned the head of
  the next non-empty sub-queue, ignoring the deficit scheduler.
- **An oversize policed item is rate-limited, not stranded.** A priority-class item costing more
  than its token bucket's burst could never conform and blocked its class forever; it now conforms
  once the bucket is full and leaves a token debt repaid by refill.
- **A disposed sink stops its pump.** `QoSLink` treated `ObjectDisposedException` from the sink as a
  poison item and discarded the source's items one by one; the pump now stops and leaves them queued.

### Changed
- `qoskit.policer.exceeded` is documented as counting scheduling passes in which a class was held
  back (a throttled item can be counted more than once as consumers re-check it), which is what it
  has always measured.

## [0.2.0] - 2026-08-30

### Added
- **Per-class metrics.** Every instrument now carries a `queue.class` tag (priority band, CBWFQ/LLQ/WRR
  class, or WFQ flow), turning interface-level counters into per-class-map accounting. Opt out with
  `QoSQueueOptions.EnablePerClassMetrics = false` where dynamic WFQ flows would inflate cardinality.
- **Drop-reason tagging.** `qoskit.queue.dropped` (and `.dropped.bytes`) carry a `drop.reason` tag:
  `newest`, `oldest`, `unknown_class`, `unroutable`.
- **Cost/byte accounting.** New counters `qoskit.queue.enqueued.bytes`, `.dequeued.bytes`,
  `.dropped.bytes`, and `.rejected.bytes` record the cost-selector weight (bytes-equivalent), not just
  item count.
- **Policer accounting.** LLQ token-bucket priority classes emit `qoskit.policer.conformed` and
  `qoskit.policer.exceeded`, making priority-class throttling visible.
- **Pull gauges.** `qoskit.queue.capacity`, `qoskit.queue.peak.depth`, and `qoskit.queue.resident.bytes`
  observable gauges report configured limit, high-water mark, and resident cost at collection time.
- **Distributed tracing.** A new `QoSKit` `System.Diagnostics.ActivitySource` emits `queue.enqueue`,
  `queue.dequeue`, and `link.move` spans, decomposing pipeline latency hop by hop. Null-cost until a
  trace listener subscribes; toggled by `QoSQueueOptions.EnableTracing` and `QoSLinkOptions.EnableTracing`.

### Changed
- Metric emission builds tags with a stack-allocated `TagList` to keep the hot path allocation-free
  under the wider tag set. Existing instruments and tag keys are unchanged and remain backward
  compatible; new tags and instruments are additive.

## [0.1.1] - 2026-08-27

### Changed
- Pinned `SQLitePCLRaw.bundle_e_sqlite3` to 3.0.5 in `QoSKit.Persistence.Sqlite` to resolve the
  transitive `NU1903` advisory (GHSA-2m69-gcr7-jv3q) on the native SQLite bundle. Builds are now
  warning-free with warnings-as-errors enabled.

## [0.1.0] - 2026-08-27

### Added
- Core queue abstractions: `IQoSQueue<T>`, `IQoSSource<T>`, `IQoSSink<T>`, and `QoSQueueBase<T>`.
- Queue disciplines: FIFO, LIFO, priority, weighted fair queuing (WFQ), class-based
  weighted fair queuing (CBWFQ), low-latency queuing (LLQ), and weighted round robin (WRR/DWRR).
- Delegate-based classification with `UnknownKeyPolicy` and case-insensitive key comparison.
- Synchronous and asynchronous consumption (`Dequeue`, `TryDequeue`, `DequeueAsync`,
  `ConsumeAsync`) plus `IProducerConsumerCollection<T>` for `BlockingCollection<T>` interop.
- Chaining: `DrainTo`, `QoSChain` fluent builder, `QoSLink` (with a fault-tolerant pump that
  discards a poison item a downstream classifier rejects rather than stalling), `QoSPipeline`
  with cycle detection, and `QoSRouter`.
- Configurable depth limits and overflow policies (`Reject`, `DropNewest`, `DropOldest`, `Block`).
- Pluggable durability journal (`IQoSStore<T>`) and an optional `QoSKit.Persistence.Sqlite`
  package with write-behind and durable (`WAL` + `synchronous=FULL`) modes, recovery on startup,
  and `GuaranteedDelivery` / `Fast` presets.
- Observability via a `QoSKit` `System.Diagnostics.Metrics.Meter` with OpenTelemetry-shaped
  instruments, plus synchronous `QoSQueueStatistics` snapshots.

[Unreleased]: https://github.com/jchristn/QoSKit/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/jchristn/QoSKit/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/jchristn/QoSKit/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/jchristn/QoSKit/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/jchristn/QoSKit/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/jchristn/QoSKit/releases/tag/v0.1.0
