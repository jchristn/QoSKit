# Changelog

All notable changes to QoSKit are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/jchristn/QoSKit/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/jchristn/QoSKit/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/jchristn/QoSKit/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/jchristn/QoSKit/releases/tag/v0.1.0
