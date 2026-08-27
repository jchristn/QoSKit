# Changelog

All notable changes to QoSKit are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/jchristn/QoSKit/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/jchristn/QoSKit/releases/tag/v0.1.0
