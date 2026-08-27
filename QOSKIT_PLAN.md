# QoSKit — Build & Test Action Plan

QoSKit is a C# library for building user-space Quality-of-Service systems out of composable queues. You define a queue, enqueue work, dequeue work, and chain queues into arbitrarily deep hierarchies. The library ships the classic scheduling disciplines from networking — FIFO, LIFO, strict priority, weighted fair queuing, class-based weighted fair queuing, low-latency queuing, and weighted round robin — as first-class .NET collections that a developer can consume the same way they consume `Queue<T>` or `ConcurrentQueue<T>`.

The end goal is a toolkit rich enough to reconstruct a full network-style QoS policy in pure managed code, then point it at something other than packets — for example, scheduling and dispersing inference requests across a pool of model servers.

- **Repository:** https://github.com/jchristn/QoSKit
- **Package id:** `QoSKit`
- **Root namespace:** `QoSKit`
- **Target frameworks:** `netstandard2.0;net8.0;net10.0`

---

## Table of Contents

1. [Design Decisions (Locked)](#design-decisions-locked)
2. [Core Model](#core-model)
3. [Queue Types](#queue-types)
4. [Classification Model](#classification-model)
5. [Chaining and Fluency](#chaining-and-fluency)
6. [Exception Reference](#exception-reference)
7. [Observability](#observability)
8. [Optional Persistence (SQLite)](#optional-persistence-sqlite)
9. [Repository Layout](#repository-layout)
10. [Usage Examples](#usage-examples)
11. [Example Application (`QoSKit.Example`)](#example-application-qoskitexample)
12. [Testing Plan](#testing-plan)
13. [Documentation Plan](#documentation-plan)
14. [Build Phases and Milestones](#build-phases-and-milestones)
15. [Requirements Compliance Checklist](#requirements-compliance-checklist)
16. [Locked Default Behaviors](#locked-default-behaviors)

---

## Design Decisions (Locked)

These four choices were confirmed with the requester and drive everything below.

**Classification is delegate-based.** The scheduling queues take classifier delegates at construction time — `Func<T,int>` for priority, `Func<T,string>` for a WFQ flow key, `Func<T,int>` for cost/size — so the payload type `T` stays clean. `queue.Enqueue(item)` classifies through the delegate; explicit overloads such as `Enqueue(item, priority)` let a caller override per item. There is no mandatory envelope type. A caller who wants to carry metadata alongside the payload can make `T` a struct or record that holds it, and the classifier reads from that.

**Consumption is synchronous plus async-awaitable.** Every queue offers non-blocking `Dequeue`/`TryDequeue`/`TryPeek`, an awaitable `DequeueAsync(CancellationToken)` that completes when an item becomes available, and implements `IProducerConsumerCollection<T>` so a caller can wrap the queue in `BlockingCollection<T>` for classic blocking `Take`. The async path is what makes the model-server dispatch loop natural: a worker simply `await`s the next unit of work.

**Queues are thread-safe by default.** All implementations are safe for concurrent producers and consumers with no external locking, because the primary use case has many workers draining one queue. The synchronization strategy is documented per class. Single-threaded callers pay a small, bounded overhead, which is the right tradeoff for a work-dispersal library.

**The library multi-targets `netstandard2.0;net8.0;net10.0`.** `net8.0`/`net10.0` match the backend test architecture and give access to modern `System.Diagnostics.Metrics`. `netstandard2.0` broadens reach to .NET Framework, older runtimes, and Unity. The `netstandard2.0` build pulls `System.Diagnostics.DiagnosticSource`, `System.Threading.Tasks.Extensions`, and `Microsoft.Bcl.AsyncInterfaces` to backfill `Meter`, `ValueTask`, and `IAsyncEnumerable`; a small `#if NETSTANDARD2_0` polyfill region covers `PriorityQueue<,>` and a few `Try*` helpers absent on that target.

---

## Core Model

The core is a single abstraction that every queue implements, plus a shared base class that handles the mechanics common to all disciplines: depth limits, overflow policy, thread safety, async wakeups, statistics, and disposal. A discipline-specific queue only implements the ordering logic — where an item goes on enqueue, and which item comes off on dequeue.

### Interfaces

`IQoSSink<T>` and `IQoSSource<T>` split the two halves of a queue so that chaining code can talk to "somewhere I put work" and "somewhere I take work from" independently. `IQoSQueue<T>` is the union, and it also implements the standard collection contracts so the queues feel native.

```
IQoSSink<T>
    bool TryEnqueue(T item)
    void Enqueue(T item)                 // throws on rejection

IQoSSource<T>
    bool TryDequeue(out T item)
    bool TryPeek(out T item)
    T Dequeue()                          // throws when empty
    ValueTask<T> DequeueAsync(CancellationToken)
    IAsyncEnumerable<T> ConsumeAsync(CancellationToken)   // drains until cancelled

IQoSQueue<T> : IQoSSink<T>, IQoSSource<T>,
               IProducerConsumerCollection<T>,     // TryAdd/TryTake -> BlockingCollection<T>
               IReadOnlyCollection<T>
    string Name { get; }
    int Count { get; }
    bool IsEmpty { get; }
    int MaxDepth { get; }                // 0 == unbounded
    OverflowPolicy OverflowPolicy { get; }
    QoSQueueStatistics Statistics { get; }
    event EventHandler<QoSItemEventArgs<T>>? ItemEnqueued;
    event EventHandler<QoSItemEventArgs<T>>? ItemDequeued;
    event EventHandler<QoSDropEventArgs<T>>? ItemDropped;
    void Clear();
```

Implementing `IProducerConsumerCollection<T>` is the "implement queueing interfaces where possible" clause from the brief: it makes every QoSKit queue a legal backing store for `BlockingCollection<T>`, which is how the BCL expresses bounded/blocking producer-consumer semantics. `TryAdd`/`TryTake` forward to `TryEnqueue`/`TryDequeue`.

### Base class

`QoSQueueBase<T>` (abstract) owns:

- **Depth and overflow.** `MaxDepth` (0 = unbounded, default) enforced on enqueue. `OverflowPolicy` enum: `Reject` (default; `TryEnqueue` returns false, `Enqueue` throws `QueueFullException`), `DropNewest`, `DropOldest`, and `Block` (only honored on the async/`BlockingCollection` paths). Drops raise `ItemDropped` and increment stats.
- **Thread safety.** A per-instance lock (or `ReaderWriterLockSlim` where reads dominate, e.g. `Peek`/`Count`) guards structure. Documented in XML on each class.
- **Async wakeups.** A lightweight waiter registry signals `DequeueAsync` callers when an item lands, without spinning. Implemented over a `SemaphoreSlim`-style gate plus the lock, or a channel on modern targets — chosen once and reused by all disciplines.
- **Statistics.** `QoSQueueStatistics` snapshot: enqueued, dequeued, dropped, rejected counts; current and peak depth; total and average wait time. Updated with `Interlocked` where possible to keep the hot path cheap.
- **Events.** Nullable events, invoked only when subscribed, so the throughput path allocates nothing when nobody is listening.
- **Disposal.** Full `IDisposable`/`IAsyncDisposable` pattern (`protected virtual void Dispose(bool)`), disposing internal sync primitives and completing outstanding waiters with `OperationCanceledException`.
- **Backing store.** Leaf ordering is held behind an `IQoSStore<T>` seam rather than a hardcoded `LinkedList<T>`, with `InMemoryQoSStore<T>` as the default. This is the single seam that lets [optional SQLite persistence](#optional-persistence-sqlite) drop in without touching discipline logic. In the default in-memory build the store is a thin wrapper with no measurable overhead.

Concrete disciplines implement two protected members — `EnqueueCore(item, classification)` and `TryDequeueCore(out item)` — plus expose their construction-time delegates. Everything else is inherited.

### Configuration

`QoSQueueOptions` carries the cross-cutting settings so constructors stay readable and future options don't churn signatures: `Name`, `MaxDepth`, `OverflowPolicy`, `EnableMetrics`, `Meter` override, `WaitStrategy`, and `TimeProvider`. Per the code style rules, nothing that a developer might reasonably tune is a bare constant — each is a public property with a private backing field and a documented default, minimum, and maximum.

### Time source (for deterministic behavior and tests)

Two disciplines depend on wall-clock time: priority **aging** and the LLQ **token-bucket policer**. Both read time through an injected `IQoSTimeProvider` abstraction (`UtcNow` plus a monotonic tick) rather than calling `DateTime.UtcNow` or `Stopwatch` directly. The default implementation wraps the system clock; a `ManualQoSTimeProvider` test double lets the suite advance time by exact amounts, so aging and policing are verified deterministically instead of with sleeps and tolerances. Statistics wait-time also reads through this provider. This is a design requirement, not a test-only convenience — it keeps every time-dependent code path exercisable without real delays.

### Concurrency and liveness guarantees

Because the primary use case is many workers draining one queue, the synchronization design is written to a standard stricter than "thread-safe": it must never let a held lock or an unreleased semaphore permit stall servicing, and it must survive a caller-supplied delegate throwing at the worst possible moment. These are hard commitments the [Testing Plan](#testing-plan) verifies directly (Suites R and S).

- **Locks are held only for O(1) structural work.** An internal lock guards the enqueue/dequeue bookkeeping and is released before returning. No lock is ever held across an `await`, across I/O, or across a call into user code.
- **User delegates run outside every internal lock.** Classifiers, the cost selector, event handlers, and drop callbacks are invoked with no QoSKit lock held. A delegate that throws therefore cannot leak a lock, and one that re-enters the queue (enqueues from an `ItemDequeued` handler) cannot self-deadlock. The classifier is evaluated, and only its result is applied under the lock.
- **Every acquire is paired with a release in `finally`.** Locks use the `lock` statement (compiler-emitted `try/finally`); every `SemaphoreSlim.Wait`/`WaitAsync` and every manual `Monitor` path sits in an explicit `try/finally`. There is no code path — success, exception, or cancellation — that acquires a primitive and returns without releasing it.
- **The async-wait registry is self-healing.** A pending `DequeueAsync` waiter that is cancelled or whose queue is disposed removes itself and releases its slot; a completed waiter is dequeued from the registry exactly once. Waiter count and semaphore permits return to baseline after any mix of completion, cancellation, and disposal — they do not drift.
- **A single lock per queue, no nested lock ordering.** Composite disciplines coordinate their children through each child's own public, already-synchronized surface rather than by holding two locks at once, so there is no lock-ordering cycle to deadlock on.
- **Testable internals.** The base class exposes internal diagnostic counters — pending waiter count and the base semaphore's current permit count — via `[assembly: InternalsVisibleTo("Test.Shared")]`, so liveness tests assert the exact free-state after a scenario rather than inferring it from a timeout alone.

---

## Queue Types

Each discipline is one class in its own file. "Subordinate instances defined beforehand" from the brief maps to explicit definition types — `PriorityBand<T>`, `WeightedFlow<T>`, `TrafficClass<T>` — that the caller constructs before wiring up the parent queue.

**`FifoQoSQueue<T>`** — first in, first out. Backed by a linked-list/ring buffer. The baseline, and the default leaf queue used inside the composite disciplines.

**`LifoQoSQueue<T>`** — last in, first out. Stack semantics; `TryTake` removes the most recent item, consistent with `ConcurrentStack<T>`.

**`PriorityQoSQueue<T>`** — strict priority across a fixed number of bands. Constructed with a band count and a `Func<T,int> prioritySelector` (lower value = higher priority, matching DSCP/`ThreadPriority` intuition, documented explicitly). Each band is its own leaf queue; dequeue serves the highest-priority non-empty band. Optional **aging** promotes items that have waited beyond a configurable threshold to prevent starvation — off by default, enabled through options.

**`WeightedFairQoSQueue<T>` (WFQ)** — approximates generalized processor sharing. Constructed with a `Func<T,string> flowSelector`, a set of `WeightedFlow` definitions (flow key + weight), and a `Func<T,int> costSelector` (defaults to 1, i.e. per-item fairness; set it to a byte size for size-fair scheduling). Internally tracks a virtual clock and assigns each enqueued item a virtual finish time `= max(virtualNow, lastFinish[flow]) + cost/weight`; dequeue pops the smallest virtual finish time. New or unlisted flows fall to a default weight.

**`ClassBasedWeightedFairQoSQueue<T>` (CBWFQ)** — WFQ over named classes instead of hashed flows. The caller defines `TrafficClass<T>` instances up front, each with a matcher `Func<T,bool>`, a weight, its own leaf queue (any discipline), and an optional depth limit; a default class catches unmatched items. Enqueue routes to the first matching class; dequeue applies the weighted-fair choice across classes. This is the workhorse for tenant/QoS-tier scheduling.

**`LowLatencyQoSQueue<T>` (LLQ)** — CBWFQ plus one or more strict-priority classes that are always served first, optionally subject to a **policer**. A priority class may carry a rate limit (token bucket, configurable rate and burst): traffic within the rate jumps the queue and traffic above it is demoted to fair scheduling. A priority class constructed with a null rate limit is unpoliced strict priority — the XML docs flag prominently that an unpoliced class can starve fair traffic. This is the discipline that keeps interactive/low-latency work ahead of batch work without letting it starve everything else.

**`WeightedRoundRobinQoSQueue<T>` (WRR)** — a set of weighted sub-queues served in round-robin proportion to weight. Two scheduling modes on the dequeue side: classic WRR (serve `weight` items per visit) and **Deficit WRR** (DWRR, default when a `costSelector` is supplied), which uses per-queue deficit counters against the cost function for byte-accurate fairness with variable-size items. Two *ingress* modes on the enqueue side: **balancer mode** (no selector — the queue itself spreads incoming items across sub-queues by weight, the load-dispersal shape used in Example 6) and **classifier mode** (a `Func<T,string> subQueueSelector` routes each item to a named sub-queue, where an unrecognized name is governed by [`UnknownKeyPolicy`](#classification-model)). Sub-queues are defined beforehand as weighted definitions and may themselves be any discipline.

All composite disciplines (Priority, WFQ, CBWFQ, LLQ, WRR) accept child queues, so a "priority band" or "traffic class" can itself be a WFQ or another composite — depth is unbounded by construction, satisfying the "no limit to the depth of the chain" requirement even within a single logical queue.

---

## Classification Model

Classification is how an incoming item is mapped to the place it waits — a priority band, a fair-queuing flow, a traffic class, a round-robin sub-queue. QoSKit does this with delegates the caller supplies at construction, so the payload type `T` never has to implement an interface, carry attributes, or wrap itself in an envelope. A classifier is an ordinary function from `T` to a band index, a flow key, a match decision, or a sub-queue name. The engine invokes it; the caller writes it.

### The classifier delegates (the callbacks)

Each discipline takes exactly the classifier it needs. These delegates are the classification callbacks — there is no separate registration API, no `IClassifier` object to implement (though you are free to make your delegate a method group on such a type). The cost selector is a second, orthogonal callback used only by the size-fair disciplines.

| Discipline | Classifier delegate | Returns | Cost callback |
|------------|--------------------|---------|---------------|
| FIFO / LIFO | none | — | — |
| Priority | `Func<T,int> prioritySelector` | band index (clamped to `[0, levels-1]`) | — |
| WFQ | `Func<T,string> flowSelector` | flow **key** (open namespace) | `Func<T,int> costSelector` |
| CBWFQ | per-class `Func<T,bool> matcher` | match / no-match, first-match-wins | `Func<T,int> costSelector` |
| LLQ | per-class `Func<T,bool> matcher` (priority + fair classes) | match / no-match | `Func<T,int> costSelector` |
| WRR (classifier mode) | `Func<T,string> subQueueSelector` | sub-queue **name** (closed set) | `Func<T,int> costSelector` |
| WRR (balancer mode) | none | — | `Func<T,int> costSelector` |

**Invocation semantics** are uniform and matter for correctness:

- A classifier runs **once per enqueue**, synchronously, on the calling thread, **before** the item is admitted or any counter moves. Because queues are thread-safe and many producers may enqueue at once, a classifier can be called concurrently — it must be pure and thread-safe. Keep it fast; it sits on the hot path.
- If a classifier or the cost selector **throws**, the exception propagates to the caller and the enqueue is abandoned with the queue unchanged — no half-inserted item, no counter movement (this is test case A36).
- The cost selector is bounded by the locked rule: a negative cost throws `ArgumentOutOfRangeException`, a zero cost is clamped to a floor of 1.

### Two classification paradigms

QoSKit deliberately offers two, and which one a discipline uses determines whether an "unrecognized string" is even possible.

**Predicate classification (match-based)** drives CBWFQ and LLQ. You declare classes with `Func<T,bool>` matchers; the engine evaluates them in declared order and the first match wins. There is always an implicit, non-removable `class-default`, so **an item can never fail to classify** — worst case it lands in the default class. Class *names* here are labels only: they tag metrics, enforce uniqueness, and identify a class in logs. They are never looked up to route an item. If you want name-driven classes, express it in the matcher itself — `matcher: t => Categorize(t) == "gold"`.

**Key classification (name-based)** drives WFQ and WRR-in-classifier-mode. Your selector returns a string that the engine looks up against a set of keys. This is the only place a string flows into a routing decision, and therefore the only place an "unrecognized string" can occur. The two disciplines treat the lookup namespace differently, which is the crux of the answer below.

### Names versus keys (where strings appear, and how they compare)

Two distinct uses of strings run through the library, and conflating them is the usual source of confusion:

- **Definition names** — every `PriorityBand`, `TrafficClass`, `WeightedFlow`, and `WeightedSubQueue` has a `Name`. Names identify a subordinate queue for statistics tags, uniqueness checks (a duplicate name throws `ArgumentException` at construction), and human-readable diagnostics. In the predicate disciplines a name has no effect on where an item goes.
- **Classification keys** — the strings a `flowSelector` or `subQueueSelector` returns. These are compared against defined keys to route the item.

A single `KeyComparer` option governs both, and it defaults to `StringComparer.OrdinalIgnoreCase`. Names and keys are human-authored identifiers far more often than not, so `"Gold"` and `"gold"` should name the same class and route the same way — a case mismatch is almost always a typo, not intent. Case-insensitive matching is the forgiving default the brief asks for. Two consequences follow: duplicate-name detection at construction is also case-insensitive, so declaring both `"Gold"` and `"gold"` throws `ArgumentException` as a collision; and a selector returning `"GPU-A"` matches a sub-queue defined as `"gpu-a"`. The one place case can legitimately matter is a WFQ flow key drawn from a machine-generated token (a base64 id, a signature) where two values differ only in case — a caller in that situation passes `KeyComparer = StringComparer.Ordinal` to get case-sensitive, culture-invariant matching back.

### Unrecognized keys: `UnknownKeyPolicy`

When a `flowSelector`/`subQueueSelector` returns a key that is not in the defined set — a typo, a case mismatch under ordinal comparison, a `null`/empty string, or simply a value the caller never registered — the outcome is governed by `UnknownKeyPolicy`:

| Policy | Behavior on an unrecognized key |
|--------|---------------------------------|
| `CreateDynamic` | Admit the key as a new flow/sub-queue at the configured default weight, created on first sighting. The WFQ model — flow keys (tenant ids, 5-tuples) are typically not enumerable ahead of time. |
| `RouteToDefault` | Send the item to the sub-queue/flow the caller designated as default at construction. Requires a default to exist. |
| `Reject` | Do not admit. `TryEnqueue` returns `false`; `Enqueue` throws `UnknownClassificationException`. `ItemDropped` fires with reason `UnknownClass`. |
| `Throw` | Always raise `UnknownClassificationException`, even from `TryEnqueue` — for callers who treat an unknown key as a hard programming error. |

A `null` or empty string returned by a selector is treated as an unrecognized key and follows the same policy — it is not a special-cased throw, so one code path covers typos and nulls alike.

**Per-discipline defaults, and the direct answer to the scenario.** WFQ defaults to `CreateDynamic` with a default flow weight of 1: a flow key the engine has never seen becomes a new fairly-scheduled flow, which is what makes WFQ usable over an open key space. WRR-in-classifier-mode defaults to `Throw` when no default sub-queue was designated, and to `RouteToDefault` when one was — because a WRR sub-queue set is closed and enumerated by the caller, so an unknown name is far more likely a bug than a legitimate new class.

So, concretely: if you build a WRR with sub-queues `"gpu-a"` and `"gpu-b"` and enqueue an item whose selector returns `"gpu-c"`, the default behavior throws `UnknownClassificationException` from `Enqueue` and returns `false` from `TryEnqueue`. A selector returning `"GPU-A"` is *not* unknown under the default case-insensitive comparer — it routes to `"gpu-a"`. Designate `"cpu"` as the default sub-queue, and the genuinely-unknown `"gpu-c"` item is routed there instead. Set the policy to `Reject`, and it is dropped with an `ItemDropped(UnknownClass)` event. Feed the identical selector to a WFQ, and `"gpu-c"` is simply admitted as a brand-new flow weighted at the default — no exception at all. The library never silently discards the item under any policy: it is admitted, routed to a default, or reported through an exception and a drop event.

### Explicit overrides

Every keyed or banded discipline also exposes an override that bypasses the selector for a single item, mirroring `Enqueue(item, priority)` from Example 2: `Enqueue(item, string key)` on WFQ and WRR, and `Enqueue(item, int band)` on Priority. An override is subject to the same `UnknownKeyPolicy` — passing an unregistered name to `WRR.Enqueue(item, "gpu-c")` under the default policy throws `UnknownClassificationException`, exactly as if a selector had produced it. Overrides are the seam that makes chaining clean: a downstream queue can reclassify an item on its own terms without the payload ever needing to carry classification metadata.

---

## Chaining and Fluency

Chaining is the feature the brief leans on hardest — arbitrary-depth queue hierarchies, wired in code — so it gets three levels of support: raw interfaces for full control, a fluent chain builder for readable composition, and a startable pipeline for lifecycle management. Fluency is a first-class requirement here: the chain-building calls return the builder so a whole topology reads as one expression, and the queue configuration helpers return the queue so setup reads the same way.

**Manual chaining** is just the interfaces: dequeue from a level-1 queue and enqueue into a level-2 queue in your own loop. Because everything is `IQoSSource<T>`/`IQoSSink<T>`, this needs no special support — it is the literal example from the brief (LLQ + WFQ feeding a priority queue, then dequeue the priority queue to get the next item to service). See Example 4.

**Fluent chaining** builds the topology as a single expression. `IQoSSource<T>.ChainTo(next)` returns a `QoSChain<T>` builder whose own `ChainTo` continues the chain; `Merge(...)` adds more upstreams into the current stage (the many-to-one case); `AsPipeline(name)` finalizes it; `StartAsync(token)` returns the running pipeline. The tail queue is what you dequeue from. See Examples 5 and 6.

```csharp
await using QoSPipeline<Job> pipeline = ingress
    .ChainTo(classifier)     // FIFO         -> CBWFQ
    .ChainTo(priority)       // CBWFQ        -> Priority
    .ChainTo(backends)       // Priority     -> WRR   (tail)
    .AsPipeline("dispatch")
    .StartAsync(token)
    .ConfigureAwait(false);

Job next = await backends.DequeueAsync(token).ConfigureAwait(false);
```

**Fluent configuration** applies the same style to a single queue. Each `With*` helper validates its argument with a guard clause, sets the backing field, and returns the concrete queue type so calls chain (`CODE_STYLE.md`: guard clauses at method start, explicit backing fields, no bare tunable constants — every knob is a settable property with a documented default/min/max):

```csharp
FifoQoSQueue<Job> ingress = new FifoQoSQueue<Job>()
    .WithName("ingress")
    .WithMaxDepth(10_000)
    .WithOverflowPolicy(OverflowPolicy.DropOldest);
```

**Underlying primitives.** The fluent surface is sugar over three explicit types, each usable directly:

- `IQoSSource<T>.DrainTo(IQoSSink<T> sink, int max)` moves up to `max` items downstream in one call and returns the `int` count moved — deliberately an `int`, not a tuple, per the "I hate tuples" rule in `CODE_STYLE.md`. Its async partner `DrainToAsync(sink, max, CancellationToken)` follows the rule that every `IEnumerable`/sync method that can have one gets a cancellable async variant.
- `QoSLink<T>` runs a background pump that continuously drains one or more upstream sources into a downstream sink, honoring backpressure (respects the sink's overflow policy; pauses when a `Block`-policy sink is full). It holds a `CancellationTokenSource` as a class member, so its async methods check cancellation rather than each taking a token (`CODE_STYLE.md` exception for classes that own a token). Full `IAsyncDisposable`.
- `QoSPipeline<T>` composes links into a named topology, exposes `StartAsync`/`StopAsync`/`DisposeAsync`, surfaces aggregate `Statistics`, and validates the graph (throws a specific `PipelineCycleException` on an unintended cycle rather than a generic exception). A pipeline is how "two level-1 queues feeding one level-2 priority queue" becomes a single startable unit.

Fan-out (one source, many sinks by a routing predicate) is provided by a `QoSRouter<T>` sink that classifies and forwards, so a full tree — classify, fan out to per-class queues, schedule, merge, schedule again — is expressible end to end and to unbounded depth.

---

## Exception Reference

Every throw in QoSKit is intentional and documented. This section is the authoritative contract: it lists every scenario that raises an exception, the exact type, and the context the exception carries. Each public method's XML docs carry the matching `<exception>` tags (`CODE_STYLE.md`), and each row here maps to a negative-path case in the [Testing Plan](#testing-plan).

Three rules govern the whole surface:

1. **Guard clauses run first.** Argument validation happens at method entry, before any state changes. A rejected call never leaves a queue half-mutated — if `Enqueue` throws, the item is not stored and no counter moves.
2. **`Try*` methods do not throw for the condition they test.** `TryEnqueue` returns `false` when full; `TryDequeue`/`TryPeek` return `false` when empty. They still throw for programming errors — a `null` item or a disposed instance — because those are bugs, not runtime conditions.
3. **A caller can catch broadly or narrowly.** All four domain exceptions derive from an abstract `QoSException`, so `catch (QoSException)` handles every QoSKit-specific failure, while the concrete types allow precise handling.

### Custom exception types (namespace `QoSKit.Exceptions`)

| Type | Base | Raised when | Context carried |
|------|------|-------------|-----------------|
| `QoSException` | `Exception` | Never thrown directly; the abstract base for the four below, so consumers can `catch (QoSException)`. | — |
| `QueueFullException` | `QoSException` | `Enqueue` is called on a queue at `MaxDepth` under `OverflowPolicy.Reject`. (`TryEnqueue` returns `false` instead of throwing.) | Queue `Name`, `MaxDepth`, current `Count`. |
| `QueueEmptyException` | `QoSException` | `Dequeue()` or `Peek()` is called on an empty queue. (`TryDequeue`/`TryPeek` return `false` instead.) | Queue `Name`. |
| `PipelineCycleException` | `QoSException` | A `QoSPipeline` is started (or validated) whose links form a cycle, including a queue linked to itself. | The offending `Name`s on the cycle path. |
| `UnroutableItemException` | `QoSException` | `Enqueue` on a `QoSRouter` with no matching route and no default route. (`TryEnqueue` returns `false` instead.) | Router `Name`. |
| `UnknownClassificationException` | `QoSException` | A `flowSelector`/`subQueueSelector` (or an explicit key override) yields a key not in the defined set, under `UnknownKeyPolicy.Throw`, or under `Reject` via `Enqueue`. See [Classification Model](#classification-model). | Queue `Name`, the offending key. |

Domain-specific failures use these types rather than a generic `Exception` (`CODE_STYLE.md`: "Use specific exception types," "custom exception types for domain-specific errors," "meaningful error messages with context").

### Standard exceptions and when they occur

**Construction** — every constructor guard-validates before building anything.

| Surface | Condition | Exception |
|---------|-----------|-----------|
| Any scheduling queue | `null` classifier delegate (`prioritySelector`, `flowSelector`, class `matcher`, route predicate) | `ArgumentNullException` |
| `PriorityQoSQueue` | `levels <= 0` | `ArgumentOutOfRangeException` |
| `WeightedFairQoSQueue` | `null` flow collection | `ArgumentNullException` |
| `WeightedFairQoSQueue` / `WeightedRoundRobin` / CBWFQ class | any flow/sub-queue/class `weight < 1` | `ArgumentOutOfRangeException` |
| `WeightedFairQoSQueue` / CBWFQ / WRR | duplicate flow key / class name / sub-queue name (compared under `KeyComparer`, case-insensitive by default) | `ArgumentException` |
| `WeightedRoundRobinQoSQueue` | `null` sub-queue collection, or a `null` element | `ArgumentNullException` |
| `WeightedRoundRobinQoSQueue` | empty sub-queue collection | `ArgumentException` |
| `TokenBucket` (LLQ policer) | `ratePerSecond <= 0`, or `burst < 0` | `ArgumentOutOfRangeException` |
| Definition types (`PriorityBand`, `WeightedFlow`, `TrafficClass`, `WeightedSubQueue`) | `null` name/matcher/queue | `ArgumentNullException` |
| Definition types | empty/whitespace name | `ArgumentException` |
| `QoSQueueOptions` | `MaxDepth < 0` | `ArgumentOutOfRangeException` |
| `QoSQueueOptions` | empty/whitespace `Name` (a `null` name auto-generates one) | `ArgumentException` |

CBWFQ constructed with an empty class list is **valid** (the implicit `class-default` serves everything). LLQ constructed with a `null` rate limit on a priority class is **valid** (unpoliced strict priority). Out-of-range priority bands from a selector are **clamped, not thrown** (see [Locked Default Behaviors](#locked-default-behaviors)).

**Enqueue**

| Method | Condition | Result |
|--------|-----------|--------|
| `Enqueue(item)` / `Enqueue(item, priority)` | `null` item (reference `T`) | `ArgumentNullException` |
| `Enqueue(item)` | queue full, policy `Reject` | `QueueFullException` |
| `Enqueue(item)` | queue full, policy `DropNewest`/`DropOldest` | no throw — a drop is recorded, `ItemDropped` fires |
| `Enqueue(item)` | after `Dispose()` | `ObjectDisposedException` |
| `Enqueue(item)` | the user's classifier/cost delegate itself throws | that delegate's exception propagates unchanged; queue state untouched |
| `Enqueue(item)` / `Enqueue(item, key)` | unrecognized classification key, policy `Throw` or `Reject` | `UnknownClassificationException` (see [Classification Model](#classification-model)) |
| `TryEnqueue(item)` | unrecognized key, policy `Throw` | `UnknownClassificationException` (the one case `TryEnqueue` throws for a classification miss) |
| `TryEnqueue(item)` | unrecognized key, policy `Reject` | returns `false` + `ItemDropped(UnknownClass)` |
| `TryEnqueue(item)` | `null` item | `ArgumentNullException` (validation still applies) |
| `TryEnqueue(item)` | queue full, policy `Reject` | returns `false` (no throw) |
| `TryEnqueue(item)` | after `Dispose()` | `ObjectDisposedException` |
| `IProducerConsumerCollection.TryAdd` | mirrors `TryEnqueue` | as above |

**Dequeue and peek**

| Method | Condition | Result |
|--------|-----------|--------|
| `Dequeue()` | empty queue | `QueueEmptyException` |
| `Dequeue()` / `Peek()` | after `Dispose()` | `ObjectDisposedException` |
| `Peek()` | empty queue | `QueueEmptyException` |
| `TryDequeue(out)` / `TryPeek(out)` | empty queue | returns `false` (no throw) |
| `TryDequeue(out)` / `TryPeek(out)` | after `Dispose()` | `ObjectDisposedException` |
| `IProducerConsumerCollection.TryTake` | mirrors `TryDequeue` | as above |
| `CopyTo(array, index)` | `null` array | `ArgumentNullException` |
| `CopyTo(array, index)` | `index < 0` | `ArgumentOutOfRangeException` |
| `CopyTo(array, index)` | insufficient space from `index` | `ArgumentException` |

**Async consumption**

| Method | Condition | Result |
|--------|-----------|--------|
| `DequeueAsync(ct)` | `ct` already cancelled | `OperationCanceledException` (thrown before waiting) |
| `DequeueAsync(ct)` | `ct` cancelled while waiting on an empty queue | `OperationCanceledException`; queue unchanged, no phantom dequeue |
| `DequeueAsync(ct)` | instance disposed while a waiter is pending | that waiter completes with `OperationCanceledException` |
| `DequeueAsync(ct)` | called after `Dispose()` | `ObjectDisposedException` |
| `ConsumeAsync(ct)` | `ct` cancelled | enumeration ends by throwing `OperationCanceledException` from the pending `MoveNextAsync` |

`DequeueAsync(CancellationToken.None)` on a queue that never receives an item waits indefinitely by design — it is unblocked only by an enqueue, cancellation, or disposal. This is documented on the method, not treated as an error.

**Fluent configuration and setters** — property setters and their `With*` equivalents share one validation path.

| Member | Condition | Exception |
|--------|-----------|-----------|
| `WithMaxDepth(n)` / `MaxDepth` setter | `n < 0` | `ArgumentOutOfRangeException` |
| `WithName(s)` / `Name` setter | `null`/empty/whitespace | `ArgumentException` |
| `WithOverflowPolicy(p)` | undefined enum value | `ArgumentOutOfRangeException` |

Lowering `MaxDepth` below the current `Count` does **not** throw and does **not** evict; the queue simply rejects new items until it drains below the new limit.

**Chaining, links, and pipelines**

| Method | Condition | Result |
|--------|-----------|--------|
| `DrainTo(sink, max)` / `DrainToAsync` | `null` sink | `ArgumentNullException` |
| `DrainTo(sink, max)` | `max < 0` | `ArgumentOutOfRangeException` |
| `DrainTo` | source or sink disposed | `ObjectDisposedException` |
| `DrainTo` | sink full (policy `Reject`) | no throw — drain stops, returns the count actually moved |
| `DrainToAsync(sink, max, ct)` | `ct` cancelled mid-drain | `OperationCanceledException`; already-moved items stay moved |
| `ChainTo(next)` | `null` next | `ArgumentNullException` |
| `Merge(sources)` | `null` array or a `null` element | `ArgumentNullException` |
| `Merge(sources)` | empty array | `ArgumentException` |
| `AsPipeline(name)` | `null` name | `ArgumentNullException` |
| `AsPipeline(name)` | empty/whitespace name | `ArgumentException` |
| `QoSPipeline.Link(from, to)` | `null` argument | `ArgumentNullException` |
| `QoSLink.StartAsync` / `QoSPipeline.StartAsync` | already running | `InvalidOperationException` |
| `QoSPipeline.StartAsync` | link graph contains a cycle or self-loop | `PipelineCycleException` |
| `StartAsync` / `StopAsync` | after `Dispose()`/`DisposeAsync()` | `ObjectDisposedException` |
| `QoSLink.StopAsync` / `QoSPipeline.StopAsync` | never started | no throw (safe no-op) |

**Router**

| Method | Condition | Result |
|--------|-----------|--------|
| `QoSRouter` ctor | `null` route table, or `null` predicate/sink in a route | `ArgumentNullException` |
| `QoSRouter` ctor | empty route table | `ArgumentException` |
| `Enqueue(item)` | no matching route, no default route | `UnroutableItemException` + `ItemDropped(Unroutable)` |
| `TryEnqueue(item)` | no matching route, no default route | returns `false` + `ItemDropped(Unroutable)` |
| `Enqueue(item)` | `null` item | `ArgumentNullException` |

## Observability

The telemetry requirements target HTTP services with the Watson stack, Prometheus, and Grafana; none of that applies to an in-process library, and there is no Docker or REST surface here. What does carry over is the principle: **emit measurements from the first commit, cheaply, and let the host collect them.**

QoSKit exposes a `System.Diagnostics.Metrics.Meter` named `QoSKit` with OpenTelemetry-shaped instrument names, so any host already running an OTel `MeterListener` or the .NET OTel SDK picks them up with zero QoSKit-specific glue:

- `qoskit.queue.depth` (observable gauge, tagged `queue.name`, `queue.type`)
- `qoskit.queue.enqueued` / `qoskit.queue.dequeued` / `qoskit.queue.dropped` / `qoskit.queue.rejected` (counters)
- `qoskit.queue.wait.duration` (histogram, ms)
- `qoskit.link.moved` (counter, for pipeline links)

Metrics are on by default and allocate nothing until a listener subscribes, matching the "ship it observable" stance. For hosts that don't run OTel, the same numbers are available synchronously through the `Statistics` snapshot on every queue and pipeline, so observability never requires the full stack. No `Console.WriteLine` anywhere in the library.

---

## Optional Persistence (SQLite)

In-memory is the default and stays that way: zero dependencies, zero disk, microsecond enqueue/dequeue. Persistence is purely additive and opt-in. Turned off — the shipping default — nothing below exists on the hot path. Turned on, queued items survive a process restart or crash, which is what a work-dispatch system needs when losing enqueued work is unacceptable. The whole feature hangs off **one seam** already built into the core, so no discipline, no classifier, and no chaining code changes to gain it.

### The seam: a pluggable backing store

Every leaf holds its items behind `IQoSStore<T>`, not a hardcoded structure. A priority band, a WFQ flow, a CBWFQ class, and a WRR sub-queue are all leaves, so making the leaf store pluggable makes every discipline persistable through the same seam. The default `InMemoryQoSStore<T>` is a thin wrapper over the in-memory structure; `SqliteQoSStore<T>` is the durable implementation.

The model is **in-memory index, SQLite journal** — not "read from SQLite on every dequeue." The scheduler keeps its fast in-memory index (the virtual-finish-time heap, band bitmap, round-robin pointer, deficit counters); SQLite is the write-through durability log behind it. SQLite is read on exactly one occasion: **recovery** at startup, when persisted entries are loaded, deserialized, and replayed in order to rebuild the index. Steady-state dequeues never touch disk to *read*; they only schedule a *delete*.

### What the architecture has to add

- **A serializer, because `T` is opaque.** The library cannot serialize an arbitrary payload, so persistence requires an `IQoSSerializer<T>` (`Serialize(T) → byte[]`, `Deserialize(byte[]) → T`). A `JsonQoSSerializer<T>` over `System.Text.Json` is the convenience default; anyone with a hotter path supplies their own (protobuf, MessagePack). A payload that fails to serialize throws on enqueue and is not admitted — persistence never silently drops.
- **A stored-entry envelope, internal only.** Rows carry `(seq, leafKey, cost, virtualFinishTime, enqueuedUtc, status, payload BLOB)`. The public API stays clean `T`; the envelope lives entirely inside the store. `seq` is a monotonic sequence that reproduces FIFO order and breaks virtual-time ties on recovery. A tiny per-queue scheduler-state row persists the scalar state (virtual clock, RR pointer, deficits) so recovery restores exact scheduling order rather than an approximation.
- **`EnqueueAsync`.** Sync `Enqueue` keeps working, but durable-write-acknowledged enqueue is inherently async, so `EnqueueAsync(item, CancellationToken)` joins the surface. It pairs with the existing `DequeueAsync` and is a no-cost pass-through in the in-memory build.
- **`QoSPersistenceOptions`.** `Enabled`, `FilePath`/connection string, `Serializer`, `Mode` (`WriteBehind`/`Durable`), `Synchronous` (`Normal`/`Full`/`Extra`, default `Full` under `Durable`), `GroupCommit` + window, `FlushInterval`/`BatchSize` (write-behind), and `Delivery` (`RemoveOnDequeue`/`LeaseAck`) with a `LeaseTimeout`. Two presets — `GuaranteedDelivery(filePath)` and `Fast(filePath)` — encode the two sensible ends of the spectrum. Validated with the same guard-clause discipline as every other options object.
- **A separate package.** `QoSKit.Persistence.Sqlite` references `Microsoft.Data.Sqlite` and provides `SqliteQoSStore<T>`; the core `QoSKit` package defines only the `IQoSStore<T>`/`IQoSSerializer<T>` abstractions and stays dependency-free with no native binary. You do not pull SQLite unless you install the package — that is what makes "optional" real rather than nominal.

### Durability modes, and how "no loss" is actually achieved

The performance-friendly ordering — index first, disk after — is correct for `WriteBehind`, where a small loss window is the accepted trade. It is **wrong for strict durability**, and the fix is the crux of a non-lossy guarantee: in `Durable` mode the commit happens *before* the item becomes visible to any consumer. An item that is not yet on stable storage must not be dequeuable, because a consumer could otherwise take and act on an item whose only record then vanishes in a crash. So the two modes deliberately invert their ordering.

| Mode | Ordering | Enqueue returns after | Loss window |
|------|----------|----------------------|-------------|
| `WriteBehind` (default when enabled) | visible in the index first; SQLite write batched by a background writer | the item is in the in-memory index | items written after the last batch commit are lost on a hard crash |
| `Durable` | **commit first, then make visible** | the `INSERT` has committed to stable storage (via `EnqueueAsync`) | **none** — once `EnqueueAsync` returns, the item is on disk and will be recovered |

`Durable` is only genuinely crash-proof if SQLite is told to flush all the way to the platter, so the mode carries its own pragmas rather than trusting defaults: `journal_mode=WAL` with `synchronous=FULL` (or `EXTRA` for the strictest ordering guarantee), so a committed transaction is fsync'd before it is acknowledged. `synchronous=NORMAL` — which can drop the last transaction on OS crash or power loss — is available but is not what `Durable` selects by default; choosing it is a conscious downgrade. Per-item fsync is slow (hundreds of commits per second), so `Durable` supports **group commit**: concurrent `EnqueueAsync` calls coalesce into one fsync and each returns only after the shared commit that includes it lands. You keep the strict guarantee and recover most of the throughput under concurrency, which is what makes strict durability practical rather than theoretical.

The liveness rule from the [concurrency guarantees](#concurrency-and-liveness-guarantees) still holds in both modes: **the fsync wait never happens under the queue's structural lock.** In `Durable` mode the enqueue reserves a depth slot under the short lock (so a bounded queue does not over-admit), releases the lock, awaits the commit off-lock, and only then re-enters to publish the item and signal waiters. A slow or stalled disk delays that one enqueue's acknowledgement; it never freezes dequeues or other producers. This is exactly what Suite T re-verifies by running the liveness suite against a deliberately-delayed SQLite store.

### The consumer side: why enqueue-durability alone is not end-to-end no-loss

Durable enqueue guarantees that **no acknowledged, not-yet-taken item is ever lost** — the queue survives restart with its backlog intact. It says nothing about an item that is *in flight* in a consumer when the process dies. With the default remove-on-dequeue (the row is deleted as the item is handed out), a consumer crash after dequeue but before the work completes loses that item — at-most-once, same as the in-memory queue. That is fine for a cache-warming workload and unacceptable for a payments workload, so the choice is explicit.

For end-to-end no-loss you also opt into the **lease/ack** delivery model: `TryLease` hands out an item marked invisible and returns a handle; the row is deleted only when the consumer calls `Ack` after the work is durably done; `Nack` or a lease-timeout returns the item for redelivery. A crash between lease and ack redelivers the item on restart. The honest cost is that this is **at-least-once**: a crash after the side effect but before the ack causes a duplicate. QoSKit cannot make an external consumer's side effect and the row deletion atomic on its own, so strict exactly-once requires the consumer to participate — either by being idempotent (dedupe on an item key) or, when the consumer's own state lives in the same SQLite database, by sharing the deletion's transaction. The library gives you no-loss unconditionally and hands you the two honest routes to exactly-once; it does not pretend to deliver exactly-once for free.

### The guaranteed-delivery profile

Because getting every knob right by hand is error-prone, the strict configuration is a named preset: `QoSPersistenceOptions.GuaranteedDelivery(filePath)` returns `Durable` + `WAL`/`synchronous=FULL` + group commit + lease/ack, the combination that loses nothing an acknowledged enqueue accepted, redelivers anything in flight during a crash, and requires only consumer idempotency to reach exactly-once. It is one call, it is a conscious opt-in, and it trades throughput for the guarantee — precisely the deliberate choice this should be. `QoSPersistenceOptions.Fast(filePath)` is the other preset (`WriteBehind`, remove-on-dequeue) for callers who want durability of the backlog with minimal overhead and accept a small loss window.

### Persistence across a chain

Each queue persists to its own table, keyed by the queue's `Name` — another reason names are stable, unique identifiers. Within a **single** SQLite database, a `QoSLink` hand-off (delete from the upstream table, insert into the downstream table) runs as one transaction, giving exactly-once movement across a stage boundary — a crash mid-move leaves the item in exactly one table, never both, never neither. This is the key to a **non-lossy pipeline**: put every queue in the chain in one database and use transactional hand-off, and an item is durable and accounted-for at every stage from head to tail, with no gap where a crash could drop it between queues. Across **separate** databases the hand-off degrades to at-least-once via lease/ack (crash after the downstream insert but before the upstream ack yields a duplicate, never a loss). A pipeline's durability is therefore composable and configurable per link, and the throughput-versus-guarantee choice is the operator's, not a library limitation.

### What does not change

The public queue API, every discipline's scheduling logic, the classification model, the exception contract, and all of chaining are untouched. Persistence is a store swap plus an options object plus one async enqueue overload. That containment is the point of routing everything through `IQoSStore<T>` from day one, even though the SQLite implementation lands in a later milestone.

---

## Repository Layout

```
QoSKit/
├── .gitignore                         # Visual Studio / .NET
├── README.md
├── CHANGELOG.md
├── LICENSE.md                         # MIT
├── QoSKit.sln
├── assets/
│   └── logo.png                       # referenced by README via explicit repo URL
├── src/
│   ├── QoSKit/
│   │   ├── QoSKit.csproj              # netstandard2.0;net8.0;net10.0
│   │   ├── Abstractions/              # interfaces + base class + options + enums + IQoSStore/IQoSSerializer
│   │   ├── Queues/                    # one file per discipline
│   │   ├── Definitions/               # PriorityBand, WeightedFlow, TrafficClass, etc.
│   │   ├── Chaining/                  # QoSLink, QoSPipeline, QoSRouter
│   │   ├── Persistence/               # InMemoryQoSStore, JsonQoSSerializer, QoSPersistenceOptions
│   │   ├── Telemetry/                 # Meter wiring, statistics
│   │   └── Exceptions/                # QoSException base + QueueFull/QueueEmpty/PipelineCycle/UnroutableItem
│   ├── QoSKit.Persistence.Sqlite/     # optional: SqliteQoSStore (Microsoft.Data.Sqlite); no dep in core
│   │   └── QoSKit.Persistence.Sqlite.csproj
│   ├── Test.Shared/                   # Touchstone.Core descriptors only
│   ├── Test.Automated/               # Touchstone.Cli console runner
│   ├── Test.Xunit/                    # Touchstone.XunitAdapter
│   ├── Test.Nunit/                    # Touchstone.NunitAdapter
│   └── QoSKit.Example/                # console demo — three 10s scenarios (see below)
│       ├── Program.cs                 # runs the three scenarios in sequence
│       ├── Scenarios/                 # THE QUEUING CODE — pure QoSKit, zero Console.*
│       │   ├── IDemoScenario.cs
│       │   ├── PriorityScenario.cs
│       │   ├── WeightedFairScenario.cs
│       │   └── ChainedScenario.cs
│       └── Harness/                   # THE DEMO/OUTPUT CODE — all formatting & IO
│           ├── IDemoReporter.cs       # the seam scenarios call
│           ├── ConsoleReporter.cs     # neat formatted console rendering
│           ├── ScenarioRunner.cs      # 10s window, producer/consumer tasks, summary
│           └── DemoItem.cs            # shared demo payload type
└── docs/
    └── GETTING_STARTED.md
```

Every source file holds exactly one class or one enum, using statements sit inside the namespace with system usings first (both alphabetized), nullable reference types on, and full XML docs on all public surface — per the code style rules. No `DOCKERHUB_README.md`, `REST_API.md`, `MCP_API.md`, Docker, or healthcheck files, because the library exposes no container, REST, or MCP surface; the plan will note this exclusion in the README rather than ship empty scaffolding.

---

## Usage Examples

Every snippet obeys `CODE_STYLE.md`: explicit types (never `var`), no tuples, `.ConfigureAwait(false)` on awaits, `CancellationToken` on async calls, and `_PascalCase` private fields. They are the contract the public API is being designed to satisfy, and they double as the acceptance criteria for the API surface.

### Example 1 — FIFO and LIFO: the `new / Enqueue / Dequeue` baseline

```csharp
FifoQoSQueue<string> fifo = new FifoQoSQueue<string>();
fifo.Enqueue("first");
fifo.Enqueue("second");
string a = fifo.Dequeue();               // "first"

LifoQoSQueue<string> lifo = new LifoQoSQueue<string>();
lifo.Enqueue("first");
lifo.Enqueue("second");
string b = lifo.Dequeue();               // "second"

if (fifo.TryDequeue(out string item))    // non-blocking, no exception when empty
{
    // handle 'item'
}
```

### Example 2 — Priority queue: delegate classifier plus per-item override

```csharp
// Lower selector value = higher priority (documented convention).
PriorityQoSQueue<Job> queue = new PriorityQoSQueue<Job>(
    levels: 4,
    prioritySelector: job => job.Urgency);

queue.Enqueue(job);                       // classified through the selector
queue.Enqueue(emergencyJob, priority: 0); // explicit override for one item

Job next = queue.Dequeue();               // highest-priority band first
```

### Example 3 — Bounded queue with overflow policy, configured fluently

```csharp
FifoQoSQueue<Packet> queue = new FifoQoSQueue<Packet>()
    .WithName("uplink")
    .WithMaxDepth(1_000)                          // 0 == unbounded (default)
    .WithOverflowPolicy(OverflowPolicy.DropOldest);

queue.ItemDropped += (sender, args) =>
    _Log.Warn($"dropped {args.Item.Id}: {args.Reason}");

for (int i = 0; i < 5_000; i++)
    queue.TryEnqueue(new Packet(i));              // oldest evicted past depth 1000
```

### Example 4 — Manual chaining (the brief's exact topology)

Two level-one queues — a low-latency queue and a weighted-fair queue — feeding one level-two priority queue, wired with nothing but the interfaces.

```csharp
LowLatencyQoSQueue<Job> realtime = new LowLatencyQoSQueue<Job>(/* classes... */);
WeightedFairQoSQueue<Job> fair    = new WeightedFairQoSQueue<Job>(
    flowSelector: job => job.TenantId,
    flows: tenantWeights);
PriorityQoSQueue<Job> level2      = new PriorityQoSQueue<Job>(
    levels: 2,
    prioritySelector: job => job.Interactive ? 0 : 1);

// The caller owns the relationship: drain level 1 into level 2.
realtime.DrainTo(level2, max: 256);
fair.DrainTo(level2, max: 256);

// Service whatever is next from the second-level queue.
Job serviced = level2.Dequeue();
```

### Example 5 — Fluent chaining into a running pipeline

The same intent as Example 4, expressed as one fluent chain with the runtime managing the pumps and backpressure.

```csharp
await using QoSPipeline<Job> pipeline = realtime
    .Merge(fair)                 // both level-1 sources...
    .ChainTo(level2)             // ...into the level-2 priority queue (tail)
    .AsPipeline("servicing")
    .StartAsync(token)
    .ConfigureAwait(false);

// Dequeue from the tail to get the next thing to service.
Job serviced = await level2.DequeueAsync(token).ConfigureAwait(false);
```

### Example 6 — Full user-space QoS for model-server dispatch (the uber goal)

Classify inference requests, keep interactive work ahead of batch with a policed low-latency class, schedule tenants fairly, then spread the survivors across backend model servers by weight — a complete two-level QoS system in code.

```csharp
namespace QoSKit.Examples
{
    using System.Threading;
    using System.Threading.Tasks;
    using QoSKit;

    public static class ModelServerDispatch
    {
        public static async Task RunAsync(CancellationToken token)
        {
            // Level 1: interactive jumps the line (rate-policed); tenants share the rest fairly.
            LowLatencyQoSQueue<InferenceRequest> ingress = new LowLatencyQoSQueue<InferenceRequest>(
                priorityClasses: new[]
                {
                    new TrafficClass<InferenceRequest>(
                        name: "interactive",
                        matcher: request => request.Interactive,
                        weight: 0,
                        rateLimit: new TokenBucket(ratePerSecond: 500, burst: 100))
                },
                fairClasses: new[]
                {
                    new TrafficClass<InferenceRequest>("gold",   r => r.Tier == Tier.Gold,   weight: 5),
                    new TrafficClass<InferenceRequest>("silver", r => r.Tier == Tier.Silver, weight: 3),
                    new TrafficClass<InferenceRequest>("bronze", r => r.Tier == Tier.Bronze, weight: 1)
                },
                costSelector: request => request.EstimatedTokens);

            // Level 2: weighted round robin across model-server backends.
            WeightedRoundRobinQoSQueue<InferenceRequest> backends =
                new WeightedRoundRobinQoSQueue<InferenceRequest>(
                    subQueues: new[]
                    {
                        new WeightedSubQueue<InferenceRequest>("gpu-a", weight: 4),
                        new WeightedSubQueue<InferenceRequest>("gpu-b", weight: 4),
                        new WeightedSubQueue<InferenceRequest>("cpu",   weight: 1)
                    });

            await using QoSPipeline<InferenceRequest> pipeline = ingress
                .ChainTo(backends)
                .AsPipeline("dispatch")
                .StartAsync(token)
                .ConfigureAwait(false);

            // Producers enqueue; the pipeline schedules; workers await the next unit of work.
            ingress.Enqueue(new InferenceRequest());
            InferenceRequest work = await backends.DequeueAsync(token).ConfigureAwait(false);
            // dispatch 'work' to the selected backend...
        }
    }
}
```

### Example 7 — Worker fan-out and BCL interop

Many workers draining one queue with no external locking (thread-safe by default), and the same queue standing in as a `BlockingCollection<T>` backing store via `IProducerConsumerCollection<T>`.

```csharp
FifoQoSQueue<Job> queue = new FifoQoSQueue<Job>();

// N workers await work concurrently.
Task[] workers = Enumerable.Range(0, workerCount).Select(_ => Task.Run(async () =>
{
    await foreach (Job job in queue.ConsumeAsync(token).ConfigureAwait(false))
        await DispatchAsync(job, token).ConfigureAwait(false);
})).ToArray();

// Or hand the queue to the BCL for classic blocking Take semantics.
using BlockingCollection<Job> blocking = new BlockingCollection<Job>(queue);
Job taken = blocking.Take(token);
```

---

## Example Application (`QoSKit.Example`)

`QoSKit.Example` is a plain console app (`dotnet run --project src/QoSKit.Example`) that runs three scenarios back to back, each for **ten seconds**, printing a clean live view of items being enqueued and dequeued and a summary when the window closes. It exists to make the library legible in thirty seconds: a reader sees priority ordering, weighted fairness, and a chained pipeline actually happening, not described.

### The one architectural rule: queuing code is quarantined from harness code

The project is split so a reader can jump straight to "here is the queuing part" without wading through console formatting, and vice versa. The seam is a reporter interface:

- **`Scenarios/` is the queuing code.** Each scenario builds a QoSKit queue, enqueues, and dequeues — and does nothing else. It contains **zero `Console.*` calls**. When it wants to say something happened, it calls `reporter.Enqueued(item)` / `reporter.Dequeued(item)` / `reporter.Note(...)`. A scenario file is short enough to read in one sitting and is pure QoSKit usage — exactly the code a prospective user came to see.
- **`Harness/` is the demo/output code.** `ConsoleReporter` implements `IDemoReporter` and owns every formatting decision: the per-scenario banner, the live counters, the scrolling event log, colors, and the closing summary table. `ScenarioRunner` owns the ten-second window — it starts producer and consumer tasks, drives the countdown, cancels cleanly at the deadline, and asks the reporter to render the summary. `DemoItem` is the shared payload.

```
Scenarios/PriorityScenario.cs   →  depends on QoSKit + IDemoReporter only   (the queuing)
Harness/ConsoleReporter.cs      →  depends on System.Console only            (the presentation)
```

Because scenarios depend only on `IDemoReporter`, the console can be swapped for a silent or capturing reporter without touching a line of queue code — the same separation the library itself draws between scheduling logic and observability. Each scenario implements a tiny `IDemoScenario` (`Name`, `Description`, `RunAsync(IDemoReporter, CancellationToken)`), and `Program.cs` simply iterates the three, handing each to `ScenarioRunner` with a 10-second budget.

### The three scenarios

**Scenario 1 — Simple priority queue.** A `PriorityQoSQueue<DemoItem>` with three bands (High / Normal / Low). A producer enqueues a mix of priorities at a steady rate; a consumer dequeues continuously. The live view shows enqueued-vs-serviced counts per band and a scrolling log, and the summary makes the point visible: High-band items are serviced ahead of Low even when they arrive later. Demonstrates `Enqueue`/`Dequeue` and strict priority ordering.

**Scenario 2 — Weighted fair queue.** A `WeightedFairQoSQueue<DemoItem>` with three flows weighted 5 : 3 : 1 (e.g. `tenant-a`, `tenant-b`, `tenant-c`), all kept backlogged so the scheduler is the thing under observation. The live view renders each flow's share of dequeues as a bar; over the ten seconds the ratio converges toward 5 : 3 : 1, and the summary prints measured service counts beside the configured weights. Demonstrates fair sharing under contention.

**Scenario 3 — Chained queues.** Two level-1 queues (a low-latency queue for interactive items and a weighted-fair queue for the rest) feed a level-2 `PriorityQoSQueue<DemoItem>` through a `QoSPipeline`; the consumer dequeues only from the level-2 tail. The live view shows items entering at stage 1, the pipeline moving them, and the tail servicing them in priority order — the arbitrary-depth composition story in motion. Demonstrates chaining and dequeuing "whatever is next to be serviced" from the end of a multi-stage topology.

### Output shape

Each scenario prints a banner (name, one-line description, a ten-second countdown), then a compact live dashboard updated as work flows, then a summary block separated by a rule from the next scenario. Output is ANSI-colored on a TTY and plain when redirected (matching the Touchstone runner's redirection-awareness), so piping to a file stays readable. It is an ordinary foreground console app that exits 0 when the third summary has printed. `QoSKit.Example` is a demo, not a test target — the behaviors it shows are what Suites D, E, and L verify — so it is excluded from the test run but included in the build so it can never rot.

---

## Testing Plan

The goal is coverage approaching 100% of lines and branches across every discipline and the entire chaining layer, split deliberately between positive cases (correct inputs produce correct outputs and ordering) and negative cases (bad inputs are rejected with the documented exception, and no operation leaves a queue in a corrupt state). The catalog below is the actual work list — each row becomes one `TestCaseDescriptor` in `Test.Shared`. It is written to be exhaustive on purpose, then reviewed twice for gaps (see [Coverage Review Passes](#coverage-review-passes)); the review findings are already folded into the tables.

### Tooling, structure, and determinism

Tests follow the Touchstone four-project layout from `BACKEND_TEST_ARCHITECTURE.md` exactly: descriptors live in `Test.Shared` (references `Touchstone.Core` `0.1.12` and `QoSKit` only, no console output, assertions are thrown exceptions), executed through `Test.Automated` (console runner, `--results results.json`, exit 0/1), `Test.Xunit`, and `Test.Nunit`. All test projects multi-target `net8.0;net10.0`; the library also builds `netstandard2.0`, so the suite runs against the modern TFMs while the `netstandard2.0` polyfill paths are covered by conditional cases that assert identical behavior. Any loopback helper uses `127.0.0.1`, never `localhost`.

Determinism is engineered in, because flaky QoS tests are worthless:

- **Injected time.** Aging and the policer read `IQoSTimeProvider`; tests use `ManualQoSTimeProvider` and advance time explicitly. No `Thread.Sleep`, no wall-clock tolerances on time-based logic.
- **Fixed-seed randomness.** Any test that generates a workload distribution seeds its RNG with a constant. `Date.now()`/`Math.random()` equivalents are never used to drive assertions.
- **Fairness tolerance.** Weighted-share assertions run a large fixed sample (e.g. 100k items) and assert measured service ratios fall within a stated tolerance band of configured weights — the band is a named constant per suite, documented with its rationale.
- **Async without races.** `DequeueAsync` wakeup tests use `Task.WhenAny` against a bounded timeout token; a timeout is a failure, not a hang.

### Shared test helpers (`Test.Shared`, built once, reused by every suite)

- `Item` / `KeyedItem` — small reference and value payload types carrying an `int Id`, an optional `Priority`, `FlowKey`, `Cost`, and `Tier`, so classification, ordering, and conservation can all be asserted on identity.
- `QueueFactories` — a list pairing each discipline name with a `Func<IQoSQueue<Item>>` that constructs a minimal valid instance, so the **Shared Queue Contract** suite (Suite A) runs against all seven disciplines from one descriptor set.
- `Conservation` — wraps a queue or pipeline and asserts the invariant `enqueued == dequeued + dropped + rejected + stillResident` after any scenario. This is the single most important cross-cutting check and every non-trivial scenario ends with it.
- `Drain` — pulls every item out of a source into an ordered `List<Item>` for ordering assertions; async variant with a timeout.
- `ServiceRatios` — dequeues N items from a scheduling queue and returns per-class/flow counts for fairness assertions (returns a `Dictionary`, never a tuple).
- `ConcurrencyHarness` — spins up P producers enqueuing disjoint id ranges and C consumers draining into a concurrent bag, then asserts the union of consumed ids equals the produced set (no loss, no duplication) and the queue ends empty.
- `Recorder` — subscribes to `ItemEnqueued`/`ItemDequeued`/`ItemDropped` and records ordered event args for assertion, plus a mode that leaves events unsubscribed to prove the no-listener path allocates and fires nothing.
- `Watchdog` — runs a scenario under a bounded timeout and fails (rather than hangs) if it does not complete, capturing a thread/stack snapshot on expiry. Every concurrency and liveness case runs inside a `Watchdog` so a deadlock surfaces as a red test in seconds, never as a stuck CI job.
- `FaultInjector` — produces classifiers, cost selectors, event handlers, and drop callbacks that throw on demand: on the Nth call, on a seeded pseudo-random schedule, or on a specific item id. Used to drive every exception path while other threads keep enqueuing and dequeuing.
- `SyncProbe` — reads the internal waiter-count and semaphore permit-count exposed to `Test.Shared` and asserts they equal their baseline (0 pending waiters, full permits) once a scenario has quiesced, turning "no leak" from a hope into an assertion.
- `SoakConfig` — resolves soak duration from `QOSKIT_SOAK_SECONDS` (default 10s for PR CI; the nightly job sets 300s), so the same descriptors serve both a fast gate and a long endurance run.

---

### Suite A — Shared Queue Contract (runs against **all seven** disciplines via `QueueFactories`)

Every row here executes once per discipline. A discipline that cannot satisfy a row (there are none expected) would mark it `skip` with a reason rather than silently omit it.

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| A01 | + | New queue: `Count == 0`, `IsEmpty == true`, `TryPeek` false, `TryDequeue` false. |
| A02 | + | Enqueue one, `Count == 1`, `IsEmpty == false`, `TryPeek` true and does **not** remove. |
| A03 | + | Enqueue then Dequeue returns the item; queue empty again; stats show 1 enqueued / 1 dequeued. |
| A04 | − | `Dequeue()` on empty throws `QueueEmptyException`; queue still empty; no stats change. |
| A05 | + | Enqueue N, Dequeue N: exactly N items out, queue empty, conservation holds. |
| A06 | + | Interleaved enqueue/dequeue (enqueue 3, deq 1, enqueue 2, deq 4) leaves correct residual count. |
| A07 | + | `Clear()` on a populated queue empties it, raises no dequeue events, and updates depth to 0. |
| A08 | + | `Clear()` on an empty queue is a no-op and does not throw. |
| A09 | + | `Peek`/`TryPeek` never mutates: repeated peeks return the same next item, `Count` unchanged. |
| A10 | + | `Count`, `IsEmpty`, and `Statistics.CurrentDepth` stay consistent through a mixed operation sequence. |
| A11 | + | `Statistics` counters (enqueued, dequeued, peak depth) are exact after a scripted sequence. |
| A12 | + | Peak depth records the high-water mark even after the queue drains back down. |
| A13 | + | `MaxDepth == 0` means unbounded: enqueue a large N with no rejection. |
| A14 | − | `MaxDepth == k`, policy `Reject`: k+1-th `TryEnqueue` returns false; `Enqueue` throws `QueueFullException`; depth stays k; `Statistics.Rejected == 1`. |
| A15 | + | Policy `DropNewest` at capacity: new item rejected silently, `ItemDropped` fires with reason `Newest`, existing order intact. |
| A16 | + | Policy `DropOldest` at capacity: oldest evicted, new item admitted, `ItemDropped` fires with the evicted item and reason `Oldest`, depth stays k. |
| A17 | + | `DequeueAsync` on a non-empty queue completes synchronously with the next item. |
| A18 | + | `DequeueAsync` on an empty queue completes once an item is enqueued from another task. |
| A19 | − | `DequeueAsync` with an already-cancelled token throws `OperationCanceledException` immediately. |
| A20 | − | `DequeueAsync` waiting on empty, then token cancelled: throws `OperationCanceledException`, queue still empty, no phantom dequeue. |
| A21 | + | Two `DequeueAsync` waiters, two enqueues: both complete, each gets a distinct item, wakeup order is FIFO. |
| A22 | + | `ConsumeAsync` yields items in order and stops cleanly when its token cancels; no item lost mid-stream. |
| A23 | + | `IProducerConsumerCollection`: `TryAdd`/`TryTake` mirror enqueue/dequeue; `ToArray` and `CopyTo` return a consistent snapshot. |
| A24 | + | Wrapped in `BlockingCollection<T>`: `Add`/`Take` work; bounded `BlockingCollection` respects `MaxDepth`. |
| A25 | + | `GetEnumerator` returns a point-in-time snapshot; enumerating does not remove items and does not throw if the queue mutates concurrently. |
| A26 | + | `IReadOnlyCollection.Count` matches `Count`. |
| A27 | − | Enqueue `null` (reference `T`) throws `ArgumentNullException`, uniformly across all disciplines; queue unchanged, no stats change. |
| A28 | + | Value-type `T` (`int`) round-trips through enqueue/dequeue with correct ordering. |
| A29 | + | Concurrency: `ConcurrencyHarness` with 8 producers / 8 consumers, 100k items — union consumed equals produced, queue ends empty, conservation holds. |
| A30 | + | Concurrent `Count`/`Statistics` reads during load never throw and never report negative or out-of-range values. |
| A31 | + | `Dispose()` is idempotent (second call is a no-op, no throw). |
| A32 | − | Any operation after `Dispose()` throws `ObjectDisposedException`. |
| A33 | + | `Dispose()` with outstanding `DequeueAsync` waiters completes them with `OperationCanceledException` — no hang. |
| A34 | + | `IAsyncDisposable.DisposeAsync` mirrors `Dispose` and awaits any pump shutdown. |
| A35 | − | Reentrancy: an `ItemDequeued` handler that enqueues back into the same queue does not deadlock and does not corrupt order. |
| A36 | − | A classifier/selector that throws propagates the exception to the caller and leaves the queue unchanged (no half-enqueued item, depth unchanged). |
| A37 | + | No-listener path: with no event handlers attached, a scripted run produces identical results (proving events are opt-in and side-effect-free). |

---

### Suite B — FIFO ordering

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| B01 | + | Enqueue 1..N, dequeue all → exact insertion order. |
| B02 | + | Interleaved enqueue/dequeue preserves FIFO across the interleaving. |
| B03 | + | Duplicate-valued items retain relative arrival order (stability). |
| B04 | + | `DropOldest` at capacity evicts the front (true oldest), remaining order preserved. |
| B05 | + | Snapshot enumeration yields front-to-back order. |

### Suite C — LIFO ordering

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| C01 | + | Enqueue 1..N, dequeue all → reverse order. |
| C02 | + | Enqueue 3, dequeue 1 (newest), enqueue 2 more, dequeue rest → strict stack order throughout. |
| C03 | + | `TryTake` (via `IProducerConsumerCollection`) removes the top, matching `ConcurrentStack` semantics. |
| C04 | + | `DropOldest` at capacity on a LIFO evicts the bottom-of-stack item; `DropNewest` rejects the incoming push. |
| C05 | + | Peek returns the top without removing it. |

### Suite D — Priority queue

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| D01 | + | Mixed priorities in, dequeue order is strictly by band (lower value first). |
| D02 | + | Within one band, items are FIFO. |
| D03 | + | `Enqueue(item, priority)` override beats the selector for that item only. |
| D04 | + | Empty high band is skipped; next non-empty band serves. |
| D05 | + | All-same-priority reduces to FIFO. |
| D06 | + | Selector returns a band `< 0` clamps to band 0 (highest); `>= levels` clamps to `levels-1` (lowest). Item is served at the clamped band; no throw. |
| D07 | − | Construct with `levels <= 0` throws `ArgumentOutOfRangeException`. |
| D08 | − | Construct with a null `prioritySelector` throws `ArgumentNullException`. |
| D09 | + | Aging disabled (default): a starved low band never advances while a high band is fed — proves aging is off by default. |
| D10 | + | Aging enabled: advance `ManualQoSTimeProvider` past the threshold → a waiting low-priority item is promoted and served ahead of newly-arrived high-priority items. |
| D11 | + | Aging promotion is bounded and monotonic (an item promotes at most to the top band; no oscillation). |
| D12 | + | Per-band capacity: filling one band does not reject items destined for another band. |
| D13 | + | Conservation across a mixed-priority workload. |

### Suite E — Weighted Fair Queuing (WFQ)

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| E01 | + | Two flows, weights 3:1, uniform arrivals → service ratio ≈ 3:1 within tolerance over 100k items. |
| E02 | + | Equal weights → equal service (round-robin-like fairness). |
| E03 | + | Single flow → behaves as FIFO. |
| E04 | + | Unlisted/new flow key gets the default weight and is still served. |
| E05 | + | Cost selector > 1: a flow sending large-cost items gets proportionally fewer dequeues per unit weight (byte-fairness, not packet-fairness). |
| E06 | + | Backlogged vs idle: an idle flow that becomes active is not penalized for its idle period (virtual clock does not let it monopolize — GPS property). |
| E07 | + | Virtual finish times are monotonic within a flow; dequeue always pops the global minimum. |
| E08 | + | Three flows 6:3:1 converge to the configured proportion. |
| E09 | − | Null `flowSelector` throws `ArgumentNullException`. |
| E10 | − | A flow weight `<= 0` throws `ArgumentOutOfRangeException` at construction. |
| E11 | − | Cost selector returns a negative value → `ArgumentOutOfRangeException`. |
| E12 | + | Cost selector returning 0 is clamped to the floor of 1 (no divide-by-zero in virtual-time math). |
| E13 | + | Ordering within a single flow remains FIFO regardless of interleaving with other flows. |
| E14 | + | Conservation across all flows. |

### Suite F — Class-Based WFQ (CBWFQ)

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| F01 | + | Item matching class X routes to X; dequeue reflects X's weight share. |
| F02 | + | First-match-wins when two class matchers overlap; the earlier-declared class captures the item. |
| F03 | + | Unmatched item falls to the implicit `class-default`. |
| F04 | + | `class-default` always exists and cannot be removed: a queue constructed with only specific classes still serves unmatched items via the default; its weight is configurable. |
| F05 | + | Weighted share across 3 classes converges to configured weights. |
| F06 | + | Per-class `MaxDepth`: filling class A (Reject) does not block enqueues to class B. |
| F07 | + | Per-class overflow policy is honored independently per class. |
| F08 | + | A class backed by a non-FIFO leaf (e.g. LIFO) preserves that leaf's ordering internally. |
| F09 | − | Null class list, empty class list, or null matcher → documented `ArgumentException`/`ArgumentNullException`. |
| F10 | − | Duplicate class names → `ArgumentException` at construction. |
| F11 | + | Class selection is deterministic and stable across runs for the same input. |
| F12 | + | Conservation across all classes plus the default. |

### Suite G — Low-Latency Queuing (LLQ)

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| G01 | + | Priority-class item is served ahead of all fair-class items while within its rate. |
| G02 | + | Within the token-bucket rate, priority traffic consistently jumps the line (advance `ManualQoSTimeProvider` to refill tokens). |
| G03 | + | With a rate limit set, excess priority traffic above the rate is demoted to fair scheduling rather than starving fair classes. |
| G04 | + | Burst up to bucket size is admitted at priority; beyond burst is policed. |
| G05 | + | With the priority class idle, fair classes get the full weighted share (no reserved-but-unused capacity wasted). |
| G06 | + | Multiple priority classes are served in their declared order. |
| G07 | + | Fair classes below the priority class still converge to their relative weights among themselves. |
| G08 | + | Priority class with a null rate limit is admitted as unpoliced strict priority (documented as able to starve fair classes); it is served ahead of all fair classes with no policing. |
| G09 | − | Invalid token-bucket params (rate `<= 0`, burst `< 0`) throw `ArgumentOutOfRangeException`. |
| G10 | + | Policer refill is time-correct: exactly `rate * elapsed` tokens accrue as manual time advances, capped at burst. |
| G11 | + | Conservation across priority + fair classes including any policed demotions/drops. |

### Suite H — Weighted Round Robin / Deficit WRR

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| H01 | + | Classic WRR, weights 2:1, all backlogged → over one full round each sub-queue is served weight-many times. |
| H02 | + | Single sub-queue reduces to FIFO. |
| H03 | + | Empty sub-queue is skipped without stalling the rotation. |
| H04 | + | A sub-queue that empties mid-round is skipped for its remaining turns; rotation continues. |
| H05 | − | Sub-queue with weight `< 1` throws `ArgumentOutOfRangeException` at construction (no silently-never-served queue). |
| H06 | + | DWRR with a cost selector and mixed item sizes → byte-fair service proportional to weight, not item-count-fair. |
| H07 | + | DWRR deficit carries over across rounds (a sub-queue under-served in one round gets its deficit next round). |
| H08 | + | DWRR with uniform cost degenerates to classic WRR behavior. |
| H09 | + | Round-robin pointer advances fairly under continuous enqueue (no sub-queue starved, no sub-queue favored beyond weight). |
| H10 | − | Null sub-queue list, empty list, null sub-queue, or duplicate names → documented exceptions. |
| H11 | − | Negative weight → `ArgumentOutOfRangeException`. |
| H12 | + | Within each sub-queue, item ordering follows that sub-queue's discipline. |
| H13 | + | Conservation across all sub-queues. |

---

### Suite I — `DrainTo` / `DrainToAsync` primitives

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| I01 | + | `DrainTo(sink, max)` moves `min(max, available)` items and returns that exact `int` count. |
| I02 | + | Draining an empty source returns 0 and does not touch the sink. |
| I03 | + | Order is preserved: items arrive at the sink in source-dequeue order. |
| I04 | + | Draining into a bounded sink at capacity respects the sink's overflow policy (Reject stops the drain at capacity; DropOldest keeps flowing) — count reflects what was actually admitted. |
| I05 | + | `max` larger than available drains everything and returns the available count. |
| I06 | + | `max == 0` moves nothing and returns 0. |
| I07 | − | `max < 0` throws `ArgumentOutOfRangeException`; null sink throws `ArgumentNullException`. |
| I08 | + | `DrainToAsync` moves items and honors cancellation mid-drain (partial move reported, no item lost or duplicated). |
| I09 | + | Conservation across source + sink after a drain (nothing vanishes). |

### Suite J — `QoSChain` fluent builder

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| J01 | + | `a.ChainTo(b)` returns a chain whose tail is `b`; `AsPipeline` yields a pipeline referencing both. |
| J02 | + | `a.ChainTo(b).ChainTo(c)` builds a 3-stage linear topology in declared order. |
| J03 | + | `Merge(x, y).ChainTo(z)` builds a many-to-one topology with x and y as upstreams of z. |
| J04 | + | End-to-end ordering through a linear chain matches a hand-wired manual drain of the same queues. |
| J05 | − | `ChainTo(null)` / `AsPipeline(null-or-empty name)` throw the documented argument exceptions. |
| J06 | + | The tail queue returned is the one the caller dequeues from; items enqueued at the head arrive there. |

### Suite K — `QoSLink` background pump

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| K01 | + | A started link continuously moves items from source to sink until stopped. |
| K02 | + | Backpressure: with a `Block`-policy full sink, the link pauses and resumes when the sink drains — no busy-spin, no loss. |
| K03 | + | Many-to-one link fairly drains multiple sources into one sink (no source starved). |
| K04 | + | `StopAsync` halts the pump promptly; no items moved after stop; in-flight item not duplicated. |
| K05 | + | `DisposeAsync` stops the pump and releases its `CancellationTokenSource`. |
| K06 | − | `StartAsync` on an already-running link throws `InvalidOperationException`; a `StopAsync` before `StartAsync` is a safe no-op. |
| K07 | + | Batch size and drain interval from `QoSLinkOptions` are honored (observable via move-count cadence with manual time). |
| K08 | + | On-drop callback fires when a moved item is rejected by the sink. |
| K09 | + | Conservation across source, link, and sink over a full run. |

### Suite L — `QoSPipeline` lifecycle & topology

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| L01 | + | `StartAsync` starts all links; `StopAsync` stops all; `DisposeAsync` disposes all. |
| L02 | + | Two level-1 queues → one level-2 priority queue (the brief's topology): dequeuing the tail yields items honoring the level-2 priority order. |
| L03 | + | Aggregate `Statistics` sum per-link and per-queue counters correctly. |
| L04 | − | A pipeline whose links form a cycle throws `PipelineCycleException` at `StartAsync`/validate time. |
| L05 | − | A self-loop (queue chained to itself) is detected and rejected. |
| L06 | + | Deep chain (10 stages of alternating disciplines): an item enqueued at the head reaches the tail; end-to-end count and conservation hold. |
| L07 | − | Double `StartAsync` guarded; `StopAsync` before `StartAsync` is a safe no-op. |
| L08 | + | Restart (`StartAsync` after `StopAsync`) resumes movement. |
| L09 | + | Concurrency: producers at the head and consumers at the tail of a multi-stage pipeline — union of consumed equals produced, nothing lost or duplicated end to end. |
| L10 | + | Disposing the pipeline completes any tail-queue `DequeueAsync` waiters without hanging. |

### Suite M — `QoSRouter` fan-out

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| M01 | + | Router forwards each item to the sink chosen by its predicate. |
| M02 | + | Unmatched items go to the default route when one is configured. With no default route, an unmatched item is rejected (`TryEnqueue` false / `Enqueue` throws `UnroutableItemException`) and `ItemDropped` fires with reason `Unroutable`. |
| M03 | + | Every downstream receives exactly its subset; the union across downstreams equals the input (partition, no loss, no duplication). |
| M04 | + | First-match-wins for overlapping predicates. |
| M05 | − | Null predicate, null sink, or empty route table → documented exceptions. |
| M06 | + | Conservation across all downstream sinks plus the default. |

### Suite N — Composite / nested depth

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| N01 | + | A priority band whose leaf is itself a WFQ: items are scheduled by the inner WFQ within the band and by strict priority across bands. |
| N02 | + | A CBWFQ class backed by an LLQ: policing applies inside the class while the class competes fairly outside. |
| N03 | + | WRR of WFQs: two-level weighting composes (outer round-robin weight × inner fair share) to the expected joint distribution. |
| N04 | + | Five-level nested composite: conservation and end-to-end reachability hold. |
| N05 | + | Nested overflow: an inner bounded queue rejecting does not corrupt the outer scheduler's accounting. |
| N06 | + | Disposing an outer composite disposes its inner queues (no leaked pumps/waiters). |

### Suite O — End-to-end invariants & stress

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| O01 | + | Grand conservation: across a full multi-discipline pipeline under concurrent load, `producedIds == consumedIds ∪ droppedIds ∪ residentIds` with empty intersections. |
| O02 | + | No item is ever dequeued twice anywhere in a pipeline (global uniqueness under concurrency). |
| O03 | + | No item is silently lost: any item not delivered is accounted as a recorded drop/reject with a reason. |
| O04 | + | Sustained soak (bounded duration, high rate) shows stable memory (no unbounded growth) and no deadlock. |
| O05 | + | Ordering guarantee holds under load where a discipline promises it (per-flow FIFO in WFQ, per-band FIFO in Priority) even with many concurrent producers. |
| O06 | + | Cancellation storm: cancelling many `DequeueAsync`/`ConsumeAsync` consumers concurrently leaves the queue consistent and re-drainable. |

### Suite P — Telemetry & statistics

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| P01 | + | A `MeterListener` attached to the `QoSKit` meter observes `qoskit.queue.depth`, `enqueued`, `dequeued`, `dropped`, `rejected`, and `wait.duration` with correct `queue.name`/`queue.type` tags. |
| P02 | + | Counters emitted match the synchronous `Statistics` snapshot exactly. |
| P03 | + | With metrics disabled via options, no instruments are published and the hot path still works. |
| P04 | + | `qoskit.link.moved` increments per item a link moves. |
| P05 | + | Wait-duration histogram reflects `ManualQoSTimeProvider`-controlled residency time. |

### Suite Q — Classification semantics (cross-cutting)

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| Q01 | + | The classifier delegate is invoked exactly once per enqueue, on the calling thread, before admission (verified via a counting delegate). |
| Q02 | + | Priority band clamping: selector values below 0 and at/above `levels` are clamped to the end bands (mirrors D06). |
| Q03 | + | Predicate disciplines never fail to classify: a stream containing items matching no declared class all land in `class-default` (CBWFQ/LLQ). |
| Q04 | + | WFQ unknown key, default `CreateDynamic`: an unseen `flowSelector` result is admitted as a new flow at the default weight and scheduled fairly. |
| Q05 | − | WRR classifier mode, default `Throw`, unknown key from selector → `UnknownClassificationException` on `Enqueue`; `TryEnqueue` returns per the policy row. |
| Q06 | + | WRR classifier mode with a designated default sub-queue (`RouteToDefault`): unknown key is routed to the default, no throw. |
| Q07 | + | `Reject` policy: unknown key → `TryEnqueue` false / `Enqueue` throws `UnknownClassificationException`, and `ItemDropped(UnknownClass)` fires; conservation still balances. |
| Q08 | + | Key comparison is case-insensitive by default: a selector returning `"GPU-A"` matches a sub-queue/flow defined as `"gpu-a"` and routes there — no unknown-key handling triggered. |
| Q09 | + | `KeyComparer = Ordinal`: the same case-mismatched key is now distinct and treated as unknown under the queue's policy. |
| Q09b | − | Case-insensitive duplicate-name detection: declaring classes/sub-queues `"Gold"` and `"gold"` throws `ArgumentException` at construction under the default comparer. |
| Q10 | + | A `null` or empty string from a selector is handled by `UnknownKeyPolicy` (not a special-cased throw). |
| Q11 | − | Explicit override with an unknown key — `WRR.Enqueue(item, "gpu-c")` — is subject to the same policy as a selector result (throws under `Throw`). |
| Q12 | + | Explicit override with a known key bypasses the selector and routes to that key/band exactly. |
| Q13 | − | A classifier that throws aborts the enqueue with the queue unchanged and the delegate's exception surfaced (shared with A36). |
| Q14 | + | Cross-discipline: the same payload type flows through WFQ (open keys) then WRR (closed keys) with each applying its own classifier — no classification metadata on `T`. |
| Q15 | + | No silent loss under any policy: every unknown-key item is accounted as admitted, routed-to-default, or dropped-with-reason (conservation across the policy matrix). |

### Suite R — Concurrency & stress (runs against every discipline and a full pipeline)

Each row runs inside a `Watchdog`. The matrix cases run once per discipline via `QueueFactories`; the pipeline cases run against a representative multi-stage topology.

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| R01 | + | Balanced load: 8 producers / 8 consumers, 1M items — consumed id set equals produced set, queue ends empty, conservation holds. |
| R02 | + | Producer-heavy (16 producers / 2 consumers) on a bounded queue: overflow policy applies correctly under contention; admitted + dropped + rejected == offered. |
| R03 | + | Consumer-heavy (2 producers / 16 consumers): idle consumers block in `DequeueAsync` and each item is delivered to exactly one consumer (no double-delivery, no missed wakeup). |
| R04 | + | Single producer / many consumers: strict per-source ordering guarantees a discipline promises still hold (per-flow FIFO in WFQ, per-band FIFO in Priority) despite concurrent draining. |
| R05 | + | Bursty arrivals: enqueue in large bursts separated by drains; no wakeup is lost across the idle-to-busy transition (every waiter that should wake, wakes). |
| R06 | + | Contention hot-spot: all producers target one WFQ flow / one CBWFQ class / one WRR sub-queue; weighted shares still converge and no thread starves. |
| R07 | + | Concurrent `Enqueue` + `Clear` + `Count`/`Statistics` reads: no torn state, no negative or out-of-range counters, conservation reconciles (cleared items counted as removed). |
| R08 | + | Mixed sync/async consumers on the same queue (`TryDequeue` spinners alongside `DequeueAsync` awaiters): every item taken exactly once. |
| R09 | + | `BlockingCollection<T>` wrapper under concurrent `Add`/`Take` from many threads behaves and conserves. |
| R10 | + | Full pipeline under load: producers at the head, consumers at the tail of a 4-stage chain with background `QoSLink` pumps — end-to-end conservation and global dequeue-uniqueness under sustained concurrency. |
| R11 | + | Fairness-under-contention: weighted disciplines hit their target ratios within tolerance when driven by many threads, not just single-threaded (guards against a lock-serialization bias). |
| R12 | + | Rapid create/enqueue/drain/dispose churn across many short-lived queues concurrently — no cross-instance interference, every instance disposes clean. |

### Suite S — Liveness & resource integrity (no held lock/semaphore ever stalls servicing)

The purpose of this suite is to prove that no lock or semaphore permit is ever leaked — especially on exception and cancellation paths — so servicing can never wedge. Every case runs inside a `Watchdog` (a hang is a failure) and ends with a `SyncProbe` baseline assertion (pending waiters == 0, semaphore permits full) plus a **post-fault liveness check**: after the fault storm, a fresh enqueue/dequeue must complete within a short timeout, proving the queue still services items.

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| S01 | + | Throwing classifier under load: with `FaultInjector` failing a fraction of `Enqueue` classifications while many threads enqueue/dequeue, every failure aborts cleanly and the queue keeps servicing; `SyncProbe` baseline holds at the end. |
| S02 | + | Throwing cost selector under load: same guarantee on the cost path (the exception surfaces, no lock/permit leaks, servicing continues). |
| S03 | + | Throwing `ItemDequeued`/`ItemEnqueued` handler: a user event handler that throws does not corrupt state, does not leak the lock (delegates run outside it), and does not stop later dequeues. |
| S04 | + | Throwing `ItemDropped`/drop callback during overflow: exceptions on the drop path under a full bounded queue do not wedge subsequent enqueues. |
| S05 | + | Throwing `QoSLink` on-drop / pump body: a background pump whose callback throws logs/records the fault, keeps pumping, and never leaves the source or sink locked. |
| S06 | + | Cancellation storm: hundreds of `DequeueAsync`/`ConsumeAsync` waiters cancelled concurrently while producers run — every cancellation releases its waiter slot; `SyncProbe` shows zero orphaned waiters; the queue remains drainable. |
| S07 | + | Dispose-with-waiters under load: disposing a queue with many pending waiters and in-flight producers completes all waiters with `OperationCanceledException`, releases all permits, and never hangs. |
| S08 | + | Interleaved cancel + complete + dispose: a waiter cancelled at the same instant an item is enqueued resolves to exactly one outcome (either the item or cancellation, never both, never neither); permit accounting stays exact. |
| S09 | + | Reentrancy under concurrency: `ItemDequeued` handlers that re-enqueue on many threads at once never self-deadlock and never leak the lock (extends A35 to the concurrent case). |
| S10 | + | Pipeline fault endurance: a multi-stage pipeline run for `SoakConfig` seconds with continuous fault injection at every stage — throughput stays strictly positive the entire time (sampled each second; a stall to zero is a failure), memory stays bounded, and it drains clean on stop. |
| S11 | + | Long-running lock-fairness soak: sustained many-thread load for `SoakConfig` seconds shows no consumer starved beyond a bound and no monotonic latency growth (a symptom of a slow permit leak). |
| S12 | + | Post-fault liveness invariant (applied to S01–S05): after each fault scenario, an independent enqueue→dequeue round-trip completes within a short timeout — the direct assertion that "servicing did not halt." |
| S13 | + | Backpressure liveness: a `Block`-policy sink repeatedly filled and drained under load never leaves a producer permanently blocked once space frees (no lost resume signal). |
| S14 | + | Semaphore no-drift over churn: millions of enqueue/dequeue cycles leave the internal semaphore permit count exactly at baseline (catches off-by-one release bugs that only manifest after long runs). |

The soak cases (S10, S11, S14) are duration-scaled by `SoakConfig`: 10 seconds in the PR gate to stay fast, 300 seconds in a scheduled nightly endurance job. Both durations use the identical descriptors, so an endurance regression is caught by the same code that runs on every commit.

### Suite T — Optional SQLite persistence & recovery (only when the persistence package is present)

Runs against `SqliteQoSStore<T>` on a temp-file database, cleaned up per case. Skipped with a reason when the persistence package is not referenced, so the core suite stays dependency-free.

| Case | Type | Scenario → Expected |
|------|------|---------------------|
| T01 | + | Round-trip: enqueue N items, close the queue, reopen against the same file → all N recovered, deserialized, and dequeued in the original order. |
| T02 | + | Ordering survives restart per discipline: FIFO order, priority bands, WFQ virtual-finish order, and WRR rotation all resume exactly (scheduler-state row + `seq` reconstruct the index). |
| T03 | + | Conservation across a simulated crash: kill the writer mid-run (no clean close); on reopen, `recovered + never-committed == enqueued`, with the loss window bounded by the durability mode. |
| T04 | + | `Durable` mode guarantees zero loss: every item whose `EnqueueAsync` returned is present after an abrupt reopen. |
| T05 | + | `WriteBehind` mode loses only within the flush window: items enqueued after the last batch commit may be absent; everything before it is present. |
| T06 | − | A payload that fails to serialize throws on enqueue (surfacing the serializer's exception) and is not admitted; queue state and DB unchanged. |
| T07 | + | Custom `IQoSSerializer<T>` is honored end to end (round-trip through a non-JSON serializer). |
| T08 | + | Dequeue removes the row (default at-most-once): after dequeue and reopen, the item is gone. |
| T09 | + | Lease/ack: `TryLease` hides the item; `Ack` deletes it; `Nack` and lease-timeout both redeliver it exactly once on reopen. |
| T10 | + | I/O never under the lock: with the SQLite store, Suite S's liveness probes still pass — a slow/blocked disk write does not stall servicing (fault-inject a delaying store). |
| T11 | + | Single-DB transactional hand-off: a `QoSLink` moving items between two tables in one database is atomic — a crash mid-move leaves the item in exactly one table, never both, never neither. |
| T12 | + | Concurrent producers/consumers against the persistent store conserve and stay ordered (Suite R invariants hold with persistence enabled). |
| T13 | + | Recovery of a large backlog (e.g. 1M rows) completes in bounded time and rebuilds a correct index (batched load, not row-by-row). |
| T14 | − | Corrupt/locked database surfaces a specific, documented exception on open rather than a hang or a silent empty queue. |
| T15 | + | Persist-before-visible ordering: in `Durable` mode an item is never dequeuable until its commit lands — a consumer racing an in-progress `EnqueueAsync` cannot observe the item before the fsync completes (verified with a delayed store and a probing consumer). |
| T16 | + | Kill-mid-flight no-loss: hammer `EnqueueAsync` in `Durable`+`synchronous=Full`, hard-abort the process (separate host process), reopen → every enqueue that returned is present; zero acknowledged items lost across many iterations. |
| T17 | + | Group commit correctness: many concurrent `Durable` enqueues coalesce into fewer fsyncs, yet each returns only after a commit that includes it — post-abort recovery contains exactly the set whose calls returned. |
| T18 | + | `synchronous` setting is honored: `Durable` opens with `WAL`+`FULL` by default; selecting `Normal` is reflected in the connection pragma (guards against a silent durability downgrade). |
| T19 | + | Lease/ack is no-loss under consumer crash: lease an item, abort before `Ack` → item redelivered on reopen (at-least-once); a duplicate is possible but a loss never is. |
| T20 | + | `GuaranteedDelivery` preset end-to-end: a full pipeline in one database under load, abruptly killed repeatedly, reconciles with zero lost items across head-to-tail (duplicates tolerated), proving the composed no-loss story. |
| T21 | + | Bounded queue under `Durable`: the depth slot is reserved before the off-lock commit, so concurrent durable enqueues never over-admit past `MaxDepth` and never write a row they would reject. |

Cases T16, T17, T19, and T20 use a **separate-process abort** harness (spawn a child that enqueues, `kill` it without cleanup, reopen in the parent) so the crash is real rather than a simulated close — the only way to actually prove "durable under all circumstances" rather than assert it.

---

### Coverage Review Passes

The catalog above is the product of five deliberate review passes, each of which added rows that the previous pass missed. Recording them here keeps the reasoning visible and gives the implementer a checklist of the subtle cases that are easy to drop.

**Pass 1 — mechanical coverage.** Walk every public member of every type and ensure at least one positive and one negative case touches it: constructors (valid + each invalid argument), `Enqueue`/`TryEnqueue`/`Dequeue`/`TryDequeue`/`TryPeek`/`Clear`/`Count`/`IsEmpty`/`Dispose`, plus each `OverflowPolicy` value and each interface implementation (`IProducerConsumerCollection`, `IReadOnlyCollection`, `IAsyncDisposable`). This pass produced Suites A–H and the argument-validation rows.

**Pass 2 — behavioral and concurrency gaps.** Re-read the discipline semantics and the concurrency model looking for scenarios mechanical coverage cannot see. It added: the GPS "idle flow not penalized" property (E06), aging monotonicity and off-by-default proof (D09, D11), DWRR deficit carryover (H07), policer time-correctness with injected time (G10, P05), FIFO wakeup ordering of multiple async waiters (A21), the no-listener side-effect-free path (A37), reentrant event handlers (A35), classifier-throws leaving state intact (A36), and the grand conservation / global-uniqueness invariants (O01–O03). This pass also forced the injectable `IQoSTimeProvider` into the design, since G02/G10/D10/P05 are untestable without it.

**Pass 3 — composition, lifecycle, and boundary re-review.** Focus on the chaining layer and the seams between components, plus a boundary sweep. It added: cycle and self-loop detection (L04, L05), backpressure without busy-spin (K02), double-start / stop-before-start guards (K06, L07), restart after stop (L08), disposal completing tail waiters through a pipeline (L10), nested-overflow accounting integrity (N05), nested disposal not leaking pumps (N06), the fan-out partition invariant (M03), snapshot enumeration during concurrent mutation (A25), and value-type vs reference-type payload parity (A27, A28). The boundary sweep confirmed `max == 0`, `MaxDepth == 0` (unbounded), weight/cost `== 0`, empty sources, and single-element degenerate cases each have an explicit row (I06, A13, E12, H05, I02, E03/H02).

**Pass 4 — classification and string-key edge cases.** A focused sweep of the classification model produced Suite Q: the predicate-vs-key split (Q03 vs Q04–Q11), the full `UnknownKeyPolicy` matrix across WFQ and WRR (Q04–Q07), ordinal vs case-insensitive key comparison (Q08, Q09), `null`/empty keys funneled through the same policy (Q10), explicit overrides subject to the same rules as selectors (Q11, Q12), the once-per-enqueue invocation contract (Q01), and the no-silent-loss guarantee across every policy (Q15). This pass is what surfaced the `UnknownKeyPolicy` enum, the `KeyComparer` option, and the `UnknownClassificationException` type as first-class parts of the design rather than incidental behavior.

**Pass 5 — concurrency stress and liveness.** A dedicated look at multi-threaded behavior and at the ways a synchronization primitive can wedge servicing. It produced Suite R (the producer/consumer ratio matrix, bursty and hot-spot contention, fairness-under-contention, mixed sync/async consumers, full-pipeline load) and Suite S (lock/semaphore leak hunting on every exception and cancellation path, dispose-with-waiters, the interleaved cancel-vs-enqueue race, backpressure resume, and duration-scaled endurance soaks). This pass added the `Watchdog`, `FaultInjector`, `SyncProbe`, and `SoakConfig` helpers, the `InternalsVisibleTo` diagnostic counters, and — most consequentially — the standing design rule that **user delegates are invoked outside every internal lock**, which is what makes a throwing classifier or event handler incapable of leaking a lock. The post-fault liveness check (S12) is the blunt statement of the requirement: after any fault storm, a plain enqueue→dequeue must still complete within a short timeout, or the queue has failed.

Two standing rules keep coverage honest as the code grows: every new public member ships with its positive and negative cases in the same phase (a member with no test fails review), and every scenario that moves items ends with a `Conservation` assertion so "no silent loss" is enforced everywhere rather than spot-checked.

---

## Documentation Plan

- **README.md** — what QoSKit is, the queue catalog with a one-paragraph explanation and a minimal snippet each, the chaining model, the model-server dispatch walkthrough, a pointer to `QoSKit.Example`, observability notes, the dedicated **Persistence** section below, install/build/test instructions, and an explicit note that Docker/REST/MCP artifacts are intentionally absent for a library. Reviewed for human voice per the writing guidance; the asset/logo reference uses an explicit `raw.githubusercontent.com/jchristn/QoSKit/...` URL.
- **README.md → Persistence section (dedicated).** Its own top-level section covering: enabling persistence and installing `QoSKit.Persistence.Sqlite`; the durability spectrum (`WriteBehind` vs `Durable`, `synchronous` pragmas, group commit); the `GuaranteedDelivery` / `Fast` presets and the honest at-least-once vs exactly-once discussion; recovery behavior on restart; and a prominent **"Running under Docker with WAL"** subsection. That subsection states the trap plainly: in WAL mode SQLite keeps its data across **three** files — `qoskit.db`, `qoskit.db-wal`, and `qoskit.db-shm` — and the `-wal` file holds committed transactions not yet checkpointed into the main database. Persisting only `qoskit.db` therefore **loses the most recently committed data on restart**, silently defeating the durability guarantee. The guidance: mount a **volume at the directory** that holds the database, not a bind-mount of the single `.db` file (a single-file bind-mount also breaks SQLite's create/rename of the sidecar files); keep the database on the mounted volume rather than the container's overlay layer; give the container user write permission to that directory (call out UID/ownership); prefer a local volume over a networked filesystem (NFS/SMB can break WAL locking and `mmap`); and rely on the library's graceful-shutdown `wal_checkpoint(TRUNCATE)` (run on `DisposeAsync`) to fold the WAL back into the main file on a clean stop, while stressing that a clean stop must not be assumed — the sidecar files must be on the persistent volume for the crash case. A short, correct `compose.yaml` snippet shows the directory volume mount and the DB path pointing inside it.
- **CHANGELOG.md** — Keep-a-Changelog format, starting at `0.1.0`.
- **LICENSE.md** — MIT, holder Joel Christner.
- **docs/GETTING_STARTED.md** — from zero to a running two-level pipeline.
- **XML documentation** — every public type, member, constructor, and method, including documented defaults/min/max and `<exception>` tags, enforced by treating missing-doc warnings as build errors on the library project.

---

## Build Phases and Milestones

Each phase ends compiling clean with **zero warnings** across all three target frameworks and green tests for what it added — `CODE_STYLE.md` requires compiling free of errors *and* warnings, and the library project sets `TreatWarningsAsErrors` plus doc-comment warning `CS1591` on so missing XML docs fail the build. One class or one enum per file throughout (`CODE_STYLE.md`). Package versions are pinned to the `BACKEND_TEST_ARCHITECTURE.md` reference: `Touchstone.*` `0.1.12`, `xunit` `2.9.3`, `NUnit` `4.3.2`, `Microsoft.NET.Test.Sdk` `17.14.1`.

1. **Scaffold.** `QoSKit.sln`; `src/QoSKit/QoSKit.csproj` multi-targeting `netstandard2.0;net8.0;net10.0` with `<Nullable>enable</Nullable>`, `ImplicitUsings` disabled, and the `netstandard2.0` polyfill package references. Repo files required by `REPOSITORY_REQUIREMENTS.md`: `.gitignore` (item 1), `README.md` stub (item 3), `CHANGELOG.md` (item 5), MIT `LICENSE.md` (item 8); source only under `src/` (item 6). Four Touchstone projects (`Test.Shared`, `Test.Automated`, `Test.Xunit`, `Test.Nunit`) plus `QoSKit.Example`, all building. Gate: `dotnet build QoSKit.sln` clean; empty console runner exits 0.
2. **Core.** `Abstractions/` — `IQoSSink.cs`, `IQoSSource.cs`, `IQoSQueue.cs`, `IQoSTimeProvider.cs`, `QoSQueueBase.cs`, `QoSQueueOptions.cs`, `OverflowPolicy.cs`; `Exceptions/` — `QoSException.cs` (abstract base), `QueueFullException.cs`, `QueueEmptyException.cs`; `Telemetry/` — `QoSQueueStatistics.cs`, `SystemQoSTimeProvider.cs`; `Persistence/` — `IQoSStore.cs`, `IQoSSerializer.cs`, `InMemoryQoSStore.cs` (the store seam, in-memory only for now); event-arg types. Includes the async wait mechanism and the `IProducerConsumerCollection<T>` bridge. `Queues/FifoQoSQueue.cs` and `Queues/LifoQoSQueue.cs` written against `IQoSStore<T>`. Build the `Test.Shared` helpers (`QueueFactories`, `Conservation`, `Drain`, `ConcurrencyHarness`, `Recorder`, `Watchdog`, `FaultInjector`, `SyncProbe`, `SoakConfig`, `ManualQoSTimeProvider`) here. The base class ships the concurrency/liveness guarantees (delegates invoked outside the lock, `try/finally` on every acquire, self-healing waiter registry) and the `[assembly: InternalsVisibleTo("Test.Shared")]` diagnostic counters. Gate: **Suite A** (all 37 contract rows, run against FIFO + LIFO), **Suite B**, **Suite C**, and the FIFO/LIFO rows of **Suite R** and **Suite S** (including a short-duration S10/S14 soak) green through `Test.Automated`.
3. **Priority and fairness.** `Queues/PriorityQoSQueue.cs` (optional aging via `IQoSTimeProvider`), `Queues/WeightedFairQoSQueue.cs`; `Definitions/PriorityBand.cs`, `Definitions/WeightedFlow.cs`; the classification plumbing `Abstractions/UnknownKeyPolicy.cs` and `Exceptions/UnknownClassificationException.cs` (WFQ's open-key model needs both). Register both queues in `QueueFactories` so Suite A now also runs against them. Gate: **Suite A** (Priority, WFQ), **Suite D**, **Suite E**, and the WFQ/priority rows of **Suite Q** — convergence within tolerance, aging monotonic and off-by-default, priority-band clamping, WFQ dynamic-key admission and ordinal key comparison.
4. **Class-based and low-latency.** `Definitions/TrafficClass.cs`, `Telemetry/TokenBucket.cs` (policer, time via `IQoSTimeProvider`), `Queues/ClassBasedWeightedFairQoSQueue.cs`, `Queues/LowLatencyQoSQueue.cs`; add to `QueueFactories`. Gate: **Suite A** (CBWFQ, LLQ), **Suite F**, **Suite G** — first-match routing, policer time-correctness with manual time.
5. **Round robin.** `Queues/WeightedRoundRobinQoSQueue.cs` (classic + deficit scheduling; balancer + classifier ingress modes), `Definitions/WeightedSubQueue.cs`; add to `QueueFactories`. Gate: **Suite A** (WRR), **Suite H**, and the full **Suite Q** — DWRR byte-fairness and deficit carryover, WRR unknown-key policy matrix (`Throw`/`RouteToDefault`/`Reject`), explicit overrides, and no-silent-loss conservation. At this point Suite A has run against all seven disciplines and classification is fully covered end to end.
6. **Chaining.** `Chaining/` — `DrainTo` extensions, `QoSChain.cs`, `QoSLink.cs`, `QoSLinkOptions.cs`, `QoSRouter.cs`, `QoSPipeline.cs`; `Exceptions/PipelineCycleException.cs`, `Exceptions/UnroutableItemException.cs`. Gate: **Suites I, J, K, L, M, N** — fluent chain, many-to-one merge, backpressure without busy-spin, cycle/self-loop detection, deep and nested composites; the pipeline-level liveness rows of **Suite R** (R10) and **Suite S** (S05, S10, S13) — pump-fault endurance and backpressure resume; Examples 4–7 run verbatim as cases.
7. **Observability.** `Telemetry/QoSMetrics.cs` — the `QoSKit` `Meter` with the OTel-named instruments, wired into the base class and links; verified with a `MeterListener`. No `Console.WriteLine` anywhere (`CODE_STYLE.md`). Gate: **Suite P**, **Suite O** (grand conservation, global uniqueness, soak, cancellation storm), and the **full Suites R and S** across the now-complete surface — the complete concurrency matrix and every liveness/fault-injection case at PR-gate duration.
8. **Docs and polish.** `README.md` (accurate per `CODE_STYLE.md` item 18: "If a README exists… ensure it is accurate"), `docs/GETTING_STARTED.md`, the `QoSKit.Example` three-scenario console app (see [Example Application](#example-application-qoskitexample)) built and verified to run its 3×10s demo, CHANGELOG `0.1.0`, README asset URL under `assets/` per `REPOSITORY_REQUIREMENTS.md` item 4's explicit-URL rule. The README's dedicated Persistence section lands with the persistence package in Phase 9. Final gate for the in-memory core: warning-free build on all three frameworks; **every suite (A–S)** green through `Test.Automated`, `Test.Xunit`, and `Test.Nunit`; GitHub Actions workflow from `BACKEND_TEST_ARCHITECTURE.md` running all three runners at PR-gate soak duration, plus a scheduled **nightly endurance job** that sets `QOSKIT_SOAK_SECONDS=300` to run Suites R and S long-form; a coverage collector (coverlet, already referenced by the test projects) confirming line/branch coverage at target.
9. **Optional SQLite persistence (additive milestone, ships after the core is stable).** `src/QoSKit.Persistence.Sqlite/` — `SqliteQoSStore.cs` (schema, WAL, write-behind and durable modes, group commit, recovery, and a graceful-shutdown `wal_checkpoint(TRUNCATE)` on `DisposeAsync`), plus `JsonQoSSerializer.cs`, `QoSPersistenceOptions.cs`, and the `EnqueueAsync` overload and lease/ack surface in core `Persistence/`. Because the store seam already exists (Phase 2), no discipline changes. Authors the README **Persistence** section, including the WAL-in-Docker guidance (persist the `.db`, `.db-wal`, and `.db-shm` files via a directory volume mount) and the `compose.yaml` snippet. Gate: **Suite T** (round-trip, per-discipline ordering across restart, crash conservation, durability modes, serializer errors, lease/ack, single-DB transactional hand-off, large-backlog recovery) — including the strict-durability cases T15–T21 that use a **separate-process abort harness** to prove `Durable`+`FULL`+group-commit loses no acknowledged item and the `GuaranteedDelivery` preset is no-loss end-to-end across a killed pipeline — plus a rerun of **Suites R and S** against `SqliteQoSStore<T>` to prove the liveness rules (no I/O under the lock, persist-before-visible) hold with a real disk behind the queue. Core `QoSKit` remains dependency-free; only `QoSKit.Persistence.Sqlite` references `Microsoft.Data.Sqlite`.

---

## Requirements Compliance Checklist

**Repository requirements.** `.gitignore` (1), `README.md` (3), `CHANGELOG.md` (5), MIT `LICENSE.md` (8) all included. Source lives under `src/` (6). Docker items (2, 9, 10, 11, 12), `DOCKERHUB_README.md` (4), SDK layout (7), `REST_API.md` + Postman (13), and `MCP_API.md` (14) do not apply — QoSKit is an in-process library with no container, REST, or MCP surface — and their absence is called out in the README so it reads as a decision, not an omission.

**Code style.** Namespace-first with usings inside and system usings alphabetized ahead of the rest; full XML docs on public surface and none on private members; explicit types (no `var`); no tuples; `_PascalCase` private fields; `.ConfigureAwait(false)` and `CancellationToken` on async methods with cancellation checks; specific/custom exception types with `<exception>` docs and guard clauses; nullable reference types on; `Interlocked`/`ReaderWriterLockSlim` for the concurrency primitives; full `IDisposable`/`IAsyncDisposable` pattern; one class or enum per file; sync `IEnumerable`-returning methods paired with cancellable async variants; no bare tunable constants; no `Console.WriteLine` in library code.

**Test architecture.** Touchstone `Test.Shared` / `Test.Automated` / `Test.Xunit` / `Test.Nunit`, descriptors with no console output, exceptions as assertions, exit code 0/1, GitHub Actions running all three runners.

**Telemetry.** Adapted to a library: OTel-named `Meter` instruments plus synchronous statistics, on by default and free until observed, no HTTP/Grafana/Docker stack (not applicable).

**Writing.** README and getting-started reviewed for human voice per the writing guidance.

---

## Locked Default Behaviors

No open questions remain. These defaults are decided and drive the assertions in the test catalog; each is reversible later without reshaping the design.

- **Priority ordering convention:** lower selector value means higher priority (DSCP/`ThreadPriority`-style), documented prominently.
- **Out-of-range priority band:** clamp — a selector value `< 0` maps to the top band, `>= levels` to the bottom band; no throw (D06).
- **CBWFQ default class:** an implicit, non-removable `class-default` always exists and catches unmatched items; its weight is configurable (F03, F04).
- **Names and keys are case-insensitive:** the single `KeyComparer` governs both definition-name uniqueness and classification-key lookup, defaulting to `StringComparer.OrdinalIgnoreCase`, so `"Gold"` and `"gold"` are the same class and declaring both is a duplicate-name `ArgumentException`. Pass `StringComparer.Ordinal` for case-sensitive keys (e.g. machine-generated WFQ flow tokens).
- **Unknown classification key:** `UnknownKeyPolicy` defaults to `CreateDynamic` for WFQ (unknown key → new flow at the default weight) and to `Throw` for WRR classifier mode, or `RouteToDefault` when a default sub-queue is designated. A `null`/empty key follows the same policy. No item is ever silently discarded (Suite Q).
- **LLQ priority policing:** a rate limit is optional. Null means unpoliced strict priority (can starve fair classes, flagged in XML docs); when set, above-rate traffic is demoted to fair scheduling (G03, G08).
- **Weights must be `>= 1`:** WRR sub-queue, WFQ flow, and CBWFQ class weights below 1 throw `ArgumentOutOfRangeException` at construction — no silently-never-served queue (H05, E10).
- **Null items:** `Enqueue(null)` for a reference `T` throws `ArgumentNullException`, uniformly across disciplines (A27).
- **Cost bounds:** negative cost throws `ArgumentOutOfRangeException`; zero clamps to a floor of 1 (E11, E12). Default cost is 1 per item, so WFQ/DWRR are packet-count-fair until a `costSelector` is supplied.
- **Router with no default route:** unmatched items are rejected (`TryEnqueue` false / `Enqueue` throws `UnroutableItemException`) with an `ItemDropped` reason of `Unroutable` (M02).
- **Lifecycle guards:** starting an already-running `QoSLink`/`QoSPipeline` throws `InvalidOperationException`; stopping one that never started is a safe no-op (K06, L07).
- **Default overflow policy:** `Reject`; default depth is unbounded (`MaxDepth = 0`), so bounding is opt-in.
- **Disposal of pending waiters:** outstanding `DequeueAsync` waiters complete with `OperationCanceledException`; subsequent operations throw `ObjectDisposedException` (A32, A33).
- **Persistence:** off by default (in-memory, zero dependencies). When enabled, `WriteBehind` + remove-on-dequeue (at-most-once) is the default, preserving in-memory semantics. **Strict no-loss is a conscious opt-in:** `GuaranteedDelivery()` selects `Durable` (commit-before-visible) + `WAL`/`synchronous=FULL` + group commit + lease/ack, so no acknowledged enqueue is ever lost and in-flight items are redelivered on crash (at-least-once; exactly-once needs consumer idempotency). It trades throughput for the guarantee, by design. SQLite lives only in the `QoSKit.Persistence.Sqlite` package.
- **License holder:** Joel Christner, MIT. **Initial version:** `0.1.0`.
