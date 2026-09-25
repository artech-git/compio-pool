# 0008 — Metrics are `Relaxed` atomics, not a consistent snapshot

## Context

With state spread across per-thread shards ([0001](0001-per-thread-shards.md)) there is no single
place holding the truth about the pool. `Pool::metrics()` has to produce a process-wide view
anyway, because that is what an operator needs.

A genuinely consistent snapshot would require either a lock taken on every counter update, or a
seqlock-style protocol, on paths that exist specifically to be lock-free.

## Decision

One `Arc<Counters>` of `AtomicU64`s shared by every shard, updated with `Ordering::Relaxed`, and
`snapshot()` reads them one at a time.

`Metrics` separates two kinds of number:

**Gauges** — `live`, `idle`, `parked` — go up and down. `parked` is read from the exchange
(`ArrayQueue::len`), the other two from the counters. They are summed across threads without a
lock, so a snapshot can be momentarily internally inconsistent: `idle` may be read after a pop
that `live` was read before.

**Counters** — `created`, `closed`, `acquires`, `waits`, `timeouts`, `poisoned`,
`recycle_failures`, `unparked` — are monotonic and never decrease.

`Relaxed` is sufficient because nothing is published through these values. They order no memory and
guard no data; they are observability. The real synchronisation is the thread-local boundary and
the exchange's own queue.

## Consequences

**Do not assert exact equality on gauges across threads in a live pool.** Derived quantities
(`live - idle` as "checked out") can be off by a small amount under concurrency. Alert on trends
and on counters, not on instantaneous gauge arithmetic.

**Counter balance is a real invariant, and it is tested.** The fuzz oracle asserts:

```text
created == closed + live + parked + taken + cleared
```

after every step. `taken` is `Pooled::take`, which hands ownership out of the pool by design.
`cleared` is `Exchange::clear` — reached from both `close()` and `invalidate()` — which drops
parked connections *without* counting them closed, so `created` ends up ahead. The test driver
tracks that term explicitly rather than pretending it does not happen; see
[../testing.md](../testing.md).

**Every destruction goes through one function.** `Pool::destroy_conn` is the only place that runs
`disconnect`, decrements `live`, increments `closed` and refunds the shard, so the counters cannot
drift between call sites. The two exceptions are deliberate and documented in place:
`Shard::drop` (thread teardown, which does the same arithmetic in bulk) and the `Reserved` guard
(cancellation, where the connection is dropped by a future that is already unwinding).
