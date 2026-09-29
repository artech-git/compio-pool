# 0010 — The exchange is a `Mutex<VecDeque>`, not a lock-free queue

Supersedes [0006](0006-lock-free-reservoir.md).

## Context

[0006](0006-lock-free-reservoir.md) chose a `crossbeam_queue::ArrayQueue` for the cross-thread
exchange, on tail latency rather than throughput: the `Mutex<Vec>` it replaced was up to 2.1×
faster on the mean and 5–6× worse at p99/p99.9. That decision stands up. It is being reversed
anyway, for a reason the benchmark cannot see.

A fallible push is the problem. `ArrayQueue::push` can fail when full, and by then `Detach::detach`
has already consumed the connection with no synchronous way to rebuild it — the pool would be
holding a detached socket it cannot return to anyone. 0006 closed that with an `admitted`
`AtomicUsize` claimed by CAS *before* detaching, released on pop and on each of the failure paths.
It works, it is fuzzed, and it is a second source of truth about occupancy that every code path has
to remember to maintain. A lock does not need it: the capacity check, the `detach` and the push sit
in one critical section, so the push cannot fail and occupancy is whatever the deque says it is.

## Decision

`parking_lot::Mutex<VecDeque<Entry>>`, bounded at construction, drained from the front.

`crossbeam-queue` moves to a dev-dependency; the ring stays compiled into `benches/exchange.rs` as
the arm to beat, so the comparison below is re-runnable rather than remembered.

```rust
fn park(&self, conn: M::Connection, meta: SlotMeta) -> Parked<M> {
    let mut queue = self.queue.lock();
    if queue.len() == self.capacity {
        return Parked::Refused(conn, meta);   // still whole; costs the shard nothing
    }
    let Some(parked) = M::detach(conn) else {
        return Parked::Destroyed;             // detach declined and consumed it
    };
    queue.push_back(Entry { parked, meta });  // cannot fail: same critical section
    self.len.store(queue.len() as u64, Relaxed);
    Parked::Accepted
}
```

`detach` runs under the lock. It is synchronous and does no IO — it unwraps an fd — so the critical
section stays at tens of nanoseconds. `attach` does not: it is the one async step, and it runs
after the guard has been dropped. The lock is never held across an await.

`parked()` reads a `len` mirror published `Relaxed` under the lock rather than taking it, so
metrics polling cannot convoy behind the threads doing real work. That is the same latitude every
other gauge has — see [0008](0008-relaxed-counters.md).

## What it costs

`cargo bench --bench exchange`, macOS/Darwin 25.3, four designs alternating in one binary. Best of
two passes each, so these are the floor, not a distribution.

```text
park + unpark round trip, ns/op (worst thread)
 threads       Mutex   FairMutex  ArrayQueue  Mutex<Vec>
       1        26.8        25.3        22.1        26.8
       2       120.7       145.8       179.6       130.6
       4       361.8     19434.5       422.9       417.7
       8      1116.8     39488.5      1814.5       945.2
```

Throughput is fine — 1.6× better than the ring at eight threads. The tail is the bill:

```text
latency distribution at 8 threads, ns
                   p50      p90      p99      p99.9        max
       Mutex       250     5792    37375      68917     164667
   FairMutex     39625    44625    49416      59500     143750
  ArrayQueue      1541     3167     5750       8917     132833
  Mutex<Vec>        42     3000    29417      57541     305667
```

**6.5× worse p99 and 7.7× worse p99.9 than the queue.** This is 0006's finding reproduced against a
different mutex: `parking_lot`'s `Mutex` barges, so the thread that just unlocked reacquires ahead
of everyone queued, and the p50 of 250ns against a mean of 1117ns is that unfairness showing.

On the full acquire path — real work between exchange operations, which is the actual workload —
the ring also keeps its 0006 win at the top end:

```text
full acquire path, min_idle=0, ns/op
 threads       Mutex   FairMutex  ArrayQueue  Mutex<Vec>
       1        80.0        79.5        81.4        82.9
       2       294.4       304.4       304.8       356.5
       4       666.1     18983.3       552.6       841.1
       8      4095.8     40727.4      1784.2      2751.3
```

So this is a deliberate trade of tail latency for one less concurrency invariant. It is the
opposite of the trade 0006 made, on the same evidence.

## Why not `FairMutex`

Fair handoff is the obvious answer to a barging lock's tail, and it is the wrong one here. It is in
the tables above so that it stays wrong on the record.

`FairMutex` passes ownership directly to the longest waiter on every unlock, which means waking
that thread through the OS. For a critical section measured in microseconds that is a good trade.
This one is ~25ns, so it buys a ~20µs park/unpark to protect a length check and a `push_back`:
**19–39µs per operation at 4–8 threads, against 400–1100ns for everything else.** The p50 of
39.6µs is the tell — the distribution is tight, but it is uniformly catastrophic rather than
occasionally bad. Fairness is not what is expensive; waking a thread is.

Measured on macOS. Linux futex wakeups are cheaper and the absolute numbers would be smaller, but
an OS wakeup per unlock is orders of magnitude above a 25ns critical section on any platform, so
the shape holds.

## Why FIFO

Unchanged from [0006](0006-lock-free-reservoir.md#why-fifo), and the reason it is a `VecDeque` and
not the `Vec` the original design used. Draining from the front claims the connection parked
longest first — the opposite of the per-shard free list, which is LIFO because the most recently
returned connection is the warmest.

The bias is right for an overflow pool. Residency here is bounded, so a parked connection cannot
sit at the bottom of a stack going stale while newer arrivals churn above it. The cost is a little
cache warmth, which matters far less than a socket the far end has quietly dropped.

## Consequences

**Exchange-heavy workloads get a worse tail.** If shards cross connections constantly, size against
p99, not the mean. It is also worth asking whether `min_idle` is low enough to be pushing traffic
through the exchange that should be staying local — the fastest exchange is the one a shard never
reaches for.

**The admission counter is gone**, with the CAS-before-detach protocol and the failure paths that
released it. `fuzz/fuzz_targets/reservoir_ops.rs` still asserts that capacity never leaks across
arbitrary sequences of parks, claims, failed detaches, failed attaches and clears — the property
now holds by construction, and the target exists to prove the construction.

**Capacity is still fixed at construction.** `Reservoir::new(capacity)` asserts non-zero, the deque
is allocated at capacity and never allowed past it, so `push_back` never reallocates under the
lock.

**`benches/exchange.rs` carries all four designs.** Anyone revisiting this gets the comparison by
running it, and `crossbeam-queue` stays a dev-dependency for exactly that reason.
