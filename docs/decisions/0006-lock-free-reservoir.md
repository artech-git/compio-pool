# 0006 — The exchange is a lock-free `ArrayQueue`

## Context

The cross-thread exchange ([0005](0005-detach-is-opt-in.md)) was first built as a
`Mutex<Vec<Entry>>` — a shared stack. It worked. Replacing it with a bounded, lock-free
`crossbeam_queue::ArrayQueue` was not obviously an improvement, and the measurement that followed
says it is not, on the metric most people look at first.

## Decision

Use the `ArrayQueue`, and record the measurement that argues against it so nobody "optimises" it
back by looking only at the mean.

Hammered with nothing between operations, the mutex is up to 2.1× faster on the mean:

```text
park + unpark, mean ns/op (worst thread)
  threads    ArrayQueue    Mutex<Vec>
        1          28.7          30.1
        2         183.1         157.1
        4         556.1         403.3
        8        1821.8         857.4
```

The distribution says why:

```text
8 threads          p50     p99    p99.9       max
  ArrayQueue      1500    5875    10834    331209
  Mutex<Vec>        42   29291    62958    477917
```

A p50 of 42 ns against a mean of 857 ns is a bimodal, unfair distribution: a thread that already
holds the lock reacquires it while everyone else queues behind. The queue trades median for
fairness — 5.0× better p99, 5.8× better p99.9.

With real work between exchange operations, which is the actual workload, the queue also wins
outright:

```text
full acquire path, min_idle=0
  threads    ArrayQueue    Mutex<Vec>
        1          73.8          77.5
        2         386.8         389.5
        4         543.5         782.5
        8        1907.8        2695.3
```

So the justification is tail latency and behaviour as threads are added, not raw throughput. See
[../performance.md](../performance.md) for how these are measured.

## Admission control

`ArrayQueue::push` can fail when full. By then the connection has already been detached and there
is no synchronous way to rebuild it — the pool would be holding a detached socket it cannot return
to anyone. So a slot is claimed with a CAS **before** detaching:

```rust
fn admit(&self) -> bool {
    self.admitted
        .fetch_update(AcqRel, Acquire, |n| (n < self.queue.capacity()).then_some(n + 1))
        .is_ok()
}
```

`admitted` counts entries in the queue *plus* those in flight between the check and the push, and
is capped at the queue's capacity. A full reservoir therefore refuses the offer while the
connection is still whole (`Parked::Refused` hands it back), and the push that follows cannot fail.
The count is released on pop and on each failure path. `fuzz/fuzz_targets/reservoir_ops.rs` exists
to prove this never leaks: whatever sequence of parks, claims, failed detaches, failed attaches
and clears happens, it must still be possible to fill the reservoir to exactly `capacity`.

## Why FIFO

`ArrayQueue` is FIFO, so the connection parked longest is claimed first — the opposite of the
per-shard free list, which is LIFO because the most recently returned connection is the warmest.

The bias is right for an overflow pool. Residency here is bounded, so a parked connection cannot
sit at the bottom of a stack going stale while newer arrivals churn above it. The cost is a little
cache warmth, which matters far less than a socket the far end has quietly dropped.

## Consequences

**Neither pre-existing benchmark could see this change.** `benches/acquire` and
`examples/ncat_bench` both drive the pool with `NoExchange`, whose park path is a plain move; and
the acquire microbenchmark's 4-thread figure swings 362–837 ns/op across runs of a single binary,
so it cannot resolve a change of this size anyway. `benches/exchange.rs` was written for this
decision specifically, compiling **both** designs into one binary and alternating them, so the
comparison does not ride on run-to-run scheduling variance.

**Capacity is now fixed at construction.** `Reservoir::new(capacity)` asserts non-zero and the
ring never grows. That is what makes both ends allocation-free.
