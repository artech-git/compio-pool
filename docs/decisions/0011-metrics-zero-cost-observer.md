# 0011 — Metrics is an off-by-default, zero-cost observer

## Constraint

Operators want per-pool counters — checkouts served directly or after waiting, timeouts,
connections opened and retired, time spent parked — summed across the whole process. But the lease
path is deliberately a single thread's `Rc` / `Cell` / `RefCell` with no atomic and no lock
([0002](0002-blueprint-and-local-pool.md), [0003](0003-reserve-before-create.md)), and the crate's
ethos is that the default build pays for nothing it did not ask for.

## Decision

Metrics is an **optional** `metrics` Cargo feature, off by default. With it on:

* Each `LocalPool` owns one `Metrics` block — relaxed `AtomicU32` / `AtomicU64` counters — and
  registers a clone (`Arc`) in the `Pool`'s `Mutex<Vec<Arc<Metrics>>>`.
* Only the owning thread ever writes its block, so the atomics are plain `Relaxed` with no
  cross-thread contention. The live gauges mirror the real `slots` / idle / waiters; **pool logic
  never reads them back** — metrics is a pure observer, so it can be compiled out without changing a
  single decision on the hot path.
* `LocalPool::state` / `statistics` read this thread's block, lock-free. `Pool::state` /
  `statistics` take the registry lock — a cold path — and sum every block for a process-wide view.
  `Statistics` is monotonic, so two snapshots subtracted give the activity between them.

With the feature **off**, `Metrics`, the registry and the atomics drop out of the build entirely.
`slots` stays a plain `Cell`, so the cap still works; all four accessors still compile and return
`State` / `Statistics` of zeroes, so calling code builds unchanged either way.

## Rejected

* **Always-on counters.** Atomics on a path that is otherwise lock-free — a tax every default build
  pays whether or not anyone scrapes.
* **One global registry of atomics the lease path writes.** Cross-thread contention on the exact
  operation the per-thread design exists to keep contention-free.
* **Reading the gauges back inside pool logic** (say, to pick a thread). Would make metrics
  load-bearing and forbid compiling it out; the slot count in `slots` is the single source of truth
  instead.

## Costs

* One more feature flag, and a second build configuration for CI to cover.
* Dashboards need the feature on; without it the counters read zero rather than erroring, which is
  the right failure but can surprise someone who forgot to enable it.
* `Pool::state` / `statistics` take a lock — fine on a scrape or health check, not for the hot path.

Same zero-cost opt-in shape as the `affinity` feature ([0009](0009-core-affinity-harness.md)): a
default build with neither feature pulls in nothing extra and keeps `#![forbid(unsafe_code)]`
intact.
