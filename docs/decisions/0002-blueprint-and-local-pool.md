# 0002 — A `Send` blueprint, a `!Send` per-thread pool

## Constraint

A `compio` connection is bound to the ring that opened it and is `!Send`: its buffers are
registered with that ring and its completions are delivered there. So, unlike bb8 on `tokio`, the
pooled connections cannot be one shared, work-stealing store — a connection can only ever be used
from the thread whose ring created it. What *is* the same on every thread is the manager that opens
connections and the configuration that governs them.

## Decision

Split the pool in two along exactly that line:

* **`Pool<M>`** is the blueprint: `Arc<Shared<M>>` holding the manager, the `PoolConfig`, the
  shutdown flag ([0006](0006-shutdown-and-the-unwinding-thread.md)) and the metrics registry
  ([0010](0010-metrics-zero-cost-observer.md)). It is `Send + Sync + Clone`; cloning is an `Arc`
  bump. It is the only thing that crosses threads.
* **`LocalPool<M>`** is the pool itself: `Rc<Inner<M>>` holding the idle free-list, the slot count
  and the waiter list, all `Cell` / `RefCell`. `Pool::local` stamps one out on the calling thread;
  it is `!Send`, so the compiler keeps it — and the ring-bound connections it holds — on that
  thread forever.

Every size is therefore **per thread**: `max_size` caps one `LocalPool`, not the process. The
`!Send` bound is the load-bearing part — it is the type system, not a convention, that proves a
connection never leaves its ring.

## Rejected

* **One shared pool, work-stealing across threads** (bb8 on `tokio`). Impossible here: the
  connections are `!Send` and ring-bound, so there is nothing a second thread could legally take.
* **Per-thread shards behind a lock, plus a cross-thread reservoir** — the design that preceded
  this one. It paid an atomic or a lock on the lease path and still could not move a ring-bound
  connection between threads; the reservoir only ever held *permits*, never connections. Retired
  wholesale for the plain `Rc`/`Cell` split.
* **Making `LocalPool: Send`.** Would mean lying to the compiler about the one invariant the whole
  design rests on.

## Costs

* No cross-thread balancing. `max_size × threads` is the ceiling, but a saturated thread cannot
  borrow an idle thread's connections; even distribution is the listener's job
  ([0001](0001-listener-per-worker.md)), not the pool's.
* `Pool::local` must be called once per thread and the handle kept (it is `Clone`, a cheap `Rc`
  bump, for several tasks on one thread).
* The only state visible across threads is read-only (the config) or cold (the metrics registry and
  the shutdown flag), so a process-wide view is a deliberate, locked, off-hot-path operation —
  never a side effect of the lease path.
