# Architecture

How the pool is put together, where every piece of state lives, and what runs on each checkout
and return. For *why* any of it is shaped this way, see [decisions/](decisions/).

## The constraint everything follows from

`compio` is completion-based and thread-per-core. Each thread runs its own `io_uring`, I/O
handles are bound to the ring that created them, and buffers are handed to the kernel by
ownership for the duration of an operation. `compio::net::TcpStream` is `!Send`.

So a connection cannot be pooled in a single shared, work-stealing store the way bb8 pools one on
`tokio`: it can only ever be used from the thread whose ring opened it. The unit of ownership is
the core. What crosses threads is not connections but the *blueprint* that stamps out a pool on
each — the manager and the configuration, behind one `Arc`.

## The two halves

```text
Pool<M>          Send + Sync + Clone — one Arc<Shared<M>>, the only thing that crosses threads
  └ Shared<M>    manager: M · config: PoolConfig · closed: AtomicBool · [metrics registry]

LocalPool<M>     !Send — one Rc<Inner<M>> per thread, from Pool::local()
  └ Inner<M>     idle: RefCell<Vec<Conn>> · slots: Cell<u32> · waiters: Waiters · [metrics block]
```

`Pool` holds nothing hot. Cloning it is an `Arc` bump; every clone shares the one manager,
config, shutdown flag and metrics registry. `LocalPool` holds everything on the lease path, all
`Rc` / `Cell` / `RefCell`, and is `!Send` — the compiler keeps it, and the ring-bound connections
it holds, on the one thread that called [`Pool::local`](../src/pool.rs). Call `local` once per
compio thread and keep the handle; it is `Clone` (a cheap `Rc` bump) if several tasks on that
thread need it.

Every size is **per thread**. `max_size` caps one thread's `LocalPool`, not the process: a
48-core server with `max_size(16)` holds up to `48 × 16` connections, 16 of them to any one ring.

## State, and which thread can see it

| state | type | visible to |
|---|---|---|
| manager, configuration | `M`, `PoolConfig` in `Arc<Shared>` | everyone, read-only |
| shutdown flag | `AtomicBool` in `Shared` | everyone; set by `Pool::close` |
| metrics registry *(metrics)* | `Mutex<Vec<Arc<Metrics>>>` | locked only on `local()` and on a scrape |
| idle free-list | `RefCell<Vec<Conn>>`, LIFO | one thread |
| slot count | `Cell<u32>` | one thread |
| waiter list | `Waiters`, one `Waker` per parked `get` | one thread |
| per-thread counters *(metrics)* | `Arc<Metrics>`, relaxed atomics | written by one thread, summed by anyone |

Nothing on the lease path is an atomic or a lock: the slot count is a `Cell`, the idle list a
`RefCell<Vec>`, the waiters a plain list. The `metrics` feature adds one `Arc<Metrics>` block per
thread, written only by its owner (relaxed atomics, no contention) and registered with the `Pool`
so an off-thread scrape can sum every thread's view. With the feature off, the block and the
registry drop out of the build; `slots` is still tracked, because the cap is enforced against it.

## A connection, and when it expires

```text
Conn<C>   raw: C · created_at: Instant · last_used: Instant
```

Expiry is a pure function of those timestamps and the config: `max_lifetime` against
`created_at`, `idle_timeout` against `last_used`, with age attributed to `max_lifetime` first. It
is evaluated **lazily** — only as a connection is popped for checkout, returned on drop, or walked
by [`reap`](#maintenance) — so a pool that goes quiet keeps its connections until maintenance runs.

## Checking a connection out

`LocalPool::get` wraps the loop below in `compio::time::timeout(connection_timeout, …)`; that
elapse is the only unbounded wait, and it surfaces as `RunError::TimedOut`.

```text
get_inner:
loop
  closed? → RunError::Closed
  wait_ready().await           park until idle is non-empty, or slots < max_size, or closed
  closed? → RunError::Closed

  pop_fresh_idle()             newest idle, discarding any expired (each discard frees its slot)
    Some(held) → test_on_check_out ? is_valid(conn).await : skip
                   invalid → note_broken ; drop held (frees slot) ; continue
                   valid   → return PooledConnection
    None → (idle empty)

  try_reserve()                slots < max_size ? take_slot → Held : None
    Some(held) → manager.connect().await
                   Ok  → fill held ; note_created ; return PooledConnection
                   Err → held drops (slot released) ; RunError::User(e)
    None → a task took the last slot between the wait and here: loop
```

Three outcomes, in order: **reuse** the warmest idle connection, **open** one if under
`max_size`, or **wait**. The check-and-take after `wait_ready` is synchronous, so what the wait
observed still holds when `get` acts on it; if another task won the slot in between, the loop just
goes round again.

## Returning one

A `PooledConnection` derefs to the connection and owns it until drop. Drop decides its fate on the
returning thread, touching no lock:

```text
PooledConnection::drop:
  closed?        → drop ; release slot
  has_broken?    → drop ; note_broken ; release slot
  expired?       → drop ; note_expired ; release slot
  else           → last_used = now ; push onto idle ; wake a waiter
```

The idle list is LIFO, so the next `get` reuses the most recently returned connection — warmest
in cache, least likely to have aged out. Either way, a release wakes parked waiters.

## The slot count and the `Held` reservation

`slots: Cell<u32>` counts idle + checked out + reserved, and is the one value enforced against
`max_size`. It is the source of truth whether or not metrics are compiled in.

A slot is reserved *before* the connection exists: `try_reserve` bumps `slots` and returns a
`Held`, an RAII guard that releases the slot if it is dropped before the connection is committed.
That is what makes every early return and cancellation inside `get` / `warm` leak-free — a
`connect()` that errors, a future dropped mid-await, a failed `is_valid` each drops its `Held` and
the slot goes back. Only `into_pooled` / `into_idle` commit it.

## Waiting and waking

`Waiters` holds one `Waker` per parked `get`, oldest at the front. Each waiter owns an id and
withdraws its waker if its future is dropped before it resolves, so nothing stale is ever woken.
Each freed slot or connection returned to the idle list wakes exactly one waiter — the oldest — so
a return costs a single task poll however deep the queue is, and a waiter woken but dropped before
it acts hands the wake on to the next. A closed pool is the one exception: it wakes everyone, since
there is nothing left to ration. A woken task re-runs the synchronous check-and-take, so a wake it
loses the race for is harmless — it simply re-parks. See
[decision 0010](decisions/0010-wake-one-fifo.md).

## Maintenance

Because expiry is lazy, a quiet pool would hold dead backend connections open. Thread-per-core has
no shared reaper thread, so maintenance is driven per worker:

* `reap` — drop idle connections past `idle_timeout` / `max_lifetime`, free their slots.
* `warm` — open connections up to `min_idle` and park them idle; doubles as the warm-up.
* `maintain` — `reap` then `warm`, the step to call on a timer.
* `clear` — drop every idle connection unconditionally, e.g. after a backend failover.

Once the pool is closed they stop doing work: `warm` / `maintain` return `Ok(0)`.

## Shutdown

`Pool::close` sets the shared `AtomicBool`. From then on every thread's `get` fails
`RunError::Closed`, a returned connection is dropped instead of recycled, and `warm` / `maintain`
become no-ops. `is_closed` reports the flag.

The flag is shared, but the wakers that unpark a parked `get` are not — they live on each worker's
own thread. A task already parked waiting for a slot therefore unblocks on its next *local* wake
(another connection returning, or `clear`), or when its `connection_timeout` elapses, not the
instant `close` is called from another thread. For a prompt drain, stop each worker's accept loop
and call `clear` on its thread.

## Metrics (the `metrics` feature)

Off by default. With it on, each `LocalPool` owns one `Metrics` block and registers a clone
(`Arc`) with the `Pool`. Only the owning thread writes its block, so the counters are plain
relaxed atomics; the live gauges mirror `slots` / idle / waiters, and pool logic never reads them
back — metrics is a pure observer.

* `State` — live gauges: `connections`, `idle_connections`, `pending_waiters`.
* `Statistics` — cumulative, monotonic counters: `get_direct`, `get_waited`, `get_timed_out`,
  `connections_created`, `connections_closed_{broken,idle_timeout,max_lifetime}`,
  `total_wait_time` (and `average_wait_time()`).

`LocalPool::state` / `statistics` read this thread's block, lock-free. `Pool::state` /
`statistics` take the registry lock — a cold path — and sum every block for a process-wide view.
Two `Statistics` snapshots subtracted give the activity in between, the shape an exporter wants.
With the feature off, all four still compile and return zeroes, so calling code builds either way.

## The thread-per-core half

The pool is the product; the accept loop, the thread spawning and the pinning are the developer's.
The crate ships the helpers for that half so they come from one place:

* `cpu` — the affinity mask ([`cpu::cores`](../src/cpu.rs)), thread pinning (`cpu::pin_current`),
  and, behind the `affinity` feature, topology-aware selection (`cpu::CoreSet`).
* [`listener::bind_reuseport`](../src/listener.rs) — a raw `SO_REUSEADDR` + `SO_REUSEPORT` socket,
  optionally with `SO_INCOMING_CPU`, so every thread binds the same address and the kernel splits
  connections by flow hash. See [0001](decisions/0001-listener-per-worker.md),
  [0005](decisions/0005-incoming-cpu-opt-in.md).
* `workers::PerCore` *(the `affinity` feature)* — spawns and pins one worker per selected core,
  hands each its `CoreId`, and returns joinable `Workers`. See
  [0009](decisions/0009-core-affinity-harness.md).

See [operations.md](operations.md) for how to size and run it.

## Invariants

These hold in every test and are what a change must keep true:

* `idle.len() + checked_out + reserved == slots`, and `slots <= max_size`, on every thread. There
  is no over-capacity path.
* A connection is only ever polled by the ring that created it. `LocalPool` is `!Send`, so the
  compiler proves connections never cross threads; nothing in the crate moves one.
* Every slot release and every idle return wakes waiters, and a woken `get` re-checks
  synchronously — so no caller acts on a stale observation, and a freed slot never strands a
  waiter while it is free.
* Expiry is checked only at checkout and return (and in `reap`), with `max_lifetime` attributed
  before `idle_timeout`.
* The metrics gauges mirror the real `slots` / idle / waiters; pool logic never reads them. With
  the feature off the counters are absent and the behaviour is identical.
