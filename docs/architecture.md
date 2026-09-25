# Architecture

How the pool is put together, where every piece of state lives, and what runs on each step of a
checkout. For *why* any of it is shaped this way, see [decisions/](decisions/).

## The constraint everything follows from

`compio` is completion-based and thread-per-core. Each thread runs its own driver (io_uring,
IOCP, or a poll fallback), IO handles are bound to the driver that created them, and buffers are
handed to the kernel by ownership for the duration of an operation. So `compio::net::TcpStream`
is `!Send`.

A pool therefore cannot keep connections in the pool handle, because the handle is shared across
threads and the connections cannot be. Every structural decision below is a consequence of
splitting those two things apart.

## State, and which thread can see it

```text
 ┌──────────────────────────────────────────────────────────────────────┐
 │ Arc<Inner<M, X>>          shared by every thread, holds no connection │
 │   id: u64                 index into each thread's SHARDS map        │
 │   manager: Arc<M>         user's Manage impl                          │
 │   config: Config          immutable after build                       │
 │   exchange: X             NoExchange (ZST) or Reservoir               │
 │   counters: Arc<Counters> relaxed atomics, see metrics                │
 │   generation: AtomicU64   bumped by invalidate()                      │
 │   closed: AtomicBool      set by close()                              │
 └──────────────────────────────────────────────────────────────────────┘
        ▲ Pool<M, X> is a clone of this Arc, and is Send + Sync
        │
 ┌──────┴───────────────────┐   ┌──────────────────────────┐
 │ thread A                 │   │ thread B                 │
 │ thread_local SHARDS      │   │ thread_local SHARDS      │
 │   HashMap<u64, Box<Any>> │   │   HashMap<u64, Box<Any>> │
 │     └─ Rc<Shard<M>>      │   │     └─ Rc<Shard<M>>      │
 │          free: Vec<Slot> │   │          free: Vec<Slot> │
 │          size: Cell      │   │          size: Cell      │
 │          waiters: VecDeq │   │          waiters: VecDeq │
 └──────────────────────────┘   └──────────────────────────┘
   !Send connections live here, reachable only from their own thread
```

`SHARDS` is keyed by pool id (a process-wide `AtomicU64` counter), so several pools coexist on one
thread, and the value is `Box<dyn Any>` because the map is shared by pools of different `M`. The
downcast is infallible in practice: a pool id uniquely determines the shard type.

The `Arc`s inside `Inner` are cloned into the shard once, at shard creation, and never on the hit
path — two atomic refcount bumps on a cache line every core writes to would undo the point of the
design.

## The types

| type | thread | role |
|---|---|---|
| `Pool<M, X>` | any | `Arc<Inner>` handle. Clone it onto every compio thread |
| `Shard<M>` | one | Free list, size counter, waiter queue. `Cell`/`RefCell`, no atomics |
| `Slot<C>` | one | A connection plus its `SlotMeta` (created_at, last_used, uses, generation) |
| `Pooled<M, X>` | one | Checkout guard. Derefs to the connection, returns it on drop |
| `OpGuard` | one | Arms cancellation protection for one operation |
| `Manage` | — | User trait: `connect`, `recycle`, `disconnect` |
| `Detach` | — | Opt-in extension: `detach`/`attach`, required by `Reservoir` |
| `Exchange<M>` | any | `park`/`unpark`. `NoExchange` is a ZST no-op; `Reservoir` is the real one |

`Shard` is deliberately built from `Cell` and `RefCell` rather than atomics. Nothing else can
reach it, so there is nothing to synchronise — that is the whole return on per-thread sharding.

## `acquire`, step by step

`Pool::acquire` wraps `acquire_inner` in `compio::time::timeout` when `acquire_timeout` is set,
counts a timeout, and returns `Error::Timeout`. `acquire_inner` is a loop over four steps:

```text
  ┌─ loop ───────────────────────────────────────────────────────────────┐
  │ 0.  closed? ──────────────────────────────────── yes ─► Error::Closed │
  │                                                                       │
  │ 1.  pop_idle()  (LIFO — warmest first, no lock, no atomic)            │
  │       expired?  ──► destroy, try the next one                         │
  │       recycle().await                                                 │
  │         Ok  ──────────────────────────────────────────► Pooled        │
  │         Err ──► recycle_failures++, destroy, try the next one         │
  │                                                                       │
  │ 2.  try_reserve(max_size)  (size < max_size ? size += 1)              │
  │       exchange.unpark().await                                         │
  │         Claimed ──► unparked++, live++ ────────────────► Pooled       │
  │         Lost    ──► closed++, fall through to dial                    │
  │         Empty   ──► fall through to dial                              │
  │       manager.connect().await                                         │
  │         Ok  ──► created++, live++ ─────────────────────► Pooled       │
  │         Err ──► release budget ───────────────────────► Error::Backend│
  │                                                                       │
  │ 3.  at max_size: waits++, shard.wait().await, then loop               │
  └───────────────────────────────────────────────────────────────────────┘
```

Three things are worth pulling out.

**The fast path is step 1 alone.** A `Vec::pop` out of a `RefCell`, a `recycle` call, and a guard
allocation. No lock, no atomic RMW, no cross-thread traffic. See
[performance.md](performance.md#the-acquire-fast-path).

**Stealing is tried before dialling.** Step 2 claims shard budget *first*, so one reservation
covers both the unpark and the connect, and a warm socket from another thread is always preferred
over a handshake.

**Waiting is local only.** Step 3 waits for one of *this thread's* checkouts to come back. A
thread at `max_size` has outstanding connections of its own, which always return, so this cannot
deadlock — and it needs no cross-thread wakeup. See
[decision 0007](decisions/0007-thread-local-waiters.md).

### Every await is a cancellation point

`acquire` can be dropped at any await: by its own timeout, or by the caller's `select!`. Between
`try_reserve` and the end of an await, the shard has claimed budget that no connection is backing
yet — drop the future there and the shard permanently believes it is one connection fuller than it
is. Repeat that and the shard sits at `max_size` holding nothing.

`Reserved` is the RAII guard that closes this. It wraps each awaited region, and on drop refunds
the budget (and, in the `holding` case, accounts for the connection that the cancelled future is
about to drop as `live--`, `closed++`). `disarm()` is called the moment the await completes and
ownership has moved on. `tests/cancellation.rs` cancels at each awaited step and asserts the
shard recovers.

## Release, step by step

Returning happens in `Pooled::drop`, which cannot await. Everything here is synchronous.

```text
  drop(Pooled)
    meta.uses += 1; meta.last_used = now
    poisoned?             ─► poisoned++,  destroy
    pool closed?          ─► destroy
    expired?              ─► destroy          (generation / lifetime / idle / uses)
    no local waiters
      and idle >= min_idle?
        exchange.park()
          Accepted  ─► live--, release budget      (now the exchange's)
          Destroyed ─► live--, closed++, release   (detach found an op in flight)
          Refused   ─► fall through
    push_idle()  ─► wake one waiter
```

`destroy` is always `manager.disconnect(conn)`, `live--`, `closed++`, `release()` — one path, so
the counters cannot drift between call sites.

The `no local waiters` check matters: a shard with someone parked on it must not give its
connection away to another thread while its own caller waits.

## Retirement

A connection is retired when `Slot::is_expired` says so, checked on the way *out* of the free
list (so nothing stale is ever handed to a caller) and by the background reaper. Four independent
reasons:

| reason | config | checked against |
|---|---|---|
| generation mismatch | `invalidate()` | `Inner::generation` |
| too old | `max_lifetime` | `meta.created_at` |
| idle too long | `idle_timeout` | `meta.last_used` |
| used enough | `max_uses` | `meta.uses` |

`invalidate()` is the interesting one: it bumps a generation counter rather than walking any
data structure, so it retires connections on *every* thread at once, at `Relaxed` cost, without
touching another thread's shard. Live checkouts keep working and are destroyed when returned.

## The reaper

One per shard, spawned on the shard's first touch, and only when the config actually needs it
(`min_idle > 0 || idle_timeout.is_some() || max_lifetime.is_some()`). It holds a `Weak<Shard>`,
so it stops on its own when the thread's shard goes away, and it is spawned *outside* the
`SHARDS.with` closure so the thread-local is not borrowed when the task first reaches for it.

Each tick: sleep `reap_interval` → if the pool is closed, drain and stop → drain expired →
refill up to `min_idle`, but never while a waiter is parked (a waiter will be served by a returning
connection sooner than by a fresh dial).

If there is no compio runtime on the thread, spawning is skipped silently and the pool still
works — expiry is then enforced at checkout instead. See
[decision 0009](decisions/0009-per-shard-reaper.md).

## The cross-thread exchange

Optional, off by default, and requires `Detach`.

```text
  thread A                  shared ArrayQueue              thread B
  --------                  -----------------              --------
  free list over min_idle
       │  admit() reserves a slot (CAS)
       │  Detach::detach — fd out of A's driver
       └──── push (cannot fail) ──► [ e, e, e ] ──┐
                                                   │ pop, readmit()
                                                   │ Detach::attach
                                                   └─► re-wrapped in B's driver
```

The `admitted` counter is not decoration. `ArrayQueue::push` can fail when full, but by then the
connection has already been detached and there is no synchronous way to rebuild it. So a slot is
claimed with a CAS *before* detaching: a full reservoir refuses the offer while the connection is
still whole, and the push that follows cannot fail. The count is released on pop, and on the two
failure paths.

The queue is FIFO, not LIFO, on purpose — see
[decision 0006](decisions/0006-lock-free-reservoir.md#why-fifo).

## The invariants

These are what `tests/fuzz.rs` and the libFuzzer targets assert after every step.

**Conservation — no connection is ever lost.**

```text
created == closed + live + parked + taken + cleared
```

`taken` is `Pooled::take`, which hands ownership out of the pool by design. `cleared` is
`Exchange::clear` (from `close()` and `invalidate()`), which drops parked connections *without*
counting them closed. The test driver tracks both explicitly rather than pretending they do not
happen.

**Capacity.** `shard.size() <= max_size`, always, including across a cancellation.

**Exclusivity.** No connection id is checked out twice at the same time.

**Retirement is final.** A connection retired by generation, lifetime, idle time or use count is
never handed out again.

**Admission never leaks.** Whatever sequence of parks, claims, failed detaches, failed attaches
and clears happens, it must still be possible to fill the reservoir to exactly `capacity`.

## Module map

| file | contents |
|---|---|
| [src/lib.rs](../src/lib.rs) | Crate docs and the public re-exports |
| [src/pool.rs](../src/pool.rs) | `Pool`, `Builder`, `Inner`, the `SHARDS` thread-local, `Reserved`, the reaper |
| [src/shard.rs](../src/shard.rs) | `Shard`, the free list, the waiter queue and `WaitForSlot` |
| [src/slot.rs](../src/slot.rs) | `Slot`, `SlotMeta`, `is_expired` |
| [src/guard.rs](../src/guard.rs) | `Pooled`, `OpGuard`, poisoning |
| [src/manage.rs](../src/manage.rs) | `Manage`, `Detach` |
| [src/exchange.rs](../src/exchange.rs) | `Exchange`, `NoExchange`, `Reservoir`, `Parked`, `Unparked` |
| [src/config.rs](../src/config.rs) | `Config` and its setters |
| [src/metrics.rs](../src/metrics.rs) | `Metrics`, `Counters` |
| [src/error.rs](../src/error.rs) | `Error<E>` |
