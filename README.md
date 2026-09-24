# compio-pool

A connection pool for [`compio`](https://github.com/compio-rs/compio), the completion-based,
thread-per-core Rust runtime.

```toml
[dependencies]
compio-pool = "0.1"
```

Connections live in **per-thread shards**, never behind a shared mutex. The acquire fast path
touches no atomics and takes no lock, `Manage::Connection` is never required to be `Send`, and
every connection is driven by the compio driver that created it.

---

## Why not just use `bb8` / `deadpool` / `r2d2`?

Because they cannot hold a compio connection at all.

Every general-purpose Rust pool is built on the same assumption: connections are `Send`, so the
pool can keep one `Arc<Mutex<Vec<Conn>>>` and let any worker take any connection. That assumption
holds for tokio, where IO is readiness-based — a `tokio::net::TcpStream` is a registered fd plus a
waker, and any worker thread can poll it.

compio is completion-based. Each thread runs its own driver (io_uring, IOCP, or a poll fallback),
IO handles are bound to the driver that created them, and buffers are handed to the kernel *by
ownership* for the duration of an operation. The consequence is that `compio::net::TcpStream` is
`!Send`. It cannot go into an `Arc<Mutex<_>>`, which means `bb8::ManageConnection`'s
`Connection: Send` bound rules it out at the type level. This is not a missing feature in those
crates; it is a different runtime model.

So "share one connection across threads" is not the thing to build. Two different ideas hide
behind that phrase:

* **Concurrent use of a single connection** — only meaningful for protocols that multiplex
  (HTTP/2, Redis pipelining). Otherwise a connection is an exclusive resource and "sharing" means
  taking turns.
* **A pool every thread can check out of** — any thread can get *some* connection. That is what
  this crate provides.

### The shape of the difference

| | tokio pools (`bb8`, `deadpool`, `r2d2`) | `compio-pool` |
|---|---|---|
| Connection bound | `Send` required | none — `!Send` is the normal case |
| Storage | one shared `Mutex`/semaphore | one shard per thread, in a thread-local |
| Fast-path cost | lock + atomics, contended by every worker | no lock, no atomics |
| `max_size` | per process | **per thread** (see below) |
| Work stealing | implicit: any worker, any connection | explicit and opt-in ([`Reservoir`](#letting-connections-migrate-between-threads)) |
| Cancelling mid-operation | readiness-based: safe to drop the future | completion-based: the op is still running — the connection must be destroyed |
| Thread affinity | none | a connection is owned by one driver |

---

## Quickstart

```rust
use std::time::Duration;
use compio_pool::{Config, Manage, Pool, SlotMeta};

struct Tcp(String);

impl Manage for Tcp {
    // Note: no `Send` anywhere. This is the whole point.
    type Connection = compio::net::TcpStream;
    type Error = std::io::Error;

    async fn connect(&self) -> std::io::Result<Self::Connection> {
        compio::net::TcpStream::connect(&self.0).await
    }

    async fn recycle(
        &self,
        conn: &mut Self::Connection,
        _meta: &SlotMeta,
    ) -> std::io::Result<()> {
        conn.peer_addr().map(|_| ())
    }
}

let pool = Pool::builder(Tcp("127.0.0.1:6379".into()))
    .max_size(8)                                 // per thread
    .min_idle(2)
    .acquire_timeout(Duration::from_secs(5))
    .build();

// Clone `pool` onto every compio thread; each transparently gets its own shard.
let mut conn = pool.acquire().await?;

let op = conn.begin_op();
// ... use `conn` ...
op.complete();
```

`Pool` is `Send + Sync` and cheap to clone, but holds no connections itself. Clone it onto every
compio thread; the shard is created on first touch.

---

## Design

### Per-thread shards

The `Pool` handle is an `Arc<Inner>` containing the manager, the config and counters — no
connections. The connections live in a thread-local shard keyed by pool id. That is what lets a
`Send + Sync` handle own `!Send` resources: a value in a thread-local is, by construction, only
reachable from its own thread. At thread exit the shard drops and closes its connections *on the
thread whose driver owns them*.

### `max_size` is per shard, not per process

A pool with `max_size = 8` on 4 compio threads can hold up to 32 connections. This is deliberate:
a process-wide cap requires a cross-thread semaphore on the acquire fast path, which is exactly
the contention thread-per-core runtimes exist to avoid. **Size your backend for
`max_size × threads`.** If your database has a hard connection limit, this is the number you have
to do arithmetic on, and it is the most common way to get this crate wrong.

### `recycle` runs on acquire, not on return

Returning a connection happens in `Drop`, which cannot await. So validation and protocol-state
reset (roll back an open transaction, drain a pipeline) happen at the *next* checkout. Returning
`Err` from `recycle` discards the connection and the pool transparently tries the next idle one or
dials a fresh connection.

### Cancellation is the sharp edge

This is the part that differs most from a readiness-based pool, and it is a genuine burden this
crate pushes onto you.

With completion-based IO, dropping a future with an operation in flight does **not** unwind the
operation. The kernel still owns the buffer and the peer's response is still coming. compio keeps
the *buffer* sound, but your *protocol* is now out of step: the next reader on that connection
sees the tail of someone else's response. A connection cancelled mid-operation must be destroyed,
never reused.

Wrap each operation in a guard:

```rust
let op = conn.begin_op();
let n = conn.read(&mut buf).await?;   // if this await is cancelled…
op.complete();                        // …this never runs, and the conn is poisoned on drop
```

If you never cancel — no `select!`, no timeouts, no early return between submit and completion —
you can skip it. Everyone believes that about their code right up until they add a timeout.

### Letting connections migrate between threads

Pure sharding wastes connections when load is skewed: a quiet thread holds idle sockets a busy
thread could use. The optional `Reservoir` fixes that with a bounded, lock-free `ArrayQueue`
shared by every thread.

```text
  thread A                  shared ArrayQueue              thread B
  --------                  -----------------              --------
  local free list
  over min_idle
       |
       |  Detach::detach      push (never blocks)
       +--- fd out of ------------> [ fd, fd, fd ] ---+
            A's driver                                |  pop, then
                                                      |  Detach::attach
                                                      +---> re-wrapped in
                                                            B's driver
```

A shard whose free list is over `min_idle` parks the surplus; a shard whose free list is empty pops
from the queue *before* paying for a handshake. Only idle connections take this path — a connection
is offered only after its `Pooled` guard has been dropped, and a checkout cancelled mid-operation
is poisoned and destroyed instead. `Detach::detach` returns `Option`, so an implementation gets to
*check* (via `SharedFd::try_unwrap`) rather than trust that nothing is still submitted.

It is opt-in because its soundness is platform-dependent:

| driver | `attach` | connections may change threads |
|---|---|---|
| io_uring (Linux) | no-op | yes |
| poll (Unix fallback) | no-op | yes |
| IOCP (Windows) | `CreateIoCompletionPort`, once per handle | **no — do not implement `Detach`** |

Under IOCP a handle binds to one completion port for life, so a stolen socket would keep delivering
completions to the thread that opened it. See `examples/steal.rs` for a full `Detach` implementation
over a real `TcpStream`.

---

## When this crate is the right choice — and when it is not

### Situational advantages

* **It works at all with `!Send` connections.** If you are on compio, this is not a performance
  argument, it is the only argument that matters: `bb8` and `deadpool` cannot compile against
  `compio::net::TcpStream`.
* **The fast path is a thread-local `Vec::pop`.** No mutex, no semaphore, no atomic RMW on a
  cache line every core is writing to. It measures in the tens of nanoseconds (see
  [Benchmarks](#benchmarks)).
* **Per-thread cost stays flat as cores are added.** There is nothing shared and mutable on the
  hit path to ping-pong between caches. A globally-locked pool degrades here; this is the whole
  reason thread-per-core runtimes exist.
* **Connections are always driven by the driver that created them**, including at shutdown —
  thread exit closes that thread's connections on that thread.
* **Cache and NUMA locality.** A connection, its buffers and its driver stay on one core.
* **Cancellation correctness is enforceable.** `begin_op` makes "a cancelled checkout must die" a
  type-level obligation rather than a comment in a design doc.
* **Work stealing is a policy you choose**, with a tunable split point (`min_idle`) between local
  caching and sharing, rather than something the pool does implicitly on every checkout.

### Situational disadvantages

Be honest with yourself about these before adopting it.

* **No global connection cap.** `max_size × threads` is your real ceiling. Against a backend with
  a strict `max_connections` (Postgres, a rate-limited API), you must do that arithmetic yourself,
  and a pool sized for one thread will over-dial on sixteen.
* **Skewed load wastes connections** unless you install a `Reservoir` — and that requires `Detach`,
  which is unsound on Windows/IOCP. On Windows you are stuck with pure sharding.
* **More connections overall.** Per-thread `min_idle` means warm sockets multiply by thread count.
  Sixteen threads at `min_idle = 2` is 32 idle connections against a backend that may have been
  sized for 8.
* **Cancellation discipline is on you.** Forget `begin_op` around an operation you cancel, and you
  will return a desynchronized connection to the pool. The failure mode is a protocol corruption
  that surfaces on some *later* request, which is a miserable thing to debug.
* **`recycle` runs at checkout, so it is on the latency path.** A `PING` there is paid by the
  request, not by a background task.
* **Waiters are not FIFO-fair.** Under oversubscription a waiter can be repeatedly overtaken by
  later arrivals that race the free list directly, producing an unbounded tail rather than
  fair queueing. `acquire_timeout` masks this as a timeout instead of a hang. See
  [Known limitations](#known-limitations).
* **Per-thread state complicates a global view.** Gauges are summed across threads without a lock,
  so a `Metrics` snapshot can be momentarily internally inconsistent.
* **The ecosystem is small.** There is no `compio-postgres` or `compio-redis` waiting for you.
  `Manage` is easy to implement, but you are implementing it.
* **Version 0.1, no published crate, one author.** Treat it accordingly.

### Rules of thumb

Use it when you are already on compio, when your workload is many short requests over long-lived
connections, and when per-core scaling is the thing you are buying.

Do not reach for it when your backend imposes a hard global connection limit you cannot express as
`per-thread × threads`, when you are on Windows *and* need connections to migrate, or when you are
on tokio — there, `bb8` and `deadpool` are the right tools and this crate has nothing to offer you.

---

## Configuration

| Setting | Default | Notes |
|---|---|---|
| `max_size` | 16 | **per thread** |
| `min_idle` | 0 | idle connections kept thread-locally; also the local/shared split point |
| `acquire_timeout` | 30s | `None` waits forever |
| `max_lifetime` | 30min | hard age cap |
| `idle_timeout` | 10min | reaped while idle |
| `max_uses` | `None` | retire after N checkouts |
| `reap_interval` | 30s | how often each shard's background reaper runs |

With a `Reservoir` installed, `min_idle` decides how much stays local: leaving it at `0` means
*every* return crosses the shared queue. Set it to your steady-state per-thread concurrency so the
hot path stays lock-free and only the surplus is shared.

Other pool operations: `warm()` pre-opens up to `min_idle` on the current thread, `invalidate()`
retires every connection created before the call (credential rotation, failover), `close()` shuts
the pool down, and `metrics()` returns a snapshot of gauges (`live`, `idle`, `parked`) and counters
(`created`, `closed`, `acquires`, `waits`, `timeouts`, `poisoned`, `recycle_failures`, `unparked`).

---

## Benchmarks

Figures below are from the author's machine; reproduce with `cargo bench`. Treat them as shape,
not as numbers to quote.

**Acquire fast path** (`cargo bench --bench acquire`): ~65 ns/op with `acquire_timeout = None`,
~127 ns/op with a timeout armed — the difference is the timer, not the pool. The benchmark also
decomposes the cost (shard lookup, `Instant::now`, the guard allocation) and reports per-thread
scaling at 1/2/4/8 threads.

**Exchange, lock-free queue vs. the `Mutex<Vec<_>>` it replaced** (`cargo bench --bench exchange`,
both designs compiled into one binary and alternated). The result is *not* the clean win the phrase
"lock-free" suggests:

```
park + unpark, mean ns/op (worst thread)
  threads    ArrayQueue    Mutex<Vec>
        1          28.7          30.1
        2         183.1         157.1
        4         556.1         403.3
        8        1821.8         857.4
```

Hammered with nothing between operations, the mutex is up to 2.1× faster on the mean. Its latency
distribution says why:

```
8 threads          p50     p99    p99.9       max
  ArrayQueue      1500    5875    10834    331209
  Mutex<Vec>        42   29291    62958    477917
```

A p50 of 42 ns against a mean of 857 ns is a bimodal, unfair distribution: a thread already holding
the lock reacquires it while others queue behind. The queue trades median for fairness — 5.0× better
p99, 5.8× better p99.9. Once there is real work between exchange operations, which is the actual
workload, the queue also wins outright:

```
full acquire path, min_idle=0
  threads    ArrayQueue    Mutex<Vec>
        1          73.8          77.5
        2         386.8         389.5
        4         543.5         782.5
        8        1907.8        2695.3
```

The justification for the lock-free design is tail latency and behaviour as threads are added, not
raw throughput. Recorded here so the next person to look at the mean does not "optimise" it back
to a mutex.

---

## Examples

All runnable with `cargo run --example <name>`.

| Example | What it shows |
|---|---|
| `tcp` | pooling real `!Send` `TcpStream`s across several compio threads |
| `std_tcp` | interop both ways: adopting a `std::net::TcpStream`, and borrowing a pooled one back out to std-only APIs without closing it |
| `steal` | a full `Detach` implementation over a real socket, and four threads feeding a fifth with zero dials |
| `ncat_bench` | the pool against a real external `ncat` server: steady state, oversubscribed, and no pool at all — including the `TIME_WAIT`/ephemeral-port exhaustion the unpooled path hits |
| `ncat_steal_bench` | the exchange on and off against a real server: cold-start acquire, connections dialled, and what the exchange costs on the hot path |

---

## Known limitations

* **Waiter fairness.** The acquire loop checks the free list before the wait queue, so a late
  arrival can barge past a parked waiter. Under sustained oversubscription this yields an unbounded
  tail rather than FIFO-fair queueing. Tracked for a follow-up; `acquire_timeout` bounds it in the
  interim.
* **`Detach` is unsound under IOCP.** Cross-thread migration is a Unix-only capability today.
* **No global cap.** By design, but see the disadvantages above.
* **Metrics gauges are lock-free sums** and can be momentarily inconsistent with each other.
  Counters are monotonic.

---

## Requirements

Rust edition 2024 (1.85+), `compio` 0.18. The library itself depends only on `compio`
(`runtime`, `time`) and `crossbeam-queue`. CI covers Linux, macOS and Windows on stable, plus beta
on Linux.

## License

MIT OR Apache-2.0, at your option.
