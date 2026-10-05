# compio-pool

## [ NOTE: Pre-1.0 and under active development. The pool core, its metrics, maintenance and graceful shutdown are in place and tested; the API may still change before 1.0. ]

[![crates.io](https://img.shields.io/crates/v/compio-pool.svg)](https://crates.io/crates/compio-pool)
[![docs.rs](https://img.shields.io/docsrs/compio-pool)](https://docs.rs/compio-pool)
[![CI](https://github.com/artech-git/compio-pool/actions/workflows/ci.yml/badge.svg)](https://github.com/artech-git/compio-pool/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/rustc-1.95+-blue.svg)](#requirements)
[![license](https://img.shields.io/crates/l/compio-pool.svg)](#license)

A bb8-style connection pool for [`compio`](https://github.com/compio-rs/compio) on Linux
`io_uring`.

You build one `Pool` — a manager plus a handful of `Builder` knobs — then clone that handle onto
each thread of your own thread-per-core runtime. On every thread you call `Pool::local` once to
get that thread's `LocalPool`, and `LocalPool::get` to lease a connection. The pool is the
product; spawning threads, pinning them and running an accept loop are yours (the crate ships the
`cpu` and `listener` helpers for that half).

```toml
[dependencies]
compio-pool = "0.1"
```

📖 **[API documentation on docs.rs](https://docs.rs/compio-pool)**

**Design notes** — [architecture](docs/architecture.md) · [design decisions](docs/decisions/) ·
[operating guide](docs/operations.md) · [performance](docs/performance.md) ·
[testing](docs/testing.md)

---

## Why it is shaped this way

A `compio` connection is bound to the `io_uring` ring of the thread that opened it: its buffers
are registered with that ring and its completions are delivered there. It cannot be used from
another thread. So, unlike bb8 on `tokio`, the pool of connections cannot be a single shared,
work-stealing store. What *is* shared is only the blueprint:

* **`Pool` — `Send + Sync + Clone`.** The manager and the configuration, behind one `Arc`.
  Cloning is an `Arc` bump. This is the only thing that crosses threads.
* **`LocalPool` — `!Send`, one per thread.** The idle connections and the slot count, all
  `Rc`/`Cell`, no lock and no atom on the hot path. The compiler keeps it — and the ring-bound
  connections it holds — on its own thread.

Every size is **per thread**: `max_size` caps one thread's `LocalPool`, not the process. A
48-core server with `max_size(16)` can hold up to `48 × 16` connections, 16 of them to any one
thread's ring.

---

## How it works internally

Everything on the lease path is thread-local and lock-free. A `LocalPool` is an `Rc<Inner>` over
three pieces of state, all `Cell` / `RefCell`:

* an **idle free-list** — a `Vec` used LIFO, so `get` hands back the most recently returned
  connection (warmest in cache, least likely to have aged out);
* a **slot count** — connections this thread has open right now (idle + leased), capped at
  `max_size`;
* a **waiter list** — tasks parked inside `get` because the thread is at `max_size`.

`LocalPool::get` does one of three things, in order:

1. **Reuse.** Pop the newest idle connection. With `test_on_check_out` set it runs `is_valid`
   first; a connection that fails is dropped (its slot freed) and the next one tried.
2. **Open.** If none is idle and the slot count is below `max_size`, claim a slot and `await` the
   manager's `connect`.
3. **Wait.** Otherwise register one waker and park up to `connection_timeout`, then retry; a
   timeout returns `RunError::TimedOut`.

The returned `PooledConnection` derefs to the connection. On **drop** the pool asks the manager
`has_broken` and checks `idle_timeout` / `max_lifetime`: a healthy, in-date connection is pushed
back onto the idle list; anything else is dropped and its slot freed. Either way one parked waiter
is woken. None of this touches an atomic or a lock — it is a `Vec` push/pop and a counter bump on
a single thread.

The only atomics in the crate are the **per-thread metric counters** behind `State` /
`Statistics`, kept relaxed so an off-thread scrape can sum every worker's view without disturbing
the hot path. Expiry is **lazy** — a connection's age is checked only as it is handed out or
returned — so a pool that goes quiet keeps its connections until you run
[`maintain`](#maintenance) on a timer.

---

## Quickstart

Define a manager, build the pool once, and lease connections on each worker:

```rust
use std::io;

use compio_pool::{ManageConnection, Pool};

/// A pool of reusable 16 KiB scratch buffers. A real manager would open a
/// backend connection (Redis, Postgres, an upstream socket) in `connect`.
struct Buffers;

impl ManageConnection for Buffers {
    type Connection = Vec<u8>;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<Vec<u8>> {
        Ok(Vec::with_capacity(16 * 1024))
    }
    async fn is_valid(&self, _buf: &mut Vec<u8>) -> io::Result<()> {
        Ok(())
    }
    fn has_broken(&self, _buf: &mut Vec<u8>) -> bool {
        false
    }
}

// Build once; `Pool` is `Send + Clone`, so hand a clone to each thread.
let pool = Pool::builder().max_size(1024).build(Buffers);

// On each compio thread you spawn (see `examples/echo.rs` for the full
// pinned, SO_REUSEPORT thread-per-core setup):
let local = pool.local(); // this thread's own pool, `!Send`
let mut buf = local.get().await?; // PooledConnection, returned on drop
buf.clear();
```

One trait is all you implement:

* **`ManageConnection`** teaches the pool how to `connect`, how to `is_valid` a reused connection
  as it is checked out, and how to tell if one `has_broken` as it is returned. The manager value
  is `Send + Sync` (it is shared behind an `Arc`); the connections it makes are not, and never
  leave their thread.

`LocalPool::get` reuses a live idle connection, or opens one while under `max_size`, or waits up
to `connection_timeout` for a slot. The returned `PooledConnection` derefs to the connection and
returns it to this thread's pool when dropped.

---

## Runtime designs

The pool is a blueprint (`Send`) plus one `!Send` `LocalPool` per thread, so it drops into any
compio runtime shape without change: you call `pool.local()` once per runtime and lease from it.
compio owns the ring; you choose how many rings there are and how concurrency spreads across them.
Four native shapes, smallest to largest — each is a runnable example.

### 1. One ring, the macro — `#[compio::main]`

The least ceremony: a single `io_uring` on the current thread. Good for CLIs, tests and
single-tenant tools.

```rust
#[compio::main]
async fn main() -> std::io::Result<()> {
    let pool = Pool::builder().max_size(16).build(Buffers);
    let local = pool.local();          // this runtime's pool
    let mut buf = local.get().await?;  // lease, returned on drop
    buf.clear();
    Ok(())
}
```

### 2. One ring, built by hand — `Runtime::builder`

Same single ring, but you own `main` (and can tune the ring — see below). This is what most
examples do.

```rust
use compio::runtime::Runtime;

let runtime = Runtime::builder().build()?;
runtime.block_on(async {
    let pool = Pool::builder().max_size(16).build(Buffers);
    let local = pool.local();
    // ... accept loop / client work on this one ring ...
});
```

### 3. One ring, many tasks — task-per-connection

Concurrency comes from compio *tasks*, not threads: one ring serves thousands of connections,
each a cheap task spawned with `compio::runtime::spawn`. No pinning, no `SO_REUSEPORT`.
See [`examples/task_echo.rs`](examples/task_echo.rs),
[`fanout_requests`](examples/fanout_requests.rs),
[`scatter_gather`](examples/scatter_gather.rs), [`http_proxy`](examples/http_proxy.rs).

```rust
let local = pool.local();
loop {
    let (stream, _peer) = listener.accept().await?;
    // each connection is a task on this same ring; clone the LocalPool (Rc bump)
    compio::runtime::spawn(serve(local.clone(), stream)).detach();
}
```

### 4. One ring per core — thread-per-core + `SO_REUSEPORT`

The design that scales: one pinned OS thread per core, each with its own runtime, its own
`SO_REUSEPORT` listener (the kernel splits connections by flow hash), and its own `LocalPool`
carved from the one shared `Pool`. See [`examples/echo.rs`](examples/echo.rs) and
[`udp_echo`](examples/udp_echo.rs), [`file_server`](examples/file_server.rs),
[`tcp_pool`](examples/tcp_pool.rs), [`wal`](examples/wal.rs), [`unix_echo`](examples/unix_echo.rs).

```rust
use compio::{net::TcpListener, runtime::Runtime};
use compio_pool::{bind_reuseport, cpu};

for core in cpu::cores() {
    let pool = pool.clone();           // Send blueprint; one clone per worker
    std::thread::spawn(move || {
        cpu::pin_current(core);
        let runtime = Runtime::builder().build().expect("ring");
        runtime.block_on(async move {
            let local = pool.local();  // this thread's !Send pool
            let std = bind_reuseport(addr, 1024, None).expect("SO_REUSEPORT");
            let listener = TcpListener::from_std(std).expect("wrap in ring");
            loop {
                let Ok((stream, _)) = listener.accept().await else { continue };
                compio::runtime::spawn(serve(local.clone(), stream)).detach();
            }
        })
    });
}
```

For a thread pool that *dispatches* work onto a set of background rings rather than owning an
accept loop, compio also ships [`compio::dispatcher`](https://docs.rs/compio-dispatcher) — build
one `Pool` and call `pool.local()` on each dispatched runtime exactly as above.

### Tuning the ring (any design)

The ring is configured through a `ProactorBuilder` handed to `Runtime::builder().with_proactor(…)`.
All of these are **off by default**:

```rust
use compio::driver::ProactorBuilder;

let mut proactor = ProactorBuilder::new();
proactor
    .capacity(1024)          // SQ/CQ entries
    .single_issuer(true)     // only this thread submits — fits thread-per-core
    .coop_taskrun(true)      // batch completions, fewer wakeups
    .taskrun_flag(true);     // let the kernel signal pending completions
// .sqpoll_idle(Duration::from_millis(100)) // kernel-side submission polling

let runtime = compio::runtime::Runtime::builder()
    .with_proactor(proactor)
    .build()?;
```

`single_issuer` and the `defer_taskrun`-family flags need newer kernels than compio's 5.19
baseline — see [Requirements](#requirements). They are a per-thread win for the thread-per-core
design, where exactly one thread ever touches each ring.

---

## Configuration

Every knob is **per thread**. Defaults match bb8.

| knob | default | meaning |
|---|---|---|
| `max_size` | 10 | most connections one thread keeps open at once (idle + checked out) |
| `min_idle` | 0 | connections each thread opens up front and keeps idle (`warm` / `maintain`) |
| `connection_timeout` | 30 s | how long `get` waits for a slot and a live connection before `TimedOut` |
| `idle_timeout` | 10 min | drop an idle connection unused for this long (`None` = never) |
| `max_lifetime` | 30 min | drop a connection older than this, idle or not (`None` = never) |
| `test_on_check_out` | true | call `ManageConnection::is_valid` on a reused connection as it is checked out |

```rust
let pool = Pool::builder()
    .max_size(16)
    .min_idle(4)
    .connection_timeout(Duration::from_secs(5))
    .build(manager);
```

---

## Running it in production

Three things a long-lived server needs are built on the same `Send` / `!Send` split.

### Metrics

Each `LocalPool` keeps its own lock-free counters and registers them with the `Pool`. Read one
thread's, or sum every thread's for a process-wide view to feed a metrics endpoint:

```rust
let here = local.state();       // this thread: State { connections, idle_connections, pending_waiters }
let all  = pool.state();        // summed across every thread

let s = pool.statistics();      // cumulative Statistics across every thread
// s.get_direct, s.get_waited, s.get_timed_out,
// s.connections_created,
// s.connections_closed_broken / _idle_timeout / _max_lifetime,
// s.total_wait_time (and s.average_wait_time())
```

`Pool::state` / `Pool::statistics` take a lock to walk the registry, so call them off the hot
path (a scrape, a health check). `LocalPool::state` / `LocalPool::statistics` are lock-free.

### Maintenance

Expiry is lazy — a connection is only checked as it is handed out or returned — so a pool that
goes quiet would hold dead backend connections open. Thread-per-core has no shared reaper thread,
so you drive maintenance per worker on a timer:

```rust
compio::runtime::spawn(async move {
    loop {
        compio::time::sleep(Duration::from_secs(30)).await;
        let _ = local.maintain().await; // reap expired idle, then warm back to min_idle
    }
})
.detach();
```

`maintain` = `reap` (drop idle connections past `idle_timeout` / `max_lifetime`, free their
slots) + `warm` (top back up to `min_idle`). `LocalPool::clear` drains a thread's idle
connections unconditionally — useful after a backend failover.

### Shutdown

`Pool::close` makes every thread's `get` fail with `RunError::Closed`, stops recycling (a
returned connection is dropped, not parked), and turns `warm` / `maintain` into no-ops.
`is_closed` reports the flag. For a prompt drain, stop each worker's accept loop and call
`LocalPool::clear` on its thread — the shutdown flag is shared, but the wakers that unpark a
parked `get` live on each worker's own thread.

---

## The thread-per-core half

The crate ships the two helpers that building your own accept loop needs, so the pool and the
runtime around it come from one place:

* **`cpu`** — `cpu::cores()` returns the affinity mask (honours `taskset` / cgroup cpusets),
  `cpu::pin_current(core)` pins a thread, `cpu::affinity_mask(cpu)` formats an IRQ mask.
* **`listener::bind_reuseport`** — a raw TCP socket with `SO_REUSEADDR` + `SO_REUSEPORT` set
  before `bind` (so every thread binds the same address and the kernel splits connections by flow
  hash), and optional `SO_INCOMING_CPU`.

Worked examples:

```sh
# Pool reusable buffers behind a thread-per-core SO_REUSEPORT echo server:
cargo run --release --example echo -- 0.0.0.0:7000

# The canonical use — pool upstream TCP client connections to a backend:
cargo run --release --example echo    -- 127.0.0.1:7001          # a backend
UPSTREAM=127.0.0.1:7001 \
cargo run --release --example tcp_pool -- 0.0.0.0:7000 64        # the pooling front

# Watch State snapshots as a single pool warms, lends and reaps:
cargo run --example warmup

# Drive any of the above:
cargo run --release --example load -- 127.0.0.1:7000 --conns 64 --seconds 5
```

---

## Porting from tokio: sharp edges

compio is completion-based (`io_uring`), not readiness-based (`epoll`), and thread-per-core, not
work-stealing. Most of the friction moving code over comes from those two facts. The ones that
bite:

* **Connections are `!Send`.** A `compio::net::TcpStream` (and anything holding one, including a
  `PooledConnection`) is bound to the ring that made it and cannot move to another thread. There is
  no `tokio::spawn`-style work-stealing: you cannot accept on one thread and serve on another, and
  you cannot stash a connection in a `Send` global. This is *why* the pool is per-thread — a single
  shared, work-stealing pool is impossible, so don't reach for one.

* **Buffers are moved, not borrowed.** I/O takes ownership of the buffer for the duration of the
  op and gives it back in a `BufResult(result, buf)`: `let BufResult(n, buf) = stream.read(buf).await;`.
  There is no `&mut [u8]` read. Code ported from tokio has to thread the buffer through every call
  and take it back out — hence the `std::mem::take(&mut *lease)` / `*lease = buf` dance in the echo
  examples.

* **Dropping a future with I/O in flight is not free.** The kernel owns a submitted buffer until
  the op completes. compio keeps it alive and cancels on drop, but a connection whose `get()`/op
  you cancel may not free its slot until the in-flight op resolves. Don't assume `tokio`-style
  instant cancellation of a half-finished read.

* **`tokio::` is not `compio::`.** `tokio::time` → `compio::time`, `tokio::spawn` →
  `compio::runtime::spawn` (returns a `!Send` task; `.detach()` or await it on the same thread).
  There is no `tokio::select!`; use `futures` combinators or `compio::time::timeout`. compio's
  `AsyncRead` / `AsyncWrite` traits are its own (ownership-passing), *not* tokio's, so tokio-based
  middleware and most of the `tower` / `hyper` ecosystem will not compile against compio streams
  without a bridge. Budget for a thin adapter layer or compio-native crates.

* **Blocking still blocks the core.** With one ring per thread there is no second worker to absorb a
  blocking call or a long CPU loop — it stalls every connection on that core. Keep handlers
  `await`-ing; push CPU-heavy work off the ring deliberately.

* **Shutdown during a panic can abort the process.** compio's executor aborts if a task waker runs
  on a thread that is already unwinding. Drive shutdown explicitly (close the pool, stop accept
  loops, drain) rather than relying on drop order while panicking — see
  [decision 0006](docs/decisions/0006-shutdown-and-the-unwinding-thread.md).

* **Maintenance is yours.** tokio pools (bb8/deadpool) run a background reaper thread. Thread-per-core
  has none, so expiry is lazy and you must call [`maintain`](#maintenance) on a timer per worker, or
  idle connections never reap.

---

## Host tuning

```sh
scripts/tune-nic.sh --check                       # what the NIC can do; exits 2 if the recipe cannot apply
sudo scripts/tune-nic.sh --cpus 0,1,2,3,4,5,6,7   # same order as your worker cores
```

The script sizes the NIC queues to the worker count, masks `irqbalance`, pins each RX queue's
IRQ to its worker's core, enables ntuple filters for accelerated RFS and sizes the flow tables.
It refuses nothing it cannot do: on a single-queue virtual NIC it reports the gap and configures
software RFS only. `SO_INCOMING_CPU` (via `listener::bind_reuseport`) is worth turning on only
after the NIC is tuned — see [decision 0005](docs/decisions/0005-incoming-cpu-opt-in.md) and the
[operating guide](docs/operations.md).

---

## Numbers

Benchmarked against `tokio` + `deadpool` on cloud VMs; full method, tables and caveats live under
[docs/results/](docs/results/) and [performance.md](docs/performance.md).

### ARM — `compio-bench-arm-36`

GCP `t2a-standard-32` (Ampere Altra, Neoverse-N1, **32 vCPU**, 1 thread/core, Ubuntu 24.04 /
kernel 7.0, rustc 1.99), 2026-10-04. This run isolates the **echo** path — the one the pool sits
directly on — against two tokio baselines: the work-stealing default and a pinned
`current_thread`-per-core, both on epoll.

| 512 B echo | tokio-default | tokio-per-core | **compio-pool** | vs per-core |
|---|--:|--:|--:|:--:|
| throughput, W=4 | 330 k | 348 k | **436 k** req/s | 1.25× |
| throughput, W=8 | 633 k | 688 k | **871 k** req/s | 1.27× |
| throughput, W=12 (peak) | 716 k | 1.08 M | **1.20 M** req/s | 1.11× |
| p99 latency, W=8 | 834 µs | 482 µs | **365 µs** | — |
| server CPU per request | 11.4 µs | 11.4 µs | **9.1 µs** | ~20 % less |

In short: **1.25–1.29×** the throughput of tokio-per-core at 512 B (and **1.5–1.67×** the
work-stealing default), with **~20 % less server CPU per request** and the tightest tail everywhere
(p99 **365 µs** vs **834 µs** at W=8). While saturated, compio issues **~0.03–0.13 syscalls/request
against ~2.0** for both tokio variants (io_uring batches submit+complete; epoll pays a read and a
write each time), holds **5–9 MB RSS vs ~11–12 MB**, and records **zero cpu-migrations** where
work-stealing bounced tasks **~50,000 times** in the saturated 32-worker run. The edge narrows as
payloads grow — by 8 KiB the per-core baseline draws level, because `memcpy`, not the runtime,
dominates. The bb8 pool itself adds no measurable hot-path cost. Full tables:
[docs/results/gcp-t2a-standard-32/](docs/results/gcp-t2a-standard-32/).

### x86 — `n2-standard-48`

GCP n2-standard (**48 vCPU**), 2026-10-04: wins file I/O **1.8–4×**, ties small echo, loses 64 K
echo and connection churn, and scales close to linearly thread-per-core (work-stealing collapses at
the top end). [docs/results/gcp-n2-standard-48/](docs/results/gcp-n2-standard-48/), plus a Redis
proxy run under [gcp-n2-standard-48-redis/](docs/results/gcp-n2-standard-48-redis/).

---

## Requirements

* **Linux.** `SO_REUSEPORT` load balancing and `io_uring` are Linux semantics. The crate
  type-checks on other Unixes so editors work; nothing is tested there.
* **Kernel:** 5.19+ for compio's default `io_uring` flags; newer for `single_issuer` /
  `defer_taskrun` if you enable them in the runtime.
* **Rust 1.95+.** compio 0.19's executor uses `cfg_select!`, stable since 1.95; every toolchain
  from 1.88 to 1.94 was tried in the VM and fails to build the dependency tree. Enforced by CI.

## Development

All building, testing and tuning happens on Linux (`io_uring` and `SO_REUSEPORT` are Linux
semantics); a Linux VM works fine when the host is not. See [testing.md](docs/testing.md) for the
exact loop.

## License

MIT OR Apache-2.0.
