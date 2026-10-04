# compio-pool

## [ NOTE: This crate is not production ready yet and under active development ]

[![crates.io](https://img.shields.io/crates/v/compio-pool.svg)](https://crates.io/crates/compio-pool)
[![docs.rs](https://img.shields.io/docsrs/compio-pool)](https://docs.rs/compio-pool)
[![CI](https://github.com/artech-git/compio-pool/actions/workflows/ci.yml/badge.svg)](https://github.com/artech-git/compio-pool/actions/workflows/ci.yml)
[![MSRV](https://img.shields.io/badge/rustc-1.95+-blue.svg)](#requirements)
[![license](https://img.shields.io/crates/l/compio-pool.svg)](#license)

A thread-per-core accept server for [`compio`](https://github.com/compio-rs/compio) on Linux
`io_uring`.

One pinned worker per CPU core. Each worker owns its own ring (a thread-local compio runtime),
its own `SO_REUSEPORT` listener on the shared address, and a strictly thread-local pool of
resources that connections borrow while they are served. The kernel hashes each incoming
connection to one listener, so a connection is accepted, served and closed on one core, and
nothing about it is ever shared.

When a worker's pool is full it strips the accepted socket back to a raw file descriptor and
pushes it onto a single bounded, lock-free channel. Every worker polls that channel whenever it
has a free slot, rebuilds the stream on *its* ring, and serves it there. Overflow flows to
whichever core is idle; that channel and a set of per-core counters are the only shared state.

```toml
[dependencies]
compio-pool = "0.1"
```

📖 **[API documentation on docs.rs](https://docs.rs/compio-pool)**

**Design notes** — [architecture](docs/architecture.md) · [design decisions](docs/decisions/) ·
[operating guide](docs/operations.md) · [performance](docs/performance.md) ·
[testing](docs/testing.md)

---

## The recipe

The crate is the Rust half of a seventeen-step deployment recipe. The first four steps are
host configuration and live in [`scripts/tune-nic.sh`](scripts/tune-nic.sh); the rest are the
library.

| step | what | where |
|---|---|---|
| 1 | NIC RX/TX queue count = worker count | `tune-nic.sh` (`ethtool -L`) |
| 2 | stop, disable and mask `irqbalance` | `tune-nic.sh` |
| 3 | pin each RX queue's IRQ to its worker's core | `tune-nic.sh` (`/proc/irq/*/smp_affinity`) |
| 4 | accelerated RFS + flow table sizing; `SO_INCOMING_CPU` on each listener | `tune-nic.sh`, [`Config::incoming_cpu`](https://docs.rs/compio-pool/latest/compio_pool/struct.Config.html#structfield.incoming_cpu) |
| 5 | `compio`, `socket2`, `core_affinity`, `flume` at their current versions | [`Cargo.toml`](Cargo.toml) |
| 6 | one bounded MPMC channel for fd handoff | [`handoff`](src/handoff.rs) |
| 7 | enumerate cores, one thread each | [`cpu`](src/cpu.rs), [`server`](src/server.rs) |
| 8 | pin each thread inside its closure | [`worker`](src/worker.rs) |
| 9 | raw `socket2` socket, `SO_REUSEADDR` + `SO_REUSEPORT` before `bind` | [`listener`](src/listener.rs) |
| 10 | wrap it in the thread's own compio runtime | [`worker`](src/worker.rs) |
| 11 | thread-local `Rc`-based resource pool | [`pool`](src/pool.rs) |
| 12 | accept loop on this ring | [`worker`](src/worker.rs) |
| 13 | capacity check on arrival; serve locally if there is room | [`worker`](src/worker.rs) |
| 14 | detach the fd from the ring when the pool is full | [`handoff::detach`](src/handoff.rs) |
| 15 | push it onto the channel | [`worker`](src/worker.rs) |
| 16 | claim loop on every worker | [`worker`](src/worker.rs) |
| 17 | re-attach the fd to the idle thread's ring | [`handoff::attach`](src/handoff.rs) |

---

## Quickstart

```rust
use std::io;

use compio::{BufResult, io::{AsyncRead, AsyncWriteExt}};
use compio_pool::{Connection, Resource, Server, Service, WorkerContext, Workers};

/// One buffer per in-flight connection, allocated on the core that uses it.
struct Buf(Vec<u8>);

impl Resource for Buf {
    async fn create(_cx: &WorkerContext) -> io::Result<Self> {
        Ok(Buf(Vec::with_capacity(16 * 1024)))
    }
}

#[derive(Clone)]
struct Echo;

impl Service for Echo {
    type Resource = Buf;

    async fn handle(&self, mut conn: Connection, buf: &mut Buf) -> io::Result<()> {
        loop {
            let mut b = std::mem::take(&mut buf.0);
            b.clear();
            let BufResult(read, b) = conn.stream.read(b).await;
            match read {
                Ok(0) => { buf.0 = b; return Ok(()); }
                Ok(_) => {}
                Err(e) => { buf.0 = b; return Err(e); }
            }
            let BufResult(written, b) = conn.stream.write_all(b).await;
            buf.0 = b;
            written?;
        }
    }
}

fn main() -> io::Result<()> {
    let server = Server::builder(Echo)
        .bind("0.0.0.0:7000".parse().unwrap())
        .workers(Workers::AllCores)
        .capacity(1024) // connections in service per worker
        .start()?;
    println!("{} workers on {}", server.workers(), server.local_addr());
    server.join().expect("a worker panicked");
    Ok(())
}
```

Two traits and you are done:

* **`Resource`** is what a worker keeps a bounded number of and lends to one connection at a
  time — a buffer, a parser, a connection to a backend. It is created, used and dropped on one
  thread, so nothing about it needs to be `Send`.
* **`Service`** is the per-connection handler. One clone lives on each worker. The stream it is
  handed belongs to that worker's ring and must not leave the thread.

Run the shipped reference server and drive it:

```sh
cargo run --release --example echo -- 0.0.0.0:7000 1024          # addr, capacity/worker, [workers]
cargo run --release --example load -- 127.0.0.1:7000 --conns 64 --seconds 5
cargo run --release --example load -- 127.0.0.1:7000 --conns 64 --reconnect 1   # exercises handoff
```

---

## What happens to a connection

```text
                 kernel: SO_REUSEPORT group, one socket per worker, pick by flow hash
                                   │                     │
                         ┌─────────▼────────┐   ┌────────▼─────────┐
                         │ worker 0 (cpu 0) │   │ worker 1 (cpu 1) │
                         │  io_uring        │   │  io_uring        │
                         │  listener        │   │  listener        │
                         │  LocalPool  ◄────┼───┼──► LocalPool     │   Rc/Cell, never shared
                         └──┬───────────▲───┘   └──┬───────────▲───┘
            accept ──► try_reserve      │ claim     │           │ claim
                        │   ok ──► serve│           │           │
                        │   full        │           │           │
                        └► detach fd ─► │  flume::bounded<Overflow { fd, peer, .. }>  ◄──┘
                                        └───────────────────────┘
```

1. The kernel delivers the connection to one worker's listener. Its `accept` completes on that
   worker's ring.
2. The worker calls `LocalPool::try_reserve`. No lock, no atomic: it is a `Cell` compare. With
   room, a `serve` task is spawned on the same ring and the connection never leaves the core.
3. Without room, the stream is taken apart into its `OwnedFd` and pushed onto the channel. If
   the channel is full too, the connection is served over capacity or closed, by policy.
4. Every worker runs a claim loop that waits for a free slot *first*, then takes one fd off the
   channel and rebuilds the stream with `TcpStream::from_std` on its own ring. The slot is not
   held while waiting, so an idle worker keeps its whole capacity for its own listener.
5. If the slot vanished between the wait and the claim (the listener got there first), the fd
   goes back onto the channel, up to `max_hops` times, then is served over capacity.

The handler sees how its connection arrived in `Connection::route`, and every one of these
transitions is a counter in `Server::stats()`.

---

## Configuration

| knob | default | meaning |
|---|---|---|
| `workers` | `AllCores` | one worker per core in the process's affinity mask; or `Count(n)`; or `Cores(vec)` |
| `capacity` | 1024 | connections in service **per worker** |
| `handoff_capacity` | 4096 | accepted-but-unserved connections that can wait, process-wide |
| `max_hops` | 4 | re-pushes before a claimed-then-lost connection is served over capacity |
| `overflow` | `ServeLocally` | or `Reject`: what happens when the channel is full as well |
| `prewarm` | 0 | resources created per worker before accepting |
| `backlog` | 4096 | `listen(2)` backlog per listener |
| `nodelay` | true | `TCP_NODELAY` on accepted streams |
| `incoming_cpu` | **false** | `SO_INCOMING_CPU` on each listener. Turn on after `tune-nic.sh`; see below |
| `pin` | true | pin each worker thread to its core |
| `drain_timeout` | 5 s | how long shutdown waits for in-flight connections |
| `uring` | coop_taskrun + single_issuer | `io_uring_setup` flags per ring; see [operations](docs/operations.md#io_uring-flags) |

**`capacity` is per worker.** Multiply by `workers` for the process. A global ceiling would need a
shared counter on the accept path, which is exactly what this design exists to avoid.

**`incoming_cpu` is off until the NIC is tuned.** The kernel picks the listener whose
`SO_INCOMING_CPU` matches the CPU the SYN arrived on and only falls back to the hash when none
matches. Without queue pinning, whichever worker shares a core with the CPU processing SYNs
gets every connection. In the test VM, with the client on one worker's CPU, that worker took
64 of 64 connections in 20 of 20 trials. See
[decision 0005](docs/decisions/0005-incoming-cpu-opt-in.md).

---

## Host tuning

```sh
scripts/tune-nic.sh --check                       # what the NIC can do; exits 2 if the recipe cannot apply
sudo scripts/tune-nic.sh --cpus 0,1,2,3,4,5,6,7   # same order as Workers::Cores
```

The script sizes the queues, masks `irqbalance`, writes one `smp_affinity` mask per RX queue
IRQ, enables ntuple filters for accelerated RFS and sizes `rps_sock_flow_entries` /
`rps_flow_cnt`. It refuses nothing it cannot do: on a single-queue virtual NIC it reports the
gap and configures software RFS only. The reference server prints the exact `--cpus` list and
each worker's mask at startup. Details in [operations](docs/operations.md).

---

## Numbers

Measured in a 12-vCPU Linux VM (lima/vz, kernel 6.14, loopback, release build; the load
generator's 64 client threads share the same 12 vCPUs, so these are relative, not absolute).

| workers | req/s (64 persistent conns, 512 B echo) | p50 | p99 |
|---|---|---|---|
| 1 | 157k | 388 µs | 521 µs |
| 4 | 459k | 106 µs | 735 µs |
| 8 | 1.41M | 34 µs | 222 µs |
| 12 | 1.21M | 30 µs | 461 µs |

With one request per connection and 64 clients against 12 workers of capacity 1 — every
connection but the first on each core goes through detach → channel → attach — the server
sustained 16.7k connections/s with 73,241 handoffs, 4 bounces and no rejections; the
median cost over the roomy configuration was about 0.4 ms, most of it queueing for one of the
12 slots. Method, tables and caveats in [performance.md](docs/performance.md).

---

## Requirements

* **Linux.** `SO_REUSEPORT` load balancing, `io_uring` and the IRQ recipe are Linux semantics.
  The crate type-checks on other Unixes so editors work; nothing is tested there.
* **Kernel:** 5.19+ for the default `coop_taskrun`, 6.0+ for `single_issuer`, 6.1+ if you turn
  on `defer_taskrun`. Older kernels: clear those flags in `UringConfig`.
* **Rust 1.95+.** compio 0.19's executor uses `cfg_select!`, stable since 1.95; every toolchain
  from 1.88 to 1.94 was tried in the VM and fails to build the dependency tree. Enforced by CI.

## Development

All building, testing and tuning happens on a Linux machine; the author's is a `limactl` VM.
See [testing.md](docs/testing.md) for the exact loop.

## License

MIT OR Apache-2.0.
