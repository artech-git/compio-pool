//! Thread-per-core accept server for [`compio`] on Linux `io_uring`.
//!
//! One pinned worker thread per CPU core. Each worker owns its own `io_uring`
//! (a thread-local [`compio`] runtime), its own `SO_REUSEPORT` listener on the
//! shared address, and a strictly thread-local pool of [`Resource`]s. The kernel
//! hashes each incoming connection to one listener, so a connection is accepted,
//! served and closed on a single core with no cross-thread traffic at all.
//!
//! When a worker's pool is saturated it strips the accepted socket back to a raw
//! file descriptor and pushes it onto one bounded, lock-free [`flume`] channel.
//! Every worker also polls that channel whenever it has a free slot, rebuilds the
//! stream on its own ring, and serves it there. Overflow therefore flows to
//! whichever core is idle, and nothing else is ever shared.
//!
//! # The recipe this implements
//!
//! | step | what | where |
//! |---|---|---|
//! | 1–4 | NIC queues = worker count, irqbalance off, RX IRQ → core pins, accelerated RFS | [`scripts/tune-nic.sh`](https://github.com/artech-git/compio-pool/blob/experimental/scripts/tune-nic.sh) |
//! | 5 | `compio`, `socket2`, `core_affinity`, `flume` at their current versions | `Cargo.toml` |
//! | 6 | one bounded MPMC channel for fd handoff | [`handoff`] |
//! | 7–8 | enumerate cores, one thread each, pinned inside the thread closure | [`cpu`], [`worker`] |
//! | 9–10 | raw `socket2` socket, `SO_REUSEADDR` + `SO_REUSEPORT`, bind, wrap in the thread's runtime | [`listener`], [`worker`] |
//! | 11 | thread-local `Rc`-based resource pool | [`pool`] |
//! | 12–13 | accept loop; serve locally while the pool has room | [`worker`] |
//! | 14–15 | detach the fd from the ring, push it onto the channel | [`handoff::detach`], [`worker`] |
//! | 16–17 | claim loop; re-attach the fd to the idle thread's ring | [`handoff::attach`], [`worker`] |
//!
//! # Quickstart
//!
//! ```no_run
//! use std::io;
//!
//! use compio::{BufResult, io::{AsyncRead, AsyncWriteExt}};
//! use compio_pool::{Connection, Resource, Server, Service, Workers, WorkerContext};
//!
//! /// One 16 KiB buffer per in-flight connection, owned by the core that uses it.
//! struct Buf(Vec<u8>);
//!
//! impl Resource for Buf {
//!     async fn create(_cx: &WorkerContext) -> io::Result<Self> {
//!         Ok(Buf(Vec::with_capacity(16 * 1024)))
//!     }
//! }
//!
//! #[derive(Clone)]
//! struct Echo;
//!
//! impl Service for Echo {
//!     type Resource = Buf;
//!
//!     async fn handle(&self, mut conn: Connection, buf: &mut Buf) -> io::Result<()> {
//!         loop {
//!             let mut b = std::mem::take(&mut buf.0);
//!             b.clear();
//!             let BufResult(read, b) = conn.stream.read(b).await;
//!             match read {
//!                 Ok(0) => { buf.0 = b; return Ok(()); }
//!                 Ok(_) => {}
//!                 Err(e) => { buf.0 = b; return Err(e); }
//!             }
//!             let BufResult(written, b) = conn.stream.write_all(b).await;
//!             buf.0 = b;
//!             written?;
//!         }
//!     }
//! }
//!
//! fn main() -> io::Result<()> {
//!     let server = Server::builder(Echo)
//!         .bind("0.0.0.0:7000".parse().unwrap())
//!         .workers(Workers::AllCores)
//!         .capacity(1024) // per worker
//!         .start()?;
//!     println!("listening on {} with {} workers", server.local_addr(), server.workers());
//!     server.join().expect("a worker panicked");
//!     Ok(())
//! }
//! ```
//!
//! # What is and is not shared
//!
//! * **Never shared:** connections, resources, the pool, the runtime, the ring.
//!   All of it is `Rc`/`Cell` and lives on one thread.
//! * **Shared, lock-free:** the handoff channel (a bounded [`flume`] queue) and
//!   the per-worker statistics counters (relaxed atomics on their own cache
//!   lines, see [`stats`]).
//! * **Shared, read-only:** the [`Config`].
//!
//! # Platform
//!
//! Linux is the only supported target. `SO_REUSEPORT` load balancing, `io_uring`
//! and the IRQ-affinity recipe are Linux semantics. The crate type-checks on
//! other Unixes so editors work, but nothing is tested there.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

#[cfg(not(unix))]
compile_error!(
    "compio-pool needs SO_REUSEPORT and an fd-based socket model; it supports Linux and only type-checks on other Unixes"
);

pub mod config;
pub mod cpu;
pub mod handoff;
pub mod listener;
pub mod pool;
pub mod server;
pub mod service;
pub mod stats;
pub mod worker;

mod util;

pub use config::{Config, OverflowPolicy, UringConfig, Workers};
pub use cpu::CoreId;
pub use pool::{Lease, LocalPool, Permit, Resource};
pub use server::{Builder, Server};
pub use service::{Connection, Route, Service};
pub use stats::{Counters, Stats, WorkerSnapshot};
pub use worker::WorkerContext;
