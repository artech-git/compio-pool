//! A connection pool for [`compio`], the completion-based, thread-per-core
//! Rust runtime.
//!
//! # Why compio needs its own pool
//!
//! General-purpose pools (`bb8`, `deadpool`, `r2d2`) assume connections are
//! `Send`: one shared `Mutex<Vec<Conn>>`, any worker takes any connection.
//! `compio` breaks that assumption. Each thread runs its own driver
//! (io_uring, IOCP or poll), IO handles are bound to the driver that created
//! them, and buffers are handed to the kernel by ownership — so
//! [`compio::net::TcpStream`] is `!Send` and cannot be put behind an `Arc<Mutex<_>>`
//! at all.
//!
//! So "share one connection across threads simultaneously" is not the thing to
//! build. Two different things hide behind that phrase:
//!
//! * **Concurrent use of a single connection** — only meaningful for protocols
//!   that multiplex (HTTP/2, Redis pipelining). Otherwise a connection is an
//!   exclusive resource and sharing means taking turns.
//! * **A pool shared by every thread** — any thread can check out *some*
//!   connection. That is what this crate provides.
//!
//! # Design
//!
//! [`Pool`] is `Send + Sync` and cheap to clone, but holds no connections. The
//! connections live in a **per-thread shard** stored in a thread-local, so:
//!
//! * the acquire fast path touches no atomics and takes no lock;
//! * [`Manage::Connection`] never needs to be `Send`;
//! * a connection is always driven by the compio driver that created it;
//! * at thread exit, that thread's connections close on their own thread.
//!
//! [`Config::max_size`] is therefore **per shard**, not per process. A global
//! cap would need a cross-thread semaphore on the hot path, which is the exact
//! contention thread-per-core exists to avoid.
//!
//! ## Letting connections migrate
//!
//! Pure sharding wastes connections when load is skewed: a quiet thread holds
//! idle sockets a busy thread could use. The optional [`Reservoir`] fixes that
//! with a bounded, lock-free `ArrayQueue` shared by every thread: a shard whose
//! free list is over `min_idle` detaches the surplus into it, and a shard whose
//! free list is empty pops from it before paying for a handshake. The claiming
//! thread re-wraps the socket in its own runtime via [`Detach::attach`].
//!
//! It requires [`Detach`], which is **platform-dependent and deliberately
//! opt-in**:
//!
//! | driver | `attach` | connections may change threads |
//! |---|---|---|
//! | io_uring (Linux) | no-op | yes |
//! | poll (Unix fallback) | no-op | yes |
//! | IOCP (Windows) | `CreateIoCompletionPort`, once per handle | **no** |
//!
//! ## Cancellation is the sharp edge
//!
//! With completion-based IO, dropping a future mid-operation does not undo the
//! operation. The connection's protocol state is then unknown and it must be
//! destroyed rather than returned. See [`Pooled::begin_op`].
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//! use compio_pool::{Config, Manage, Pool, SlotMeta};
//!
//! struct Tcp(String);
//!
//! impl Manage for Tcp {
//!     type Connection = compio::net::TcpStream;
//!     type Error = std::io::Error;
//!
//!     async fn connect(&self) -> std::io::Result<Self::Connection> {
//!         compio::net::TcpStream::connect(&self.0).await
//!     }
//!
//!     async fn recycle(
//!         &self,
//!         conn: &mut Self::Connection,
//!         _meta: &SlotMeta,
//!     ) -> std::io::Result<()> {
//!         conn.peer_addr().map(|_| ())
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = Pool::builder(Tcp("127.0.0.1:6379".into()))
//!     .max_size(8)                                   // per thread
//!     .min_idle(2)
//!     .acquire_timeout(Duration::from_secs(5))
//!     .build();
//!
//! // Clone `pool` onto every compio thread; each gets its own shard.
//! let mut conn = pool.acquire().await?;
//!
//! let op = conn.begin_op();
//! // ... use `conn` ...
//! op.complete_op();
//! # Ok(())
//! # }
//! ```

#![warn(missing_docs)]

mod config;
mod error;
mod exchange;
mod guard;
mod manage;
mod metrics;
mod pool;
mod shard;
mod slot;

#[cfg(test)]
mod test_support;

pub use crate::{
    config::Config,
    error::Error,
    exchange::{Exchange, NoExchange, Parked, Reservoir, Unparked},
    guard::{OpGuard, Pooled},
    manage::{Detach, Manage},
    metrics::Metrics,
    pool::{Builder, Pool},
    slot::SlotMeta,
};
