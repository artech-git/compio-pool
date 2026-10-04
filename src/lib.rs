//! A bb8-style connection pool for [`compio`] on Linux `io_uring`.
//!
//! You build one [`Pool`] — the manager plus a handful of [`Builder`] knobs —
//! then clone that handle onto each thread of your own thread-per-core runtime.
//! On every thread you call [`Pool::local`] once to get that thread's
//! [`LocalPool`], and [`LocalPool::get`] to lease a connection. The pool is the
//! product; spawning threads, pinning them and running an accept loop are yours.
//!
//! # Why it is shaped this way
//!
//! A `compio` connection is bound to the `io_uring` ring of the thread that
//! opened it: its buffers are registered with that ring and its completions are
//! delivered there. It cannot be used from another thread. So, unlike bb8 on
//! `tokio`, the pool of connections cannot be a single shared, work-stealing
//! store. What *is* shared is only the blueprint:
//!
//! * **[`Pool`] — `Send + Sync + Clone`.** The manager and the configuration,
//!   behind one `Arc`. Cloning is an `Arc` bump. This is the only thing that
//!   crosses threads.
//! * **[`LocalPool`] — `!Send`, one per thread.** The idle connections and the
//!   slot count, all `Rc`/`Cell`, no lock, no atom. The compiler keeps it, and
//!   the ring-bound connections it holds, on its own thread.
//!
//! The parts of bb8 that make sense per thread are here — [`max_size`], idle and
//! lifetime timeouts, `test_on_check_out`, [`min_idle`] warm-up — sized per
//! thread, not per process.
//!
//! [`max_size`]: Builder::max_size
//! [`min_idle`]: Builder::min_idle
//!
//! # Quickstart
//!
//! Define a manager, build the pool, and use it from a compio task:
//!
//! ```no_run
//! use std::io;
//!
//! use compio_pool::{ManageConnection, Pool};
//!
//! /// A pool of reusable 16 KiB scratch buffers. A real manager would open a
//! /// backend connection (Redis, Postgres, an upstream socket) in `connect`.
//! struct Buffers;
//!
//! impl ManageConnection for Buffers {
//!     type Connection = Vec<u8>;
//!     type Error = io::Error;
//!
//!     async fn connect(&self) -> io::Result<Vec<u8>> {
//!         Ok(Vec::with_capacity(16 * 1024))
//!     }
//!     async fn is_valid(&self, _buf: &mut Vec<u8>) -> io::Result<()> {
//!         Ok(())
//!     }
//!     fn has_broken(&self, _buf: &mut Vec<u8>) -> bool {
//!         false
//!     }
//! }
//!
//! // Build once; `Pool` is `Send + Clone`, so hand a clone to each thread.
//! let pool = Pool::builder().max_size(1024).build(Buffers);
//!
//! // On each compio thread you spawn (see `examples/echo.rs` for the full
//! // pinned, SO_REUSEPORT thread-per-core setup):
//! let local = pool.local(); // this thread's own pool, `!Send`
//! # async fn run(
//! #     local: compio_pool::LocalPool<Buffers>,
//! # ) -> Result<(), compio_pool::RunError<io::Error>> {
//! let mut buf = local.get().await?; // PooledConnection, returned on drop
//! buf.clear();
//! # Ok(())
//! # }
//! ```
//!
//! The crate also ships the two helpers that building your own thread-per-core
//! loop needs: [`cpu`] for core enumeration and pinning, and
//! [`listener::bind_reuseport`] for a shared-address `SO_REUSEPORT` socket.
//!
//! # Platform
//!
//! Linux is the only supported target: `SO_REUSEPORT` load balancing and
//! `io_uring` are Linux semantics. The crate type-checks on other Unixes so
//! editors work, but nothing is tested there.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![warn(missing_docs, rust_2018_idioms)]

#[cfg(not(unix))]
compile_error!(
    "compio-pool needs SO_REUSEPORT and an fd-based socket model; it supports Linux and only type-checks on other Unixes"
);

pub mod builder;
pub mod cpu;
pub mod listener;
pub mod manage;
pub mod pool;

pub use builder::Builder;
pub use cpu::CoreId;
pub use listener::bind_reuseport;
pub use manage::{ManageConnection, RunError};
pub use pool::{LocalPool, Pool, PooledConnection, State};
