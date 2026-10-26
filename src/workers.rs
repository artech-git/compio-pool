//! An opt-in thread-per-core harness — the optional `affinity` feature, layer (b).
//!
//! [`PerCore`] owns *only* the boilerplate every thread-per-core server repeats:
//! enumerate the cores, name and spawn one thread each, pin it with
//! [`cpu::pin_current`] **before** the ring is built,
//! build that thread's `compio` runtime, collect the [`JoinHandle`]s, and
//! propagate a spawn error. It never binds a listener, never coordinates the
//! bind, and never hands off an fd: the accept loop stays in your closure, one
//! `SO_REUSEPORT` listener per worker, exactly as [ADR 0001] prescribes.
//!
//! ```no_run
//! use std::io;
//!
//! use compio_pool::{cpu::CoreSet, workers::PerCore, ManageConnection, Pool};
//! # struct Buffers;
//! # impl ManageConnection for Buffers {
//! #     type Connection = Vec<u8>;
//! #     type Error = io::Error;
//! #     async fn connect(&self) -> io::Result<Vec<u8>> { Ok(Vec::new()) }
//! #     async fn is_valid(&self, _: &mut Vec<u8>) -> io::Result<()> { Ok(()) }
//! #     fn has_broken(&self, _: &mut Vec<u8>) -> bool { false }
//! # }
//!
//! let pool = Pool::builder().max_size(1024).build(Buffers);
//! // One pinned worker per physical core, spread across the machine:
//! let workers = PerCore::new(CoreSet::permitted().physical_only().spread())
//!     .name("echo")
//!     .block_on_each(move |_ctx| {
//!         let pool = pool.clone();
//!         async move {
//!             let local = pool.local();
//!             // bind your own SO_REUSEPORT listener here and run the accept loop…
//!             let _ = local;
//!         }
//!     })?;
//! workers.join();
//! # Ok::<(), io::Error>(())
//! ```
//!
//! [ADR 0001]: https://github.com/artech-git/compio-pool/blob/experimental/docs/decisions/0001-listener-per-worker.md

use std::{
    future::Future,
    io,
    thread::{self, JoinHandle},
};

use compio::{driver::ProactorBuilder, runtime::Runtime};

use crate::cpu::{self, CoreId};

/// What a worker is told about itself when its body runs.
///
/// `core.id` is the CPU to pass as `SO_INCOMING_CPU` once the NIC is tuned
/// (`Some(ctx.core.id)` — off by default, see [ADR 0005]); `pinned` reports
/// whether the kernel accepted the pin (a restrictive cpuset can refuse a core,
/// and the worker then runs unpinned rather than not at all).
///
/// [ADR 0005]: https://github.com/artech-git/compio-pool/blob/experimental/docs/decisions/0005-incoming-cpu-opt-in.md
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct WorkerCtx {
    /// This worker's index, `0..n` in the order the cores were given.
    pub index: usize,
    /// The core this worker was pinned to.
    pub core: CoreId,
    /// Whether the kernel accepted the pin.
    pub pinned: bool,
}

/// Spawns one pinned worker thread per core.
///
/// Build it with [`new`](Self::new) from any iterator of [`CoreId`] — a
/// [`CoreSet`](crate::cpu::CoreSet), [`cpu::cores()`](crate::cpu::cores), or your
/// own list — then start the workers with [`spawn_each`](Self::spawn_each) (you
/// build the runtime) or [`block_on_each`](Self::block_on_each) (it builds a
/// `compio` runtime and drives your future). Both return [`Workers`], whose
/// handles you own.
#[derive(Debug)]
pub struct PerCore {
    cores: Vec<CoreId>,
    name: String,
    proactor: Option<ProactorBuilder>,
}

impl PerCore {
    /// One worker per core in `cores`, in the given order.
    ///
    /// `PerCore::new(cpu::cores())` reproduces the hand-rolled examples exactly:
    /// every permitted logical CPU, SMT siblings included. Pass a
    /// [`CoreSet`](crate::cpu::CoreSet) to select a subset first.
    pub fn new(cores: impl IntoIterator<Item = CoreId>) -> PerCore {
        PerCore {
            cores: cores.into_iter().collect(),
            name: String::from("worker"),
            proactor: None,
        }
    }

    /// Set the thread-name prefix; worker `i` is named `"{prefix}/{i}"`.
    /// Default `"worker"`.
    pub fn name(mut self, prefix: impl Into<String>) -> PerCore {
        self.name = prefix.into();
        self
    }

    /// Hand every worker's runtime a clone of this [`ProactorBuilder`], so you
    /// can set `single_issuer` / `defer_taskrun` / other ring flags.
    ///
    /// Applies only to [`block_on_each`](Self::block_on_each), which builds the
    /// runtime; [`spawn_each`](Self::spawn_each) builds its own. Left unset, each
    /// worker gets the bare default runtime the examples use. This is a thin
    /// passthrough — `compio` owns the flag semantics.
    pub fn proactor(mut self, proactor: ProactorBuilder) -> PerCore {
        self.proactor = Some(proactor);
        self
    }

    /// Spawn one pinned thread per core and run `body` on each; you build and
    /// drive the runtime yourself.
    ///
    /// The thread is pinned with [`cpu::pin_current`]
    /// before `body` is called, so anything `body` allocates — including a
    /// runtime it builds — belongs to that core. Use this for a UDP or Unix
    /// listener, a non-`compio` runtime, or any setup
    /// [`block_on_each`](Self::block_on_each) does not cover. `body` is cloned
    /// once per core and called once on its own thread.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::Unsupported`] if `cores` is empty, or the OS
    /// error if a thread fails to spawn.
    pub fn spawn_each<F, T>(self, body: F) -> io::Result<Workers<T>>
    where
        F: FnOnce(WorkerCtx) -> T + Clone + Send + 'static,
        T: Send + 'static,
    {
        if self.cores.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "no cores to run on",
            ));
        }
        let mut handles = Vec::with_capacity(self.cores.len());
        for (index, core) in self.cores.iter().copied().enumerate() {
            let body = body.clone();
            let handle = thread::Builder::new()
                .name(format!("{}/{index}", self.name))
                .spawn(move || {
                    let pinned = cpu::pin_current(core);
                    body(WorkerCtx {
                        index,
                        core,
                        pinned,
                    })
                })?;
            handles.push(handle);
        }
        Ok(Workers { handles })
    }

    /// Spawn one pinned thread per core, build that thread's `compio` runtime,
    /// and `block_on` the future `body` returns — the common case.
    ///
    /// `body` is cloned once per core and called once on its thread, so it may
    /// move its captures (a `pool.clone()`) into the future. The future runs only
    /// on that thread and is never moved, so it need not be `Send`. The runtime is
    /// the bare default unless [`proactor`](Self::proactor) was set. Your body
    /// still binds its own `SO_REUSEPORT` listener and runs its own accept loop.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::Unsupported`] if `cores` is empty, or the OS
    /// error if a thread fails to spawn. A runtime that fails to build panics on
    /// its worker thread and surfaces as that worker's [`join`](Workers::join)
    /// `Err`.
    pub fn block_on_each<F, Fut, T>(self, body: F) -> io::Result<Workers<T>>
    where
        F: FnOnce(WorkerCtx) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = T>,
        T: Send + 'static,
    {
        let proactor = self.proactor.clone();
        self.spawn_each(move |ctx| {
            let mut builder = Runtime::builder();
            if let Some(proactor) = proactor.clone() {
                builder.with_proactor(proactor);
            }
            let runtime = builder.build().expect("build compio runtime");
            runtime.block_on(body(ctx))
        })
    }
}

/// The spawned workers, holding one [`JoinHandle`] each.
///
/// You own these: the harness keeps no shutdown channel and nothing whose `Drop`
/// wakes a `compio` task, so dropping `Workers` only detaches the threads and can
/// never trip the executor's abort-on-unwinding-thread guard ([ADR 0006]).
/// Graceful shutdown is yours to add — and a shutdown signal that wakes a task
/// must follow 0006's panicking-thread detour.
///
/// [ADR 0006]: https://github.com/artech-git/compio-pool/blob/experimental/docs/decisions/0006-shutdown-and-the-unwinding-thread.md
#[must_use = "workers run until joined; dropping these handles just detaches the threads"]
pub struct Workers<T = ()> {
    handles: Vec<JoinHandle<T>>,
}

impl<T> std::fmt::Debug for Workers<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Workers")
            .field("len", &self.handles.len())
            .finish()
    }
}

impl<T> Workers<T> {
    /// How many workers were spawned.
    pub fn len(&self) -> usize {
        self.handles.len()
    }

    /// Whether there are no workers (never true for a set [`spawn_each`] built,
    /// which rejects an empty core list).
    ///
    /// [`spawn_each`]: PerCore::spawn_each
    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    /// The join handles, to `park`/signal them your own way.
    pub fn handles(&self) -> &[JoinHandle<T>] {
        &self.handles
    }

    /// Take the join handles out.
    pub fn into_handles(self) -> Vec<JoinHandle<T>> {
        self.handles
    }

    /// Join every worker in spawn order, returning each one's result; a worker
    /// panic comes back as its `Err`. Accept loops never return, so this parks
    /// until the process is signalled.
    pub fn join(self) -> Vec<thread::Result<T>> {
        self.handles.into_iter().map(JoinHandle::join).collect()
    }
}
