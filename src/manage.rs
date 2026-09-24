//! The manager trait: how the pool creates, checks and moves connections.

use std::future::Future;

use crate::slot::SlotMeta;

/// Creates and validates connections for a [`Pool`](crate::Pool).
///
/// This is the trait you implement for your protocol (Postgres, Redis, an HTTP
/// client, …).
///
/// # Why there are no `Send` bounds on the futures
///
/// `compio` is a completion-based, thread-per-core runtime: its IO handles are
/// bound to the driver of the thread that created them and are `!Send`. A pool
/// built on `bb8`'s trait shape cannot hold a `compio::net::TcpStream` at all,
/// because `bb8` requires `Connection: Send`.
///
/// So `Manage` requires `Send + Sync` on the **manager** (it is shared across
/// threads inside the pool handle) but places no bounds whatsoever on
/// [`Connection`](Manage::Connection) or on the returned futures. Connections
/// live in thread-local shards and never cross a thread boundary unless you
/// additionally implement [`Detach`].
pub trait Manage: Send + Sync + 'static {
    /// The pooled resource. May be `!Send`; must be owned (`'static`),
    /// since shards are stored in thread-local slots.
    type Connection: 'static;

    /// Error produced when establishing or validating a connection.
    type Error;

    /// Establishes a new connection.
    ///
    /// Called on the thread that will own the connection, so it is safe (and
    /// expected) to touch the current `compio` runtime here.
    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>>;

    /// Prepares a previously-used connection for hand-off.
    ///
    /// Called on acquire, not on return, because returning happens in `Drop`,
    /// which cannot await. Use it for a cheap liveness check and to reset
    /// protocol state (roll back an open transaction, drain a pipeline).
    ///
    /// Returning `Err` discards the connection; the pool then transparently
    /// tries the next idle one or opens a fresh connection.
    fn recycle(
        &self,
        conn: &mut Self::Connection,
        meta: &SlotMeta,
    ) -> impl Future<Output = Result<(), Self::Error>>;

    /// Called before a connection is dropped, for graceful shutdown.
    ///
    /// The default is a no-op: dropping the connection closes it. Override this
    /// only for protocols with an explicit goodbye message (Redis `QUIT`,
    /// Postgres `Terminate`). It is synchronous by design — it runs from `Drop`
    /// paths and during thread teardown, where awaiting is not possible.
    fn disconnect(&self, conn: Self::Connection) {
        let _ = conn;
    }
}

/// Opt-in marker for connections that can legally change threads.
///
/// Implementing this lets a connection be parked in the cross-thread
/// [`Reservoir`](crate::Reservoir), so an idle connection on one compio thread
/// can be picked up by a busy one instead of sitting unused.
///
/// # Only idle connections, never one with an operation in flight
///
/// Moving a handle between drivers is sound only when nothing is still
/// submitted against it. The pool upholds half of that for you: a connection
/// reaches [`detach`](Detach::detach) only after its [`Pooled`](crate::Pooled)
/// guard has been dropped, and a checkout cancelled mid-operation is poisoned
/// and destroyed rather than parked. [`detach`](Detach::detach) returning
/// `Option` is the other half — the implementation gets to *check*, rather
/// than trust, that the handle is genuinely unshared.
///
/// # Platform reality
///
/// Whether this is sound depends on the driver, and the answer differs by OS:
///
/// * **io_uring and poll (Unix)** — `Driver::attach` is a no-op. The fd table
///   is process-wide and any thread may submit ops on any fd, so moving a
///   connection between compio threads costs nothing. `Detach` is safe here.
/// * **IOCP (Windows)** — `attach` calls `CreateIoCompletionPort`, and compio
///   documents that "a handle can and only can attach once to one driver".
///   A socket bound to thread A's port keeps delivering completions to thread
///   A's port forever. **Do not implement `Detach` on Windows** for types
///   backed by an IOCP handle.
///
/// Beyond the fd itself, everything you carry in [`Parked`](Detach::Parked)
/// must be `Send`: buffers, TLS session state, prepared-statement caches. A
/// connection whose state includes an `Rc` cannot implement this trait, which
/// is precisely the point of separating it from [`Manage`].
pub trait Detach: Manage {
    /// The connection reduced to a form that may cross threads.
    ///
    /// Typically an `OwnedFd`/`SharedFd` plus whatever protocol state is `Send`.
    type Parked: Send + 'static;

    /// Converts a connection into its thread-portable form.
    ///
    /// Returns `None` when the connection cannot be made portable — in
    /// practice, when an IO operation still holds a reference to the
    /// underlying handle. **The connection is consumed either way**: `None`
    /// means it has been dropped, and the pool accounts for it as closed
    /// rather than parked, then dials a replacement.
    ///
    /// With `compio`, the check is
    /// [`SharedFd::try_unwrap`](compio::driver::SharedFd::try_unwrap), which
    /// succeeds exactly when no submission still references the fd. See
    /// `examples/steal.rs`.
    fn detach(conn: Self::Connection) -> Option<Self::Parked>;

    /// Rebuilds a connection on the current thread.
    ///
    /// This is where the socket is re-wrapped in *this* thread's runtime —
    /// `TcpStream::from_std`, or a `from_raw_fd` constructor, depending on the
    /// compio version. Under io_uring and poll that is bookkeeping only; the
    /// fd table is process-wide.
    ///
    /// Returning `Err` drops the parked connection; the caller falls back to
    /// opening a fresh one.
    fn attach(parked: Self::Parked) -> impl Future<Output = Result<Self::Connection, Self::Error>>;
}
