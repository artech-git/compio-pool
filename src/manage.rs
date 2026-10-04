//! The manager trait and the error [`get`](crate::LocalPool::get) can return —
//! the bb8 vocabulary, adapted for a completion runtime where a connection never
//! leaves the thread it was opened on.

use std::{error::Error, fmt, future::Future};

/// Teaches a [`Pool`](crate::Pool) how to open and check the connections it
/// hands out.
///
/// One manager is shared — behind an `Arc` — by every thread's
/// [`LocalPool`](crate::LocalPool), so the manager value itself is `Send + Sync`.
/// The connections it produces are **not**: each belongs to the `io_uring` ring
/// of the thread that opened it and must live and die there. That is the one
/// difference from bb8's `ManageConnection` — [`Connection`](Self::Connection)
/// carries no `Send` bound, and every future returned here runs on a single
/// thread.
pub trait ManageConnection: Send + Sync + 'static {
    /// The connection this manager opens: a `compio` `TcpStream`, a framed
    /// client on top of one, a reusable buffer — anything a thread keeps a
    /// bounded number of. Ring-bound, hence not `Send`.
    type Connection: 'static;

    /// What [`connect`](Self::connect) and [`is_valid`](Self::is_valid) fail
    /// with.
    type Error: Error + 'static;

    /// Open a new connection. Called on the thread whose
    /// [`LocalPool`](crate::LocalPool) asked for it, inside that thread's runtime,
    /// so its I/O is submitted to that thread's ring.
    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>>;

    /// Check a connection as it is handed out, when
    /// [`test_on_check_out`](crate::Builder::test_on_check_out) is set. Returning
    /// `Err` discards the connection; the pool then opens a fresh one in its
    /// place.
    fn is_valid(&self, conn: &mut Self::Connection)
    -> impl Future<Output = Result<(), Self::Error>>;

    /// A synchronous, infallible liveness check run as a connection is returned.
    /// Returning `true` drops it instead of putting it back on the idle list.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool;
}

/// What [`LocalPool::get`](crate::LocalPool::get) returns on failure.
#[derive(Debug)]
pub enum RunError<E> {
    /// The manager failed to open or validate a connection.
    User(E),
    /// No connection became available within
    /// [`connection_timeout`](crate::Builder::connection_timeout).
    TimedOut,
}

impl<E: fmt::Display> fmt::Display for RunError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RunError::User(e) => write!(f, "{e}"),
            RunError::TimedOut => f.write_str("timed out waiting for a pooled connection"),
        }
    }
}

impl<E: Error + 'static> Error for RunError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            RunError::User(e) => Some(e),
            RunError::TimedOut => None,
        }
    }
}
