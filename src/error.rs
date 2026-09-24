//! Error types returned by pool operations.

use core::fmt;

/// Failure modes of [`Pool::acquire`](crate::Pool::acquire).
///
/// `E` is the manager's own error type ([`Manage::Error`](crate::Manage::Error)).
#[derive(Debug)]
pub enum Error<E> {
    /// The acquire timeout elapsed before a connection became available.
    Timeout,
    /// The pool was closed via [`Pool::close`](crate::Pool::close).
    Closed,
    /// The manager failed to establish a new connection.
    Backend(E),
}

impl<E> Error<E> {
    /// Returns the underlying manager error, if this is a [`Error::Backend`].
    pub fn into_backend(self) -> Option<E> {
        match self {
            Error::Backend(e) => Some(e),
            _ => None,
        }
    }
}

impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Timeout => f.write_str("timed out waiting for a pooled connection"),
            Error::Closed => f.write_str("connection pool is closed"),
            Error::Backend(e) => write!(f, "failed to establish connection: {e}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for Error<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Backend(e) => Some(e),
            _ => None,
        }
    }
}
