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

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    fn io() -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused")
    }

    #[test]
    fn into_backend_yields_the_manager_error() {
        assert_eq!(Error::Backend("boom").into_backend(), Some("boom"));
    }

    #[test]
    fn into_backend_is_none_for_pool_side_failures() {
        assert_eq!(Error::<&str>::Timeout.into_backend(), None);
        assert_eq!(Error::<&str>::Closed.into_backend(), None);
    }

    #[test]
    fn display_names_the_failure_mode() {
        assert_eq!(
            Error::<&str>::Timeout.to_string(),
            "timed out waiting for a pooled connection"
        );
        assert_eq!(
            Error::<&str>::Closed.to_string(),
            "connection pool is closed"
        );
        assert_eq!(
            Error::Backend("refused").to_string(),
            "failed to establish connection: refused"
        );
    }

    #[test]
    fn a_backend_error_is_the_source() {
        let e = Error::Backend(io());
        let source = e.source().expect("Backend must expose its cause");
        assert_eq!(source.to_string(), io().to_string());
    }

    #[test]
    fn pool_side_failures_have_no_source() {
        assert!(Error::<std::io::Error>::Timeout.source().is_none());
        assert!(Error::<std::io::Error>::Closed.source().is_none());
    }

    #[test]
    fn is_debug() {
        assert!(format!("{:?}", Error::<&str>::Timeout).contains("Timeout"));
        assert!(format!("{:?}", Error::<&str>::Closed).contains("Closed"));
        assert!(format!("{:?}", Error::Backend("x")).contains("Backend"));
    }

    /// `Error<E>` is only an `std::error::Error` when `E` is; this is the
    /// compile-time half of that contract.
    #[test]
    fn implements_std_error_for_std_error_payloads() {
        fn assert_std_error<T: std::error::Error>(_: &T) {}
        assert_std_error(&Error::Backend(io()));
    }
}
