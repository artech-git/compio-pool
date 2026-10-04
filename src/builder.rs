//! Build a [`Pool`] from a manager and a handful of knobs — the bb8 `Builder`,
//! minus the settings that only make sense for a single shared pool.

use std::{marker::PhantomData, time::Duration};

use crate::{manage::ManageConnection, pool::Pool};

/// The resolved configuration every thread's pool reads. Cheap to copy; it lives
/// behind the pool's `Arc` and is shared read-only across threads.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PoolConfig {
    pub max_size: u32,
    pub min_idle: u32,
    pub connection_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
    pub test_on_check_out: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 10,
            min_idle: 0,
            connection_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            test_on_check_out: true,
        }
    }
}

/// Configures and builds a [`Pool`].
///
/// Every size here is **per thread**: [`max_size`](Self::max_size) caps one
/// thread's [`LocalPool`](crate::LocalPool), not the process. A 48-core server
/// with `max_size(16)` can therefore hold up to `48 × 16` connections, 16 of
/// them to any one thread's ring.
///
/// Defaults match bb8: `max_size` 10, `min_idle` 0, `connection_timeout` 30s,
/// `idle_timeout` 10 min, `max_lifetime` 30 min, `test_on_check_out` on.
pub struct Builder<M: ManageConnection> {
    config: PoolConfig,
    _marker: PhantomData<fn() -> M>,
}

impl<M: ManageConnection> Default for Builder<M> {
    fn default() -> Self {
        Self {
            config: PoolConfig::default(),
            _marker: PhantomData,
        }
    }
}

impl<M: ManageConnection> Builder<M> {
    /// A builder with the default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// The most connections one thread's pool keeps open at once (idle plus
    /// checked out). Default 10.
    pub fn max_size(mut self, n: u32) -> Self {
        self.config.max_size = n;
        self
    }

    /// How many connections each thread opens up front and keeps idle, created
    /// by [`LocalPool::warm`](crate::LocalPool::warm). Capped at
    /// [`max_size`](Self::max_size). Default 0.
    pub fn min_idle(mut self, n: u32) -> Self {
        self.config.min_idle = n;
        self
    }

    /// How long [`get`](crate::LocalPool::get) waits — for a free slot and a live
    /// connection — before returning [`RunError::TimedOut`](crate::RunError::TimedOut).
    /// Default 30s.
    pub fn connection_timeout(mut self, d: Duration) -> Self {
        self.config.connection_timeout = d;
        self
    }

    /// Drop an idle connection that has gone unused for this long. `None` keeps
    /// idle connections forever. Default 10 minutes.
    pub fn idle_timeout(mut self, d: Option<Duration>) -> Self {
        self.config.idle_timeout = d;
        self
    }

    /// Drop a connection older than this, idle or not, the next time it is
    /// checked out or returned. `None` never retires on age. Default 30 minutes.
    pub fn max_lifetime(mut self, d: Option<Duration>) -> Self {
        self.config.max_lifetime = d;
        self
    }

    /// Call [`ManageConnection::is_valid`] on a reused connection as it is
    /// checked out. Default `true`.
    pub fn test_on_check_out(mut self, yes: bool) -> Self {
        self.config.test_on_check_out = yes;
        self
    }

    /// Finish, producing the `Send + Clone` [`Pool`] blueprint.
    ///
    /// # Panics
    ///
    /// If `max_size` is 0.
    pub fn build(self, manager: M) -> Pool<M> {
        assert!(self.config.max_size > 0, "max_size must be at least 1");
        let mut config = self.config;
        config.min_idle = config.min_idle.min(config.max_size);
        Pool::new(manager, config)
    }
}
