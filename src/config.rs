//! Pool sizing and lifecycle configuration.

use std::time::Duration;

/// Tunables for a [`Pool`](crate::Pool).
///
/// # Sizing is per-shard
///
/// `compio` is a thread-per-core runtime, so this pool keeps one independent
/// *shard* per runtime thread (see the crate docs). [`Config::max_size`] and
/// [`Config::min_idle`] therefore apply **per shard**, not to the process. A
/// pool with `max_size = 8` running on 4 compio threads can hold up to 32
/// connections in total.
///
/// This is deliberate: a global cap requires a cross-thread semaphore on the
/// acquire fast path, which is exactly the contention thread-per-core runtimes
/// exist to avoid.
#[derive(Debug, Clone)]
pub struct Config {
    pub(crate) max_size: usize,
    pub(crate) min_idle: usize,
    pub(crate) acquire_timeout: Option<Duration>,
    pub(crate) max_lifetime: Option<Duration>,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) max_uses: Option<u64>,
    pub(crate) reap_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_size: 16,
            min_idle: 0,
            acquire_timeout: Some(Duration::from_secs(30)),
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            max_uses: None,
            reap_interval: Duration::from_secs(30),
        }
    }
}

impl Config {
    /// Starts from the defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Maximum live connections **per runtime thread**. Must be non-zero.
    pub fn max_size(mut self, n: usize) -> Self {
        assert!(n > 0, "max_size must be greater than zero");
        self.max_size = n;
        self
    }

    /// Idle connections each shard keeps thread-locally. Clamped to `max_size`.
    ///
    /// This doubles as the split point between local caching and sharing: with
    /// a [`Reservoir`](crate::Reservoir) installed, returned connections beyond
    /// `min_idle` are parked for other threads to claim. Leaving it at `0`
    /// therefore means *every* connection goes back to the shared stack and
    /// each checkout pays a mutex. Set it to your steady-state per-thread
    /// concurrency to keep the hot path lock-free and share only the surplus.
    pub fn min_idle(mut self, n: usize) -> Self {
        self.min_idle = n;
        self
    }

    /// How long [`Pool::acquire`](crate::Pool::acquire) waits before returning
    /// [`Error::Timeout`](crate::Error::Timeout). `None` waits forever.
    pub fn acquire_timeout(mut self, t: impl Into<Option<Duration>>) -> Self {
        self.acquire_timeout = t.into();
        self
    }

    /// Hard age cap; a connection older than this is closed instead of reused.
    pub fn max_lifetime(mut self, t: impl Into<Option<Duration>>) -> Self {
        self.max_lifetime = t.into();
        self
    }

    /// How long a connection may sit unused in the free list before being reaped.
    pub fn idle_timeout(mut self, t: impl Into<Option<Duration>>) -> Self {
        self.idle_timeout = t.into();
        self
    }

    /// Retire a connection after this many checkouts.
    pub fn max_uses(mut self, n: impl Into<Option<u64>>) -> Self {
        self.max_uses = n.into();
        self
    }

    /// How often each shard's background reaper runs.
    ///
    /// The reaper is what actually enforces [`idle_timeout`](Config::idle_timeout)
    /// and [`max_lifetime`](Config::max_lifetime) on connections nobody is
    /// asking for, and what refills [`min_idle`](Config::min_idle).
    pub fn reap_interval(mut self, t: Duration) -> Self {
        assert!(!t.is_zero(), "reap_interval must be non-zero");
        self.reap_interval = t;
        self
    }

    pub(crate) fn needs_reaper(&self) -> bool {
        self.min_idle > 0 || self.idle_timeout.is_some() || self.max_lifetime.is_some()
    }

    pub(crate) fn effective_min_idle(&self) -> usize {
        self.min_idle.min(self.max_size)
    }
}
