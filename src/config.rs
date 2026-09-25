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

#[cfg(test)]
mod tests {
    use super::*;

    /// A config with no timed policy at all, so a test can add exactly one.
    fn untimed() -> Config {
        Config::new().max_lifetime(None).idle_timeout(None)
    }

    #[test]
    fn defaults_are_documented_values() {
        let c = Config::default();
        assert_eq!(c.max_size, 16);
        assert_eq!(c.min_idle, 0);
        assert_eq!(c.acquire_timeout, Some(Duration::from_secs(30)));
        assert_eq!(c.max_lifetime, Some(Duration::from_secs(30 * 60)));
        assert_eq!(c.idle_timeout, Some(Duration::from_secs(10 * 60)));
        assert_eq!(c.max_uses, None);
        assert_eq!(c.reap_interval, Duration::from_secs(30));
    }

    #[test]
    fn new_is_default() {
        let a = Config::new();
        let b = Config::default();
        assert_eq!(a.max_size, b.max_size);
        assert_eq!(a.min_idle, b.min_idle);
        assert_eq!(a.acquire_timeout, b.acquire_timeout);
        assert_eq!(a.max_lifetime, b.max_lifetime);
        assert_eq!(a.idle_timeout, b.idle_timeout);
        assert_eq!(a.max_uses, b.max_uses);
        assert_eq!(a.reap_interval, b.reap_interval);
    }

    #[test]
    fn every_setter_round_trips() {
        let c = Config::new()
            .max_size(7)
            .min_idle(3)
            .acquire_timeout(Duration::from_millis(11))
            .max_lifetime(Duration::from_millis(22))
            .idle_timeout(Duration::from_millis(33))
            .max_uses(44u64)
            .reap_interval(Duration::from_millis(55));

        assert_eq!(c.max_size, 7);
        assert_eq!(c.min_idle, 3);
        assert_eq!(c.acquire_timeout, Some(Duration::from_millis(11)));
        assert_eq!(c.max_lifetime, Some(Duration::from_millis(22)));
        assert_eq!(c.idle_timeout, Some(Duration::from_millis(33)));
        assert_eq!(c.max_uses, Some(44));
        assert_eq!(c.reap_interval, Duration::from_millis(55));
    }

    /// The `impl Into<Option<_>>` setters must accept both a bare value and
    /// `None`, which is the whole reason for that signature.
    #[test]
    fn optional_setters_accept_none() {
        let c = Config::new()
            .acquire_timeout(None)
            .max_lifetime(None)
            .idle_timeout(None)
            .max_uses(None);

        assert_eq!(c.acquire_timeout, None);
        assert_eq!(c.max_lifetime, None);
        assert_eq!(c.idle_timeout, None);
        assert_eq!(c.max_uses, None);
    }

    #[test]
    fn optional_setters_accept_an_explicit_some() {
        let c = Config::new()
            .acquire_timeout(Some(Duration::from_secs(1)))
            .max_lifetime(Some(Duration::from_secs(2)))
            .idle_timeout(Some(Duration::from_secs(3)))
            .max_uses(Some(4u64));

        assert_eq!(c.acquire_timeout, Some(Duration::from_secs(1)));
        assert_eq!(c.max_lifetime, Some(Duration::from_secs(2)));
        assert_eq!(c.idle_timeout, Some(Duration::from_secs(3)));
        assert_eq!(c.max_uses, Some(4));
    }

    #[test]
    #[should_panic(expected = "max_size must be greater than zero")]
    fn a_zero_max_size_panics() {
        // A shard that may hold nothing would make `acquire` wait forever.
        let _ = Config::new().max_size(0);
    }

    #[test]
    #[should_panic(expected = "reap_interval must be non-zero")]
    fn a_zero_reap_interval_panics() {
        // Otherwise the reaper becomes a busy loop on the runtime.
        let _ = Config::new().reap_interval(Duration::ZERO);
    }

    #[test]
    fn min_idle_is_stored_unclamped_but_read_back_clamped() {
        let c = Config::new().max_size(2).min_idle(9);
        assert_eq!(c.min_idle, 9, "the setter records what was asked for");
        assert_eq!(c.effective_min_idle(), 2, "but never exceeds max_size");
    }

    #[test]
    fn effective_min_idle_passes_through_below_max_size() {
        assert_eq!(
            Config::new().max_size(9).min_idle(2).effective_min_idle(),
            2
        );
        assert_eq!(
            Config::new().max_size(9).min_idle(0).effective_min_idle(),
            0
        );
        assert_eq!(
            Config::new().max_size(4).min_idle(4).effective_min_idle(),
            4
        );
    }

    #[test]
    fn a_config_with_no_timed_policy_needs_no_reaper() {
        assert!(!untimed().needs_reaper());
    }

    #[test]
    fn each_timed_policy_on_its_own_needs_a_reaper() {
        assert!(
            untimed().min_idle(1).needs_reaper(),
            "min_idle has to be refilled by someone"
        );
        assert!(
            untimed()
                .idle_timeout(Duration::from_secs(1))
                .needs_reaper(),
            "an idle connection nobody asks for ages out only in the reaper"
        );
        assert!(
            untimed()
                .max_lifetime(Duration::from_secs(1))
                .needs_reaper(),
            "same for the hard age cap"
        );
    }

    /// `max_uses` is checked on the way out of the free list, so it never needs
    /// a timer of its own.
    #[test]
    fn max_uses_alone_needs_no_reaper() {
        assert!(!untimed().max_uses(1u64).needs_reaper());
    }

    #[test]
    fn is_clone_and_debug() {
        let c = Config::new().max_size(3).min_idle(1);
        let d = c.clone();
        assert_eq!(d.max_size, 3);
        assert_eq!(d.min_idle, 1);
        assert!(format!("{c:?}").contains("max_size"));
    }
}
