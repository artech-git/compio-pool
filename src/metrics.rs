//! Pool instrumentation.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// A point-in-time snapshot of pool activity, from [`Pool::metrics`](crate::Pool::metrics).
///
/// Gauges (`live`, `idle`, `parked`) are summed across threads without a lock,
/// so they can be momentarily inconsistent with each other. Counters are
/// monotonic.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Metrics {
    /// Connections currently owned by a shard, idle or checked out.
    pub live: u64,
    /// Connections sitting in a shard free list.
    pub idle: u64,
    /// Connections sitting in the cross-thread reservoir.
    pub parked: u64,
    /// Connections opened since the pool was created.
    pub created: u64,
    /// Connections closed since the pool was created.
    pub closed: u64,
    /// Successful checkouts.
    pub acquires: u64,
    /// Checkouts that had to wait for a connection to come back.
    pub waits: u64,
    /// Checkouts that gave up at the acquire timeout.
    pub timeouts: u64,
    /// Connections discarded because a checkout was cancelled mid-operation.
    pub poisoned: u64,
    /// Connections discarded because `recycle` rejected them.
    pub recycle_failures: u64,
    /// Connections taken from the reservoir by a thread that did not open them.
    pub unparked: u64,
}

#[derive(Debug, Default)]
pub(crate) struct Counters {
    pub live: AtomicU64,
    pub idle: AtomicU64,
    pub created: AtomicU64,
    pub closed: AtomicU64,
    pub acquires: AtomicU64,
    pub waits: AtomicU64,
    pub timeouts: AtomicU64,
    pub poisoned: AtomicU64,
    pub recycle_failures: AtomicU64,
    pub unparked: AtomicU64,
}

impl Counters {
    pub(crate) fn inc(c: &AtomicU64) {
        c.fetch_add(1, Relaxed);
    }

    pub(crate) fn dec(c: &AtomicU64) {
        c.fetch_sub(1, Relaxed);
    }

    pub(crate) fn snapshot(&self, parked: u64) -> Metrics {
        Metrics {
            live: self.live.load(Relaxed),
            idle: self.idle.load(Relaxed),
            parked,
            created: self.created.load(Relaxed),
            closed: self.closed.load(Relaxed),
            acquires: self.acquires.load(Relaxed),
            waits: self.waits.load(Relaxed),
            timeouts: self.timeouts.load(Relaxed),
            poisoned: self.poisoned.load(Relaxed),
            recycle_failures: self.recycle_failures.load(Relaxed),
            unparked: self.unparked.load(Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero() {
        assert_eq!(Counters::default().snapshot(0), Metrics::default());
    }

    #[test]
    fn inc_and_dec_move_one_at_a_time() {
        let c = Counters::default();
        Counters::inc(&c.live);
        Counters::inc(&c.live);
        Counters::dec(&c.live);
        assert_eq!(c.live.load(Relaxed), 1);
    }

    /// Each field must be read from its own atomic — a copy-paste slip in
    /// `snapshot` would otherwise go unnoticed.
    #[test]
    fn snapshot_maps_every_field_to_its_own_counter() {
        let c = Counters::default();
        c.live.store(1, Relaxed);
        c.idle.store(2, Relaxed);
        c.created.store(3, Relaxed);
        c.closed.store(4, Relaxed);
        c.acquires.store(5, Relaxed);
        c.waits.store(6, Relaxed);
        c.timeouts.store(7, Relaxed);
        c.poisoned.store(8, Relaxed);
        c.recycle_failures.store(9, Relaxed);
        c.unparked.store(10, Relaxed);

        assert_eq!(
            c.snapshot(11),
            Metrics {
                live: 1,
                idle: 2,
                parked: 11,
                created: 3,
                closed: 4,
                acquires: 5,
                waits: 6,
                timeouts: 7,
                poisoned: 8,
                recycle_failures: 9,
                unparked: 10,
            }
        );
    }

    /// `parked` lives in the exchange, not in `Counters`, so it is passed in
    /// rather than loaded.
    #[test]
    fn parked_comes_from_the_argument() {
        let c = Counters::default();
        assert_eq!(c.snapshot(0).parked, 0);
        assert_eq!(c.snapshot(42).parked, 42);
    }

    #[test]
    fn metrics_is_copy_eq_and_debug() {
        let m = Counters::default().snapshot(1);
        let copy = m;
        assert_eq!(m, copy);
        assert_ne!(m, Metrics::default());
        assert!(format!("{m:?}").contains("parked"));
        assert!(format!("{:?}", Counters::default()).contains("live"));
    }
}
