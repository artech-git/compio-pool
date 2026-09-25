//! Per-connection bookkeeping.

use std::time::{Duration, Instant};

use crate::config::Config;

/// Lifecycle metadata the pool tracks for every connection.
///
/// Handed to [`Manage::recycle`](crate::Manage::recycle) so a manager can make
/// policy decisions of its own (for example, pinging only connections that have
/// been idle for a while).
#[derive(Debug, Clone)]
pub struct SlotMeta {
    /// When the connection was established.
    pub created_at: Instant,
    /// When the connection was last returned to the pool.
    pub last_used: Instant,
    /// Number of completed checkouts.
    pub uses: u64,
    /// Pool generation this connection was created under.
    pub generation: u64,
}

impl SlotMeta {
    pub(crate) fn new(generation: u64) -> Self {
        let now = Instant::now();
        Self {
            created_at: now,
            last_used: now,
            uses: 0,
            generation,
        }
    }

    /// How long the connection has existed.
    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// How long since the connection was last handed back.
    pub fn idle_for(&self) -> Duration {
        self.last_used.elapsed()
    }
}

/// A connection plus its metadata, as stored in a shard's free list.
#[derive(Debug)]
pub(crate) struct Slot<C> {
    pub conn: C,
    pub meta: SlotMeta,
}

impl<C> Slot<C> {
    pub(crate) fn new(conn: C, generation: u64) -> Self {
        Self {
            conn,
            meta: SlotMeta::new(generation),
        }
    }

    /// True when the slot must be destroyed rather than reused.
    ///
    /// Checked on the way out of the free list, so a connection that aged out
    /// while idle is never handed to a caller.
    pub(crate) fn is_expired(&self, cfg: &Config, current_generation: u64) -> bool {
        if self.meta.generation != current_generation {
            return true;
        }
        if let Some(max) = cfg.max_lifetime
            && self.meta.age() >= max
        {
            return true;
        }
        if let Some(idle) = cfg.idle_timeout
            && self.meta.idle_for() >= idle
        {
            return true;
        }
        if let Some(max_uses) = cfg.max_uses
            && self.meta.uses >= max_uses
        {
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every limit switched off, so a test can enable exactly one.
    fn no_limits() -> Config {
        Config::new()
            .max_lifetime(None)
            .idle_timeout(None)
            .max_uses(None)
    }

    #[test]
    fn a_fresh_meta_starts_unused_at_its_generation() {
        let m = SlotMeta::new(7);
        assert_eq!(m.uses, 0);
        assert_eq!(m.generation, 7);
        assert_eq!(
            m.created_at, m.last_used,
            "a connection counts as just returned"
        );
    }

    #[test]
    fn age_and_idle_for_advance() {
        let m = SlotMeta::new(0);
        // Both read a monotonic clock, so the only safe assertion is ordering.
        assert!(m.age() <= m.created_at.elapsed());
        assert!(m.idle_for() <= m.last_used.elapsed());
    }

    #[test]
    fn meta_is_clone_and_debug() {
        let m = SlotMeta::new(3);
        let c = m.clone();
        assert_eq!(c.generation, 3);
        assert!(format!("{m:?}").contains("generation"));
    }

    #[test]
    fn a_new_slot_carries_its_connection_and_generation() {
        let slot = Slot::new("conn", 4);
        assert_eq!(slot.conn, "conn");
        assert_eq!(slot.meta.generation, 4);
        assert!(format!("{slot:?}").contains("conn"));
    }

    #[test]
    fn a_slot_with_no_limits_never_expires() {
        let slot = Slot::new((), 0);
        assert!(!slot.is_expired(&no_limits(), 0));
    }

    #[test]
    fn a_stale_generation_expires_the_slot() {
        let slot = Slot::new((), 0);
        assert!(
            slot.is_expired(&no_limits(), 1),
            "invalidate() bumps the generation; older connections must go"
        );
    }

    /// A generation *ahead* of the pool's cannot happen, but the check is an
    /// inequality, so pin the behaviour either way.
    #[test]
    fn any_generation_mismatch_expires_the_slot() {
        let slot = Slot::new((), 5);
        assert!(slot.is_expired(&no_limits(), 4));
        assert!(!slot.is_expired(&no_limits(), 5));
    }

    #[test]
    fn max_lifetime_expires_the_slot() {
        let slot = Slot::new((), 0);
        // `age() >= ZERO` always holds, so this is deterministic.
        assert!(slot.is_expired(&no_limits().max_lifetime(Duration::ZERO), 0));
    }

    #[test]
    fn a_lifetime_that_has_not_elapsed_does_not_expire_the_slot() {
        let slot = Slot::new((), 0);
        assert!(!slot.is_expired(&no_limits().max_lifetime(Duration::from_secs(3600)), 0));
    }

    #[test]
    fn idle_timeout_expires_the_slot() {
        let slot = Slot::new((), 0);
        assert!(slot.is_expired(&no_limits().idle_timeout(Duration::ZERO), 0));
    }

    #[test]
    fn an_idle_timeout_that_has_not_elapsed_does_not_expire_the_slot() {
        let slot = Slot::new((), 0);
        assert!(!slot.is_expired(&no_limits().idle_timeout(Duration::from_secs(3600)), 0));
    }

    #[test]
    fn max_uses_expires_the_slot_once_reached() {
        let mut slot = Slot::new((), 0);
        let cfg = no_limits().max_uses(2u64);

        assert!(!slot.is_expired(&cfg, 0), "unused");
        slot.meta.uses = 1;
        assert!(!slot.is_expired(&cfg, 0), "one checkout of two");
        slot.meta.uses = 2;
        assert!(slot.is_expired(&cfg, 0), "the cap is inclusive");
        slot.meta.uses = 3;
        assert!(slot.is_expired(&cfg, 0), "and stays tripped past it");
    }

    /// With every limit configured but none tripped, `is_expired` has to fall
    /// all the way through to `false`.
    #[test]
    fn all_limits_set_and_none_tripped() {
        let cfg = Config::new()
            .max_lifetime(Duration::from_secs(3600))
            .idle_timeout(Duration::from_secs(3600))
            .max_uses(10u64);
        assert!(!Slot::new((), 0).is_expired(&cfg, 0));
    }
}
