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
