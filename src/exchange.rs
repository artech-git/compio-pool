//! Cross-thread connection exchange strategies.
//!
//! A [`Pool`](crate::Pool) keeps one independent shard per compio thread. The
//! `Exchange` is the optional back channel between those shards: it lets an
//! idle connection on a quiet thread be picked up by a busy one instead of
//! sitting unused until it ages out.
//!
//! [`NoExchange`] (the default) is a zero-sized no-op — pure thread-per-core,
//! no atomics, no lock. [`Reservoir`] is the real implementation and requires
//! [`Detach`], which is only sound on platforms where an fd may change drivers.

use std::{
    future::Future,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

use crate::{
    manage::{Detach, Manage},
    slot::SlotMeta,
};

/// Strategy for moving idle connections between per-thread shards.
pub trait Exchange<M: Manage>: Send + Sync + 'static {
    /// Offers an idle connection to other threads.
    ///
    /// Returns `Some` if the offer was refused, handing ownership back to the
    /// caller. Must not block.
    fn park(&self, conn: M::Connection, meta: SlotMeta) -> Option<(M::Connection, SlotMeta)>;

    /// Tries to claim a connection parked by another thread and reattach it
    /// to this one.
    fn unpark(&self) -> impl Future<Output = Option<(M::Connection, SlotMeta)>>;

    /// Connections currently parked.
    fn parked(&self) -> u64;

    /// Drops every parked connection. Called by [`Pool::close`](crate::Pool::close).
    fn clear(&self) {}
}

/// The default: shards never share. Zero-sized, zero-cost.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoExchange;

impl<M: Manage> Exchange<M> for NoExchange {
    fn park(&self, conn: M::Connection, meta: SlotMeta) -> Option<(M::Connection, SlotMeta)> {
        Some((conn, meta))
    }

    async fn unpark(&self) -> Option<(M::Connection, SlotMeta)> {
        None
    }

    fn parked(&self) -> u64 {
        0
    }
}

struct Parked<P> {
    parked: P,
    meta: SlotMeta,
}

/// A bounded, shared stack of detached connections.
///
/// Idle connections beyond a shard's `min_idle` are detached into this stack,
/// where any thread may claim them. The stack is LIFO so the warmest
/// connections are reused and the coldest age out.
///
/// The mutex is held only long enough to push or pop a pointer-sized value —
/// [`Detach::attach`], which is async, always runs after the guard is dropped.
///
/// # Platform support
///
/// Only construct this where [`Detach`] is sound: io_uring and poll drivers
/// treat `attach` as a no-op, so fds move freely. Under IOCP a handle is bound
/// to one completion port for life. See [`Detach`] for the details.
pub struct Reservoir<M: Detach> {
    capacity: usize,
    stack: Mutex<Vec<Parked<M::Parked>>>,
    len: AtomicU64,
}

impl<M: Detach> Reservoir<M> {
    /// Creates a reservoir holding at most `capacity` connections.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            stack: Mutex::new(Vec::with_capacity(capacity)),
            len: AtomicU64::new(0),
        }
    }
}

impl<M: Detach> Default for Reservoir<M> {
    fn default() -> Self {
        Self::new(64)
    }
}

impl<M: Detach> std::fmt::Debug for Reservoir<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reservoir")
            .field("capacity", &self.capacity)
            .field("len", &self.len.load(Relaxed))
            .finish()
    }
}

impl<M: Detach> Exchange<M> for Reservoir<M> {
    fn park(&self, conn: M::Connection, meta: SlotMeta) -> Option<(M::Connection, SlotMeta)> {
        let mut stack = match self.stack.lock() {
            Ok(g) => g,
            // A poisoned mutex means a panic while holding it; the stack is
            // still structurally valid (we only push/pop), so carry on.
            Err(p) => p.into_inner(),
        };
        if stack.len() >= self.capacity {
            return Some((conn, meta));
        }
        stack.push(Parked {
            parked: M::detach(conn),
            meta,
        });
        self.len.store(stack.len() as u64, Relaxed);
        None
    }

    async fn unpark(&self) -> Option<(M::Connection, SlotMeta)> {
        // Pop under the lock, then release it before awaiting: `attach` may do
        // real IO and must never run with a shared mutex held.
        let entry = {
            let mut stack = match self.stack.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            let entry = stack.pop()?;
            self.len.store(stack.len() as u64, Relaxed);
            entry
        };
        match M::attach(entry.parked).await {
            Ok(conn) => Some((conn, entry.meta)),
            Err(_) => None,
        }
    }

    fn parked(&self) -> u64 {
        self.len.load(Relaxed)
    }

    fn clear(&self) {
        let mut stack = match self.stack.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        stack.clear();
        self.len.store(0, Relaxed);
    }
}
