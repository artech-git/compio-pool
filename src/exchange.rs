//! Cross-thread connection exchange: the overflow / steal pool.
//!
//! A [`Pool`](crate::Pool) keeps one independent shard per compio thread. The
//! `Exchange` is the optional back channel between those shards: it lets an
//! idle connection on a quiet thread be picked up by a busy one instead of
//! sitting unused until it ages out.
//!
//! [`NoExchange`] (the default) is a zero-sized no-op — pure thread-per-core,
//! no atomics, no lock. [`Reservoir`] is the real implementation: a bounded,
//! lock-free [`ArrayQueue`] that every thread shares.
//!
//! # How a connection crosses a thread
//!
//! ```text
//!   thread A                  shared ArrayQueue              thread B
//!   --------                  -----------------              --------
//!   local free list
//!   over min_idle
//!        |
//!        |  Detach::detach      push (never blocks)
//!        +--- fd out of ------------> [ fd, fd, fd ] ---+
//!             A's driver                                |  pop, then
//!                                                       |  Detach::attach
//!                                                       +---> re-wrapped in
//!                                                             B's driver
//! ```
//!
//! Only **idle** connections take this path. A connection is offered here only
//! after its [`Pooled`](crate::Pooled) guard has been dropped, and a checkout
//! cancelled mid-operation is poisoned and destroyed instead — so nothing with
//! an operation in flight can be handed to another driver. [`Detach::detach`]
//! re-checks that invariant rather than trusting it.
//!
//! A thread dips into the queue only when its own free list is empty, and
//! *before* it dials: stealing a warm socket beats a handshake.
//!
//! # Why a queue and not a stack
//!
//! [`ArrayQueue`] is FIFO, so the connection that has been parked longest is
//! claimed first. That is the right bias for an overflow pool: residency is
//! bounded, so a parked connection cannot sit at the bottom of a stack going
//! stale while newer arrivals churn above it. The cost is a little cache
//! warmth, which matters far less than a socket the far end has quietly
//! dropped.

use std::{
    future::Future,
    sync::atomic::{
        AtomicUsize,
        Ordering::{AcqRel, Acquire},
    },
};

use crossbeam_queue::ArrayQueue;

use crate::{
    manage::{Detach, Manage},
    slot::SlotMeta,
};

/// What an [`Exchange`] did with a connection offered to [`Exchange::park`].
pub enum Parked<M: Manage> {
    /// Taken; the connection now belongs to the exchange, not to the shard.
    Accepted,
    /// Refused — the exchange is full, or does not share at all. Ownership is
    /// handed back, and the shard keeps the connection locally.
    Refused(M::Connection, SlotMeta),
    /// Taken, but it could not be made portable and has been destroyed.
    /// See [`Detach::detach`].
    Destroyed,
}

/// What an [`Exchange`] produced for [`Exchange::unpark`].
pub enum Unparked<M: Manage> {
    /// Claimed from another thread and re-wrapped in this thread's runtime.
    Claimed(M::Connection, SlotMeta),
    /// Nothing was available; the caller should dial.
    Empty,
    /// One was taken, but [`Detach::attach`] failed and it has been destroyed.
    /// The caller should dial. Reported separately so the pool can keep its
    /// `created`/`closed` counters balanced.
    Lost,
}

/// Strategy for moving idle connections between per-thread shards.
pub trait Exchange<M: Manage>: Send + Sync + 'static {
    /// Offers an idle connection to other threads. Must not block.
    fn park(&self, conn: M::Connection, meta: SlotMeta) -> Parked<M>;

    /// Tries to claim a connection parked by another thread and reattach it
    /// to this one.
    fn unpark(&self) -> impl Future<Output = Unparked<M>>;

    /// Connections currently parked and claimable.
    fn parked(&self) -> u64;

    /// Drops every parked connection. Called by [`Pool::close`](crate::Pool::close).
    fn clear(&self) {}
}

/// The default: shards never share. Zero-sized, zero-cost.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoExchange;

impl<M: Manage> Exchange<M> for NoExchange {
    fn park(&self, conn: M::Connection, meta: SlotMeta) -> Parked<M> {
        Parked::Refused(conn, meta)
    }

    async fn unpark(&self) -> Unparked<M> {
        Unparked::Empty
    }

    fn parked(&self) -> u64 {
        0
    }
}

struct Entry<P> {
    parked: P,
    meta: SlotMeta,
}

/// A bounded, lock-free pool of detached connections shared by every thread.
///
/// Idle connections beyond a shard's [`min_idle`](crate::Config::min_idle) are
/// detached into this queue, where any thread may claim them. Both ends are
/// wait-free in the common case: [`ArrayQueue`] is a fixed-size ring, so
/// parking and stealing never allocate and never take a lock. The only async
/// step, [`Detach::attach`], runs after the entry is already out of the queue.
///
/// # Platform support
///
/// Only construct this where [`Detach`] is sound: io_uring and poll drivers
/// treat `attach` as a no-op, so fds move freely. Under IOCP a handle is bound
/// to one completion port for life. See [`Detach`] for the details.
pub struct Reservoir<M: Detach> {
    queue: ArrayQueue<Entry<M::Parked>>,
    /// Slots claimed: entries in the queue, plus those in flight between the
    /// admission check and the push. Capped at the queue's capacity, which is
    /// what makes the push below infallible — without it, a queue that filled
    /// up between `detach` and `push` would leave us holding a detached
    /// connection with no way to rebuild it synchronously.
    admitted: AtomicUsize,
}

impl<M: Detach> Reservoir<M> {
    /// Creates a reservoir holding at most `capacity` connections.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "reservoir capacity must be greater than zero");
        Self {
            queue: ArrayQueue::new(capacity),
            admitted: AtomicUsize::new(0),
        }
    }

    /// The most connections this reservoir will hold.
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Connections currently parked and claimable.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// True when nothing is parked.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Claims one slot, unless the reservoir is already full.
    fn admit(&self) -> bool {
        self.admitted
            .fetch_update(AcqRel, Acquire, |n| {
                (n < self.queue.capacity()).then_some(n + 1)
            })
            .is_ok()
    }

    /// Gives a claimed slot back.
    fn readmit(&self) {
        self.admitted.fetch_sub(1, AcqRel);
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
            .field("capacity", &self.queue.capacity())
            .field("len", &self.queue.len())
            .finish()
    }
}

impl<M: Detach> Exchange<M> for Reservoir<M> {
    fn park(&self, conn: M::Connection, meta: SlotMeta) -> Parked<M> {
        // Reserve before detaching, so the connection is still whole if the
        // reservoir turns out to be full.
        if !self.admit() {
            return Parked::Refused(conn, meta);
        }

        let Some(parked) = M::detach(conn) else {
            // An operation was still in flight, so `detach` consumed and
            // dropped it. Nothing to push.
            self.readmit();
            return Parked::Destroyed;
        };

        if self.queue.push(Entry { parked, meta }).is_err() {
            // Unreachable: `admitted` never exceeds capacity, and every
            // admitted slot is either pushed or handed back. Handled rather
            // than asserted, because the alternative is losing a live socket
            // without telling the pool about it.
            self.readmit();
            return Parked::Destroyed;
        }
        Parked::Accepted
    }

    async fn unpark(&self) -> Unparked<M> {
        let Some(entry) = self.queue.pop() else {
            return Unparked::Empty;
        };
        self.readmit();

        // Nothing is held across this await — the queue is lock-free and the
        // entry is already ours. `attach` re-wraps the socket in the calling
        // thread's runtime, which is why it must run here and not at push time.
        match M::attach(entry.parked).await {
            Ok(conn) => Unparked::Claimed(conn, entry.meta),
            Err(_) => Unparked::Lost,
        }
    }

    fn parked(&self) -> u64 {
        self.queue.len() as u64
    }

    fn clear(&self) {
        // A `park` caught mid-flight (admitted, not yet pushed) leaves its slot
        // claimed. That is harmless: `clear` runs from `Pool::close`, after
        // which nothing parks again.
        while self.queue.pop().is_some() {
            self.readmit();
        }
    }
}
