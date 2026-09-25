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

#[cfg(test)]
mod tests {
    use std::{
        pin::pin,
        sync::atomic::Ordering::SeqCst,
        task::{Context, Poll, Waker},
    };

    use super::*;
    use crate::test_support::{CAN_ATTACH, CAN_DETACH, MovableConn, MovableManager, detach_lock};

    /// Both `unpark` implementations, and the test `attach`, complete without
    /// ever yielding — the queue is lock-free and the entry is already ours. One
    /// poll is therefore enough, and asserting that keeps the claim honest.
    fn drive<F: Future>(fut: F) -> F::Output {
        let mut fut = pin!(fut);
        match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(v) => v,
            Poll::Pending => panic!("an exchange future must not yield"),
        }
    }

    fn meta(generation: u64, uses: u64) -> SlotMeta {
        let mut m = SlotMeta::new(generation);
        m.uses = uses;
        m
    }

    fn reservoir(capacity: usize) -> Reservoir<MovableManager> {
        Reservoir::new(capacity)
    }

    // ---- NoExchange: the zero-cost default ----------------------------------

    #[test]
    fn no_exchange_refuses_and_hands_the_connection_straight_back() {
        let x = NoExchange;
        match Exchange::<MovableManager>::park(&x, MovableConn::new(7), meta(3, 5)) {
            Parked::Refused(conn, meta) => {
                assert_eq!(conn.id, 7, "the caller keeps its connection");
                assert_eq!(meta.generation, 3);
                assert_eq!(meta.uses, 5);
            }
            _ => panic!("NoExchange must never take ownership"),
        }
    }

    #[test]
    fn no_exchange_never_produces_a_connection() {
        let x = NoExchange;
        assert!(matches!(
            drive(Exchange::<MovableManager>::unpark(&x)),
            Unparked::Empty
        ));
        assert_eq!(Exchange::<MovableManager>::parked(&x), 0);
    }

    #[test]
    fn no_exchange_clear_is_the_default_no_op() {
        // `clear` has a default body; calling it on the default exchange is the
        // path `Pool::close` takes when no reservoir is installed.
        Exchange::<MovableManager>::clear(&NoExchange);
    }

    #[test]
    fn no_exchange_is_zero_sized_and_derives_the_usual_traits() {
        fn assert_traits<T: Copy + Clone + Default + std::fmt::Debug>() {}
        assert_traits::<NoExchange>();

        // Zero-sized is the whole point: the default exchange must cost a pool
        // that never shares absolutely nothing.
        assert_eq!(std::mem::size_of::<NoExchange>(), 0);
        assert_eq!(format!("{NoExchange:?}"), "NoExchange");
    }

    // ---- Reservoir: construction and inspection -----------------------------

    #[test]
    fn a_new_reservoir_is_empty() {
        let r = reservoir(4);
        assert_eq!(r.capacity(), 4);
        assert_eq!(r.len(), 0);
        assert!(r.is_empty());
        assert_eq!(Exchange::<MovableManager>::parked(&r), 0);
    }

    #[test]
    #[should_panic(expected = "reservoir capacity must be greater than zero")]
    fn a_zero_capacity_reservoir_panics() {
        // A reservoir that can hold nothing would detach connections and then
        // have nowhere to put them.
        let _ = reservoir(0);
    }

    #[test]
    fn the_default_reservoir_holds_sixty_four() {
        assert_eq!(Reservoir::<MovableManager>::default().capacity(), 64);
    }

    #[test]
    fn debug_reports_capacity_and_occupancy() {
        let _lock = detach_lock();
        let r = reservoir(4);
        r.park(MovableConn::new(0), meta(0, 0));
        let s = format!("{r:?}");
        assert!(s.contains("capacity: 4"), "got {s}");
        assert!(s.contains("len: 1"), "got {s}");
    }

    // ---- Reservoir: parking ------------------------------------------------

    #[test]
    fn parking_takes_ownership() {
        let _lock = detach_lock();
        let r = reservoir(2);

        assert!(matches!(
            r.park(MovableConn::new(1), meta(0, 0)),
            Parked::Accepted
        ));
        assert_eq!(r.len(), 1);
        assert!(!r.is_empty());
        assert_eq!(Exchange::<MovableManager>::parked(&r), 1);
    }

    #[test]
    fn a_full_reservoir_refuses_without_detaching() {
        let _lock = detach_lock();
        let r = reservoir(2);
        r.park(MovableConn::new(0), meta(0, 0));
        r.park(MovableConn::new(1), meta(0, 0));

        // The third offer must come back whole: admission is checked *before*
        // `detach`, so a full reservoir cannot cost the shard a connection.
        match r.park(MovableConn::new(2), meta(9, 4)) {
            Parked::Refused(conn, meta) => {
                assert_eq!(conn.id, 2);
                assert_eq!((meta.generation, meta.uses), (9, 4));
            }
            _ => panic!("a full reservoir must refuse"),
        }
        assert_eq!(r.len(), 2, "and must not grow past capacity");
    }

    /// `detach` consumes the connection either way. When it declines, the
    /// connection is already gone, so the reservoir reports `Destroyed` rather
    /// than pretending it can be handed back.
    #[test]
    fn a_connection_that_cannot_detach_is_reported_destroyed() {
        let _lock = detach_lock();
        let r = reservoir(2);
        CAN_DETACH.store(false, SeqCst);

        assert!(matches!(
            r.park(MovableConn::new(0), meta(0, 0)),
            Parked::Destroyed
        ));
        assert_eq!(r.len(), 0, "nothing was queued");

        // The admission slot must have been handed back, or the reservoir would
        // leak capacity on every failed detach.
        CAN_DETACH.store(true, SeqCst);
        r.park(MovableConn::new(1), meta(0, 0));
        r.park(MovableConn::new(2), meta(0, 0));
        assert_eq!(r.len(), 2, "failed detaches must not consume capacity");
    }

    // ---- Reservoir: claiming -----------------------------------------------

    #[test]
    fn claiming_an_empty_reservoir_tells_the_caller_to_dial() {
        let r = reservoir(2);
        assert!(matches!(
            drive(Exchange::<MovableManager>::unpark(&r)),
            Unparked::Empty
        ));
    }

    #[test]
    fn a_claimed_connection_keeps_its_identity_and_metadata() {
        let _lock = detach_lock();
        let r = reservoir(2);
        r.park(MovableConn::new(42), meta(3, 8));

        match drive(Exchange::<MovableManager>::unpark(&r)) {
            Unparked::Claimed(conn, meta) => {
                assert_eq!(conn.id, 42, "the same socket, re-wrapped");
                assert_eq!(
                    (meta.generation, meta.uses),
                    (3, 8),
                    "lifecycle bookkeeping survives the trip"
                );
            }
            _ => panic!("the parked connection should have been claimed"),
        }
        assert!(r.is_empty());
    }

    /// FIFO by design: the connection parked longest is claimed first, so
    /// nothing goes stale at the bottom of a stack.
    #[test]
    fn claims_are_first_in_first_out() {
        let _lock = detach_lock();
        let r = reservoir(4);
        for id in 0..3 {
            r.park(MovableConn::new(id), meta(0, 0));
        }

        let mut order = Vec::new();
        while let Unparked::Claimed(conn, _) = drive(Exchange::<MovableManager>::unpark(&r)) {
            order.push(conn.id);
        }
        assert_eq!(order, vec![0, 1, 2]);
    }

    #[test]
    fn a_connection_that_cannot_reattach_is_reported_lost() {
        let _lock = detach_lock();
        let r = reservoir(2);
        r.park(MovableConn::new(0), meta(0, 0));
        CAN_ATTACH.store(false, SeqCst);

        assert!(matches!(
            drive(Exchange::<MovableManager>::unpark(&r)),
            Unparked::Lost
        ));
        assert!(r.is_empty(), "it was popped, not left behind");
    }

    /// Claiming frees the admission slot as well as the queue slot; otherwise a
    /// reservoir would accept only `capacity` parks over its whole lifetime.
    #[test]
    fn claiming_frees_capacity_for_a_later_park() {
        let _lock = detach_lock();
        let r = reservoir(1);
        assert!(matches!(
            r.park(MovableConn::new(0), meta(0, 0)),
            Parked::Accepted
        ));
        assert!(matches!(
            r.park(MovableConn::new(1), meta(0, 0)),
            Parked::Refused(..)
        ));

        drive(Exchange::<MovableManager>::unpark(&r));
        assert!(
            matches!(r.park(MovableConn::new(2), meta(0, 0)), Parked::Accepted),
            "the slot freed by the claim must be reusable"
        );
    }

    #[test]
    fn a_lost_connection_also_frees_its_capacity() {
        let _lock = detach_lock();
        let r = reservoir(1);
        r.park(MovableConn::new(0), meta(0, 0));
        CAN_ATTACH.store(false, SeqCst);
        assert!(matches!(
            drive(Exchange::<MovableManager>::unpark(&r)),
            Unparked::Lost
        ));

        CAN_ATTACH.store(true, SeqCst);
        assert!(matches!(
            r.park(MovableConn::new(1), meta(0, 0)),
            Parked::Accepted
        ));
    }

    // ---- Reservoir: clearing ----------------------------------------------

    #[test]
    fn clear_drops_everything_and_frees_all_capacity() {
        let _lock = detach_lock();
        let r = reservoir(2);
        r.park(MovableConn::new(0), meta(0, 0));
        r.park(MovableConn::new(1), meta(0, 0));

        Exchange::<MovableManager>::clear(&r);

        assert_eq!(r.len(), 0);
        assert!(r.is_empty());
        assert_eq!(Exchange::<MovableManager>::parked(&r), 0);
        // Capacity is back, which is what lets a pool keep working after
        // `invalidate` rather than only after `close`.
        assert!(matches!(
            r.park(MovableConn::new(2), meta(0, 0)),
            Parked::Accepted
        ));
        assert!(matches!(
            r.park(MovableConn::new(3), meta(0, 0)),
            Parked::Accepted
        ));
    }

    #[test]
    fn clearing_an_empty_reservoir_is_harmless() {
        let r = reservoir(2);
        Exchange::<MovableManager>::clear(&r);
        assert!(r.is_empty());
    }

    /// Admission is the invariant that makes the push in `park` infallible, so
    /// exercise it right at the boundary.
    #[test]
    fn admission_never_exceeds_capacity() {
        let _lock = detach_lock();
        let r = reservoir(3);
        for id in 0..3 {
            assert!(matches!(
                r.park(MovableConn::new(id), meta(0, 0)),
                Parked::Accepted
            ));
        }
        for id in 3..6 {
            assert!(matches!(
                r.park(MovableConn::new(id), meta(0, 0)),
                Parked::Refused(..)
            ));
        }
        assert_eq!(r.len(), 3);
        assert!(r.len() <= r.capacity());
    }
}
