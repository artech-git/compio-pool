//! Per-thread state. Never shared across threads, so it uses `Cell`/`RefCell`
//! and plain `Rc` — no atomics on the acquire fast path.

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    future::Future,
    pin::Pin,
    rc::Rc,
    sync::{Arc, atomic::Ordering::Relaxed},
    task::{Context, Poll, Waker},
};

use crate::{config::Config, manage::Manage, metrics::Counters, slot::Slot};

/// One parked `acquire` on this thread.
struct Waiter {
    notified: Cell<bool>,
    waker: RefCell<Option<Waker>>,
}

/// A single compio thread's slice of the pool.
pub(crate) struct Shard<M: Manage> {
    /// Idle connections, LIFO: the most recently returned is the warmest.
    free: RefCell<Vec<Slot<M::Connection>>>,
    /// Connections this thread owns, idle *and* checked out.
    size: Cell<usize>,
    waiters: RefCell<VecDeque<Rc<Waiter>>>,
    counters: Arc<Counters>,
    manager: Arc<M>,
}

impl<M: Manage> Shard<M> {
    pub(crate) fn new(counters: Arc<Counters>, manager: Arc<M>) -> Self {
        Self {
            free: RefCell::new(Vec::new()),
            size: Cell::new(0),
            waiters: RefCell::new(VecDeque::new()),
            counters,
            manager,
        }
    }

    pub(crate) fn idle_len(&self) -> usize {
        self.free.borrow().len()
    }

    pub(crate) fn size(&self) -> usize {
        self.size.get()
    }

    pub(crate) fn has_waiters(&self) -> bool {
        !self.waiters.borrow().is_empty()
    }

    /// Takes the warmest idle connection, if any. Does **not** check expiry;
    /// the caller does that, because discarding needs the manager.
    pub(crate) fn pop_idle(&self) -> Option<Slot<M::Connection>> {
        let slot = self.free.borrow_mut().pop();
        if slot.is_some() {
            Counters::dec(&self.counters.idle);
        }
        slot
    }

    pub(crate) fn push_idle(&self, slot: Slot<M::Connection>) {
        self.free.borrow_mut().push(slot);
        Counters::inc(&self.counters.idle);
        self.wake_one();
    }

    /// Claims budget for one more connection on this thread.
    ///
    /// `max_size` is per-shard by design — see [`Config`](crate::Config).
    pub(crate) fn try_reserve(&self, max_size: usize) -> bool {
        if self.size.get() < max_size {
            self.size.set(self.size.get() + 1);
            true
        } else {
            false
        }
    }

    /// Gives back budget claimed by `try_reserve`, waking one waiter.
    pub(crate) fn release(&self) {
        self.size.set(self.size.get().saturating_sub(1));
        self.wake_one();
    }

    /// Removes every idle connection that has aged out, returning them so the
    /// caller can run `Manage::disconnect` on each.
    pub(crate) fn drain_expired(&self, cfg: &Config, generation: u64) -> Vec<Slot<M::Connection>> {
        let mut free = self.free.borrow_mut();
        let mut expired = Vec::new();
        let mut i = 0;
        while i < free.len() {
            if free[i].is_expired(cfg, generation) {
                expired.push(free.swap_remove(i));
            } else {
                i += 1;
            }
        }
        self.counters.idle.fetch_sub(expired.len() as u64, Relaxed);
        expired
    }

    /// Empties the free list, for `Pool::close`.
    pub(crate) fn drain_all(&self) -> Vec<Slot<M::Connection>> {
        let drained: Vec<_> = self.free.borrow_mut().drain(..).collect();
        self.counters.idle.fetch_sub(drained.len() as u64, Relaxed);
        drained
    }

    fn wake_one(&self) {
        let waiter = self.waiters.borrow_mut().pop_front();
        if let Some(w) = waiter {
            w.notified.set(true);
            let waker = w.waker.borrow_mut().take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    /// Waits until a connection is returned or budget frees up on this thread.
    ///
    /// Cross-thread wakeups are deliberately not part of this: a thread that is
    /// at `max_size` has outstanding connections *of its own* that will come
    /// back, so waiting locally always makes progress.
    pub(crate) fn wait(&self) -> WaitForSlot<'_, M> {
        WaitForSlot {
            shard: self,
            waiter: None,
        }
    }
}

impl<M: Manage> Drop for Shard<M> {
    fn drop(&mut self) {
        // Reached at thread exit. Every `Pooled` guard holds an `Rc<Shard>`, so
        // by construction nothing is checked out here — `size` is all idle.
        //
        // This runs on the thread whose compio driver owns these connections,
        // which is the only thread allowed to close them.
        let n = self.size.get() as u64;
        let idle: Vec<_> = self.free.borrow_mut().drain(..).collect();
        self.counters.idle.fetch_sub(idle.len() as u64, Relaxed);
        for slot in idle {
            self.manager.disconnect(slot.conn);
        }
        self.counters.live.fetch_sub(n, Relaxed);
        self.counters.closed.fetch_add(n, Relaxed);
    }
}

/// Future returned by [`Shard::wait`].
pub(crate) struct WaitForSlot<'a, M: Manage> {
    shard: &'a Shard<M>,
    waiter: Option<Rc<Waiter>>,
}

impl<M: Manage> Future for WaitForSlot<'_, M> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        match &this.waiter {
            None => {
                let waiter = Rc::new(Waiter {
                    notified: Cell::new(false),
                    waker: RefCell::new(Some(cx.waker().clone())),
                });
                this.shard.waiters.borrow_mut().push_back(waiter.clone());
                this.waiter = Some(waiter);
                Poll::Pending
            }
            Some(waiter) => {
                if waiter.notified.get() {
                    // Clear it so `Drop` knows the wakeup was consumed.
                    this.waiter = None;
                    Poll::Ready(())
                } else {
                    *waiter.waker.borrow_mut() = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        }
    }
}

impl<M: Manage> Drop for WaitForSlot<'_, M> {
    fn drop(&mut self) {
        let Some(waiter) = self.waiter.take() else {
            return;
        };
        if waiter.notified.get() {
            // We were handed a wakeup but are being cancelled (acquire timeout,
            // or the caller's future was dropped). Passing it on is mandatory:
            // the connection that triggered it is idle, and if we swallow the
            // notification the next waiter sleeps until its own timeout.
            self.shard.wake_one();
        } else {
            let mut queue = self.shard.waiters.borrow_mut();
            if let Some(pos) = queue.iter().position(|w| Rc::ptr_eq(w, &waiter)) {
                queue.remove(pos);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::task::Context;

    use super::*;
    use crate::{
        slot::Slot,
        test_support::{Counts, TestConn, TestManager, counting_waker},
    };

    /// A shard wired to fresh counters, plus handles to observe both.
    fn shard() -> (Rc<Shard<TestManager>>, Arc<Counters>, Arc<Counts>) {
        let manager = Arc::new(TestManager::new());
        let counts = manager.counts();
        let counters = Arc::new(Counters::default());
        (
            Rc::new(Shard::new(counters.clone(), manager)),
            counters,
            counts,
        )
    }

    fn slot(id: u64, generation: u64) -> Slot<TestConn> {
        Slot::new(TestConn::new(id), generation)
    }

    fn untimed() -> Config {
        Config::new().max_lifetime(None).idle_timeout(None)
    }

    #[test]
    fn a_new_shard_is_empty() {
        let (shard, counters, _) = shard();
        assert_eq!(shard.size(), 0);
        assert_eq!(shard.idle_len(), 0);
        assert!(!shard.has_waiters());
        assert_eq!(counters.idle.load(Relaxed), 0);
    }

    #[test]
    fn push_and_pop_track_the_idle_gauge() {
        let (shard, counters, _) = shard();

        shard.push_idle(slot(0, 0));
        shard.push_idle(slot(1, 0));
        assert_eq!(shard.idle_len(), 2);
        assert_eq!(counters.idle.load(Relaxed), 2);

        let popped = shard.pop_idle().expect("two were pushed");
        assert_eq!(popped.conn.id, 1, "LIFO: the warmest comes back first");
        assert_eq!(counters.idle.load(Relaxed), 1);
    }

    #[test]
    fn popping_an_empty_free_list_leaves_the_gauge_alone() {
        let (shard, counters, _) = shard();
        assert!(shard.pop_idle().is_none());
        assert_eq!(
            counters.idle.load(Relaxed),
            0,
            "a miss must not decrement past zero"
        );
    }

    #[test]
    fn try_reserve_stops_at_max_size() {
        let (shard, _, _) = shard();

        assert!(shard.try_reserve(2));
        assert!(shard.try_reserve(2));
        assert!(!shard.try_reserve(2), "max_size is a hard per-shard cap");
        assert_eq!(shard.size(), 2, "a refused reservation must not count");
    }

    #[test]
    fn release_gives_budget_back() {
        let (shard, _, _) = shard();
        assert!(shard.try_reserve(1));
        shard.release();
        assert_eq!(shard.size(), 0);
        assert!(shard.try_reserve(1), "the slot is usable again");
    }

    /// `release` saturates rather than wrapping: a bookkeeping slip must not
    /// turn `size` into `usize::MAX` and wedge the shard forever.
    #[test]
    fn releasing_an_empty_shard_saturates() {
        let (shard, _, _) = shard();
        shard.release();
        assert_eq!(shard.size(), 0);
    }

    #[test]
    fn drain_expired_removes_only_the_stale_slots() {
        let (shard, counters, _) = shard();
        // Generation is the easiest expiry knob to drive deterministically.
        for (id, generation) in [(0, 0), (1, 1), (2, 0), (3, 1)] {
            shard.push_idle(slot(id, generation));
        }

        let expired = shard.drain_expired(&untimed(), 1);

        let mut ids: Vec<_> = expired.iter().map(|s| s.conn.id).collect();
        ids.sort();
        assert_eq!(ids, vec![0, 2], "only the older generation goes");
        assert_eq!(shard.idle_len(), 2);
        assert_eq!(counters.idle.load(Relaxed), 2, "the gauge follows");
    }

    #[test]
    fn drain_expired_keeps_everything_when_nothing_aged_out() {
        let (shard, counters, _) = shard();
        shard.push_idle(slot(0, 0));
        shard.push_idle(slot(1, 0));

        assert!(shard.drain_expired(&untimed(), 0).is_empty());
        assert_eq!(shard.idle_len(), 2);
        assert_eq!(counters.idle.load(Relaxed), 2);
    }

    #[test]
    fn drain_all_empties_the_free_list() {
        let (shard, counters, _) = shard();
        shard.push_idle(slot(0, 0));
        shard.push_idle(slot(1, 0));

        assert_eq!(shard.drain_all().len(), 2);
        assert_eq!(shard.idle_len(), 0);
        assert_eq!(counters.idle.load(Relaxed), 0);
        assert!(shard.drain_all().is_empty(), "draining twice is harmless");
    }

    #[test]
    fn waking_with_no_waiters_is_a_no_op() {
        let (shard, _, _) = shard();
        shard.push_idle(slot(0, 0)); // push_idle wakes one; there is nobody
        shard.release();
        assert_eq!(shard.idle_len(), 1);
    }

    #[test]
    fn a_waiter_is_registered_and_then_woken_by_a_returned_connection() {
        let (shard, _, _) = shard();
        let (woken, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = Box::pin(shard.wait());

        // First poll registers; there is nothing to hand out yet.
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert!(shard.has_waiters());
        assert_eq!(woken.count(), 0);

        // A spurious re-poll must stay pending and keep the waiter queued.
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert!(shard.has_waiters());

        shard.push_idle(slot(0, 0));
        assert_eq!(woken.count(), 1, "push_idle must wake a waiter");
        assert!(!shard.has_waiters(), "wake_one dequeues as it notifies");
        assert!(fut.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn freeing_budget_also_wakes_a_waiter() {
        let (shard, _, _) = shard();
        let (woken, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = Box::pin(shard.wait());
        assert!(fut.as_mut().poll(&mut cx).is_pending());

        // Not only returned connections: budget freed by a destroyed one counts.
        shard.release();
        assert_eq!(woken.count(), 1);
        assert!(fut.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn a_waiter_dropped_before_being_woken_leaves_the_queue() {
        let (shard, _, _) = shard();
        let (_woken, waker) = counting_waker();
        let mut cx = Context::from_waker(&waker);

        let mut fut = Box::pin(shard.wait());
        assert!(fut.as_mut().poll(&mut cx).is_pending());
        assert!(shard.has_waiters());

        drop(fut);
        assert!(
            !shard.has_waiters(),
            "an abandoned waiter must not keep receiving wakeups"
        );
    }

    /// The wakeup is a resource: whoever cannot use it has to pass it on, or the
    /// idle connection that caused it sits there while the next waiter sleeps.
    #[test]
    fn a_notified_but_cancelled_waiter_forwards_its_wakeup() {
        let (shard, _, _) = shard();
        let (first_woken, first_waker) = counting_waker();
        let (second_woken, second_waker) = counting_waker();

        let mut first = Box::pin(shard.wait());
        let mut second = Box::pin(shard.wait());
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&first_waker))
                .is_pending()
        );
        assert!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(&second_waker))
                .is_pending()
        );

        // FIFO: the notification goes to `first`, which is then cancelled.
        shard.release();
        assert_eq!(first_woken.count(), 1);
        assert_eq!(second_woken.count(), 0);

        drop(first);
        assert_eq!(second_woken.count(), 1, "the wakeup was handed along");
        assert!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(&second_waker))
                .is_ready()
        );
    }

    #[test]
    fn a_waiter_dropped_without_ever_being_polled_is_harmless() {
        let (shard, _, _) = shard();
        drop(shard.wait());
        assert!(!shard.has_waiters());
    }

    /// A completed `WaitForSlot` has already cleared its waiter, so its `Drop`
    /// must not forward a second wakeup.
    #[test]
    fn a_consumed_wakeup_is_not_forwarded_twice() {
        let (shard, _, _) = shard();
        let (first_woken, first_waker) = counting_waker();
        let (second_woken, second_waker) = counting_waker();

        let mut first = Box::pin(shard.wait());
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&first_waker))
                .is_pending()
        );
        shard.release();
        assert!(
            first
                .as_mut()
                .poll(&mut Context::from_waker(&first_waker))
                .is_ready()
        );

        let mut second = Box::pin(shard.wait());
        assert!(
            second
                .as_mut()
                .poll(&mut Context::from_waker(&second_waker))
                .is_pending()
        );
        drop(first);
        assert_eq!(
            second_woken.count(),
            0,
            "the first waiter already used its wakeup"
        );
        assert_eq!(first_woken.count(), 1);
    }

    /// At thread exit the shard closes what it holds — on the thread whose
    /// driver owns those handles — and squares the global gauges.
    #[test]
    fn dropping_a_shard_closes_its_idle_connections() {
        let (shard, counters, counts) = shard();
        counters.live.store(2, Relaxed);
        assert!(shard.try_reserve(2));
        assert!(shard.try_reserve(2));
        shard.push_idle(slot(0, 0));
        shard.push_idle(slot(1, 0));

        drop(shard);

        assert_eq!(counts.disconnected(), 2, "closed through the manager");
        assert_eq!(counters.idle.load(Relaxed), 0);
        assert_eq!(counters.live.load(Relaxed), 0);
        assert_eq!(counters.closed.load(Relaxed), 2);
    }

    #[test]
    fn dropping_an_untouched_shard_changes_nothing() {
        let (shard, counters, counts) = shard();
        drop(shard);
        assert_eq!(counts.disconnected(), 0);
        assert_eq!(counters.closed.load(Relaxed), 0);
    }
}
