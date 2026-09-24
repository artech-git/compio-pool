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
