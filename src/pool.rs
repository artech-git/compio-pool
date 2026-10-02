//! The strictly thread-local resource pool (recipe step 11).
//!
//! A [`LocalPool`] is an `Rc` around `Cell`s and `RefCell`s. It is never sent
//! anywhere, so it needs no lock and no atomic: the only thread that can touch
//! it is the one whose ring the connections live on.
//!
//! Capacity is reserved before a resource is produced. [`LocalPool::try_reserve`]
//! hands out a [`Permit`] synchronously — that is the check the accept loop makes
//! the moment a connection arrives (step 13) — and the permit is later turned
//! into a [`Lease`] by popping an idle resource or creating one. Dropping either
//! gives the slot back.

use std::{
    cell::{Cell, RefCell},
    future::Future,
    io,
    ops::{Deref, DerefMut},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use crate::worker::WorkerContext;

/// Something a worker keeps a bounded number of and lends to one connection at
/// a time: a buffer, a parser, a connection to a backend.
///
/// Resources are created, used and dropped on one thread, so there is no `Send`
/// bound anywhere.
pub trait Resource: Sized + 'static {
    /// Build one resource on the worker that will own it. Runs on the pinned
    /// worker thread, inside its runtime, so it may do I/O.
    fn create(cx: &WorkerContext) -> impl Future<Output = io::Result<Self>>;

    /// Called when a lease ends, before the resource goes back on the idle
    /// list. Return `false` to drop it instead — for a backend connection that
    /// is no longer usable, say. The default keeps everything.
    fn recycle(&mut self) -> bool {
        true
    }
}

/// Wakers of futures waiting on one condition. Each waiting future owns an id,
/// so it can take its waker out again when it is dropped before the condition
/// holds; nothing stale is ever woken.
#[derive(Default)]
struct Waiters {
    next_id: Cell<u64>,
    entries: RefCell<Vec<(u64, Waker)>>,
}

impl Waiters {
    fn register(&self, id: &mut Option<u64>, waker: &Waker) {
        let mut entries = self.entries.borrow_mut();
        if let Some(id) = *id
            && let Some(entry) = entries.iter_mut().find(|(i, _)| *i == id)
        {
            if !entry.1.will_wake(waker) {
                entry.1 = waker.clone();
            }
            return;
        }
        let new_id = self.next_id.get();
        self.next_id.set(new_id.wrapping_add(1));
        entries.push((new_id, waker.clone()));
        *id = Some(new_id);
    }

    fn unregister(&self, id: Option<u64>) {
        if let Some(id) = id {
            self.entries.borrow_mut().retain(|(i, _)| *i != id);
        }
    }

    /// Wake everyone. The borrow is released before any waker runs, so a waker
    /// that polls something inline can register again without a double borrow.
    fn wake_all(&self) {
        let wakers: Vec<Waker> = self
            .entries
            .borrow_mut()
            .drain(..)
            .map(|(_, w)| w)
            .collect();
        for waker in wakers {
            waker.wake();
        }
    }
}

struct Inner<R> {
    cx: WorkerContext,
    capacity: usize,
    idle: RefCell<Vec<R>>,
    /// Permits plus leases outstanding. May exceed `capacity` only through
    /// [`LocalPool::reserve_unbounded`].
    taken: Cell<usize>,
    created: Cell<u64>,
    /// Waiting for `taken < capacity`.
    capacity_waiters: Waiters,
    /// Waiting for `taken == 0`.
    drain_waiters: Waiters,
}

/// A bounded, thread-local pool of [`Resource`]s. Cloning shares the pool.
pub struct LocalPool<R> {
    inner: Rc<Inner<R>>,
}

impl<R> Clone for LocalPool<R> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<R> std::fmt::Debug for LocalPool<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalPool")
            .field("capacity", &self.inner.capacity)
            .field("taken", &self.inner.taken.get())
            .field("idle", &self.inner.idle.borrow().len())
            .finish()
    }
}

impl<R> LocalPool<R> {
    /// The worker this pool belongs to.
    pub fn context(&self) -> &WorkerContext {
        &self.inner.cx
    }

    /// Slots in total.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Permits and leases outstanding.
    pub fn taken(&self) -> usize {
        self.inner.taken.get()
    }

    /// Resources sitting on the idle list.
    pub fn idle(&self) -> usize {
        self.inner.idle.borrow().len()
    }

    /// `capacity - taken`, saturating.
    pub fn available(&self) -> usize {
        self.inner.capacity.saturating_sub(self.inner.taken.get())
    }

    /// Whether [`try_reserve`](Self::try_reserve) would succeed right now.
    pub fn has_capacity(&self) -> bool {
        self.inner.taken.get() < self.inner.capacity
    }

    /// Resources created so far, including ones since dropped.
    pub fn created(&self) -> u64 {
        self.inner.created.get()
    }

    /// Take a slot if one is free. Synchronous and never blocks: this is the
    /// capacity check at accept time.
    pub fn try_reserve(&self) -> Option<Permit<R>> {
        if self.has_capacity() {
            Some(self.reserve_unbounded())
        } else {
            None
        }
    }

    /// Take a slot whether or not one is free. The pool goes over capacity by
    /// one until the permit or its lease is dropped. This is how
    /// [`OverflowPolicy::ServeLocally`](crate::OverflowPolicy::ServeLocally)
    /// admits a connection that nobody else can take.
    pub fn reserve_unbounded(&self) -> Permit<R> {
        self.inner.taken.set(self.inner.taken.get() + 1);
        Permit {
            pool: self.clone(),
            live: true,
        }
    }

    /// Wait for a slot and take it.
    pub fn reserve(&self) -> impl Future<Output = Permit<R>> + '_ {
        Wait {
            pool: self,
            kind: Kind::Capacity,
            ready: |pool: &LocalPool<R>| pool.try_reserve(),
            id: None,
        }
    }

    /// Resolve as soon as a slot is free, without taking it. The claim loop
    /// waits on this before it offers to take a connection off the channel.
    pub fn wait_available(&self) -> impl Future<Output = ()> + '_ {
        Wait {
            pool: self,
            kind: Kind::Capacity,
            ready: |pool: &LocalPool<R>| pool.has_capacity().then_some(()),
            id: None,
        }
    }

    /// Resolve when nothing is outstanding. Used to drain on shutdown.
    pub fn drained(&self) -> impl Future<Output = ()> + '_ {
        Wait {
            pool: self,
            kind: Kind::Drain,
            ready: |pool: &LocalPool<R>| (pool.taken() == 0).then_some(()),
            id: None,
        }
    }

    fn waiters(&self, kind: Kind) -> &Waiters {
        match kind {
            Kind::Capacity => &self.inner.capacity_waiters,
            Kind::Drain => &self.inner.drain_waiters,
        }
    }

    fn release(&self) {
        let inner = &self.inner;
        let taken = inner.taken.get().saturating_sub(1);
        inner.taken.set(taken);
        // Wake every capacity waiter, not just one. A `wait_available` waiter
        // does not consume the slot it was woken for, so waking one could leave
        // a `reserve` waiter asleep while a slot is free. There are only ever a
        // couple of waiters per worker, so this costs nothing measurable.
        if taken < inner.capacity {
            inner.capacity_waiters.wake_all();
        }
        if taken == 0 {
            inner.drain_waiters.wake_all();
        }
    }
}

impl<R: Resource> LocalPool<R> {
    /// An empty pool with `capacity` slots for worker `cx`.
    pub fn new(cx: WorkerContext, capacity: usize) -> Self {
        Self {
            inner: Rc::new(Inner {
                cx,
                capacity,
                idle: RefCell::new(Vec::with_capacity(capacity.min(1024))),
                taken: Cell::new(0),
                created: Cell::new(0),
                capacity_waiters: Waiters::default(),
                drain_waiters: Waiters::default(),
            }),
        }
    }

    /// Create up to `n` idle resources ahead of demand, never more than fit.
    /// Returns how many were created.
    pub async fn prewarm(&self, n: usize) -> io::Result<usize> {
        let room = self
            .inner
            .capacity
            .saturating_sub(self.inner.taken.get() + self.idle());
        let n = n.min(room);
        for _ in 0..n {
            let resource = R::create(&self.inner.cx).await?;
            self.inner.created.set(self.inner.created.get() + 1);
            self.inner.idle.borrow_mut().push(resource);
        }
        Ok(n)
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Capacity,
    Drain,
}

/// A future that polls `ready` and, while it says no, keeps exactly one waker
/// registered with the pool — removed again if the future is dropped first.
struct Wait<'a, R, F> {
    pool: &'a LocalPool<R>,
    kind: Kind,
    ready: F,
    id: Option<u64>,
}

impl<R, T, F: FnMut(&LocalPool<R>) -> Option<T> + Unpin> Future for Wait<'_, R, F> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        if let Some(value) = (this.ready)(this.pool) {
            this.pool.waiters(this.kind).unregister(this.id.take());
            return Poll::Ready(value);
        }
        this.pool
            .waiters(this.kind)
            .register(&mut this.id, cx.waker());
        Poll::Pending
    }
}

impl<R, F> Drop for Wait<'_, R, F> {
    fn drop(&mut self) {
        self.pool.waiters(self.kind).unregister(self.id.take());
    }
}

/// A reserved slot that has not been given a resource yet.
///
/// Dropping it unreserves the slot. Turn it into a [`Lease`] with
/// [`acquire`](Self::acquire).
pub struct Permit<R> {
    pool: LocalPool<R>,
    live: bool,
}

impl<R> std::fmt::Debug for Permit<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Permit")
            .field("live", &self.live)
            .field("pool", &self.pool)
            .finish()
    }
}

impl<R> Permit<R> {
    /// The pool the slot belongs to.
    pub fn pool(&self) -> &LocalPool<R> {
        &self.pool
    }
}

impl<R: Resource> Permit<R> {
    /// Pop an idle resource, or create one, and lease it against this slot.
    ///
    /// If creation fails the slot is released and the error returned.
    pub async fn acquire(mut self) -> io::Result<Lease<R>> {
        let idle = self.pool.inner.idle.borrow_mut().pop();
        let resource = match idle {
            Some(resource) => resource,
            None => {
                let resource = R::create(&self.pool.inner.cx).await?;
                self.pool
                    .inner
                    .created
                    .set(self.pool.inner.created.get() + 1);
                resource
            }
        };
        self.live = false;
        Ok(Lease {
            pool: self.pool.clone(),
            resource: Some(resource),
        })
    }
}

impl<R> Drop for Permit<R> {
    fn drop(&mut self) {
        if self.live {
            self.pool.release();
        }
    }
}

/// A resource checked out of its pool. Derefs to the resource; dropping it
/// recycles the resource and frees the slot.
pub struct Lease<R: Resource> {
    pool: LocalPool<R>,
    resource: Option<R>,
}

impl<R: Resource> std::fmt::Debug for Lease<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lease")
            .field("held", &self.resource.is_some())
            .field("pool", &self.pool)
            .finish()
    }
}

impl<R: Resource> Lease<R> {
    /// The pool the resource came from.
    pub fn pool(&self) -> &LocalPool<R> {
        &self.pool
    }

    /// End the lease and drop the resource instead of recycling it.
    pub fn discard(mut self) {
        drop(self.resource.take());
    }
}

impl<R: Resource> Deref for Lease<R> {
    type Target = R;

    fn deref(&self) -> &R {
        self.resource
            .as_ref()
            .expect("lease resource present until drop")
    }
}

impl<R: Resource> DerefMut for Lease<R> {
    fn deref_mut(&mut self) -> &mut R {
        self.resource
            .as_mut()
            .expect("lease resource present until drop")
    }
}

impl<R: Resource> Drop for Lease<R> {
    fn drop(&mut self) {
        if let Some(mut resource) = self.resource.take() {
            let inner = &self.pool.inner;
            // `taken` still counts this lease, so `idle + taken <= capacity` is
            // exactly "there is room for one more idle resource once we leave".
            let room = inner.idle.borrow().len() + inner.taken.get() <= inner.capacity;
            if room && resource.recycle() {
                inner.idle.borrow_mut().push(resource);
            }
        }
        self.pool.release();
    }
}
