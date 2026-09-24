//! The pool handle, the acquire path, and the per-shard reaper.

use std::{
    any::Any,
    cell::RefCell,
    collections::HashMap,
    rc::{Rc, Weak},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    time::Instant,
};

use crate::{
    config::Config,
    error::Error,
    exchange::{Exchange, NoExchange, Parked, Unparked},
    guard::Pooled,
    manage::Manage,
    metrics::{Counters, Metrics},
    shard::Shard,
    slot::Slot,
};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(0);

/// Holds a shard's capacity claim across an `await`, giving it back if the
/// `acquire` future is cancelled.
///
/// `acquire` claims budget with `Shard::try_reserve` *before* awaiting
/// `Manage::connect`, `Exchange::unpark` or `Manage::recycle`, and `acquire`
/// is itself cancellable - by the acquire timeout, or by the caller's own
/// `select!`. Without this guard, a cancellation between the claim and the end
/// of the await would leak that capacity permanently, and a shard would
/// eventually sit at `max_size` holding no connections at all.
struct Reserved<'a, M: Manage> {
    shard: &'a Rc<Shard<M>>,
    counters: &'a Counters,
    /// True when a real connection is riding along, counted in `live`.
    holds_conn: bool,
    armed: bool,
}

impl<'a, M: Manage> Reserved<'a, M> {
    /// Budget claimed, no connection behind it yet.
    fn empty(shard: &'a Rc<Shard<M>>, counters: &'a Counters) -> Self {
        Self {
            shard,
            counters,
            holds_conn: false,
            armed: true,
        }
    }

    /// Budget claimed and a live connection is out of the free list.
    fn holding(shard: &'a Rc<Shard<M>>, counters: &'a Counters) -> Self {
        Self {
            shard,
            counters,
            holds_conn: true,
            armed: true,
        }
    }

    /// The await completed; ownership has passed on.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl<M: Manage> Drop for Reserved<'_, M> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if self.holds_conn {
            // The connection is dropped by the cancelled future without a
            // graceful `Manage::disconnect`; there is no way to await one here.
            Counters::dec(&self.counters.live);
            Counters::inc(&self.counters.closed);
        }
        self.shard.release();
    }
}

thread_local! {
    /// Every pool's shard for *this* thread, keyed by pool id.
    ///
    /// This is what makes `Pool` `Send + Sync` while `M::Connection` is not:
    /// the `Arc<Inner>` holds no connections at all. They live here, and a
    /// value in a thread-local is by construction only reachable from its own
    /// thread. At thread exit the map drops, closing that thread's connections
    /// on the thread whose compio driver owns them.
    static SHARDS: RefCell<HashMap<u64, Box<dyn Any>>> = RefCell::new(HashMap::new());
}

struct Inner<M: Manage, X: Exchange<M>> {
    id: u64,
    manager: Arc<M>,
    config: Config,
    exchange: X,
    counters: Arc<Counters>,
    generation: AtomicU64,
    closed: AtomicBool,
}

/// A sharded connection pool for `compio`.
///
/// Cheap to clone and `Send + Sync`: clone it onto every compio thread. Each
/// thread transparently gets its own shard, so the acquire fast path touches no
/// atomics and no locks.
///
/// The `X` parameter selects the cross-thread strategy. The default,
/// [`NoExchange`], keeps threads fully independent. Swap in a
/// [`Reservoir`](crate::Reservoir) — which requires [`Detach`](crate::Detach) —
/// to let idle connections migrate to whichever thread needs them.
pub struct Pool<M: Manage, X: Exchange<M> = NoExchange> {
    inner: Arc<Inner<M, X>>,
}

impl<M: Manage, X: Exchange<M>> Clone for Pool<M, X> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<M: Manage> Pool<M> {
    /// Builds a pool with no cross-thread sharing.
    pub fn new(manager: M, config: Config) -> Self {
        Self::with_exchange(manager, config, NoExchange)
    }

    /// Starts a [`Builder`].
    pub fn builder(manager: M) -> Builder<M, NoExchange> {
        Builder {
            manager,
            config: Config::default(),
            exchange: NoExchange,
        }
    }
}

impl<M: Manage, X: Exchange<M>> Pool<M, X> {
    /// Builds a pool with an explicit cross-thread [`Exchange`].
    pub fn with_exchange(manager: M, config: Config, exchange: X) -> Self {
        Self {
            inner: Arc::new(Inner {
                id: NEXT_POOL_ID.fetch_add(1, Relaxed),
                manager: Arc::new(manager),
                config,
                exchange,
                counters: Arc::new(Counters::default()),
                generation: AtomicU64::new(0),
                closed: AtomicBool::new(false),
            }),
        }
    }

    /// The manager, for callers that need to reach configuration through it.
    pub fn manager(&self) -> &M {
        self.inner.manager.as_ref()
    }

    /// The configuration this pool was built with.
    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    /// Connections owned by *this thread's* shard, idle plus checked out.
    pub fn local_size(&self) -> usize {
        self.shard().size()
    }

    /// Idle connections sitting in *this thread's* shard.
    pub fn local_idle(&self) -> usize {
        self.shard().idle_len()
    }

    /// A snapshot of pool activity.
    pub fn metrics(&self) -> Metrics {
        self.inner.counters.snapshot(self.inner.exchange.parked())
    }

    /// Retires every connection created before this call.
    ///
    /// Live checkouts keep working and are closed when returned. Use after a
    /// credential rotation or a failover, where existing sockets point at the
    /// wrong place but the pool itself is still wanted.
    pub fn invalidate(&self) {
        self.inner.generation.fetch_add(1, Relaxed);
        self.inner.exchange.clear();
    }

    /// Closes the pool.
    ///
    /// Subsequent [`acquire`](Pool::acquire) calls fail with
    /// [`Error::Closed`]. This thread's idle connections are closed
    /// immediately; other threads' are closed when they next touch the pool or
    /// when their thread exits.
    pub fn close(&self) {
        self.inner.closed.store(true, Relaxed);
        self.inner.exchange.clear();
        let shard = self.shard();
        for slot in shard.drain_all() {
            self.destroy(&shard, slot);
        }
    }

    /// Whether [`close`](Pool::close) has been called.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Relaxed)
    }

    /// Opens connections on the current thread up to `min_idle`.
    ///
    /// Call this once per compio thread at startup so the first real request
    /// does not pay for a handshake.
    pub async fn warm(&self) -> Result<(), Error<M::Error>> {
        let shard = self.shard();
        let target = self.inner.config.effective_min_idle();
        while shard.idle_len() < target {
            if !shard.try_reserve(self.inner.config.max_size) {
                break;
            }
            match self.inner.manager.connect().await {
                Ok(conn) => {
                    Counters::inc(&self.inner.counters.created);
                    Counters::inc(&self.inner.counters.live);
                    let generation = self.inner.generation.load(Relaxed);
                    shard.push_idle(Slot::new(conn, generation));
                }
                Err(e) => {
                    shard.release();
                    return Err(Error::Backend(e));
                }
            }
        }
        Ok(())
    }

    /// Checks out a connection, waiting if the shard is at `max_size`.
    ///
    /// Must be called on a compio runtime thread. Honours
    /// [`Config::acquire_timeout`].
    pub async fn acquire(&self) -> Result<Pooled<M, X>, Error<M::Error>> {
        match self.inner.config.acquire_timeout {
            Some(limit) => match compio::time::timeout(limit, self.acquire_inner()).await {
                Ok(result) => result,
                Err(_elapsed) => {
                    Counters::inc(&self.inner.counters.timeouts);
                    Err(Error::Timeout)
                }
            },
            None => self.acquire_inner().await,
        }
    }

    async fn acquire_inner(&self) -> Result<Pooled<M, X>, Error<M::Error>> {
        let shard = self.shard();
        loop {
            if self.inner.closed.load(Relaxed) {
                return Err(Error::Closed);
            }
            let generation = self.inner.generation.load(Relaxed);

            // 1. Warmest idle connection on this thread. No atomics, no lock.
            while let Some(slot) = shard.pop_idle() {
                if slot.is_expired(&self.inner.config, generation) {
                    self.destroy(&shard, slot);
                    continue;
                }
                let Slot { mut conn, meta } = slot;
                // The connection is now out of the free list but not yet in a
                // guard: if this await is cancelled, `reserved` accounts for it.
                let reserved = Reserved::holding(&shard, &self.inner.counters);
                let recycled = self.inner.manager.recycle(&mut conn, &meta).await;
                reserved.disarm();
                match recycled {
                    Ok(()) => {
                        Counters::inc(&self.inner.counters.acquires);
                        return Ok(Pooled::new(Slot { conn, meta }, shard, self.clone()));
                    }
                    Err(_) => {
                        Counters::inc(&self.inner.counters.recycle_failures);
                        self.destroy_conn(&shard, conn);
                    }
                }
            }

            // 2. Room for one more on this thread? Claim the budget first, so
            //    that unpark and connect are both covered by one reservation.
            if shard.try_reserve(self.inner.config.max_size) {
                // Both awaits below are cancellable; `reserved` hands the
                // budget back if either is dropped part-way through.
                let reserved = Reserved::empty(&shard, &self.inner.counters);
                let unparked = self.inner.exchange.unpark().await;
                reserved.disarm();

                match unparked {
                    // Someone else's idle socket, re-wrapped in this thread's
                    // runtime. Cheaper than a handshake, so it is tried first.
                    Unparked::Claimed(conn, meta) => {
                        Counters::inc(&self.inner.counters.unparked);
                        Counters::inc(&self.inner.counters.live);
                        Counters::inc(&self.inner.counters.acquires);
                        return Ok(Pooled::new(Slot { conn, meta }, shard, self.clone()));
                    }
                    // It was popped but could not be reattached, so it is gone.
                    // Parking already decremented `live`; balance `created`
                    // here and fall through to dialling a replacement.
                    Unparked::Lost => Counters::inc(&self.inner.counters.closed),
                    Unparked::Empty => {}
                }
                let reserved = Reserved::empty(&shard, &self.inner.counters);
                let connected = self.inner.manager.connect().await;
                reserved.disarm();
                return match connected {
                    Ok(conn) => {
                        Counters::inc(&self.inner.counters.created);
                        Counters::inc(&self.inner.counters.live);
                        Counters::inc(&self.inner.counters.acquires);
                        Ok(Pooled::new(
                            Slot::new(conn, generation),
                            shard,
                            self.clone(),
                        ))
                    }
                    Err(e) => {
                        shard.release();
                        Err(Error::Backend(e))
                    }
                };
            }

            // 3. At capacity. Wait for one of *this thread's* checkouts to come
            //    back; they always will, so this cannot deadlock.
            Counters::inc(&self.inner.counters.waits);
            shard.wait().await;
        }
    }

    /// Return path for [`Pooled::drop`].
    pub(crate) fn release(
        &self,
        shard: &Rc<Shard<M>>,
        mut slot: Slot<M::Connection>,
        poisoned: bool,
    ) {
        slot.meta.uses += 1;
        slot.meta.last_used = Instant::now();

        if poisoned {
            Counters::inc(&self.inner.counters.poisoned);
            return self.destroy(shard, slot);
        }
        if self.inner.closed.load(Relaxed) {
            return self.destroy(shard, slot);
        }
        let generation = self.inner.generation.load(Relaxed);
        if slot.is_expired(&self.inner.config, generation) {
            return self.destroy(shard, slot);
        }

        // Surplus idle connections are offered to other threads rather than
        // held here until they age out. Only surplus: keep `min_idle` local,
        // and never give one away while someone on this thread is waiting.
        if !shard.has_waiters() && shard.idle_len() >= self.inner.config.effective_min_idle() {
            let Slot { conn, meta } = slot;
            match self.inner.exchange.park(conn, meta) {
                Parked::Accepted => {
                    // The reservoir took it; it is no longer this shard's.
                    Counters::dec(&self.inner.counters.live);
                    shard.release();
                    return;
                }
                // `detach` found an operation still in flight and dropped it
                // rather than hand a busy handle to another driver.
                Parked::Destroyed => {
                    Counters::dec(&self.inner.counters.live);
                    Counters::inc(&self.inner.counters.closed);
                    shard.release();
                    return;
                }
                Parked::Refused(conn, meta) => slot = Slot { conn, meta },
            }
        }

        shard.push_idle(slot);
    }

    /// Drops a connection out of the pool without closing it, for
    /// [`Pooled::take`].
    pub(crate) fn forget(&self, shard: &Rc<Shard<M>>) {
        Counters::dec(&self.inner.counters.live);
        shard.release();
    }

    fn destroy(&self, shard: &Rc<Shard<M>>, slot: Slot<M::Connection>) {
        self.destroy_conn(shard, slot.conn);
    }

    fn destroy_conn(&self, shard: &Rc<Shard<M>>, conn: M::Connection) {
        self.inner.manager.disconnect(conn);
        Counters::dec(&self.inner.counters.live);
        Counters::inc(&self.inner.counters.closed);
        shard.release();
    }

    /// This thread's shard, creating it (and its reaper) on first touch.
    fn shard(&self) -> Rc<Shard<M>> {
        let id = self.inner.id;

        let (shard, created) = SHARDS.with(|shards| {
            let mut shards = shards.borrow_mut();
            match shards.get(&id) {
                Some(any) => (
                    any.downcast_ref::<Rc<Shard<M>>>()
                        .expect("pool id uniquely determines shard type")
                        .clone(),
                    false,
                ),
                None => {
                    // Cloned here, not above: these are `Arc`s shared by every
                    // thread, and bumping their refcounts on the hit path would
                    // put two atomic RMWs on a contended cache line into every
                    // single checkout.
                    let shard = Rc::new(Shard::<M>::new(
                        self.inner.counters.clone(),
                        self.inner.manager.clone(),
                    ));
                    shards.insert(id, Box::new(shard.clone()));
                    (shard, true)
                }
            }
        });

        // Spawn outside the `with` closure: the thread-local is no longer
        // borrowed, so the reaper is free to reach for its own shard.
        if created && self.inner.config.needs_reaper() {
            self.spawn_reaper(&shard);
        }
        shard
    }

    fn spawn_reaper(&self, shard: &Rc<Shard<M>>) {
        // Outside a compio runtime there is nothing to spawn onto. The pool
        // still works; expired connections are then caught on acquire instead.
        if compio::runtime::Runtime::try_with_current(|_| ()).is_err() {
            return;
        }
        let pool = self.clone();
        let shard = Rc::downgrade(shard);
        compio::runtime::spawn(async move { pool.reap_loop(shard).await }).detach();
    }

    /// Enforces idle/lifetime limits and refills `min_idle` on one thread.
    ///
    /// Holds only a `Weak` to the shard, so it stops on its own once the
    /// thread's shard is gone.
    async fn reap_loop(self, shard: Weak<Shard<M>>) {
        let interval = self.inner.config.reap_interval;
        loop {
            compio::time::sleep(interval).await;

            let Some(shard) = shard.upgrade() else { return };
            if self.inner.closed.load(Relaxed) {
                for slot in shard.drain_all() {
                    self.destroy(&shard, slot);
                }
                return;
            }

            let generation = self.inner.generation.load(Relaxed);
            for slot in shard.drain_expired(&self.inner.config, generation) {
                self.destroy(&shard, slot);
            }

            let target = self.inner.config.effective_min_idle();
            while shard.idle_len() < target && !shard.has_waiters() {
                if !shard.try_reserve(self.inner.config.max_size) {
                    break;
                }
                match self.inner.manager.connect().await {
                    Ok(conn) => {
                        Counters::inc(&self.inner.counters.created);
                        Counters::inc(&self.inner.counters.live);
                        shard.push_idle(Slot::new(conn, generation));
                    }
                    Err(_) => {
                        shard.release();
                        break;
                    }
                }
            }
        }
    }
}

impl<M: Manage, X: Exchange<M>> std::fmt::Debug for Pool<M, X> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("id", &self.inner.id)
            .field("closed", &self.inner.closed.load(Relaxed))
            .field("metrics", &self.metrics())
            .finish_non_exhaustive()
    }
}

/// Fluent constructor for [`Pool`].
pub struct Builder<M: Manage, X: Exchange<M>> {
    manager: M,
    config: Config,
    exchange: X,
}

impl<M: Manage, X: Exchange<M>> Builder<M, X> {
    /// Replaces the whole configuration.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Maximum live connections per runtime thread.
    pub fn max_size(mut self, n: usize) -> Self {
        self.config = self.config.clone().max_size(n);
        self
    }

    /// Idle connections each shard keeps warm.
    pub fn min_idle(mut self, n: usize) -> Self {
        self.config = self.config.clone().min_idle(n);
        self
    }

    /// How long `acquire` waits before giving up.
    pub fn acquire_timeout(mut self, t: impl Into<Option<std::time::Duration>>) -> Self {
        self.config = self.config.clone().acquire_timeout(t);
        self
    }

    /// Hard age cap on a connection.
    pub fn max_lifetime(mut self, t: impl Into<Option<std::time::Duration>>) -> Self {
        self.config = self.config.clone().max_lifetime(t);
        self
    }

    /// How long a connection may sit idle before being reaped.
    pub fn idle_timeout(mut self, t: impl Into<Option<std::time::Duration>>) -> Self {
        self.config = self.config.clone().idle_timeout(t);
        self
    }

    /// Retire a connection after this many checkouts.
    pub fn max_uses(mut self, n: impl Into<Option<u64>>) -> Self {
        self.config = self.config.clone().max_uses(n);
        self
    }

    /// Installs a cross-thread [`Exchange`], changing the pool's type.
    pub fn exchange<Y: Exchange<M>>(self, exchange: Y) -> Builder<M, Y> {
        Builder {
            manager: self.manager,
            config: self.config,
            exchange,
        }
    }

    /// Builds the pool.
    pub fn build(self) -> Pool<M, X> {
        Pool::with_exchange(self.manager, self.config, self.exchange)
    }
}
