//! The pool: a `Send` blueprint you build once and clone onto every thread, and
//! the strictly thread-local pool it stamps out on each.
//!
//! [`Pool`] holds nothing hot — only the manager, the configuration, a shutdown
//! flag and a registry of per-thread metrics blocks, behind one `Arc`. Cloning it
//! is an `Arc` bump. The connections, the idle list and the slot count all live in
//! the [`LocalPool`] that [`Pool::local`] creates on a thread: `Rc` and `Cell`, no
//! lock and no atom on the hot path, never `Send`. The type system then stops a
//! connection — bound to one ring — from ever leaving its thread.
//!
//! # Metrics across threads
//!
//! With the `metrics` feature enabled, each [`LocalPool`] owns one `Metrics` block
//! and registers a clone of it with the [`Pool`]. Only the owning thread ever
//! writes its block, so the counters are plain relaxed atomics with no
//! cross-thread contention. [`Pool::state`] and
//! [`Pool::statistics`] lock the registry — a cold path — and sum every block to
//! give a process-wide view; [`LocalPool::state`] and [`LocalPool::statistics`]
//! read only this thread's block.

use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    fmt,
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

// Registry lock and the atomic counter types are only touched by the metrics
// machinery, so they drop out of the build when the feature is off.
#[cfg(feature = "metrics")]
use std::sync::{
    Mutex, PoisonError, Weak,
    atomic::{AtomicU32, AtomicU64},
};

use crate::{
    builder::{Builder, PoolConfig},
    manage::{ManageConnection, RunError},
};

/// A blueprint for per-thread pools: build it once, clone it onto each compio
/// thread you spawn, and call [`local`](Self::local) there.
///
/// `Clone + Send + Sync`. Cloning shares nothing that is touched on the hot path
/// — only the manager, the [`Builder`] settings, the shutdown flag and the
/// metrics registry. Every connection, idle list and slot counter belongs to one
/// thread's [`LocalPool`].
pub struct Pool<M: ManageConnection> {
    shared: Arc<Shared<M>>,
}

pub(crate) struct Shared<M: ManageConnection> {
    manager: M,
    config: PoolConfig,
    /// Set by [`Pool::close`]; read on every `get` and connection return.
    closed: AtomicBool,
    /// One metrics block per **live** [`LocalPool`], for process-wide
    /// aggregation. Held weakly so a pool that has dropped falls out of the
    /// registry instead of pinning its block forever; dead entries are pruned
    /// whenever the lock is taken (a pool is created or stats are read).
    #[cfg(feature = "metrics")]
    metrics: Mutex<Vec<Weak<Metrics>>>,
    /// The cumulative counters of every [`LocalPool`] that has since dropped,
    /// folded in by its `Drop`, so lifetime totals survive the thread that made
    /// them. Only the cumulative counters are ever written; its gauges stay zero.
    #[cfg(feature = "metrics")]
    retired: Metrics,
}

impl<M: ManageConnection> Clone for Pool<M> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<M: ManageConnection> Pool<M> {
    pub(crate) fn new(manager: M, config: PoolConfig) -> Self {
        Self {
            shared: Arc::new(Shared {
                manager,
                config,
                closed: AtomicBool::new(false),
                #[cfg(feature = "metrics")]
                metrics: Mutex::new(Vec::new()),
                #[cfg(feature = "metrics")]
                retired: Metrics::default(),
            }),
        }
    }

    /// Start configuring a pool.
    pub fn builder() -> Builder<M> {
        Builder::new()
    }

    /// The per-thread ceiling on open connections.
    pub fn max_size(&self) -> u32 {
        self.shared.config.max_size
    }

    /// Make this thread's pool.
    ///
    /// Each call creates an **independent** [`LocalPool`] bound to the calling
    /// thread. Call it once per compio thread and keep the handle; it is `Clone`
    /// (a cheap `Rc` bump) if several tasks on that thread need it. Because the
    /// returned pool is `!Send`, the compiler guarantees it — and the ring-bound
    /// connections it holds — never move to another thread.
    pub fn local(&self) -> LocalPool<M> {
        LocalPool::new(self.shared.clone())
    }

    /// Shut the pool down for every thread.
    ///
    /// After this, each thread's [`LocalPool::get`] fails immediately with
    /// [`RunError::Closed`], a returned [`PooledConnection`] is dropped instead of
    /// recycled, and [`warm`](LocalPool::warm) / [`maintain`](LocalPool::maintain)
    /// become no-ops. Connections already checked out keep working until their
    /// holders drop them.
    ///
    /// The flag is shared, but the wakers that unpark a task waiting in `get` are
    /// not — they live on each worker's own thread. Tasks already parked waiting
    /// for a slot therefore unblock on that thread's next **local** wake (another
    /// connection returning, a [`reap`](LocalPool::reap), or
    /// [`LocalPool::clear`]) — the first one to notice the pool is closed passes
    /// the news down the whole queue — or when their
    /// [`connection_timeout`](Builder::connection_timeout) elapses, not the instant
    /// `close` is called from another thread. For a prompt drain, stop each
    /// worker's accept loop and call [`LocalPool::clear`] on its thread.
    pub fn close(&self) {
        self.shared.closed.store(true, Relaxed);
    }

    /// Whether [`close`](Self::close) has been called.
    pub fn is_closed(&self) -> bool {
        self.shared.closed.load(Relaxed)
    }

    /// A process-wide snapshot: the live connection, idle and waiter counts summed
    /// across every thread's [`LocalPool`]. Takes the registry lock, so call it off
    /// the hot path (a metrics scrape, a health endpoint). For one thread's numbers
    /// use [`LocalPool::state`].
    pub fn state(&self) -> State {
        #[cfg(feature = "metrics")]
        {
            let mut state = State::default();
            self.shared
                .for_each_live_block(|m| state.add(&m.snapshot_state()));
            state
        }
        #[cfg(not(feature = "metrics"))]
        State::default()
    }

    /// Process-wide cumulative counters summed across every thread's
    /// [`LocalPool`], including pools whose threads have since exited. Like
    /// [`state`](Self::state) it takes the registry lock and is meant for a
    /// scrape, not the hot path. For one thread's totals use
    /// [`LocalPool::statistics`].
    pub fn statistics(&self) -> Statistics {
        #[cfg(feature = "metrics")]
        {
            let mut stats = self.shared.retired.snapshot_statistics();
            self.shared
                .for_each_live_block(|m| stats.add(&m.snapshot_statistics()));
            stats
        }
        #[cfg(not(feature = "metrics"))]
        Statistics::default()
    }
}

#[cfg(feature = "metrics")]
impl<M: ManageConnection> Shared<M> {
    /// Visit every live thread's metrics block under the registry lock, pruning
    /// the entries whose [`LocalPool`] has dropped on the way.
    fn for_each_live_block(&self, mut f: impl FnMut(&Metrics)) {
        let mut registry = self.metrics.lock().unwrap_or_else(PoisonError::into_inner);
        registry.retain(|weak| match weak.upgrade() {
            Some(m) => {
                f(&m);
                true
            }
            None => false,
        });
    }
}

impl<M: ManageConnection> fmt::Debug for Pool<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("max_size", &self.shared.config.max_size)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// A snapshot of live connection counts. From [`LocalPool::state`] it is one
/// thread's; from [`Pool::state`] it is the sum across every thread.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct State {
    /// Open connections right now: idle plus checked out.
    pub connections: u32,
    /// How many of those are idle on the free list.
    pub idle_connections: u32,
    /// Tasks currently parked in [`get`](LocalPool::get) waiting for a connection.
    pub pending_waiters: u32,
}

/// Cumulative counters over a pool's life. From [`LocalPool::statistics`] it is
/// one thread's; from [`Pool::statistics`] it is the sum across every thread.
///
/// Counters only ever grow (connections opened and closed are counted
/// separately), so two snapshots subtracted give the activity in between — the
/// shape a metrics exporter wants.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Statistics {
    /// [`get`](LocalPool::get) calls served without waiting — an idle connection
    /// was ready, or there was room to open one straight away.
    pub get_direct: u64,
    /// [`get`](LocalPool::get) calls that had to park for a slot before being
    /// served.
    pub get_waited: u64,
    /// [`get`](LocalPool::get) calls that gave up with
    /// [`RunError::TimedOut`].
    pub get_timed_out: u64,
    /// Connections opened by the manager.
    pub connections_created: u64,
    /// Connections dropped because [`has_broken`](ManageConnection::has_broken)
    /// or [`is_valid`](ManageConnection::is_valid) rejected them.
    pub connections_closed_broken: u64,
    /// Connections dropped for passing [`idle_timeout`](Builder::idle_timeout).
    pub connections_closed_idle_timeout: u64,
    /// Connections dropped for passing [`max_lifetime`](Builder::max_lifetime).
    pub connections_closed_max_lifetime: u64,
    /// Total time [`get`](LocalPool::get) callers spent parked waiting, across the
    /// `get_waited` calls.
    pub total_wait_time: Duration,
}

impl Statistics {
    /// Mean time a waiting caller parked, or zero if none ever waited.
    pub fn average_wait_time(&self) -> Duration {
        self.total_wait_time
            .checked_div(self.get_waited as u32)
            .unwrap_or_default()
    }

    /// Fold another snapshot into this one (used to sum across threads).
    #[cfg(feature = "metrics")]
    fn add(&mut self, other: &Statistics) {
        self.get_direct += other.get_direct;
        self.get_waited += other.get_waited;
        self.get_timed_out += other.get_timed_out;
        self.connections_created += other.connections_created;
        self.connections_closed_broken += other.connections_closed_broken;
        self.connections_closed_idle_timeout += other.connections_closed_idle_timeout;
        self.connections_closed_max_lifetime += other.connections_closed_max_lifetime;
        self.total_wait_time += other.total_wait_time;
    }
}

#[cfg(feature = "metrics")]
impl State {
    /// Fold another snapshot into this one (used to sum across threads).
    fn add(&mut self, other: &State) {
        self.connections += other.connections;
        self.idle_connections += other.idle_connections;
        self.pending_waiters += other.pending_waiters;
    }
}

/// Per-thread counters, written only by the owning thread (so plain relaxed
/// atomics suffice) and summed across threads by [`Pool::state`] /
/// [`Pool::statistics`]. The live gauges are zeroed when the owning thread's
/// [`Inner`] drops; the cumulative counters outlive it.
#[cfg(feature = "metrics")]
#[derive(Default)]
struct Metrics {
    // Live gauges.
    connections: AtomicU32,
    idle: AtomicU32,
    waiters: AtomicU32,
    // Cumulative counters.
    get_direct: AtomicU64,
    get_waited: AtomicU64,
    get_timed_out: AtomicU64,
    connections_created: AtomicU64,
    connections_closed_broken: AtomicU64,
    connections_closed_idle_timeout: AtomicU64,
    connections_closed_max_lifetime: AtomicU64,
    wait_micros: AtomicU64,
}

#[cfg(feature = "metrics")]
impl Metrics {
    fn inc_conn(&self) {
        self.connections.fetch_add(1, Relaxed);
    }

    fn dec_conn(&self, n: u32) {
        let c = self.connections.load(Relaxed);
        self.connections.store(c.saturating_sub(n), Relaxed);
    }

    fn inc_idle(&self) {
        self.idle.fetch_add(1, Relaxed);
    }

    fn dec_idle(&self, n: u32) {
        let c = self.idle.load(Relaxed);
        self.idle.store(c.saturating_sub(n), Relaxed);
    }

    /// Count a connection retired for the given expiry reason.
    fn note_expired(&self, reason: Expiry) {
        match reason {
            Expiry::IdleTimeout => self.connections_closed_idle_timeout.fetch_add(1, Relaxed),
            Expiry::MaxLifetime => self.connections_closed_max_lifetime.fetch_add(1, Relaxed),
        };
    }

    fn snapshot_state(&self) -> State {
        State {
            connections: self.connections.load(Relaxed),
            idle_connections: self.idle.load(Relaxed),
            pending_waiters: self.waiters.load(Relaxed),
        }
    }

    fn snapshot_statistics(&self) -> Statistics {
        Statistics {
            get_direct: self.get_direct.load(Relaxed),
            get_waited: self.get_waited.load(Relaxed),
            get_timed_out: self.get_timed_out.load(Relaxed),
            connections_created: self.connections_created.load(Relaxed),
            connections_closed_broken: self.connections_closed_broken.load(Relaxed),
            connections_closed_idle_timeout: self.connections_closed_idle_timeout.load(Relaxed),
            connections_closed_max_lifetime: self.connections_closed_max_lifetime.load(Relaxed),
            total_wait_time: Duration::from_micros(self.wait_micros.load(Relaxed)),
        }
    }

    /// Move this block's cumulative counters into `into`, zeroing them here, so a
    /// dropping [`LocalPool`]'s lifetime totals are kept by the [`Pool`] without
    /// keeping the block. Each counter is swapped out then added, so a scrape
    /// racing this sees a value in one place or the other, never both.
    fn retire_into(&self, into: &Metrics) {
        for (from, to) in [
            (&self.get_direct, &into.get_direct),
            (&self.get_waited, &into.get_waited),
            (&self.get_timed_out, &into.get_timed_out),
            (&self.connections_created, &into.connections_created),
            (
                &self.connections_closed_broken,
                &into.connections_closed_broken,
            ),
            (
                &self.connections_closed_idle_timeout,
                &into.connections_closed_idle_timeout,
            ),
            (
                &self.connections_closed_max_lifetime,
                &into.connections_closed_max_lifetime,
            ),
            (&self.wait_micros, &into.wait_micros),
        ] {
            to.fetch_add(from.swap(0, Relaxed), Relaxed);
        }
    }
}

/// One connection plus the timestamps that decide when it is retired.
struct Conn<C> {
    raw: C,
    created_at: Instant,
    last_used: Instant,
}

/// Why a connection is being retired, so the close can be attributed to the right
/// counter.
#[derive(Clone, Copy)]
enum Expiry {
    IdleTimeout,
    MaxLifetime,
}

/// `max_lifetime` is checked first, so a connection that is both too old and too
/// idle is attributed to age.
fn expiry<C>(conn: &Conn<C>, now: Instant, cfg: &PoolConfig) -> Option<Expiry> {
    if let Some(max) = cfg.max_lifetime
        && now.duration_since(conn.created_at) >= max
    {
        return Some(Expiry::MaxLifetime);
    }
    if let Some(idle) = cfg.idle_timeout
        && now.duration_since(conn.last_used) >= idle
    {
        return Some(Expiry::IdleTimeout);
    }
    None
}

struct Inner<M: ManageConnection> {
    shared: Arc<Shared<M>>,
    idle: RefCell<Vec<Conn<M::Connection>>>,
    /// Open slots this thread holds: idle + checked out + reserved. The count the
    /// pool enforces against `max_size` and its source of truth, tracked whether or
    /// not the `metrics` feature is on.
    slots: Cell<u32>,
    #[cfg(feature = "metrics")]
    /// This thread's counters, also held (cloned `Arc`) in the pool's registry. The
    /// live gauges mirror `slots`/`idle`/`waiters` so [`Pool::state`] can read them
    /// from another thread; nothing here is read by pool logic.
    metrics: Arc<Metrics>,
    /// Woken when a slot frees or a connection returns to the idle list.
    waiters: Waiters,
}

impl<M: ManageConnection> Inner<M> {
    /// Open slots this thread holds (idle + checked out + reserved), the count
    /// enforced against `max_size`.
    fn connections(&self) -> u32 {
        self.slots.get()
    }

    /// Connections currently idle on the free list.
    fn idle_len(&self) -> u32 {
        self.idle.borrow().len() as u32
    }

    /// Reserve a slot. Mirrors the live gauge when metrics are on.
    fn take_slot(&self) {
        self.slots.set(self.slots.get() + 1);
        #[cfg(feature = "metrics")]
        self.metrics.inc_conn();
    }

    /// Release `n` slots. Mirrors the live gauge when metrics are on.
    fn release_slots(&self, n: u32) {
        self.slots.set(self.slots.get().saturating_sub(n));
        #[cfg(feature = "metrics")]
        self.metrics.dec_conn(n);
    }

    fn closed(&self) -> bool {
        self.shared.closed.load(Relaxed)
    }

    /// Give back one slot and hand it to the next waiter.
    fn release_slot(&self) {
        self.release_slots(1);
        self.notify(1);
    }

    /// `n` units of capacity (slots or idle connections) just became available:
    /// wake that many waiters, oldest first. Each wake carries exactly one unit,
    /// so a returned connection costs one task poll, not one per parked task. A
    /// closing pool has nothing left to ration, so everyone is woken to fail fast.
    fn notify(&self, n: u32) {
        if self.closed() {
            self.waiters.wake_all();
        } else {
            self.waiters.wake(n);
        }
    }

    fn push_idle(&self, conn: Conn<M::Connection>) {
        self.idle.borrow_mut().push(conn);
        #[cfg(feature = "metrics")]
        self.metrics.inc_idle();
    }

    fn pop_idle(&self) -> Option<Conn<M::Connection>> {
        let conn = self.idle.borrow_mut().pop();
        #[cfg(feature = "metrics")]
        if conn.is_some() {
            self.metrics.dec_idle(1);
        }
        conn
    }

    /// Count a connection retired for `reason`. Metrics-only; a no-op otherwise.
    #[cfg(feature = "metrics")]
    fn note_expired(&self, reason: Expiry) {
        self.metrics.note_expired(reason);
    }
    #[cfg(not(feature = "metrics"))]
    fn note_expired(&self, _reason: Expiry) {}

    /// Count a connection the manager opened. Metrics-only; a no-op otherwise.
    #[cfg(feature = "metrics")]
    fn note_created(&self) {
        self.metrics.connections_created.fetch_add(1, Relaxed);
    }
    #[cfg(not(feature = "metrics"))]
    fn note_created(&self) {}

    /// Count a connection dropped as broken. Metrics-only; a no-op otherwise.
    #[cfg(feature = "metrics")]
    fn note_broken(&self) {
        self.metrics.connections_closed_broken.fetch_add(1, Relaxed);
    }
    #[cfg(not(feature = "metrics"))]
    fn note_broken(&self) {}
}

impl<M: ManageConnection> Drop for Inner<M> {
    fn drop(&mut self) {
        // The connections this thread held drop with `idle`; zero the live gauges
        // so a finished worker no longer shows up in `Pool::state`, and move the
        // cumulative counters into the pool's retired block so lifetime totals
        // survive the thread. The registry's weak entry dies with this block and
        // is pruned on the next scrape.
        #[cfg(feature = "metrics")]
        {
            self.metrics.connections.store(0, Relaxed);
            self.metrics.idle.store(0, Relaxed);
            self.metrics.waiters.store(0, Relaxed);
            self.metrics.retire_into(&self.shared.retired);
        }
    }
}

/// One thread's pool, created by [`Pool::local`].
///
/// Holds the idle connections and the slot count for this thread alone. `Clone`
/// shares the same per-thread pool between tasks on the thread; it is never
/// `Send`.
pub struct LocalPool<M: ManageConnection> {
    inner: Rc<Inner<M>>,
}

impl<M: ManageConnection> Clone for LocalPool<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<M: ManageConnection> LocalPool<M> {
    pub(crate) fn new(shared: Arc<Shared<M>>) -> Self {
        let cap = shared.config.max_size.min(1024) as usize;
        #[cfg(feature = "metrics")]
        let metrics = {
            let metrics = Arc::new(Metrics::default());
            let mut registry = shared
                .metrics
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            // Prune here too, so a caller that keeps making short-lived pools
            // cannot grow the registry even if nothing ever scrapes it.
            registry.retain(|weak| weak.strong_count() > 0);
            registry.push(Arc::downgrade(&metrics));
            metrics
        };
        Self {
            inner: Rc::new(Inner {
                shared,
                idle: RefCell::new(Vec::with_capacity(cap)),
                slots: Cell::new(0),
                waiters: Waiters::new(
                    #[cfg(feature = "metrics")]
                    metrics.clone(),
                ),
                #[cfg(feature = "metrics")]
                metrics,
            }),
        }
    }

    /// The per-thread ceiling on open connections.
    pub fn max_size(&self) -> u32 {
        self.inner.shared.config.max_size
    }

    /// A snapshot of **this thread's** open, idle and waiting counts. Lock-free.
    /// For the process-wide sum use [`Pool::state`]. Without the `metrics` feature
    /// it reports [`State::default`] (zeroes).
    pub fn state(&self) -> State {
        #[cfg(feature = "metrics")]
        {
            self.inner.metrics.snapshot_state()
        }
        #[cfg(not(feature = "metrics"))]
        State::default()
    }

    /// **This thread's** cumulative counters. Lock-free. For the process-wide sum
    /// use [`Pool::statistics`]. Without the `metrics` feature it reports
    /// [`Statistics::default`] (zeroes).
    pub fn statistics(&self) -> Statistics {
        #[cfg(feature = "metrics")]
        {
            self.inner.metrics.snapshot_statistics()
        }
        #[cfg(not(feature = "metrics"))]
        Statistics::default()
    }

    /// Open connections until [`min_idle`](Builder::min_idle) of them are idle on
    /// the free list, and park the new ones there. Returns how many were created.
    ///
    /// It counts the *idle* connections, not the total, so it refills the floor
    /// after a burst of checkouts has drained it — with three of five connections
    /// leased and `min_idle(3)`, it opens two more (room permitting) rather than
    /// seeing three slots already taken and opening none. Creation stops at
    /// [`max_size`](Builder::max_size), so a thread already full of leased
    /// connections may finish below the floor until some return.
    ///
    /// Tops up each time it is called, so it doubles as the warm-up and the
    /// min-idle maintenance step; [`maintain`](Self::maintain) pairs it with a
    /// reap. A no-op (returns `Ok(0)`) once the pool is [`closed`](Pool::close).
    /// Stops early and returns [`RunError::User`] if the manager fails to connect.
    ///
    /// Each dial is bounded by [`connection_timeout`](Builder::connection_timeout),
    /// so a backend that has stopped answering cannot wedge the maintenance loop:
    /// a dial that overruns returns [`RunError::TimedOut`] and frees the slot it
    /// reserved, leaving any connections already opened on the idle list.
    pub async fn warm(&self) -> Result<u32, RunError<M::Error>> {
        if self.inner.closed() {
            return Ok(0);
        }
        let target = self.inner.shared.config.min_idle;
        let timeout = self.inner.shared.config.connection_timeout;
        let mut made = 0;
        while self.inner.idle_len() < target {
            let Some(mut held) = self.try_reserve() else {
                break;
            };
            match compio::time::timeout(timeout, self.inner.shared.manager.connect()).await {
                Ok(Ok(raw)) => {
                    let now = Instant::now();
                    held.conn = Some(Conn {
                        raw,
                        created_at: now,
                        last_used: now,
                    });
                    self.inner.note_created();
                    held.into_idle();
                    made += 1;
                }
                // `held` drops in either arm, releasing the slot it reserved.
                Ok(Err(e)) => return Err(RunError::User(e)),
                Err(_elapsed) => return Err(RunError::TimedOut),
            }
        }
        Ok(made)
    }

    /// Drop idle connections that have passed [`idle_timeout`](Builder::idle_timeout)
    /// or [`max_lifetime`](Builder::max_lifetime), freeing their slots. Returns how
    /// many were reaped.
    ///
    /// Expiry is otherwise lazy — a connection is only checked as it is handed out
    /// or returned — so a pool that goes quiet would hold dead backend connections
    /// open indefinitely. Call this on a timer (see [`maintain`](Self::maintain))
    /// to retire them promptly. Only idle connections are considered; one that is
    /// checked out is left to its holder.
    pub fn reap(&self) -> u32 {
        let now = Instant::now();
        let cfg = &self.inner.shared.config;
        let mut reaped = 0u32;
        {
            let mut idle = self.inner.idle.borrow_mut();
            idle.retain(|conn| match expiry(conn, now, cfg) {
                Some(reason) => {
                    self.inner.note_expired(reason);
                    reaped += 1;
                    false
                }
                None => true,
            });
        }
        if reaped > 0 {
            #[cfg(feature = "metrics")]
            self.inner.metrics.dec_idle(reaped);
            self.inner.release_slots(reaped);
            self.inner.notify(reaped);
        }
        reaped
    }

    /// The periodic maintenance step: [`reap`](Self::reap) expired idle
    /// connections, then [`warm`](Self::warm) back up to
    /// [`min_idle`](Builder::min_idle). Returns how many fresh connections were
    /// opened. Call it from a timer on each worker thread — thread-per-core has no
    /// shared reaper thread, so maintenance is yours to drive:
    ///
    /// ```no_run
    /// # async fn run<M: compio_pool::ManageConnection>(
    /// #     local: compio_pool::LocalPool<M>,
    /// # ) {
    /// use std::time::Duration;
    /// compio::runtime::spawn(async move {
    ///     loop {
    ///         compio::time::sleep(Duration::from_secs(30)).await;
    ///         let _ = local.maintain().await;
    ///     }
    /// })
    /// .detach();
    /// # }
    /// ```
    pub async fn maintain(&self) -> Result<u32, RunError<M::Error>> {
        self.reap();
        self.warm().await
    }

    /// Drop every idle connection now, freeing their slots, and return how many
    /// were dropped. Connections currently checked out are untouched and return as
    /// usual. Use it to drain after a backend failover, or as part of a shutdown
    /// once [`Pool::close`] has been called. Unlike [`reap`](Self::reap) it ignores
    /// the timeouts and clears unconditionally.
    pub fn clear(&self) -> u32 {
        let n = {
            let mut idle = self.inner.idle.borrow_mut();
            let n = idle.len() as u32;
            idle.clear();
            n
        };
        // Same bookkeeping as `reap`: the dropped connections give their slots
        // back (the count `max_size` is enforced against, metrics or not) and the
        // freed capacity is handed to waiting tasks.
        if n > 0 {
            #[cfg(feature = "metrics")]
            self.inner.metrics.dec_idle(n);
            self.inner.release_slots(n);
            self.inner.notify(n);
        }
        n
    }

    /// Check a connection out of this thread's pool, waiting up to
    /// [`connection_timeout`](Builder::connection_timeout) for one.
    ///
    /// Reuses an idle connection when there is one (validated first if
    /// [`test_on_check_out`](Builder::test_on_check_out) is set), otherwise opens
    /// one while under `max_size`, otherwise waits for a slot. The returned
    /// [`PooledConnection`] derefs to the connection and returns it to this pool
    /// when dropped. Fails with [`RunError::Closed`] if the pool has been
    /// [`closed`](Pool::close).
    pub async fn get(&self) -> Result<PooledConnection<M>, RunError<M::Error>> {
        if self.inner.closed() {
            return Err(RunError::Closed);
        }
        let timeout = self.inner.shared.config.connection_timeout;
        match compio::time::timeout(timeout, self.get_inner()).await {
            Ok(result) => result,
            Err(_elapsed) => {
                #[cfg(feature = "metrics")]
                self.inner.metrics.get_timed_out.fetch_add(1, Relaxed);
                Err(RunError::TimedOut)
            }
        }
    }

    async fn get_inner(&self) -> Result<PooledConnection<M>, RunError<M::Error>> {
        let start = Instant::now();
        let mut waited = false;
        loop {
            if self.inner.closed() {
                return Err(RunError::Closed);
            }
            // Wait until there is an idle connection or room to open one. The
            // check-and-take below is synchronous, so what we observed still
            // holds when we act on it. `wait_ready` reports whether it parked.
            waited |= self.wait_ready().await;
            if self.inner.closed() {
                return Err(RunError::Closed);
            }

            // Prefer a live idle connection.
            if let Some(mut held) = self.pop_fresh_idle() {
                if self.inner.shared.config.test_on_check_out {
                    // Scope the borrow of `held` so it ends before the `drop`.
                    let valid = {
                        let conn = held.conn.as_mut().expect("idle held carries a connection");
                        self.inner
                            .shared
                            .manager
                            .is_valid(&mut conn.raw)
                            .await
                            .is_ok()
                    };
                    if !valid {
                        // Stale: count it closed, drop it (which frees the slot),
                        // and try again rather than failing.
                        self.inner.note_broken();
                        drop(held);
                        continue;
                    }
                }
                self.record_acquire(waited, start);
                return Ok(held.into_pooled());
            }

            // Nothing idle; open one if there is room.
            if let Some(mut held) = self.try_reserve() {
                match self.inner.shared.manager.connect().await {
                    Ok(raw) => {
                        let now = Instant::now();
                        held.conn = Some(Conn {
                            raw,
                            created_at: now,
                            last_used: now,
                        });
                        self.inner.note_created();
                        self.record_acquire(waited, start);
                        return Ok(held.into_pooled());
                    }
                    // `held` drops, releasing the reserved slot.
                    Err(e) => return Err(RunError::User(e)),
                }
            }
            // Another task took the slot between the wait and here: loop.
        }
    }

    /// Record a successful checkout: direct, or waited-with-elapsed.
    fn record_acquire(&self, waited: bool, start: Instant) {
        #[cfg(feature = "metrics")]
        {
            if waited {
                self.inner.metrics.get_waited.fetch_add(1, Relaxed);
                self.inner
                    .metrics
                    .wait_micros
                    .fetch_add(start.elapsed().as_micros() as u64, Relaxed);
            } else {
                self.inner.metrics.get_direct.fetch_add(1, Relaxed);
            }
        }
        #[cfg(not(feature = "metrics"))]
        let _ = (waited, start);
    }

    /// Reserve a slot for a new connection, if under `max_size`.
    fn try_reserve(&self) -> Option<Held<M>> {
        let taken = self.inner.connections();
        if taken < self.inner.shared.config.max_size {
            self.inner.take_slot();
            Some(Held {
                inner: self.inner.clone(),
                conn: None,
                released: false,
            })
        } else {
            None
        }
    }

    /// Pop the newest idle connection, discarding any that have expired (each
    /// discard frees its slot). `None` when the idle list is empty.
    fn pop_fresh_idle(&self) -> Option<Held<M>> {
        let now = Instant::now();
        loop {
            let conn = self.inner.pop_idle()?;
            if let Some(reason) = expiry(&conn, now, &self.inner.shared.config) {
                drop(conn);
                self.inner.note_expired(reason);
                self.inner.release_slot();
                continue;
            }
            return Some(Held {
                inner: self.inner.clone(),
                conn: Some(conn),
                released: false,
            });
        }
    }

    fn ready_now(&self) -> bool {
        self.inner.closed()
            || !self.inner.idle.borrow().is_empty()
            || self.inner.connections() < self.inner.shared.config.max_size
    }

    fn wait_ready(&self) -> WaitReady<'_, M> {
        WaitReady {
            pool: self,
            id: None,
            parked: false,
        }
    }
}

impl<M: ManageConnection> fmt::Debug for LocalPool<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state();
        f.debug_struct("LocalPool")
            .field("max_size", &self.inner.shared.config.max_size)
            .field("connections", &state.connections)
            .field("idle", &state.idle_connections)
            .field("pending_waiters", &state.pending_waiters)
            .finish()
    }
}

/// A connection, or a reserved-but-not-yet-filled slot, that is counted in
/// `connections` but not yet owned by a [`PooledConnection`]. Dropping it before
/// it is committed releases the slot (and drops the connection, if any), which
/// makes every early return and cancellation in `get`/`warm` leak-free.
struct Held<M: ManageConnection> {
    inner: Rc<Inner<M>>,
    conn: Option<Conn<M::Connection>>,
    released: bool,
}

impl<M: ManageConnection> Held<M> {
    /// Hand the connection to a [`PooledConnection`]; the slot stays taken.
    fn into_pooled(mut self) -> PooledConnection<M> {
        let conn = self.conn.take();
        self.released = true;
        PooledConnection {
            inner: self.inner.clone(),
            conn,
        }
    }

    /// Park the connection on the idle list; the slot stays taken.
    fn into_idle(mut self) {
        if let Some(mut conn) = self.conn.take() {
            conn.last_used = Instant::now();
            self.inner.push_idle(conn);
            self.inner.notify(1);
        }
        self.released = true;
    }
}

impl<M: ManageConnection> Drop for Held<M> {
    fn drop(&mut self) {
        if !self.released {
            self.conn.take();
            self.inner.release_slot();
        }
    }
}

/// A connection checked out of its [`LocalPool`]. Derefs to the connection;
/// dropping it runs [`has_broken`](ManageConnection::has_broken) and either
/// returns the connection to this thread's idle list or drops it and frees the
/// slot. If the pool has been [`closed`](Pool::close), the connection is always
/// dropped.
pub struct PooledConnection<M: ManageConnection> {
    inner: Rc<Inner<M>>,
    conn: Option<Conn<M::Connection>>,
}

impl<M: ManageConnection> Deref for PooledConnection<M> {
    type Target = M::Connection;

    fn deref(&self) -> &M::Connection {
        &self
            .conn
            .as_ref()
            .expect("connection present until drop")
            .raw
    }
}

impl<M: ManageConnection> DerefMut for PooledConnection<M> {
    fn deref_mut(&mut self) -> &mut M::Connection {
        &mut self
            .conn
            .as_mut()
            .expect("connection present until drop")
            .raw
    }
}

impl<M: ManageConnection> Drop for PooledConnection<M> {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            // A closing pool keeps nothing; drop and free the slot.
            if self.inner.closed() {
                drop(conn);
                self.inner.release_slot();
                return;
            }
            if self.inner.shared.manager.has_broken(&mut conn.raw) {
                drop(conn);
                self.inner.note_broken();
                self.inner.release_slot();
            } else if let Some(reason) = expiry(&conn, Instant::now(), &self.inner.shared.config) {
                drop(conn);
                self.inner.note_expired(reason);
                self.inner.release_slot();
            } else {
                conn.last_used = Instant::now();
                self.inner.push_idle(conn);
                // One connection is available again; wake one waiter for it.
                self.inner.notify(1);
            }
        }
    }
}

impl<M: ManageConnection> fmt::Debug for PooledConnection<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledConnection")
            .field("held", &self.conn.is_some())
            .finish()
    }
}

/// The FIFO of tasks parked in `get` because the thread is at `max_size`.
///
/// Capacity is rationed one wake per unit: a returned connection or a freed slot
/// wakes the **oldest** waiter only ([`wake`](Self::wake)), so a return costs one
/// task poll however long the queue is, and waiters are served in arrival order.
/// Waking everyone ([`wake_all`](Self::wake_all)) is reserved for a closed pool,
/// where there is nothing left to ration.
///
/// Each waiter owns an id so it can withdraw its waker if it is dropped first;
/// nothing stale is ever woken. Ids are handed out in increasing order and
/// entries are only ever pushed at the back, so the queue is sorted by id and a
/// waiter is found by binary search. Ids are popped from the front, in increasing
/// order too, so `woken_upto` — the highest id ever woken — tells a waiter
/// whether it has already been taken off the queue without searching it. The
/// queued count is mirrored into `Metrics::waiters` so [`Pool::state`] can see it
/// from another thread.
struct Waiters {
    #[cfg(feature = "metrics")]
    metrics: Arc<Metrics>,
    /// The next id to hand out; starts at 1 so `woken_upto == 0` means "none yet".
    next_id: Cell<u64>,
    /// The highest id ever woken. An id at or below it is no longer queued.
    woken_upto: Cell<u64>,
    /// Parked waiters, oldest at the front, sorted by id.
    entries: RefCell<VecDeque<(u64, Waker)>>,
}

impl Waiters {
    fn new(#[cfg(feature = "metrics")] metrics: Arc<Metrics>) -> Self {
        Self {
            #[cfg(feature = "metrics")]
            metrics,
            next_id: Cell::new(1),
            woken_upto: Cell::new(0),
            entries: RefCell::new(VecDeque::new()),
        }
    }

    /// Mirror the queue length into the cross-thread gauge.
    fn mirror(&self, len: usize) {
        #[cfg(feature = "metrics")]
        self.metrics.waiters.store(len as u32, Relaxed);
        #[cfg(not(feature = "metrics"))]
        let _ = len;
    }

    /// Queue `id`'s waker, or refresh it if `id` is still queued. A waiter that
    /// was woken but found nothing to take (a direct `get` call got there first)
    /// rejoins at the back under a fresh id.
    fn register(&self, id: &mut Option<u64>, waker: &Waker) {
        let mut entries = self.entries.borrow_mut();
        if let Some(existing) = *id
            && existing > self.woken_upto.get()
            && let Ok(pos) = entries.binary_search_by_key(&existing, |(i, _)| *i)
        {
            let entry = &mut entries[pos];
            if !entry.1.will_wake(waker) {
                entry.1 = waker.clone();
            }
            return;
        }
        let new_id = self.next_id.get();
        self.next_id.set(new_id + 1);
        entries.push_back((new_id, waker.clone()));
        *id = Some(new_id);
        self.mirror(entries.len());
    }

    /// Take `id` off the queue. Returns `false` if it was not there — meaning it
    /// had already been woken, so the caller holds a wake it has not used.
    fn unregister(&self, id: u64) -> bool {
        if id <= self.woken_upto.get() {
            return false;
        }
        let mut entries = self.entries.borrow_mut();
        match entries.binary_search_by_key(&id, |(i, _)| *i) {
            Ok(pos) => {
                entries.remove(pos);
                self.mirror(entries.len());
                true
            }
            Err(_) => false,
        }
    }

    /// Wake up to `n` waiters, oldest first — one per unit of capacity that just
    /// became available. Each waker runs with the queue unborrowed, so a waker
    /// that polls inline can register again without a double borrow.
    fn wake(&self, n: u32) {
        for _ in 0..n {
            let next = {
                let mut entries = self.entries.borrow_mut();
                let next = entries.pop_front();
                if next.is_some() {
                    self.mirror(entries.len());
                }
                next
            };
            let Some((id, waker)) = next else {
                break;
            };
            self.woken_upto.set(id);
            waker.wake();
        }
    }

    /// Wake everyone — for a closed pool, where every waiter will fail fast.
    fn wake_all(&self) {
        let queued = self.entries.borrow().len();
        self.wake(u32::try_from(queued).unwrap_or(u32::MAX));
    }
}

/// Resolves when the pool can hand out a connection — an idle one exists, there
/// is room to open one, or the pool has closed — keeping exactly one waker
/// registered until then. Its output is whether it ever parked, so `get` can tell
/// a direct checkout from one that waited.
///
/// A wake is a unit of capacity handed to this waiter. If the future is dropped
/// after being woken but before it could act — its `connection_timeout` won the
/// race — the wake is passed to the next waiter rather than lost.
struct WaitReady<'a, M: ManageConnection> {
    pool: &'a LocalPool<M>,
    id: Option<u64>,
    parked: bool,
}

impl<M: ManageConnection> Future for WaitReady<'_, M> {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<bool> {
        let this = self.get_mut();
        if this.pool.ready_now() {
            if let Some(id) = this.id.take() {
                this.pool.inner.waiters.unregister(id);
            }
            // A closed pool is "ready" for everyone: pass the news down the queue
            // so the others fail fast too instead of sitting out their timeouts.
            if this.pool.inner.closed() {
                this.pool.inner.waiters.wake_all();
            }
            Poll::Ready(this.parked)
        } else {
            this.parked = true;
            this.pool.inner.waiters.register(&mut this.id, cx.waker());
            Poll::Pending
        }
    }
}

impl<M: ManageConnection> Drop for WaitReady<'_, M> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take()
            && !self.pool.inner.waiters.unregister(id)
        {
            // Woken, but gone before acting on it: hand the capacity on.
            self.pool.inner.waiters.wake(1);
        }
    }
}
