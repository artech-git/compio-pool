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
//! Each [`LocalPool`] owns one `Metrics` block and registers a clone of it with
//! the [`Pool`]. Only the owning thread ever writes its block, so the counters are
//! plain relaxed atomics with no cross-thread contention. [`Pool::state`] and
//! [`Pool::statistics`] lock the registry — a cold path — and sum every block to
//! give a process-wide view; [`LocalPool::state`] and [`LocalPool::statistics`]
//! read only this thread's block.

use std::{
    cell::{Cell, RefCell},
    fmt,
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    rc::Rc,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed},
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
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
    /// One metrics block per [`LocalPool`] ever created, for process-wide
    /// aggregation. Locked only when a pool is created or stats are read.
    metrics: Mutex<Vec<Arc<Metrics>>>,
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
                metrics: Mutex::new(Vec::new()),
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
    /// not — they live on each worker's own thread. A task already parked waiting
    /// for a slot therefore unblocks on its next **local** wake (another
    /// connection returning, or [`LocalPool::clear`]) or when its
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
        let registry = self
            .shared
            .metrics
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut state = State::default();
        for m in registry.iter() {
            state.connections += m.connections.load(Relaxed);
            state.idle_connections += m.idle.load(Relaxed);
            state.pending_waiters += m.waiters.load(Relaxed);
        }
        state
    }

    /// Process-wide cumulative counters summed across every thread's
    /// [`LocalPool`]. Like [`state`](Self::state) it takes the registry lock and is
    /// meant for a scrape, not the hot path. For one thread's totals use
    /// [`LocalPool::statistics`].
    pub fn statistics(&self) -> Statistics {
        let registry = self
            .shared
            .metrics
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut stats = Statistics::default();
        let mut micros = 0u64;
        for m in registry.iter() {
            stats.get_direct += m.get_direct.load(Relaxed);
            stats.get_waited += m.get_waited.load(Relaxed);
            stats.get_timed_out += m.get_timed_out.load(Relaxed);
            stats.connections_created += m.connections_created.load(Relaxed);
            stats.connections_closed_broken += m.connections_closed_broken.load(Relaxed);
            stats.connections_closed_idle_timeout +=
                m.connections_closed_idle_timeout.load(Relaxed);
            stats.connections_closed_max_lifetime +=
                m.connections_closed_max_lifetime.load(Relaxed);
            micros += m.wait_micros.load(Relaxed);
        }
        stats.total_wait_time = Duration::from_micros(micros);
        stats
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
}

/// Per-thread counters, written only by the owning thread (so plain relaxed
/// atomics suffice) and summed across threads by [`Pool::state`] /
/// [`Pool::statistics`]. The live gauges are zeroed when the owning thread's
/// [`Inner`] drops; the cumulative counters outlive it.
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
    /// This thread's counters, also held (cloned `Arc`) in the pool's registry.
    metrics: Arc<Metrics>,
    /// Woken when a slot frees or a connection returns to the idle list.
    waiters: Waiters,
}

impl<M: ManageConnection> Inner<M> {
    fn connections(&self) -> u32 {
        self.metrics.connections.load(Relaxed)
    }

    fn closed(&self) -> bool {
        self.shared.closed.load(Relaxed)
    }

    /// Give back one slot and wake anyone waiting for capacity.
    fn release_slot(&self) {
        self.metrics.dec_conn(1);
        self.waiters.wake_all();
    }

    fn push_idle(&self, conn: Conn<M::Connection>) {
        self.idle.borrow_mut().push(conn);
        self.metrics.inc_idle();
    }

    fn pop_idle(&self) -> Option<Conn<M::Connection>> {
        let conn = self.idle.borrow_mut().pop();
        if conn.is_some() {
            self.metrics.dec_idle(1);
        }
        conn
    }
}

impl<M: ManageConnection> Drop for Inner<M> {
    fn drop(&mut self) {
        // The connections this thread held drop with `idle`; zero the live gauges
        // so a finished worker no longer shows up in `Pool::state`. The cumulative
        // counters stay, so lifetime totals survive the thread.
        self.metrics.connections.store(0, Relaxed);
        self.metrics.idle.store(0, Relaxed);
        self.metrics.waiters.store(0, Relaxed);
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
        let metrics = Arc::new(Metrics::default());
        shared
            .metrics
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(metrics.clone());
        Self {
            inner: Rc::new(Inner {
                shared,
                idle: RefCell::new(Vec::with_capacity(cap)),
                waiters: Waiters::new(metrics.clone()),
                metrics,
            }),
        }
    }

    /// The per-thread ceiling on open connections.
    pub fn max_size(&self) -> u32 {
        self.inner.shared.config.max_size
    }

    /// A snapshot of **this thread's** open, idle and waiting counts. Lock-free.
    /// For the process-wide sum use [`Pool::state`].
    pub fn state(&self) -> State {
        self.inner.metrics.snapshot_state()
    }

    /// **This thread's** cumulative counters. Lock-free. For the process-wide sum
    /// use [`Pool::statistics`].
    pub fn statistics(&self) -> Statistics {
        self.inner.metrics.snapshot_statistics()
    }

    /// Open connections up to [`min_idle`](Builder::min_idle) and park them on the
    /// idle list. Returns how many were created. Tops up to the floor each time it
    /// is called, so it doubles as the warm-up and the min-idle maintenance step;
    /// [`maintain`](Self::maintain) pairs it with a reap. A no-op (returns `Ok(0)`)
    /// once the pool is [`closed`](Pool::close). Stops early and returns
    /// [`RunError::User`] if the manager fails to connect.
    pub async fn warm(&self) -> Result<u32, RunError<M::Error>> {
        if self.inner.closed() {
            return Ok(0);
        }
        let target = self.inner.shared.config.min_idle;
        let mut made = 0;
        while self.inner.connections() < target {
            let Some(mut held) = self.try_reserve() else {
                break;
            };
            match self.inner.shared.manager.connect().await {
                Ok(raw) => {
                    let now = Instant::now();
                    held.conn = Some(Conn {
                        raw,
                        created_at: now,
                        last_used: now,
                    });
                    self.inner.metrics.connections_created.fetch_add(1, Relaxed);
                    held.into_idle();
                    made += 1;
                }
                // `held` drops here, releasing the slot it reserved.
                Err(e) => return Err(RunError::User(e)),
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
        let metrics = &self.inner.metrics;
        let mut reaped = 0u32;
        {
            let mut idle = self.inner.idle.borrow_mut();
            idle.retain(|conn| match expiry(conn, now, cfg) {
                Some(reason) => {
                    metrics.note_expired(reason);
                    reaped += 1;
                    false
                }
                None => true,
            });
        }
        if reaped > 0 {
            metrics.dec_idle(reaped);
            metrics.dec_conn(reaped);
            self.inner.waiters.wake_all();
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
        if n > 0 {
            self.inner.metrics.dec_idle(n);
            self.inner.metrics.dec_conn(n);
            self.inner.waiters.wake_all();
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
                        self.inner
                            .metrics
                            .connections_closed_broken
                            .fetch_add(1, Relaxed);
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
                        self.inner.metrics.connections_created.fetch_add(1, Relaxed);
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

    /// Reserve a slot for a new connection, if under `max_size`.
    fn try_reserve(&self) -> Option<Held<M>> {
        let taken = self.inner.connections();
        if taken < self.inner.shared.config.max_size {
            self.inner.metrics.inc_conn();
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
                self.inner.metrics.note_expired(reason);
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
            || self.inner.metrics.idle.load(Relaxed) > 0
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
            self.inner.waiters.wake_all();
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
                self.inner
                    .metrics
                    .connections_closed_broken
                    .fetch_add(1, Relaxed);
                self.inner.release_slot();
            } else if let Some(reason) = expiry(&conn, Instant::now(), &self.inner.shared.config) {
                drop(conn);
                self.inner.metrics.note_expired(reason);
                self.inner.release_slot();
            } else {
                conn.last_used = Instant::now();
                self.inner.push_idle(conn);
                // A connection is available again; wake a waiter.
                self.inner.waiters.wake_all();
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

/// Wakers of futures waiting for the pool to be ready to hand out a connection.
/// Each waiter owns an id so it can withdraw its waker if it is dropped first;
/// nothing stale is ever woken. The registered count is mirrored into
/// [`Metrics::waiters`] so [`Pool::state`] can see it from another thread.
struct Waiters {
    metrics: Arc<Metrics>,
    next_id: Cell<u64>,
    entries: RefCell<Vec<(u64, Waker)>>,
}

impl Waiters {
    fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            metrics,
            next_id: Cell::new(0),
            entries: RefCell::new(Vec::new()),
        }
    }

    fn register(&self, id: &mut Option<u64>, waker: &Waker) {
        let mut entries = self.entries.borrow_mut();
        if let Some(existing) = *id
            && let Some(entry) = entries.iter_mut().find(|(i, _)| *i == existing)
        {
            if !entry.1.will_wake(waker) {
                entry.1 = waker.clone();
            }
            return;
        }
        let new_id = self.next_id.get();
        self.next_id.set(new_id.wrapping_add(1));
        entries.push((new_id, waker.clone()));
        self.metrics.waiters.store(entries.len() as u32, Relaxed);
        *id = Some(new_id);
    }

    fn unregister(&self, id: Option<u64>) {
        if let Some(id) = id {
            let mut entries = self.entries.borrow_mut();
            entries.retain(|(i, _)| *i != id);
            self.metrics.waiters.store(entries.len() as u32, Relaxed);
        }
    }

    /// Wake everyone. The borrow is dropped before any waker runs, so a waker
    /// that polls inline can register again without a double borrow.
    fn wake_all(&self) {
        let wakers: Vec<Waker> = self
            .entries
            .borrow_mut()
            .drain(..)
            .map(|(_, w)| w)
            .collect();
        self.metrics.waiters.store(0, Relaxed);
        for waker in wakers {
            waker.wake();
        }
    }
}

/// Resolves when the pool can hand out a connection — an idle one exists, there
/// is room to open one, or the pool has closed — keeping exactly one waker
/// registered until then. Its output is whether it ever parked, so `get` can tell
/// a direct checkout from one that waited.
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
            this.pool.inner.waiters.unregister(this.id.take());
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
        self.pool.inner.waiters.unregister(self.id.take());
    }
}
