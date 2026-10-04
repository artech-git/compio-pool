//! The pool: a `Send` blueprint you build once and clone onto every thread, and
//! the strictly thread-local pool it stamps out on each.
//!
//! [`Pool`] holds nothing hot — only the manager and the configuration, behind
//! one `Arc`. Cloning it is an `Arc` bump. The connections, the idle list and the
//! slot count all live in the [`LocalPool`] that [`Pool::local`] creates on a
//! thread: `Rc` and `Cell`, no lock and no atom, never `Send`. The type system
//! then stops a connection — bound to one ring — from ever leaving its thread.

use std::{
    cell::{Cell, RefCell},
    fmt,
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Instant,
};

use crate::{
    builder::{Builder, PoolConfig},
    manage::{ManageConnection, RunError},
};

/// A blueprint for per-thread pools: build it once, clone it onto each compio
/// thread you spawn, and call [`local`](Self::local) there.
///
/// `Clone + Send + Sync`. Cloning shares nothing that is touched on the hot path
/// — only the manager and the [`Builder`] settings. Every connection, idle list
/// and counter belongs to one thread's [`LocalPool`].
pub struct Pool<M: ManageConnection> {
    shared: Arc<Shared<M>>,
}

pub(crate) struct Shared<M: ManageConnection> {
    manager: M,
    config: PoolConfig,
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
            shared: Arc::new(Shared { manager, config }),
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
}

impl<M: ManageConnection> fmt::Debug for Pool<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pool")
            .field("max_size", &self.shared.config.max_size)
            .finish_non_exhaustive()
    }
}

/// A snapshot of one thread's pool. See [`LocalPool::state`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct State {
    /// Open connections right now: idle plus checked out.
    pub connections: u32,
    /// How many of those are idle on the free list.
    pub idle_connections: u32,
}

/// One connection plus the timestamps that decide when it is retired.
struct Conn<C> {
    raw: C,
    created_at: Instant,
    last_used: Instant,
}

fn is_expired<C>(conn: &Conn<C>, now: Instant, cfg: &PoolConfig) -> bool {
    if let Some(max) = cfg.max_lifetime
        && now.duration_since(conn.created_at) >= max
    {
        return true;
    }
    if let Some(idle) = cfg.idle_timeout
        && now.duration_since(conn.last_used) >= idle
    {
        return true;
    }
    false
}

struct Inner<M: ManageConnection> {
    shared: Arc<Shared<M>>,
    idle: RefCell<Vec<Conn<M::Connection>>>,
    /// Open connections, idle or checked out. Never exceeds `max_size`.
    taken: Cell<u32>,
    /// Woken when a slot frees or a connection returns to the idle list.
    waiters: Waiters,
}

impl<M: ManageConnection> Inner<M> {
    /// Give back one slot and wake anyone waiting for capacity.
    fn release_slot(&self) {
        self.taken.set(self.taken.get().saturating_sub(1));
        self.waiters.wake_all();
    }
}

/// One thread's pool, created by [`Pool::local`].
///
/// Holds the idle connections and the slot count for this thread alone — all
/// `Rc`/`Cell`, no lock, no atom. `Clone` shares the same per-thread pool between
/// tasks on the thread; it is never `Send`.
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
        Self {
            inner: Rc::new(Inner {
                shared,
                idle: RefCell::new(Vec::with_capacity(cap)),
                taken: Cell::new(0),
                waiters: Waiters::default(),
            }),
        }
    }

    /// The per-thread ceiling on open connections.
    pub fn max_size(&self) -> u32 {
        self.inner.shared.config.max_size
    }

    /// A snapshot of this thread's open and idle connection counts.
    pub fn state(&self) -> State {
        State {
            connections: self.inner.taken.get(),
            idle_connections: self.inner.idle.borrow().len() as u32,
        }
    }

    /// Open connections up to [`min_idle`](Builder::min_idle) ahead of demand and
    /// park them on the idle list. Returns how many were created. Stops early and
    /// returns [`RunError::User`] if the manager fails to connect.
    pub async fn warm(&self) -> Result<u32, RunError<M::Error>> {
        let target = self.inner.shared.config.min_idle;
        let mut made = 0;
        while self.inner.taken.get() < target {
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
                    held.into_idle();
                    made += 1;
                }
                // `held` drops here, releasing the slot it reserved.
                Err(e) => return Err(RunError::User(e)),
            }
        }
        Ok(made)
    }

    /// Check a connection out of this thread's pool, waiting up to
    /// [`connection_timeout`](Builder::connection_timeout) for one.
    ///
    /// Reuses an idle connection when there is one (validated first if
    /// [`test_on_check_out`](Builder::test_on_check_out) is set), otherwise opens
    /// one while under `max_size`, otherwise waits for a slot. The returned
    /// [`PooledConnection`] derefs to the connection and returns it to this pool
    /// when dropped.
    pub async fn get(&self) -> Result<PooledConnection<M>, RunError<M::Error>> {
        let timeout = self.inner.shared.config.connection_timeout;
        match compio::time::timeout(timeout, self.get_inner()).await {
            Ok(result) => result,
            Err(_elapsed) => Err(RunError::TimedOut),
        }
    }

    async fn get_inner(&self) -> Result<PooledConnection<M>, RunError<M::Error>> {
        loop {
            // Wait until there is an idle connection or room to open one. The
            // check-and-take below is synchronous, so what we observed still
            // holds when we act on it.
            self.wait_ready().await;

            // Prefer a live idle connection.
            if let Some(mut held) = self.pop_fresh_idle() {
                if self.inner.shared.config.test_on_check_out {
                    // Scope the borrow of `held` so it ends before the `drop`.
                    let valid = {
                        let conn = held.conn.as_mut().expect("idle held carries a connection");
                        self.inner.shared.manager.is_valid(&mut conn.raw).await.is_ok()
                    };
                    if !valid {
                        // Stale: dropping `held` releases the slot and the
                        // connection; try again rather than failing.
                        drop(held);
                        continue;
                    }
                }
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
                        return Ok(held.into_pooled());
                    }
                    // `held` drops, releasing the reserved slot.
                    Err(e) => return Err(RunError::User(e)),
                }
            }
            // Another task took the slot between the wait and here: loop.
        }
    }

    /// Reserve a slot for a new connection, if under `max_size`.
    fn try_reserve(&self) -> Option<Held<M>> {
        let taken = self.inner.taken.get();
        if taken < self.inner.shared.config.max_size {
            self.inner.taken.set(taken + 1);
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
            let conn = self.inner.idle.borrow_mut().pop()?;
            if is_expired(&conn, now, &self.inner.shared.config) {
                drop(conn);
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
        !self.inner.idle.borrow().is_empty()
            || self.inner.taken.get() < self.inner.shared.config.max_size
    }

    fn wait_ready(&self) -> WaitReady<'_, M> {
        WaitReady {
            pool: self,
            id: None,
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
            .finish()
    }
}

/// A connection, or a reserved-but-not-yet-filled slot, that is counted in
/// `taken` but not yet owned by a [`PooledConnection`]. Dropping it before it is
/// committed releases the slot (and drops the connection, if any), which makes
/// every early return and cancellation in `get`/`warm` leak-free.
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
            self.inner.idle.borrow_mut().push(conn);
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
/// slot.
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
            let broken = self.inner.shared.manager.has_broken(&mut conn.raw);
            if broken || is_expired(&conn, Instant::now(), &self.inner.shared.config) {
                drop(conn);
                self.inner.release_slot();
            } else {
                conn.last_used = Instant::now();
                self.inner.idle.borrow_mut().push(conn);
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
/// nothing stale is ever woken.
#[derive(Default)]
struct Waiters {
    next_id: Cell<u64>,
    entries: RefCell<Vec<(u64, Waker)>>,
}

impl Waiters {
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
        *id = Some(new_id);
    }

    fn unregister(&self, id: Option<u64>) {
        if let Some(id) = id {
            self.entries.borrow_mut().retain(|(i, _)| *i != id);
        }
    }

    /// Wake everyone. The borrow is dropped before any waker runs, so a waker
    /// that polls inline can register again without a double borrow.
    fn wake_all(&self) {
        let wakers: Vec<Waker> = self.entries.borrow_mut().drain(..).map(|(_, w)| w).collect();
        for waker in wakers {
            waker.wake();
        }
    }
}

/// Resolves when the pool can hand out a connection — an idle one exists, or
/// there is room to open one — keeping exactly one waker registered until then.
struct WaitReady<'a, M: ManageConnection> {
    pool: &'a LocalPool<M>,
    id: Option<u64>,
}

impl<M: ManageConnection> Future for WaitReady<'_, M> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.pool.ready_now() {
            this.pool.inner.waiters.unregister(this.id.take());
            Poll::Ready(())
        } else {
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
