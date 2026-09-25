//! Test doubles for the crate's own unit tests.
//!
//! The integration suite has its own copies under `tests/common`; these exist
//! so a unit test can reach `pub(crate)` internals — [`Shard`](crate::shard::Shard),
//! [`Slot`](crate::slot::Slot), [`Counters`](crate::metrics::Counters) — without
//! going through the public API.

#![allow(dead_code)]

use std::{
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    },
    task::Waker,
};

use crate::{
    manage::{Detach, Manage},
    slot::SlotMeta,
};

/// Tally of every manager callback, so a test can assert on side effects.
#[derive(Debug, Default)]
pub(crate) struct Counts {
    pub connected: AtomicU64,
    pub disconnected: AtomicU64,
    pub recycled: AtomicU64,
}

impl Counts {
    pub(crate) fn connected(&self) -> u64 {
        self.connected.load(SeqCst)
    }

    pub(crate) fn disconnected(&self) -> u64 {
        self.disconnected.load(SeqCst)
    }

    pub(crate) fn recycled(&self) -> u64 {
        self.recycled.load(SeqCst)
    }
}

/// A connection that is `!Send` because it holds an `Rc`, like a compio IO
/// handle bound to one driver.
#[derive(Debug)]
pub(crate) struct TestConn {
    pub id: u64,
    _not_send: Rc<()>,
}

impl TestConn {
    pub(crate) fn new(id: u64) -> Self {
        Self {
            id,
            _not_send: Rc::new(()),
        }
    }
}

/// A [`Manage`] whose every outcome is switchable at runtime.
pub(crate) struct TestManager {
    counts: Arc<Counts>,
    next_id: AtomicU64,
    fail_connect: AtomicBool,
    fail_recycle: AtomicBool,
    hang_connect: AtomicBool,
    hang_recycle: AtomicBool,
}

impl TestManager {
    pub(crate) fn new() -> Self {
        Self {
            counts: Arc::new(Counts::default()),
            next_id: AtomicU64::new(0),
            fail_connect: AtomicBool::new(false),
            fail_recycle: AtomicBool::new(false),
            hang_connect: AtomicBool::new(false),
            hang_recycle: AtomicBool::new(false),
        }
    }

    pub(crate) fn counts(&self) -> Arc<Counts> {
        self.counts.clone()
    }

    pub(crate) fn fail_connect(&self, v: bool) {
        self.fail_connect.store(v, SeqCst);
    }

    pub(crate) fn fail_recycle(&self, v: bool) {
        self.fail_recycle.store(v, SeqCst);
    }

    /// Models a handshake that never completes.
    pub(crate) fn hang_connect(&self, v: bool) {
        self.hang_connect.store(v, SeqCst);
    }

    /// Models a liveness check that never completes.
    pub(crate) fn hang_recycle(&self, v: bool) {
        self.hang_recycle.store(v, SeqCst);
    }
}

impl Manage for TestManager {
    type Connection = TestConn;
    type Error = &'static str;

    async fn connect(&self) -> Result<TestConn, &'static str> {
        if self.hang_connect.load(SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.fail_connect.load(SeqCst) {
            return Err("connect refused");
        }
        self.counts.connected.fetch_add(1, SeqCst);
        Ok(TestConn::new(self.next_id.fetch_add(1, SeqCst)))
    }

    async fn recycle(&self, _conn: &mut TestConn, _meta: &SlotMeta) -> Result<(), &'static str> {
        if self.hang_recycle.load(SeqCst) {
            std::future::pending::<()>().await;
        }
        self.counts.recycled.fetch_add(1, SeqCst);
        if self.fail_recycle.load(SeqCst) {
            return Err("stale");
        }
        Ok(())
    }

    fn disconnect(&self, _conn: TestConn) {
        self.counts.disconnected.fetch_add(1, SeqCst);
    }
}

/// A [`Manage`] that uses [`Manage::disconnect`]'s default no-op body, so the
/// default is actually exercised somewhere.
pub(crate) struct DefaultDisconnectManager;

impl Manage for DefaultDisconnectManager {
    type Connection = TestConn;
    type Error = &'static str;

    async fn connect(&self) -> Result<TestConn, &'static str> {
        Ok(TestConn::new(0))
    }

    async fn recycle(&self, _conn: &mut TestConn, _meta: &SlotMeta) -> Result<(), &'static str> {
        Ok(())
    }
}

/// A connection that is `!Send` but whose identity is a plain `u64` — the shape
/// of an fd that may legally be handed to another driver.
#[derive(Debug)]
pub(crate) struct MovableConn {
    pub id: u64,
    _not_send: PhantomData<*const ()>,
}

impl MovableConn {
    pub(crate) fn new(id: u64) -> Self {
        Self {
            id,
            _not_send: PhantomData,
        }
    }
}

pub(crate) struct MovableManager {
    counts: Arc<Counts>,
    next_id: AtomicU64,
}

impl MovableManager {
    pub(crate) fn new() -> Self {
        Self {
            counts: Arc::new(Counts::default()),
            next_id: AtomicU64::new(0),
        }
    }

    pub(crate) fn counts(&self) -> Arc<Counts> {
        self.counts.clone()
    }
}

impl Manage for MovableManager {
    type Connection = MovableConn;
    type Error = &'static str;

    async fn connect(&self) -> Result<MovableConn, &'static str> {
        self.counts.connected.fetch_add(1, SeqCst);
        Ok(MovableConn::new(self.next_id.fetch_add(1, SeqCst)))
    }

    async fn recycle(&self, _conn: &mut MovableConn, _meta: &SlotMeta) -> Result<(), &'static str> {
        self.counts.recycled.fetch_add(1, SeqCst);
        Ok(())
    }

    fn disconnect(&self, _conn: MovableConn) {
        self.counts.disconnected.fetch_add(1, SeqCst);
    }
}

/// [`Detach`]'s methods are associated fns with no `self`, so a test that wants
/// them to fail has to reach them through a global. Take [`detach_lock`] first:
/// the unit-test binary runs tests in parallel.
pub(crate) static CAN_DETACH: AtomicBool = AtomicBool::new(true);
pub(crate) static CAN_ATTACH: AtomicBool = AtomicBool::new(true);

static DETACH_LOCK: Mutex<()> = Mutex::new(());

/// Serialises tests that flip [`CAN_DETACH`] / [`CAN_ATTACH`], restoring both
/// to the default when the guard drops.
pub(crate) fn detach_lock() -> DetachGuard {
    let guard = DETACH_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    CAN_DETACH.store(true, SeqCst);
    CAN_ATTACH.store(true, SeqCst);
    DetachGuard(Some(guard))
}

pub(crate) struct DetachGuard(Option<MutexGuard<'static, ()>>);

impl Drop for DetachGuard {
    fn drop(&mut self) {
        CAN_DETACH.store(true, SeqCst);
        CAN_ATTACH.store(true, SeqCst);
        drop(self.0.take());
    }
}

impl Detach for MovableManager {
    /// Stands in for an `OwnedFd`: `Send`, and enough to rebuild the connection.
    type Parked = u64;

    fn detach(conn: MovableConn) -> Option<u64> {
        // A real impl checks that no submission still holds the handle; this one
        // consults the switch. Either way the connection is consumed.
        CAN_DETACH.load(SeqCst).then_some(conn.id)
    }

    async fn attach(id: u64) -> Result<MovableConn, &'static str> {
        if !CAN_ATTACH.load(SeqCst) {
            return Err("cannot attach");
        }
        Ok(MovableConn::new(id))
    }
}

/// A [`Waker`] that records how many times it was woken, for futures driven by
/// hand rather than by a runtime.
#[derive(Debug, Default)]
pub(crate) struct CountingWaker(AtomicU64);

impl CountingWaker {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn count(&self) -> u64 {
        self.0.load(SeqCst)
    }
}

impl std::task::Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, SeqCst);
    }
}

pub(crate) fn counting_waker() -> (Arc<CountingWaker>, Waker) {
    let arc = CountingWaker::new();
    let waker = Waker::from(arc.clone());
    (arc, waker)
}
