//! Shared test doubles.
#![allow(dead_code)]

use std::{
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering::SeqCst},
    },
};

use compio_pool::{Detach, Manage, SlotMeta};

/// A connection that is `!Send` because it holds an `Rc` — exactly the property
/// that rules out `bb8`/`deadpool` for compio IO types.
#[derive(Debug)]
pub struct LocalConn {
    pub id: u64,
    _not_send: Rc<()>,
}

#[derive(Debug, Default)]
pub struct Counts {
    pub connected: AtomicU64,
    pub disconnected: AtomicU64,
    pub recycled: AtomicU64,
}

pub struct LocalManager {
    pub counts: Arc<Counts>,
    next_id: AtomicU64,
    fail_connect: AtomicBool,
    fail_recycle: AtomicBool,
    hang_connect: AtomicBool,
}

impl LocalManager {
    pub fn new() -> Self {
        Self {
            counts: Arc::new(Counts::default()),
            next_id: AtomicU64::new(0),
            fail_connect: AtomicBool::new(false),
            fail_recycle: AtomicBool::new(false),
            hang_connect: AtomicBool::new(false),
        }
    }

    pub fn counts(&self) -> Arc<Counts> {
        self.counts.clone()
    }

    pub fn set_fail_recycle(&self, v: bool) {
        self.fail_recycle.store(v, SeqCst);
    }

    pub fn set_fail_connect(&self, v: bool) {
        self.fail_connect.store(v, SeqCst);
    }

    /// Models a TCP handshake that never completes.
    pub fn set_hang_connect(&self, v: bool) {
        self.hang_connect.store(v, SeqCst);
    }
}

impl Manage for LocalManager {
    type Connection = LocalConn;
    type Error = &'static str;

    async fn connect(&self) -> Result<LocalConn, &'static str> {
        if self.hang_connect.load(SeqCst) {
            std::future::pending::<()>().await;
        }
        if self.fail_connect.load(SeqCst) {
            return Err("connect refused");
        }
        self.counts.connected.fetch_add(1, SeqCst);
        Ok(LocalConn {
            id: self.next_id.fetch_add(1, SeqCst),
            _not_send: Rc::new(()),
        })
    }

    async fn recycle(&self, _conn: &mut LocalConn, _meta: &SlotMeta) -> Result<(), &'static str> {
        self.counts.recycled.fetch_add(1, SeqCst);
        if self.fail_recycle.load(SeqCst) {
            return Err("stale");
        }
        Ok(())
    }

    fn disconnect(&self, _conn: LocalConn) {
        self.counts.disconnected.fetch_add(1, SeqCst);
    }
}

/// A connection that is `!Send` (raw pointer marker) but whose *identity* is a
/// plain `u64` — the same shape as an fd that may be handed to another driver.
#[derive(Debug)]
pub struct MovableConn {
    pub id: u64,
    _not_send: PhantomData<*const ()>,
}

pub struct MovableManager {
    pub counts: Arc<Counts>,
    next_id: AtomicU64,
}

impl MovableManager {
    pub fn new() -> Self {
        Self {
            counts: Arc::new(Counts::default()),
            next_id: AtomicU64::new(0),
        }
    }

    pub fn counts(&self) -> Arc<Counts> {
        self.counts.clone()
    }
}

impl Manage for MovableManager {
    type Connection = MovableConn;
    type Error = &'static str;

    async fn connect(&self) -> Result<MovableConn, &'static str> {
        self.counts.connected.fetch_add(1, SeqCst);
        Ok(MovableConn {
            id: self.next_id.fetch_add(1, SeqCst),
            _not_send: PhantomData,
        })
    }

    async fn recycle(&self, _conn: &mut MovableConn, _meta: &SlotMeta) -> Result<(), &'static str> {
        self.counts.recycled.fetch_add(1, SeqCst);
        Ok(())
    }

    fn disconnect(&self, _conn: MovableConn) {
        self.counts.disconnected.fetch_add(1, SeqCst);
    }
}

impl Detach for MovableManager {
    /// Stands in for `OwnedFd`: `Send`, and enough to rebuild the connection.
    type Parked = u64;

    fn detach(conn: MovableConn) -> u64 {
        conn.id
    }

    async fn attach(id: u64) -> Result<MovableConn, &'static str> {
        Ok(MovableConn {
            id,
            _not_send: PhantomData,
        })
    }
}
