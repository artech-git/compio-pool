//! The checkout guard and the cancellation-safety primitive that goes with it.

use std::{
    cell::Cell,
    ops::{Deref, DerefMut},
    rc::Rc,
};

use crate::{
    exchange::Exchange,
    manage::Manage,
    pool::Pool,
    shard::Shard,
    slot::{Slot, SlotMeta},
};

/// A connection checked out of the pool.
///
/// Derefs to the underlying connection. Returns itself to the pool on drop —
/// unless it has been poisoned, in which case it is closed instead.
///
/// # Cancellation safety
///
/// This is the part that differs most from a readiness-based pool. With
/// completion-based IO, dropping a future with an operation in flight does not
/// unwind the operation: the kernel still owns the buffer and the peer's
/// response is still coming. `compio` keeps the *buffer* sound, but your
/// *protocol* is now out of step — the next reader on this connection will see
/// the tail of someone else's response.
///
/// A connection cancelled mid-operation must therefore be destroyed, never
/// reused. Wrap each operation in [`begin_op`](Pooled::begin_op):
///
/// ```ignore
/// let op = conn.begin_op();
/// let n = conn.read(&mut buf).await?;   // if this await is cancelled…
/// op.complete();                       // …this never runs, and the conn is dropped
/// ```
///
/// If you never cancel (no `select!`, no timeouts, no early return between
/// submit and completion) you can skip it. Everyone believes that about their
/// code right up until they add a timeout.
pub struct Pooled<M: Manage, X: Exchange<M>> {
    slot: Option<Slot<M::Connection>>,
    shard: Rc<Shard<M>>,
    pool: Pool<M, X>,
    poison: Rc<Cell<bool>>,
}

impl<M: Manage, X: Exchange<M>> Pooled<M, X> {
    pub(crate) fn new(slot: Slot<M::Connection>, shard: Rc<Shard<M>>, pool: Pool<M, X>) -> Self {
        Self {
            slot: Some(slot),
            shard,
            pool,
            poison: Rc::new(Cell::new(false)),
        }
    }

    /// Lifecycle metadata: age, idle time, checkout count.
    pub fn meta(&self) -> &SlotMeta {
        &self.slot.as_ref().expect("slot present until drop").meta
    }

    /// Marks this connection unusable. It is closed on drop instead of pooled.
    ///
    /// Call this whenever you learn the connection is in an unknown state: a
    /// protocol error, a partial write, a response you could not parse.
    pub fn poison(&self) {
        self.poison.set(true);
    }

    /// True if [`poison`](Pooled::poison) has been called, directly or by a
    /// dropped [`OpGuard`].
    pub fn is_poisoned(&self) -> bool {
        self.poison.get()
    }

    /// Arms cancellation protection for one operation.
    ///
    /// The returned guard poisons this connection when dropped, unless
    /// [`OpGuard::complete`] runs first. It borrows nothing from `self`, so the
    /// connection stays fully usable while the guard is alive.
    pub fn begin_op(&self) -> OpGuard {
        OpGuard {
            poison: self.poison.clone(),
            armed: true,
        }
    }

    /// Removes the connection from the pool's control and hands it to you.
    ///
    /// The shard's budget is freed immediately, so a replacement may be opened.
    pub fn take(mut self) -> M::Connection {
        let slot = self.slot.take().expect("slot present until drop");
        self.pool.forget(&self.shard);
        slot.conn
    }
}

impl<M: Manage, X: Exchange<M>> Deref for Pooled<M, X> {
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        &self.slot.as_ref().expect("slot present until drop").conn
    }
}

impl<M: Manage, X: Exchange<M>> DerefMut for Pooled<M, X> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.slot.as_mut().expect("slot present until drop").conn
    }
}

impl<M: Manage, X: Exchange<M>> Drop for Pooled<M, X> {
    fn drop(&mut self) {
        // `Drop` cannot await, so the connection goes back synchronously and
        // any async validation happens on the next `acquire`, in
        // `Manage::recycle`. That is why `recycle` is an acquire-side hook.
        if let Some(slot) = self.slot.take() {
            self.pool.release(&self.shard, slot, self.poison.get());
        }
    }
}

impl<M: Manage, X: Exchange<M>> std::fmt::Debug for Pooled<M, X> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pooled")
            .field("poisoned", &self.poison.get())
            .field("meta", self.meta())
            .finish_non_exhaustive()
    }
}

/// Poisons its connection unless [`complete`](OpGuard::complete) is called.
///
/// Created by [`Pooled::begin_op`]. See that type's docs for why this exists.
#[must_use = "an OpGuard that is dropped immediately poisons the connection"]
pub struct OpGuard {
    poison: Rc<Cell<bool>>,
    armed: bool,
}

impl OpGuard {
    /// Disarms the guard: the operation finished, the connection is intact.
    pub fn complete(mut self) {
        self.armed = false;
    }
}

impl Drop for OpGuard {
    fn drop(&mut self) {
        if self.armed {
            self.poison.set(true);
        }
    }
}

impl std::fmt::Debug for OpGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpGuard")
            .field("armed", &self.armed)
            .finish()
    }
}
