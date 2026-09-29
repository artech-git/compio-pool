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
/// op.complete_op();                     // …this never runs, and the conn is dropped
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
    /// NOTE: It is mandatory to call [`OpGuard::complete_op`] when the operation finishes, or the
    /// connection will be poisoned. The guard does not borrow the connection, so it can be
    /// dropped before the operation completes, but that will poison the connection.
    ///
    /// The returned guard poisons this connection when dropped, unless
    /// [`OpGuard::complete_op`] runs first. It borrows nothing from `self`, so the
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

    /// Runs `func` with the connection, then returns the connection to the pool.
    ///
    /// A returned `Err` does *not* poison — an error is often just an error.
    /// Call [`poison`](Pooled::poison) yourself if it means the connection is
    /// no longer trustworthy.
    ///
    /// # Panics
    ///
    /// If `func` panics, the connection is poisoned and closed instead of
    /// pooled, and the panic propagates to the caller unchanged. A panic
    /// mid-protocol leaves the connection in the same unknown state a
    /// cancellation does, so it must not be reused.
    ///
    /// There is deliberately no [`catch_unwind`] here. Three reasons:
    ///
    /// * The destructors do the job already. On unwind the [`OpGuard`] drops
    ///   first and poisons, then `self` drops and releases the slot as
    ///   poisoned. Catching would only re-implement that, and a catch that
    ///   re-panics *after* releasing gets the ordering wrong — the earlier
    ///   version of this function leaked the slot and its shard budget on every
    ///   panic, because it re-panicked before ever calling `release`.
    /// * Catching loses the evidence. Re-raising a caught payload replaces the
    ///   original panic location and truncates the backtrace to this frame.
    /// * Swallowing it would be a lie. `Result<T, M::Error>` cannot express
    ///   "your closure blew up", so the only honest options are to propagate or
    ///   to abort, and propagating is the caller's decision to make.
    ///
    /// This matters because a panic here is not necessarily fatal. `compio`
    /// wraps spawned tasks in `catch_unwind` and parks the payload in the
    /// `JoinHandle`, so a panicking task neither kills the runtime nor stops
    /// the other tasks — and if the handle is detached, the panic is discarded
    /// silently. The pool keeps serving afterwards, which is exactly why the
    /// abandoned connection has to be poisoned rather than left to the process
    /// dying. Under `panic = "abort"` none of this runs, but then there is no
    /// surviving pool to corrupt.
    ///
    /// # Why `func` is not `Send`
    ///
    /// Because it never crosses a thread. `func` is called synchronously, in
    /// this stack frame, on the thread that owns the shard. A `Send` bound
    /// would constrain nothing and reject the ordinary callers: closures that
    /// capture an `Rc` buffer, a thread-local registry handle, or another
    /// `Pooled` from the same shard.
    ///
    /// It could not be honoured anyway. [`Manage::Connection`] is allowed to be
    /// `!Send` — that is the point of this crate — so the `&mut M::Connection`
    /// that `func` receives is generally `!Send`, as is [`Pooled`] itself
    /// (`Rc<Shard<M>>`, `Rc<Cell<bool>>`). `Send` in this crate lives on the
    /// things that genuinely move between threads: [`Manage`], [`Exchange`],
    /// and [`Detach::Parked`](crate::manage::Detach::Parked), the stripped-down form a connection is converted
    /// *into* to reach the cross-thread overflow pool. The connection itself is
    /// parked and rebuilt, never sent.
    ///
    /// [`catch_unwind`]: std::panic::catch_unwind
    pub fn run<T>(
        mut self,
        func: impl FnOnce(&mut M::Connection) -> Result<T, M::Error>,
    ) -> Result<T, M::Error> {
        // On unwind, `op` drops first and poisons, then `self` drops and
        // releases the slot as poisoned. No `catch_unwind` needed.
        let op = self.begin_op();
        let result = func(&mut *self);
        op.complete_op();
        result
    }

    /// The `async` form of [`run`](Pooled::run). Identical in every respect
    /// discussed there: no `catch_unwind`, poison-on-panic via the guards, and
    /// no `Send` bound on `func` because the shard is single-threaded.
    ///
    /// Dropping this future mid-`await` poisons the connection too, by the same
    /// two destructors — cancellation and panic are the same hazard here, and
    /// they get the same handling for free.
    pub async fn run_async<T>(
        mut self,
        func: impl AsyncFnOnce(&mut M::Connection) -> Result<T, M::Error>,
    ) -> Result<T, M::Error> {
        let op = self.begin_op();
        let result = func(&mut *self).await;
        op.complete_op();
        result
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

/// Poisons its connection unless [`complete_op`](OpGuard::complete_op) is called.
///
/// Created by [`Pooled::begin_op`]. See that type's docs for why this exists.
#[must_use = "an OpGuard that is dropped immediately poisons the connection"]
pub struct OpGuard {
    poison: Rc<Cell<bool>>,
    armed: bool,
}

impl OpGuard {
    /// Disarms the guard: the operation finished, the connection is intact.
    pub fn complete_op(mut self) {
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
