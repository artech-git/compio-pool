//! Shared test doubles.
#![allow(dead_code)]

use std::{
    marker::PhantomData,
    rc::Rc,
    sync::{
        Arc, Mutex, MutexGuard,
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

impl LocalConn {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            _not_send: Rc::new(()),
        }
    }
}

pub struct LocalManager {
    pub counts: Arc<Counts>,
    next_id: AtomicU64,
    fail_connect: AtomicBool,
    fail_recycle: AtomicBool,
    hang_connect: AtomicBool,
    hang_recycle: AtomicBool,
}

impl LocalManager {
    pub fn new() -> Self {
        Self {
            counts: Arc::new(Counts::default()),
            next_id: AtomicU64::new(0),
            fail_connect: AtomicBool::new(false),
            fail_recycle: AtomicBool::new(false),
            hang_connect: AtomicBool::new(false),
            hang_recycle: AtomicBool::new(false),
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

    /// Models a liveness check that never completes, so an `acquire` can be
    /// cancelled while holding a connection that is out of the free list but not
    /// yet inside a guard.
    pub fn set_hang_recycle(&self, v: bool) {
        self.hang_recycle.store(v, SeqCst);
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
        if self.hang_recycle.load(SeqCst) {
            std::future::pending::<()>().await;
        }
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

impl MovableConn {
    pub fn new(id: u64) -> Self {
        Self {
            id,
            _not_send: PhantomData,
        }
    }
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

/// `Detach`'s methods are associated fns with no `self`, so a test that wants
/// them to fail has to reach them through a global. Tests that flip these take
/// [`detach_lock`] first, since the test binary runs them in parallel.
pub static CAN_DETACH: AtomicBool = AtomicBool::new(true);
pub static CAN_ATTACH: AtomicBool = AtomicBool::new(true);
/// Models a reattach that never completes, so an `acquire` can be cancelled
/// while it is inside `Exchange::unpark`.
pub static HANG_ATTACH: AtomicBool = AtomicBool::new(false);

static DETACH_LOCK: Mutex<()> = Mutex::new(());

/// Serialises tests that flip [`CAN_DETACH`] / [`CAN_ATTACH`], and restores
/// both to the default when the guard drops.
pub fn detach_lock() -> DetachGuard {
    let guard = DETACH_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    CAN_DETACH.store(true, SeqCst);
    CAN_ATTACH.store(true, SeqCst);
    HANG_ATTACH.store(false, SeqCst);
    DetachGuard(Some(guard))
}

pub struct DetachGuard(Option<MutexGuard<'static, ()>>);

impl Drop for DetachGuard {
    fn drop(&mut self) {
        CAN_DETACH.store(true, SeqCst);
        CAN_ATTACH.store(true, SeqCst);
        HANG_ATTACH.store(false, SeqCst);
        drop(self.0.take());
    }
}

impl Detach for MovableManager {
    /// Stands in for `OwnedFd`: `Send`, and enough to rebuild the connection.
    type Parked = u64;

    fn detach(conn: MovableConn) -> Option<u64> {
        // A real impl checks that no operation still holds the handle; this
        // one just consults the switch. Either way the connection is consumed.
        CAN_DETACH.load(SeqCst).then_some(conn.id)
    }

    async fn attach(id: u64) -> Result<MovableConn, &'static str> {
        if HANG_ATTACH.load(SeqCst) {
            std::future::pending::<()>().await;
        }
        if !CAN_ATTACH.load(SeqCst) {
            return Err("cannot attach");
        }
        Ok(MovableConn {
            id,
            _not_send: PhantomData,
        })
    }
}

impl Counts {
    pub fn connected(&self) -> u64 {
        self.connected.load(SeqCst)
    }

    pub fn disconnected(&self) -> u64 {
        self.disconnected.load(SeqCst)
    }

    pub fn recycled(&self) -> u64 {
        self.recycled.load(SeqCst)
    }
}

/// A manager that leaves [`Manage::disconnect`] at its default no-op body, so
/// the documented default is exercised somewhere.
pub struct DefaultsManager;

impl Manage for DefaultsManager {
    type Connection = LocalConn;
    type Error = &'static str;

    async fn connect(&self) -> Result<LocalConn, &'static str> {
        Ok(LocalConn {
            id: 0,
            _not_send: Rc::new(()),
        })
    }

    async fn recycle(&self, _conn: &mut LocalConn, _meta: &SlotMeta) -> Result<(), &'static str> {
        Ok(())
    }
}

/// The baseline config for tests: the reaper's own timers are off, so a test
/// observes only what it triggers itself.
pub fn cfg() -> compio_pool::Config {
    compio_pool::Config::new()
        .max_lifetime(None)
        .idle_timeout(None)
        .acquire_timeout(std::time::Duration::from_millis(200))
}

/// `Config`'s fields are `pub(crate)`, so an integration test reads them back
/// out of `Debug` rather than reaching in.
pub fn field(config: &compio_pool::Config, name: &str) -> String {
    let s = format!("{config:?}");
    let at = s
        .find(&format!("{name}: "))
        .unwrap_or_else(|| panic!("no field `{name}` in {s}"));
    let rest = &s[at + name.len() + 2..];
    // No config field's `Debug` contains a `, ` of its own, so the first
    // separator - or the closing brace, for the last field - ends the value.
    let end = rest
        .find(", ")
        .or_else(|| rest.find(" }"))
        .expect("Debug output is malformed");
    rest[..end].to_string()
}

// ---- Randomized workload machinery ----------------------------------------
//
// The randomized suites are seeded rather than genuinely random: a failure is
// reported with the seed that produced it and replays exactly. `Op::decode`
// takes raw bytes so the same driver can be fed by the PRNG here and by
// libFuzzer's input in `fuzz/`.

/// xorshift64*. Small, fast, and reproducible from its seed alone.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        // Any non-zero state will do; the odd constant keeps seed 0 usable.
        Self((seed ^ 0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn byte(&mut self) -> u8 {
        self.next_u64() as u8
    }

    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }

    pub fn in_range(&mut self, lo: usize, hi: usize) -> usize {
        lo + self.below(hi - lo + 1)
    }

    pub fn chance(&mut self, percent: u32) -> bool {
        (self.next_u64() % 100) < percent as u64
    }
}

/// How many seeds each randomized test runs, and how many steps per seed.
///
/// The defaults keep CI under a second or so; the same tests double as a soak
/// run with `COMPIO_POOL_FUZZ_SEEDS=2000 COMPIO_POOL_FUZZ_STEPS=5000 cargo test`.
pub fn fuzz_seeds() -> u64 {
    env_or("COMPIO_POOL_FUZZ_SEEDS", 24)
}

pub fn fuzz_steps() -> usize {
    env_or("COMPIO_POOL_FUZZ_STEPS", 200) as usize
}

fn env_or(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Lets the driver identify a connection regardless of which manager made it,
/// so "was this handed out twice?" is checkable.
pub trait ConnId {
    fn conn_id(&self) -> u64;
}

impl ConnId for LocalConn {
    fn conn_id(&self) -> u64 {
        self.id
    }
}

impl ConnId for MovableConn {
    fn conn_id(&self) -> u64 {
        self.id
    }
}

/// One step of a randomized pool workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Acquire,
    /// Return a checked-out connection. The index is taken modulo what is held.
    Release(usize),
    /// Mark one unusable, then return it.
    Poison(usize),
    /// Arm an `OpGuard` and drop it without completing — a cancelled operation.
    Abandon(usize),
    /// Remove one from the pool's control entirely.
    Take(usize),
    Invalidate,
    Warm,
    Close,
}

impl Op {
    /// Decodes a step from two bytes: one selects the operation, one indexes
    /// into the currently-held guards.
    ///
    /// The weighting matters more than the spread: acquire and release have to
    /// dominate, or the run never builds up enough state for the rarer
    /// operations to be interesting.
    ///
    /// [`Op::Close`] is deliberately **not** in the distribution. It is one-way,
    /// so sampling it anywhere but the very end of a long run would leave most of
    /// that run acquiring against a closed pool and proving nothing. Tests that
    /// want it splice it in at a chosen step instead.
    pub fn decode(selector: u8, index: u8) -> Self {
        let i = index as usize;
        match selector % 64 {
            0..=24 => Op::Acquire,
            25..=46 => Op::Release(i),
            47..=52 => Op::Poison(i),
            53..=56 => Op::Abandon(i),
            57..=58 => Op::Take(i),
            59..=61 => Op::Invalidate,
            _ => Op::Warm,
        }
    }

    /// A whole workload decoded from a byte string, for the libFuzzer targets.
    pub fn decode_all(bytes: &[u8]) -> Vec<Op> {
        bytes
            .chunks(2)
            .map(|c| Op::decode(c[0], *c.get(1).unwrap_or(&0)))
            .collect()
    }

    pub fn sample(rng: &mut Rng) -> Self {
        Op::decode(rng.byte(), rng.byte())
    }
}

/// A `Waker` that counts how many times it was woken, for futures driven by
/// hand rather than by a runtime.
#[derive(Debug, Default)]
pub struct CountingWaker(AtomicU64);

impl CountingWaker {
    pub fn count(&self) -> u64 {
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

pub fn counting_waker() -> (Arc<CountingWaker>, std::task::Waker) {
    let arc = Arc::new(CountingWaker::default());
    let waker = std::task::Waker::from(arc.clone());
    (arc, waker)
}

/// What a randomized run observed, for the caller to assert on afterwards.
#[derive(Debug, Default, Clone)]
pub struct Tally {
    pub acquires: u64,
    pub timeouts: u64,
    pub backend_errors: u64,
    pub closed_errors: u64,
    /// Connections handed to the caller by `Pooled::take`, which leave the
    /// pool's accounting legitimately.
    pub taken: u64,
    /// Connections dropped by `Exchange::clear` during `invalidate` / `close`.
    ///
    /// These leave the counters *without* being counted closed, so conservation
    /// has to allow for them. See the note in `tests/fuzz.rs`.
    pub cleared: u64,
    /// Ids that must never be handed out again: poisoned, condemned by an
    /// abandoned `OpGuard`, or taken out of the pool by the caller.
    pub retired: std::collections::HashSet<u64>,
}

/// Applies a workload to a pool, re-checking the pool's own accounting after
/// **every** step.
///
/// `tally` accumulates across calls and must cover the pool's whole life, since
/// the invariants compare it against the pool's absolute counters.
///
/// Panics with the label, step number and operation on the first violation, so
/// a seeded run replays straight onto the failure. Assertion messages are built
/// lazily, which keeps a long soak run cheap.
pub async fn run_ops<M, X>(
    pool: &compio_pool::Pool<M, X>,
    max_size: usize,
    ops: &[Op],
    label: &str,
    tally: &mut Tally,
) where
    M: Manage,
    M::Connection: ConnId,
    X: compio_pool::Exchange<M>,
{
    let mut held: Vec<compio_pool::Pooled<M, X>> = Vec::new();
    let mut prev = pool.metrics();

    for (step, op) in ops.iter().enumerate() {
        match *op {
            Op::Acquire => match pool.acquire().await {
                Ok(conn) => {
                    // Manager ids are monotonic, so a retired id can never
                    // legitimately come back: seeing one means the pool reused a
                    // connection it was supposed to have destroyed.
                    let id = conn.conn_id();
                    assert!(
                        !tally.retired.contains(&id),
                        "{label} step {step}: connection {id} was handed out again after \
                         being poisoned or taken"
                    );
                    tally.acquires += 1;
                    held.push(conn);
                }
                Err(compio_pool::Error::Timeout) => tally.timeouts += 1,
                Err(compio_pool::Error::Closed) => tally.closed_errors += 1,
                Err(compio_pool::Error::Backend(_)) => tally.backend_errors += 1,
            },
            Op::Release(i) if !held.is_empty() => {
                let i = i % held.len();
                drop(held.remove(i));
            }
            Op::Poison(i) if !held.is_empty() => {
                let i = i % held.len();
                let conn = held.remove(i);
                tally.retired.insert(conn.conn_id());
                conn.poison();
                drop(conn);
            }
            Op::Abandon(i) if !held.is_empty() => {
                // Armed and dropped without completing: a cancelled operation.
                // The connection stays checked out and is condemned on return.
                let i = i % held.len();
                tally.retired.insert(held[i].conn_id());
                drop(held[i].begin_op());
            }
            Op::Take(i) if !held.is_empty() => {
                let i = i % held.len();
                let conn = held.remove(i);
                tally.retired.insert(conn.conn_id());
                let _escaped = conn.take();
                tally.taken += 1;
            }
            Op::Invalidate => {
                tally.cleared += pool.metrics().parked;
                pool.invalidate();
            }
            Op::Warm => {
                let _ = pool.warm().await;
            }
            Op::Close => {
                tally.cleared += pool.metrics().parked;
                pool.close();
            }
            // An indexed operation with nothing checked out is a no-op.
            _ => {}
        }

        check_pool_invariants(pool, max_size, &held, tally, &prev, label, step, *op);
        prev = pool.metrics();
    }
}

/// The invariants a pool must satisfy at every quiescent point, whatever it was
/// just asked to do.
#[allow(clippy::too_many_arguments)]
pub fn check_pool_invariants<M, X>(
    pool: &compio_pool::Pool<M, X>,
    max_size: usize,
    held: &[compio_pool::Pooled<M, X>],
    tally: &Tally,
    prev: &compio_pool::Metrics,
    label: &str,
    step: usize,
    op: Op,
) where
    M: Manage,
    M::Connection: ConnId,
    X: compio_pool::Exchange<M>,
{
    let m = pool.metrics();
    let (size, idle) = (pool.local_size(), pool.local_idle());

    assert!(
        size <= max_size,
        "{label} step {step} {op:?}: shard holds {size} > max_size {max_size}"
    );
    assert!(
        idle <= size,
        "{label} step {step} {op:?}: {idle} idle but only {size} owned"
    );
    assert!(
        held.len() <= max_size,
        "{label} step {step} {op:?}: {} guards out, max_size {max_size}",
        held.len()
    );
    assert!(
        m.idle <= m.live,
        "{label} step {step} {op:?}: idle exceeds live: {m:?}"
    );
    // One thread, so the global `live` gauge is exactly this shard's size.
    assert_eq!(
        m.live as usize, size,
        "{label} step {step} {op:?}: live gauge and shard size disagree: {m:?}"
    );
    assert_eq!(
        m.idle as usize, idle,
        "{label} step {step} {op:?}: idle gauge and free list disagree: {m:?}"
    );

    // Counters are monotonic; only the gauges may fall.
    for (name, was, now) in [
        ("created", prev.created, m.created),
        ("closed", prev.closed, m.closed),
        ("acquires", prev.acquires, m.acquires),
        ("waits", prev.waits, m.waits),
        ("timeouts", prev.timeouts, m.timeouts),
        ("poisoned", prev.poisoned, m.poisoned),
        (
            "recycle_failures",
            prev.recycle_failures,
            m.recycle_failures,
        ),
        ("unparked", prev.unparked, m.unparked),
    ] {
        assert!(
            now >= was,
            "{label} step {step} {op:?}: counter `{name}` went backwards, {was} -> {now}"
        );
    }

    // Nothing may be lost: every connection ever opened is checked out or idle
    // on a shard, parked in the exchange, closed, or was handed to the caller.
    assert_eq!(
        m.created,
        m.closed + m.live + m.parked + tally.taken + tally.cleared,
        "{label} step {step} {op:?}: connections unaccounted for \
         (taken {}, cleared {}): {m:?}",
        tally.taken,
        tally.cleared
    );
    assert_eq!(
        m.acquires, tally.acquires,
        "{label} step {step} {op:?}: acquire count disagrees with the driver: {m:?}"
    );

    // And nothing may be handed out twice at once.
    let mut ids: Vec<u64> = held.iter().map(|c| c.conn_id()).collect();
    let out = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(
        ids.len(),
        out,
        "{label} step {step} {op:?}: the same connection is checked out twice"
    );
}

/// [`SlotMeta`] has no public constructor — the pool always makes its own — so a
/// test that drives [`compio_pool::Exchange`] directly borrows one from a real
/// checkout and clones it.
pub async fn sample_meta() -> SlotMeta {
    let pool = compio_pool::Pool::new(LocalManager::new(), cfg().max_size(1));
    let conn = pool
        .acquire()
        .await
        .expect("a fresh single-slot pool always serves");
    conn.meta().clone()
}
