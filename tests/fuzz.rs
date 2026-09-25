//! Randomized (fuzz) testing of every component, and the invariants that must
//! survive whatever order things happen in.
//!
//! These are seeded rather than genuinely random: each test loops over a range
//! of seeds, and every assertion names the seed and step that produced it, so a
//! failure replays exactly. Turn the dial up for a soak run:
//!
//! ```text
//! COMPIO_POOL_FUZZ_SEEDS=2000 COMPIO_POOL_FUZZ_STEPS=5000 cargo test --test fuzz
//! ```
//!
//! # A note on conservation
//!
//! The central invariant is that no connection is ever lost:
//!
//! ```text
//! created == closed + live + parked + taken + cleared
//! ```
//!
//! `taken` is [`Pooled::take`], which hands ownership out of the pool by design.
//! `cleared` is the awkward one: `Exchange::clear`, reached from both
//! `Pool::close` and `Pool::invalidate`, drops parked connections *without*
//! counting them closed, so `created` ends up ahead. The driver tracks that
//! separately instead of pretending it does not happen — see the standalone
//! tests at the bottom of this file, which pin it directly.

mod common;

use std::{
    collections::HashSet,
    future::Future,
    pin::Pin,
    sync::{Arc, Barrier, Mutex, atomic::Ordering::SeqCst},
    task::{Context, Poll},
    time::Duration,
};

use common::{
    ConnId, LocalManager, MovableManager, Op, Rng, cfg, counting_waker, fuzz_seeds, fuzz_steps,
    run_ops,
};
use compio_pool::{Error, Exchange, NoExchange, Parked, Pool, Pooled, Reservoir, Unparked};

// ---- The pool, under randomized workloads ----------------------------------

/// The baseline: a single shard, no exchange, everything reachable through the
/// public API in whatever order the seed dictates.
#[compio::test]
async fn randomized_workloads_conserve_every_connection() {
    let mut total_acquires = 0;

    for seed in 0..fuzz_seeds() {
        let mut rng = Rng::new(seed);
        let max_size = rng.in_range(1, 6);
        let ops: Vec<Op> = (0..fuzz_steps()).map(|_| Op::sample(&mut rng)).collect();

        let pool = Pool::new(
            LocalManager::new(),
            cfg()
                .max_size(max_size)
                .min_idle(rng.below(max_size + 2))
                // Short, so a workload that pins the shard at max_size makes
                // progress instead of stalling the whole run. A dial itself never
                // yields here, so nothing times out except real contention.
                .acquire_timeout(Duration::from_millis(2)),
        );

        let mut tally = common::Tally::default();
        run_ops(&pool, max_size, &ops, &format!("seed {seed}"), &mut tally).await;

        // Whatever happened, the pool has to still be in a usable state.
        let m = pool.metrics();
        assert_eq!(
            m.created,
            m.closed + m.live + m.parked + tally.taken + tally.cleared,
            "seed {seed}: final accounting: {m:?} {tally:?}"
        );
        total_acquires += tally.acquires;
    }

    assert!(
        total_acquires > 0,
        "no workload ever got a connection, so the run proved nothing"
    );
}

/// The same, with a reservoir installed, so parking and stealing are in the mix
/// and `parked` participates in conservation.
#[compio::test]
async fn randomized_workloads_with_a_reservoir_conserve_every_connection() {
    let _lock = common::detach_lock();
    for seed in 0..fuzz_seeds() {
        let mut rng = Rng::new(seed ^ 0xA5A5);
        let max_size = rng.in_range(1, 6);
        let capacity = rng.in_range(1, 8);
        let ops: Vec<Op> = (0..fuzz_steps()).map(|_| Op::sample(&mut rng)).collect();

        let pool = Pool::builder(MovableManager::new())
            .config(
                cfg()
                    .max_size(max_size)
                    .min_idle(rng.below(max_size + 1))
                    .acquire_timeout(Duration::from_millis(2)),
            )
            .exchange(Reservoir::new(capacity))
            .build();

        let mut tally = common::Tally::default();
        run_ops(&pool, max_size, &ops, &format!("seed {seed}"), &mut tally).await;

        let m = pool.metrics();
        assert!(
            m.parked as usize <= capacity,
            "seed {seed}: reservoir over capacity {capacity}: {m:?}"
        );
        assert_eq!(
            m.created,
            m.closed + m.live + m.parked + tally.taken + tally.cleared,
            "seed {seed}: final accounting: {m:?} {tally:?}"
        );
    }
}

/// A backend that fails and recovers at random. Every failure path — refused
/// dial, rejected recycle, failed detach, failed attach — has to leave the
/// accounting straight and the shard usable.
#[compio::test]
async fn randomized_workloads_survive_a_flaky_backend() {
    let _lock = common::detach_lock();
    for seed in 0..fuzz_seeds() {
        let mut rng = Rng::new(seed ^ 0xDEAD_BEEF);
        let max_size = rng.in_range(1, 4);
        let capacity = rng.in_range(1, 4);

        let pool = Pool::builder(MovableManager::new())
            .config(
                cfg()
                    .max_size(max_size)
                    .acquire_timeout(Duration::from_millis(2)),
            )
            .exchange(Reservoir::new(capacity))
            .build();

        // Flip the failure switches between short bursts of work rather than
        // mid-step, so each burst runs against a coherent backend. The tally is
        // cumulative, so the invariants still line up against the pool's own
        // absolute counters across every burst.
        let mut tally = common::Tally::default();
        for burst in 0..16 {
            common::CAN_DETACH.store(rng.chance(75), SeqCst);
            common::CAN_ATTACH.store(rng.chance(75), SeqCst);

            let ops: Vec<Op> = (0..fuzz_steps() / 16 + 1)
                .map(|_| Op::sample(&mut rng))
                .collect();
            run_ops(
                &pool,
                max_size,
                &ops,
                &format!("seed {seed} burst {burst}"),
                &mut tally,
            )
            .await;
        }

        let m = pool.metrics();
        assert!(
            m.parked as usize <= capacity,
            "seed {seed}: reservoir over capacity: {m:?}"
        );
        assert_eq!(
            m.created,
            m.closed + m.live + m.parked + tally.taken + tally.cleared,
            "seed {seed}: final accounting: {m:?} {tally:?}"
        );

        // And after all that, a healthy backend must still be servable.
        common::CAN_DETACH.store(true, SeqCst);
        common::CAN_ATTACH.store(true, SeqCst);
        if !pool.is_closed() {
            assert!(
                pool.acquire().await.is_ok(),
                "seed {seed}: the shard wedged after a flaky run: {:?}",
                pool.metrics()
            );
        }
    }
}

// ---- The reservoir, under randomized churn ---------------------------------

/// Drives `Exchange` directly, so the reservoir's own bookkeeping is checked
/// without the pool in the way.
///
/// The property that matters is that admission never leaks: however many parks,
/// claims, failed detaches, failed attaches and clears happen, it must always be
/// possible to fill the reservoir back to exactly `capacity`.
#[compio::test]
async fn the_reservoir_never_leaks_admission_under_randomized_churn() {
    let _lock = common::detach_lock();
    for seed in 0..fuzz_seeds() {
        let mut rng = Rng::new(seed ^ 0x1234_5678);
        let capacity = rng.in_range(1, 8);
        let reservoir: Reservoir<MovableManager> = Reservoir::new(capacity);
        // `SlotMeta` has no public constructor, so clone a real one.
        let proto = common::sample_meta().await;

        // Ids parked and accepted, minus those claimed or destroyed.
        let mut resident: Vec<u64> = Vec::new();
        let mut next_id = 0u64;

        for step in 0..fuzz_steps() {
            let at = || format!("seed {seed} step {step}");
            common::CAN_DETACH.store(rng.chance(80), SeqCst);
            common::CAN_ATTACH.store(rng.chance(80), SeqCst);

            match rng.below(10) {
                0..=4 => {
                    let id = next_id;
                    next_id += 1;
                    match reservoir.park(common::MovableConn::new(id), proto.clone()) {
                        Parked::Accepted => resident.push(id),
                        Parked::Refused(conn, _) => {
                            assert_eq!(conn.id, id, "{}: refusal returned the wrong conn", at());
                            assert_eq!(
                                reservoir.len(),
                                capacity,
                                "{}: refused while not full",
                                at()
                            );
                        }
                        // `detach` declined and consumed it.
                        Parked::Destroyed => {}
                    }
                }
                5..=8 => match Exchange::<MovableManager>::unpark(&reservoir).await {
                    Unparked::Claimed(conn, _) => {
                        let pos =
                            resident
                                .iter()
                                .position(|&r| r == conn.id)
                                .unwrap_or_else(|| {
                                    panic!("{}: claimed an unparked id {}", at(), conn.id)
                                });
                        resident.remove(pos);
                    }
                    Unparked::Lost => {
                        assert!(!resident.is_empty(), "{}: lost something unparked", at());
                        // FIFO, so the oldest resident is the one that went.
                        resident.remove(0);
                    }
                    Unparked::Empty => {
                        assert!(
                            resident.is_empty(),
                            "{}: empty but {:?} resident",
                            at(),
                            resident
                        )
                    }
                },
                _ => {
                    Exchange::<MovableManager>::clear(&reservoir);
                    resident.clear();
                }
            }

            assert!(
                reservoir.len() <= capacity,
                "{}: len {} over capacity {capacity}",
                at(),
                reservoir.len()
            );
            assert_eq!(
                reservoir.len(),
                resident.len(),
                "{}: reservoir and model disagree: {:?}",
                at(),
                resident
            );
            // Deliberately comparing against `len`: the point is that the two
            // accessors agree, which clippy's `is_empty()` rewrite would make
            // tautological.
            #[allow(clippy::len_zero)]
            {
                assert_eq!(
                    reservoir.is_empty(),
                    reservoir.len() == 0,
                    "{}: is_empty disagrees with len",
                    at()
                );
            }
            assert_eq!(
                Exchange::<MovableManager>::parked(&reservoir) as usize,
                reservoir.len(),
                "{}: the parked gauge disagrees with len",
                at()
            );
        }

        // The real test: drain it, then prove every admission slot came back by
        // filling it to the brim.
        common::CAN_DETACH.store(true, SeqCst);
        common::CAN_ATTACH.store(true, SeqCst);
        Exchange::<MovableManager>::clear(&reservoir);
        for i in 0..capacity {
            assert!(
                matches!(
                    reservoir.park(common::MovableConn::new(next_id + i as u64), proto.clone()),
                    Parked::Accepted
                ),
                "seed {seed}: admission leaked — only {i} of {capacity} slots usable after churn"
            );
        }
        assert_eq!(reservoir.len(), capacity, "seed {seed}: should be full");
    }
}

// ---- The waiter queue, under randomized interleavings ----------------------

/// The sharpest property in the crate: **a wakeup is never lost**.
///
/// A shard at `max_size` parks its callers in a queue and notifies exactly one
/// when capacity frees up. If that one is cancelled before it can be polled, it
/// must pass the notification on — otherwise an idle connection sits there while
/// everyone else sleeps. One hand-written interleaving proves the mechanism; this
/// walks thousands of them.
///
/// Futures are driven by hand, with a waker per waiter, so cancellation lands at
/// exact points rather than wherever a timer happens to fire.
#[compio::test]
async fn no_wakeup_is_ever_lost_under_randomized_waiter_interleavings() {
    type Acquire<'a> = Pin<
        Box<
            dyn Future<Output = Result<Pooled<LocalManager, NoExchange>, Error<&'static str>>> + 'a,
        >,
    >;

    // Notifications really do fire somewhere across the whole run; a suite that
    // never woke anybody would pass every check below for the wrong reason.
    let mut total_wakes = 0;

    for seed in 0..fuzz_seeds() {
        let mut rng = Rng::new(seed ^ 0xFACE);
        // One connection, no timeout: every extra caller lands in the queue, so
        // the whole run is about who gets told.
        let pool = Pool::new(LocalManager::new(), cfg().max_size(1).acquire_timeout(None));
        let (woken, waker) = counting_waker();

        let mut holder = Some(pool.acquire().await.unwrap());
        let mut waiters: Vec<Acquire> = Vec::new();

        // Polls one waiter, taking the connection if it came ready.
        let take_ready = |fut: &mut Acquire, holder: &mut Option<_>| -> bool {
            match fut.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(r) => {
                    let conn = r.expect("an untimed acquire cannot fail here");
                    assert!(holder.is_none(), "two owners of one connection");
                    *holder = Some(conn);
                    true
                }
                Poll::Pending => false,
            }
        };

        for step in 0..fuzz_steps() {
            let at = || format!("seed {seed} step {step}");

            match rng.below(10) {
                // Add a waiter and poll it once. It comes ready straight away if
                // the connection happens to be free, otherwise it queues.
                0..=2 if waiters.len() < 6 => {
                    let mut fut: Acquire = Box::pin(pool.acquire());
                    if !take_ready(&mut fut, &mut holder) {
                        waiters.push(fut);
                    }
                }
                // Poll one waiter; it comes ready only if it was notified.
                3..=4 if !waiters.is_empty() => {
                    let i = rng.below(waiters.len());
                    if take_ready(&mut waiters[i], &mut holder) {
                        drop(waiters.remove(i));
                    }
                }
                // Cancel a waiter, which may well be the one that was just
                // notified. Its `Drop` then owes that wakeup to the next in line.
                // This must stay possible, so the check below is a separate step
                // rather than something that runs after every one - draining
                // eagerly would consume the notification before it can be lost.
                5..=6 if !waiters.is_empty() => {
                    let i = rng.below(waiters.len());
                    drop(waiters.remove(i));
                }
                // The invariant: if the connection is idle and anyone is queued
                // for it, polling everyone must hand it over. A swallowed
                // notification shows up here as nobody coming ready.
                7 => quiesce(&pool, &mut waiters, &mut holder, &take_ready, &at()),
                // Hand the connection back, notifying one queued waiter.
                //
                // Deliberately *not* asserted here: that the wake count moved.
                // A waiter already notified by an earlier return has left the
                // shard's queue while still sitting in `waiters`, so a return
                // can legitimately notify nobody. `quiesce` is the invariant
                // that holds either way.
                _ => drop(holder.take()),
            }

            assert!(
                pool.local_size() <= 1,
                "{}: shard over max_size: {}",
                at(),
                pool.local_size()
            );
        }

        // Whatever state the run ended in, a queued waiter must be servable.
        quiesce(&pool, &mut waiters, &mut holder, &take_ready, "final");

        // And nothing wedged: after dropping every waiter the shard still serves.
        drop(waiters);
        drop(holder);
        assert!(
            pool.acquire().await.is_ok(),
            "seed {seed}: the shard wedged after randomized cancellation: {:?}",
            pool.metrics()
        );
        total_wakes += woken.count();
    }

    assert!(
        total_wakes > 0,
        "no waiter was ever woken, so the interleavings proved nothing"
    );

    /// Polls every waiter. If a connection is idle and anyone is queued, at least
    /// one of them has to come ready — otherwise a wakeup was lost and that
    /// connection would sit there while everybody sleeps.
    fn quiesce<F>(
        pool: &Pool<LocalManager>,
        waiters: &mut Vec<Acquire<'_>>,
        holder: &mut Option<Pooled<LocalManager, NoExchange>>,
        take_ready: &F,
        at: &str,
    ) where
        F: Fn(&mut Acquire<'_>, &mut Option<Pooled<LocalManager, NoExchange>>) -> bool,
    {
        if holder.is_some() || pool.local_idle() == 0 || waiters.is_empty() {
            return;
        }
        for i in 0..waiters.len() {
            if take_ready(&mut waiters[i], holder) {
                drop(waiters.remove(i));
                return;
            }
        }
        panic!(
            "{at}: a connection is idle and {} waiters are queued, but none was notified \
             - a wakeup was lost",
            waiters.len()
        );
    }
}

// ---- Expiry policies, under randomized configuration ----------------------

/// Whatever the policy mix, the pool must never hand out a connection that
/// already violates it.
#[compio::test]
async fn a_randomly_configured_pool_never_hands_out_an_expired_connection() {
    for seed in 0..fuzz_seeds() {
        let mut rng = Rng::new(seed ^ 0xC0FFEE);
        let max_uses = if rng.chance(60) {
            Some(rng.in_range(1, 4) as u64)
        } else {
            None
        };
        let max_lifetime = if rng.chance(60) {
            Some(Duration::from_millis(rng.in_range(1, 20) as u64))
        } else {
            None
        };
        let idle_timeout = if rng.chance(60) {
            Some(Duration::from_millis(rng.in_range(1, 20) as u64))
        } else {
            None
        };

        let pool = Pool::new(
            LocalManager::new(),
            cfg()
                .max_size(rng.in_range(1, 4))
                .max_uses(max_uses)
                .max_lifetime(max_lifetime)
                .idle_timeout(idle_timeout)
                .acquire_timeout(Duration::from_millis(50)),
        );

        for step in 0..64 {
            // Sleep sometimes, so age and idle time actually cross the limits.
            if rng.chance(30) {
                compio::time::sleep(Duration::from_millis(rng.in_range(0, 6) as u64)).await;
            }
            let Ok(conn) = pool.acquire().await else {
                continue;
            };
            let meta = conn.meta();
            if let Some(cap) = max_uses {
                assert!(
                    meta.uses < cap,
                    "seed {seed} step {step}: handed out a connection at {} of {cap} uses",
                    meta.uses
                );
            }
            if let Some(limit) = max_lifetime {
                assert!(
                    meta.age() < limit,
                    "seed {seed} step {step}: handed out a connection aged {:?}, limit {limit:?}",
                    meta.age()
                );
            }
            drop(conn);
        }
    }
}

// ---- Many threads at once --------------------------------------------------

/// The exchange under real contention. A connection may move between threads,
/// but never be held by two at once, and never vanish.
#[test]
fn concurrent_randomized_threads_never_alias_or_lose_a_connection() {
    let _lock = common::detach_lock();
    const THREADS: usize = 8;
    let max_size = 3;
    let capacity = 8;

    let pool = Pool::builder(MovableManager::new())
        .config(
            cfg()
                .max_size(max_size)
                .min_idle(1)
                .acquire_timeout(Duration::from_millis(200)),
        )
        .exchange(Reservoir::new(capacity))
        .build();

    // Ids currently checked out anywhere in the process. An id already present
    // when a thread checks one out means two drivers hold one socket.
    let checked_out: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));
    let start = Arc::new(Barrier::new(THREADS));

    let threads: Vec<_> = (0..THREADS)
        .map(|t| {
            let pool = pool.clone();
            let checked_out = checked_out.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        let mut rng = Rng::new(0x5EED ^ t as u64);
                        start.wait();

                        let mut held = Vec::new();
                        let mut taken = 0u64;
                        for _ in 0..fuzz_steps() {
                            match rng.below(10) {
                                0..=4 if held.len() < max_size => {
                                    if let Ok(conn) = pool.acquire().await {
                                        let id = conn.conn_id();
                                        assert!(
                                            checked_out.lock().unwrap().insert(id),
                                            "thread {t}: connection {id} was already checked \
                                             out by another thread"
                                        );
                                        held.push(conn);
                                    }
                                }
                                5..=7 if !held.is_empty() => {
                                    let i = rng.below(held.len());
                                    let conn = held.remove(i);
                                    checked_out.lock().unwrap().remove(&conn.conn_id());
                                    drop(conn);
                                }
                                8 if !held.is_empty() => {
                                    let i = rng.below(held.len());
                                    let conn = held.remove(i);
                                    checked_out.lock().unwrap().remove(&conn.conn_id());
                                    conn.poison();
                                    drop(conn);
                                }
                                _ if !held.is_empty() => {
                                    let i = rng.below(held.len());
                                    let conn = held.remove(i);
                                    checked_out.lock().unwrap().remove(&conn.conn_id());
                                    let _escaped = conn.take();
                                    taken += 1;
                                }
                                _ => {}
                            }
                            assert!(
                                pool.local_size() <= max_size,
                                "thread {t}: shard over max_size"
                            );
                        }

                        for conn in held.drain(..) {
                            checked_out.lock().unwrap().remove(&conn.conn_id());
                        }
                        taken
                    })
            })
        })
        .collect();

    let taken: u64 = threads.into_iter().map(|t| t.join().unwrap()).sum();

    assert!(
        checked_out.lock().unwrap().is_empty(),
        "every thread returned what it held"
    );
    let m = pool.metrics();
    assert_eq!(m.live, 0, "every shard went with its thread: {m:?}");
    assert!(
        m.parked as usize <= capacity,
        "reservoir over capacity: {m:?}"
    );
    assert_eq!(
        m.created,
        m.closed + m.parked + taken,
        "connections lost across threads (taken {taken}): {m:?}"
    );
    assert!(
        m.unparked > 0,
        "the threads should have stolen from each other"
    );
}

// ---- The conservation gap, pinned directly --------------------------------
//
// Found by the conservation checker above. Both are recorded here on their own so
// the behaviour is explicit rather than buried in a tally field.

/// `Pool::close` clears the reservoir, and the connections it drops are not
/// counted closed.
#[compio::test]
async fn close_does_not_count_the_parked_connections_it_discards() {
    let _lock = common::detach_lock();
    let pool = Pool::builder(MovableManager::new())
        .config(cfg().max_size(4).min_idle(0))
        .exchange(Reservoir::new(8))
        .build();

    let held: Vec<_> = {
        let mut v = Vec::new();
        for _ in 0..3 {
            v.push(pool.acquire().await.unwrap());
        }
        v
    };
    drop(held);
    assert_eq!(pool.metrics().parked, 3);

    pool.close();

    let m = pool.metrics();
    assert_eq!(m.parked, 0, "they are gone");
    assert_eq!(
        (m.created, m.closed),
        (3, 0),
        "but `closed` did not move: {m:?}"
    );
    assert_eq!(
        m.created - (m.closed + m.live + m.parked),
        3,
        "three connections left the counters without being accounted for"
    );
}

/// Same for `Pool::invalidate`, which is worse in practice: the pool stays in
/// use afterwards, so the drift accumulates on every rotation.
#[compio::test]
async fn invalidate_does_not_count_the_parked_connections_it_discards() {
    let _lock = common::detach_lock();
    let pool = Pool::builder(MovableManager::new())
        .config(cfg().max_size(4).min_idle(0))
        .exchange(Reservoir::new(8))
        .build();

    for round in 1..=3u64 {
        drop(pool.acquire().await.unwrap());
        assert_eq!(pool.metrics().parked, 1);
        pool.invalidate();

        let m = pool.metrics();
        assert_eq!(
            m.created - (m.closed + m.live + m.parked),
            round,
            "round {round}: the drift grows by one per invalidate: {m:?}"
        );
    }
}

// ---- Cancellation, at randomized points ------------------------------------

/// `acquire` claims shard budget *before* awaiting `connect`, `unpark` or
/// `recycle`, and is itself cancellable. A cancellation landing inside one of
/// those awaits must hand the budget back, or the shard bleeds capacity until it
/// sits at `max_size` holding nothing.
///
/// A manager that hangs turns the acquire timeout into a cancellation at exactly
/// those points; the switches flip between bursts so each burst is coherent.
#[compio::test]
async fn randomized_cancellations_never_leak_shard_capacity() {
    let mut total_timeouts = 0;

    for seed in 0..fuzz_seeds() {
        let mut rng = Rng::new(seed ^ 0xCA11);
        let max_size = rng.in_range(1, 4);

        let pool = Pool::new(
            LocalManager::new(),
            cfg()
                .max_size(max_size)
                .min_idle(rng.below(2))
                // Every acquire in a hung burst burns this whole budget, so keep
                // it tight; 1ms is ample to land the cancellation inside the
                // awaited `connect` or `recycle`.
                .acquire_timeout(Duration::from_millis(1)),
        );

        let mut tally = common::Tally::default();
        for burst in 0..12 {
            // A hung `connect` cancels inside the empty reservation; a hung
            // `recycle` cancels while a connection is riding along with it.
            // Some bursts are forced rather than sampled, so a seed cannot get
            // through the run without ever cancelling anything.
            pool.manager()
                .set_hang_connect(burst % 4 == 1 || rng.chance(20));
            pool.manager()
                .set_hang_recycle(burst % 4 == 3 || rng.chance(20));

            // `Op::Warm` is excluded deliberately. Unlike `acquire`, `warm` is
            // not wrapped in the acquire timeout, so against a hung `connect` it
            // never returns and would wedge the whole run. Both of `warm`'s
            // cancellation problems are pinned separately below.
            let ops: Vec<Op> = (0..fuzz_steps() / 12 + 1)
                .map(|_| match Op::sample(&mut rng) {
                    Op::Warm => Op::Acquire,
                    other => other,
                })
                .collect();
            run_ops(
                &pool,
                max_size,
                &ops,
                &format!("seed {seed} burst {burst}"),
                &mut tally,
            )
            .await;
        }

        // The shard must be able to serve again once the backend is healthy. A
        // leaked reservation shows up here as a timeout that never clears.
        pool.manager().set_hang_connect(false);
        pool.manager().set_hang_recycle(false);
        if !pool.is_closed() {
            assert!(
                pool.acquire().await.is_ok(),
                "seed {seed}: the shard wedged after randomized cancellation, \
                 size {} of max {max_size}: {:?}",
                pool.local_size(),
                pool.metrics()
            );
        }
        total_timeouts += tally.timeouts;
    }

    assert!(
        total_timeouts > 0,
        "no acquire was ever cancelled, so the run proved nothing"
    );
}

// ---- `warm` is not cancellation-safe --------------------------------------
//
// Both of these were found by the randomized cancellation suite above, which is
// why it has to exclude `Op::Warm`. They are recorded here explicitly so the
// behaviour is a documented fact rather than an omission.

/// `Pool::warm` does not honour [`Config::acquire_timeout`], so a backend whose
/// handshake never completes hangs it indefinitely.
#[compio::test]
async fn warm_does_not_honour_the_acquire_timeout() {
    let pool = Pool::new(
        LocalManager::new(),
        cfg()
            .max_size(4)
            .min_idle(2)
            .acquire_timeout(Duration::from_millis(10)),
    );
    pool.manager().set_hang_connect(true);

    // The timeout here is the test's own; `warm` supplies none. `acquire` under
    // the same conditions returns `Error::Timeout` after 10ms.
    let outcome = compio::time::timeout(Duration::from_millis(60), pool.warm()).await;
    assert!(
        outcome.is_err(),
        "warm returned on its own, so it does bound its wait after all"
    );

    pool.manager().set_hang_connect(false);
    assert!(
        compio::time::timeout(Duration::from_millis(50), pool.acquire())
            .await
            .is_ok(),
        "acquire, by contrast, is bounded"
    );
}

/// And when `warm` *is* cancelled, the shard budget it claimed is lost for good.
///
/// `acquire` guards every await with a reservation that hands the claim back;
/// `warm` claims with `try_reserve` and awaits `connect` unguarded, so a
/// cancellation there is permanent. Four cancellations against `max_size` 4 wedge
/// the shard with no connections at all.
#[compio::test]
async fn a_cancelled_warm_leaks_shard_capacity_permanently() {
    let max_size = 4;
    let pool = Pool::new(
        LocalManager::new(),
        cfg()
            .max_size(max_size)
            .min_idle(max_size)
            .acquire_timeout(Duration::from_millis(20)),
    );
    pool.manager().set_hang_connect(true);

    for expected in 1..=max_size {
        let _ = compio::time::timeout(Duration::from_millis(15), pool.warm()).await;
        assert_eq!(
            pool.local_size(),
            expected,
            "each cancelled warm keeps the slot it claimed"
        );
    }

    // The shard now believes it is full while holding nothing.
    let m = pool.metrics();
    assert_eq!(pool.local_size(), max_size);
    assert_eq!((m.live, m.idle, m.created), (0, 0, 0));

    pool.manager().set_hang_connect(false);
    assert!(
        matches!(pool.acquire().await, Err(Error::Timeout)),
        "the shard is wedged at max_size with zero real connections"
    );
}

/// Closing part-way through a workload.
///
/// `Op::decode` never samples `Close`, so this splices it in at a random step and
/// keeps going: every checkout still outstanding has to come back and be
/// destroyed rather than pooled, every later acquire has to fail with
/// [`Error::Closed`], and the accounting has to stay straight across the
/// transition.
#[compio::test]
async fn randomized_workloads_closed_partway_through_stay_consistent() {
    let mut total_refusals = 0;

    for seed in 0..fuzz_seeds() {
        let mut rng = Rng::new(seed ^ 0xC105E);
        let max_size = rng.in_range(1, 4);
        let steps = fuzz_steps().max(8);
        let close_at = rng.in_range(steps / 4, steps * 3 / 4);

        let mut ops: Vec<Op> = (0..steps).map(|_| Op::sample(&mut rng)).collect();
        ops[close_at] = Op::Close;

        let pool = Pool::new(
            LocalManager::new(),
            cfg()
                .max_size(max_size)
                .acquire_timeout(Duration::from_millis(2)),
        );

        let mut tally = common::Tally::default();
        run_ops(&pool, max_size, &ops, &format!("seed {seed}"), &mut tally).await;

        assert!(pool.is_closed(), "seed {seed}: the pool should be closed");
        let m = pool.metrics();
        assert_eq!(
            m.created,
            m.closed + m.live + m.parked + tally.taken + tally.cleared,
            "seed {seed}: final accounting: {m:?} {tally:?}"
        );
        assert!(
            matches!(pool.acquire().await, Err(Error::Closed)),
            "seed {seed}: a closed pool must keep refusing"
        );
        total_refusals += tally.closed_errors;
    }

    assert!(
        total_refusals > 0,
        "no acquire ever hit the closed pool, so the transition was never exercised"
    );
}

/// A backend whose dials are refused and whose liveness checks reject, at random.
///
/// Both failure paths destroy a connection and fall through to the next option,
/// so the accounting has to stay straight, the acquire loop has to terminate
/// rather than cycling on a connection it keeps rejecting, and the shard has to
/// recover once the backend does.
#[compio::test]
async fn randomized_workloads_survive_refused_dials_and_rejected_recycles() {
    let mut total_recycle_failures = 0;
    let mut total_backend_errors = 0;

    for seed in 0..fuzz_seeds() {
        let mut rng = Rng::new(seed ^ 0x0BAD_F00D);
        let max_size = rng.in_range(1, 4);
        let pool = Pool::new(
            LocalManager::new(),
            cfg()
                .max_size(max_size)
                .min_idle(rng.below(2))
                .acquire_timeout(Duration::from_millis(2)),
        );

        let mut tally = common::Tally::default();
        for burst in 0..12 {
            // Some bursts are forced rather than sampled, so no seed gets through
            // without exercising both failure paths.
            pool.manager()
                .set_fail_connect(burst % 5 == 2 || rng.chance(15));
            pool.manager()
                .set_fail_recycle(burst % 3 == 1 || rng.chance(25));

            let ops: Vec<Op> = (0..fuzz_steps() / 12 + 1)
                .map(|_| Op::sample(&mut rng))
                .collect();
            run_ops(
                &pool,
                max_size,
                &ops,
                &format!("seed {seed} burst {burst}"),
                &mut tally,
            )
            .await;
        }

        let m = pool.metrics();
        assert_eq!(
            m.created,
            m.closed + m.live + m.parked + tally.taken + tally.cleared,
            "seed {seed}: final accounting: {m:?} {tally:?}"
        );

        pool.manager().set_fail_connect(false);
        pool.manager().set_fail_recycle(false);
        if !pool.is_closed() {
            assert!(
                pool.acquire().await.is_ok(),
                "seed {seed}: the shard wedged after a failing backend: {m:?}"
            );
        }
        total_recycle_failures += m.recycle_failures;
        total_backend_errors += tally.backend_errors;
    }

    assert!(
        total_recycle_failures > 0,
        "no recycle was ever rejected, so that path was never exercised"
    );
    assert!(
        total_backend_errors > 0,
        "no dial was ever refused, so that path was never exercised"
    );
}
