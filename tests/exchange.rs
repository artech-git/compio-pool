//! The cross-thread exchange, driven through a real pool.
//!
//! Parking and stealing are observable on a single thread too: with `min_idle`
//! at zero a returned connection is parked, and the next checkout on the same
//! shard claims it back out of the reservoir. That makes the accounting easy to
//! pin down; the genuinely cross-thread cases live at the bottom.

mod common;

use std::{
    sync::{Arc, Barrier, atomic::Ordering::SeqCst},
    time::Duration,
};

use common::{MovableManager, cfg};
use compio_pool::{Pool, Reservoir};

fn pooled(
    max_size: usize,
    min_idle: usize,
    capacity: usize,
) -> Pool<MovableManager, Reservoir<MovableManager>> {
    Pool::builder(MovableManager::new())
        .config(cfg().max_size(max_size).min_idle(min_idle))
        .exchange(Reservoir::new(capacity))
        .build()
}

#[compio::test]
async fn a_surplus_connection_is_parked_rather_than_kept() {
    let _lock = common::detach_lock();
    let pool = pooled(4, 0, 8);

    drop(pool.acquire().await.unwrap());

    let m = pool.metrics();
    assert_eq!(m.parked, 1, "min_idle is zero, so everything is surplus");
    assert_eq!(m.live, 0, "a parked connection belongs to no shard");
    assert_eq!(m.idle, 0, "and is not in any free list");
    assert_eq!(pool.local_size(), 0, "its budget went back to the shard");
    assert_eq!(pool.local_idle(), 0);
    assert_eq!(m.closed, 0, "parking is not closing");
}

#[compio::test]
async fn a_parked_connection_is_claimed_instead_of_dialling() {
    let _lock = common::detach_lock();
    let pool = pooled(4, 0, 8);
    let counts = pool.manager().counts();

    let first_id = {
        let conn = pool.acquire().await.unwrap();
        conn.id
    };
    assert_eq!(pool.metrics().parked, 1);

    let conn = pool.acquire().await.unwrap();
    assert_eq!(conn.id, first_id, "the same socket came back");
    assert_eq!(counts.connected(), 1, "and no handshake was paid for");
    let m = pool.metrics();
    assert_eq!(m.unparked, 1);
    assert_eq!(m.parked, 0);
    assert_eq!(m.live, 1, "it belongs to this shard again");
}

/// `min_idle` is the split point: that many stay local for the lock-free hot
/// path, and only the surplus is offered around.
#[compio::test]
async fn min_idle_is_kept_locally_and_only_the_surplus_is_parked() {
    let _lock = common::detach_lock();
    let pool = pooled(4, 2, 8);

    let held: Vec<_> = {
        let mut v = Vec::new();
        for _ in 0..4 {
            v.push(pool.acquire().await.unwrap());
        }
        v
    };
    drop(held);

    let m = pool.metrics();
    assert_eq!(pool.local_idle(), 2, "min_idle stays warm on this thread");
    assert_eq!(m.parked, 2, "the rest is shared");
    assert_eq!(m.live, 2);
    assert_eq!(m.created, m.live + m.parked, "nothing lost: {m:?}");
}

/// A local free list is always preferred to the reservoir: it costs no atomics.
#[compio::test]
async fn a_local_idle_connection_is_preferred_to_a_parked_one() {
    let _lock = common::detach_lock();
    let pool = pooled(4, 1, 8);

    let a = pool.acquire().await.unwrap();
    let b = pool.acquire().await.unwrap();
    let (a_id, b_id) = (a.id, b.id);
    drop(a); // min_idle 1 is empty, so this one stays local
    drop(b); // over min_idle now, so this one is parked

    assert_eq!(pool.local_idle(), 1);
    assert_eq!(pool.metrics().parked, 1);

    let conn = pool.acquire().await.unwrap();
    assert_eq!(conn.id, a_id, "the local one, not the parked one");
    assert_ne!(conn.id, b_id);
    assert_eq!(
        pool.metrics().unparked,
        0,
        "the reservoir was never touched"
    );
}

/// Somebody already waiting on this thread outranks the reservoir, however much
/// surplus there is.
#[compio::test]
async fn a_returned_connection_is_not_parked_while_a_local_waiter_wants_it() {
    let _lock = common::detach_lock();
    let pool = Pool::builder(MovableManager::new())
        .config(cfg().max_size(1).min_idle(0).acquire_timeout(None))
        .exchange(Reservoir::new(8))
        .build();

    let held = pool.acquire().await.unwrap();
    let id = held.id;
    let waiting = compio::runtime::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await.map(|c| c.id) }
    });
    compio::time::sleep(Duration::from_millis(20)).await;

    drop(held);
    assert_eq!(
        pool.metrics().parked,
        0,
        "a waiter on this thread comes before another thread's shard"
    );
    assert_eq!(waiting.await.unwrap().unwrap(), id);
    assert_eq!(pool.manager().counts().connected(), 1);
}

#[compio::test]
async fn a_connection_that_cannot_reattach_is_replaced_by_a_fresh_dial() {
    let _lock = common::detach_lock();
    let pool = pooled(4, 0, 8);
    let counts = pool.manager().counts();

    drop(pool.acquire().await.unwrap());
    assert_eq!(pool.metrics().parked, 1);

    common::CAN_ATTACH.store(false, SeqCst);
    let conn = pool.acquire().await.unwrap();

    assert_eq!(counts.connected(), 2, "the caller fell back to dialling");
    assert_eq!(conn.id, 1);
    let m = pool.metrics();
    assert_eq!(m.parked, 0);
    assert_eq!(m.unparked, 0, "nothing was successfully claimed");
    assert_eq!(
        m.closed, 1,
        "the connection lost inside attach must still be counted: {m:?}"
    );
    assert_eq!(m.created, m.closed + m.live, "conserved: {m:?}");
}

#[compio::test]
async fn close_empties_the_reservoir() {
    let _lock = common::detach_lock();
    let pool = pooled(4, 0, 8);

    drop(pool.acquire().await.unwrap());
    assert_eq!(pool.metrics().parked, 1);

    pool.close();
    assert_eq!(pool.metrics().parked, 0, "close drops what is parked");
}

/// `invalidate` clears the reservoir outright rather than bumping generations on
/// entries it cannot reach, so nothing stale survives a failover.
#[compio::test]
async fn invalidate_empties_the_reservoir() {
    let _lock = common::detach_lock();
    let pool = pooled(4, 0, 8);
    let counts = pool.manager().counts();

    let old_id = {
        let conn = pool.acquire().await.unwrap();
        conn.id
    };
    assert_eq!(pool.metrics().parked, 1);

    pool.invalidate();
    assert_eq!(pool.metrics().parked, 0);

    let conn = pool.acquire().await.unwrap();
    assert_ne!(conn.id, old_id, "a retired socket must not come back");
    assert_eq!(counts.connected(), 2);
}

/// Capacity freed by a claim must be reusable, or a reservoir would accept only
/// `capacity` parks over the pool's whole lifetime.
#[compio::test]
async fn a_reservoir_can_be_reused_indefinitely() {
    let _lock = common::detach_lock();
    let pool = pooled(2, 0, 1);
    let counts = pool.manager().counts();

    for _ in 0..20 {
        drop(pool.acquire().await.unwrap());
    }

    assert_eq!(
        counts.connected(),
        1,
        "one socket, parked and claimed 20 times"
    );
    assert_eq!(pool.metrics().parked, 1);
    assert_eq!(pool.metrics().unparked, 19);
}

/// A connection `detach` refuses is already gone, so the pool accounts for it as
/// closed and dials a replacement.
#[compio::test]
async fn a_connection_that_cannot_detach_is_accounted_for_as_closed() {
    let _lock = common::detach_lock();
    let pool = pooled(2, 0, 8);
    let counts = pool.manager().counts();
    common::CAN_DETACH.store(false, SeqCst);

    drop(pool.acquire().await.unwrap());

    let m = pool.metrics();
    assert_eq!((m.parked, m.live, m.closed), (0, 0, 1));
    assert_eq!(
        m.created, m.closed,
        "created and closed must balance: {m:?}"
    );
    assert_eq!(
        counts.disconnected(),
        0,
        "detach consumed it, so `disconnect` never saw it"
    );
    assert_eq!(pool.local_size(), 0, "and the budget came back");
}

// ---- Genuinely cross-thread ------------------------------------------------

/// The point of the reservoir: a busy thread picks up what a quiet one is not
/// using, instead of paying for a handshake.
#[test]
fn a_busy_thread_claims_a_quiet_threads_idle_connection() {
    let _lock = common::detach_lock();
    let pool = pooled(4, 0, 8);
    let counts = pool.manager().counts();

    let parked_id = std::thread::spawn({
        let pool = pool.clone();
        move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move { pool.acquire().await.unwrap().id })
        }
    })
    .join()
    .unwrap();
    assert_eq!(pool.metrics().parked, 1);

    let claimed_id = std::thread::spawn({
        let pool = pool.clone();
        move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move { pool.acquire().await.unwrap().id })
        }
    })
    .join()
    .unwrap();

    assert_eq!(claimed_id, parked_id);
    assert_eq!(counts.connected(), 1, "one dial across both threads");
    assert_eq!(pool.metrics().unparked, 1);
}

/// Under contention nothing may be lost or handed to two threads at once:
/// everything ever created is live, parked, or closed.
#[test]
fn heavy_contention_conserves_every_connection() {
    let _lock = common::detach_lock();
    let pool = pooled(4, 1, 16);

    let start = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let pool = pool.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        start.wait();
                        for _ in 0..150 {
                            let a = pool.acquire().await.unwrap();
                            let b = pool.acquire().await.unwrap();
                            assert_ne!(a.id, b.id, "one socket handed out twice");
                            drop(a);
                            drop(b);
                        }
                    })
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    let m = pool.metrics();
    assert_eq!(m.live, 0, "every shard is gone with its thread");
    assert_eq!(m.created, m.closed + m.parked, "conserved: {m:?}");
    assert!(
        m.parked <= 16,
        "the reservoir must never exceed its capacity, got {}",
        m.parked
    );
    assert!(m.unparked > 0, "threads should have stolen from each other");
}

/// A reservoir smaller than the fleet still has to behave: the overflow simply
/// stays on the shards that opened it.
#[test]
fn a_reservoir_smaller_than_the_surplus_keeps_the_rest_local() {
    let _lock = common::detach_lock();
    let pool = pooled(3, 0, 2);

    let done = Arc::new(Barrier::new(4));
    let threads: Vec<_> = (0..4)
        .map(|_| {
            let pool = pool.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        let held: Vec<_> = {
                            let mut v = Vec::new();
                            for _ in 0..3 {
                                v.push(pool.acquire().await.unwrap());
                            }
                            v
                        };
                        // Hold everything at once, so the reservoir is offered
                        // far more than it can take.
                        done.wait();
                        drop(held);
                        (pool.local_idle(), pool.metrics().parked)
                    })
            })
        })
        .collect();

    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    for (_, parked) in &results {
        assert!(*parked <= 2, "capacity exceeded: {parked}");
    }
    assert!(
        results.iter().any(|(idle, _)| *idle > 0),
        "a refused offer must leave the connection on its own shard: {results:?}"
    );

    let m = pool.metrics();
    assert_eq!(m.created, m.closed + m.parked, "conserved: {m:?}");
}
