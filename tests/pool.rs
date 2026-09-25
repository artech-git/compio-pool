//! Behavioural tests for the sharded pool.

mod common;

use std::{
    sync::{Arc, Barrier, atomic::Ordering::SeqCst},
    time::Duration,
};

use common::{LocalManager, MovableManager};
use compio_pool::{Config, Error, Pool, Reservoir};

fn cfg() -> Config {
    // Disable the reaper's own timers so tests observe only what they trigger.
    Config::new()
        .max_lifetime(None)
        .idle_timeout(None)
        .acquire_timeout(Duration::from_millis(200))
}

#[compio::test]
async fn returns_and_reuses_the_same_connection() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4));

    let first = pool.acquire().await.unwrap().id;
    // Guard dropped here: the connection goes back to this thread's free list.
    let second = pool.acquire().await.unwrap().id;

    assert_eq!(
        first, second,
        "the warm connection should be handed back out"
    );
    assert_eq!(counts.connected.load(SeqCst), 1, "no second dial");
    assert_eq!(
        counts.recycled.load(SeqCst),
        1,
        "recycle runs on reuse, not first use"
    );
}

#[compio::test]
async fn max_size_is_enforced_per_shard() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));

    let _a = pool.acquire().await.unwrap();
    let _b = pool.acquire().await.unwrap();
    assert_eq!(pool.local_size(), 2);

    // Third checkout has nowhere to go and must time out rather than grow.
    match pool.acquire().await {
        Err(Error::Timeout) => {}
        other => panic!("expected Timeout, got {:?}", other.map(|c| c.id)),
    }
    assert_eq!(pool.metrics().timeouts, 1);
}

#[compio::test]
async fn a_waiter_is_woken_when_a_connection_comes_back() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1));

    let held = pool.acquire().await.unwrap();

    let waiting = compio::runtime::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await.map(|c| c.id) }
    });

    // Let the spawned task reach the wait queue, then hand the connection back.
    compio::time::sleep(Duration::from_millis(20)).await;
    drop(held);

    let id = waiting
        .await
        .unwrap()
        .expect("waiter should be handed the returned conn");
    assert_eq!(id, 0);
    assert_eq!(pool.metrics().waits, 1);
}

/// A waiter that is notified and then cancelled before it can be polled must
/// hand the wakeup on, or the connection sits idle while the next waiter sleeps.
/// Driven by hand rather than by timers, so the interleaving is exact.
#[compio::test]
async fn a_cancelled_waiter_hands_its_wakeup_to_the_next() {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    let pool = Pool::new(LocalManager::new(), cfg().max_size(1).acquire_timeout(None));
    let held = pool.acquire().await.unwrap();

    let mut cx = Context::from_waker(Waker::noop());
    let mut first = Box::pin(pool.acquire());
    let mut second = Box::pin(pool.acquire());

    // Both futures reach the wait queue, in order.
    assert!(matches!(first.as_mut().poll(&mut cx), Poll::Pending));
    assert!(matches!(second.as_mut().poll(&mut cx), Poll::Pending));

    // Returning the connection notifies `first`; `first` is then cancelled
    // before it is ever polled again. This is the exact interleaving that
    // loses a wakeup if `WaitForSlot::drop` does not pass it along.
    drop(held);
    drop(first);

    match second.as_mut().poll(&mut cx) {
        Poll::Ready(Ok(conn)) => assert_eq!(conn.id, 0),
        Poll::Ready(Err(e)) => panic!("unexpected error: {e:?}"),
        Poll::Pending => panic!("the cancelled waiter swallowed its wakeup"),
    }
}

#[compio::test]
async fn dropping_an_op_guard_poisons_the_connection() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4));

    {
        let conn = pool.acquire().await.unwrap();
        let _op = conn.begin_op(); // never completed: models a cancelled await
        assert!(!conn.is_poisoned());
        drop(_op);
        assert!(conn.is_poisoned(), "a dropped OpGuard must poison");
    }

    assert_eq!(
        counts.disconnected.load(SeqCst),
        1,
        "poisoned conn must be closed"
    );
    assert_eq!(pool.metrics().poisoned, 1);
    assert_eq!(pool.local_idle(), 0, "a poisoned conn must not be pooled");

    // Completing the op leaves the connection reusable.
    {
        let conn = pool.acquire().await.unwrap();
        conn.begin_op().complete_op();
    }
    assert_eq!(pool.local_idle(), 1);
    assert_eq!(counts.disconnected.load(SeqCst), 1);
}

#[compio::test]
async fn invalidate_retires_existing_connections() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4));

    let old_id = pool.acquire().await.unwrap().id;
    assert_eq!(pool.local_idle(), 1);

    pool.invalidate();

    let new_id = pool.acquire().await.unwrap().id;
    assert_ne!(old_id, new_id, "stale-generation conn must not be reused");
    assert_eq!(counts.connected.load(SeqCst), 2);
    assert_eq!(counts.disconnected.load(SeqCst), 1);
}

#[compio::test]
async fn max_uses_retires_a_connection() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2).max_uses(2u64));

    for _ in 0..2 {
        let _c = pool.acquire().await.unwrap();
    }
    // Two checkouts done. `release` stamps `uses` before testing expiry, so the
    // second return is what retires it; this third call dials a replacement.
    let _c = pool.acquire().await.unwrap();
    assert_eq!(counts.connected.load(SeqCst), 2);
    assert_eq!(counts.disconnected.load(SeqCst), 1);
}

#[compio::test]
async fn a_failed_recycle_discards_and_redials() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4));

    let first = pool.acquire().await.unwrap().id;
    pool.manager().set_fail_recycle(true);

    let second = pool.acquire().await.unwrap().id;
    assert_ne!(first, second);
    assert_eq!(counts.disconnected.load(SeqCst), 1);
    assert_eq!(pool.metrics().recycle_failures, 1);
}

#[compio::test]
async fn close_rejects_further_checkouts() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4));

    let conn = pool.acquire().await.unwrap();
    pool.close();
    assert!(pool.is_closed());

    drop(conn);
    assert_eq!(
        counts.disconnected.load(SeqCst),
        1,
        "returned conn closes, not pools"
    );
    assert!(matches!(pool.acquire().await, Err(Error::Closed)));
}

#[compio::test]
async fn warm_preopens_min_idle_on_this_thread() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(8).min_idle(3));

    pool.warm().await.unwrap();
    assert_eq!(counts.connected.load(SeqCst), 3);
    assert_eq!(pool.local_idle(), 3);
}

/// Each compio thread gets its own shard, so `max_size` is per thread and a
/// connection opened on one thread is never handed to another.
#[test]
fn shards_are_independent_per_thread() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(1));

    let barrier = Arc::new(Barrier::new(2));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let pool = pool.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        let conn = pool.acquire().await.unwrap();
                        let id = conn.id;
                        // Hold it while the other thread acquires: if the shards
                        // were shared, one of the two would time out at max_size 1.
                        barrier.wait();
                        drop(conn);
                        id
                    })
            })
        })
        .collect();

    let mut ids: Vec<u64> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    ids.sort();
    assert_eq!(ids, vec![0, 1], "each thread dialled its own connection");
    assert_eq!(counts.connected.load(SeqCst), 2);
    // Both threads have exited, so their thread-locals dropped and each shard
    // closed the connection it owned - on the thread whose driver owned it.
    assert_eq!(pool.metrics().live, 0);
    assert_eq!(pool.metrics().closed, 2);
    assert_eq!(
        counts.disconnected.load(SeqCst),
        2,
        "thread exit must close connections through the manager"
    );
}

/// With a `Reservoir`, an idle connection from one thread is picked up by
/// another instead of sitting unused.
#[test]
fn reservoir_moves_a_connection_between_threads() {
    // Without the lock, a sibling test flipping `CAN_ATTACH` can make thread B's
    // reattach fail, and B then dials instead of claiming.
    let _lock = common::detach_lock();
    let manager = MovableManager::new();
    let counts = manager.counts();
    let pool = Pool::builder(manager)
        .config(cfg().max_size(4).min_idle(0))
        .exchange(Reservoir::new(8))
        .build();

    // Thread A opens one connection and returns it; min_idle is 0, so the
    // surplus is parked in the shared reservoir rather than kept locally.
    let a = std::thread::spawn({
        let pool = pool.clone();
        move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let conn = pool.acquire().await.unwrap();
                    conn.id
                })
        }
    });
    let parked_id = a.join().unwrap();
    assert_eq!(
        pool.metrics().parked,
        1,
        "surplus should be parked, not held"
    );
    assert_eq!(pool.metrics().live, 0, "a parked conn belongs to no shard");

    // Thread B claims it rather than dialling again.
    let b = std::thread::spawn({
        let pool = pool.clone();
        move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let conn = pool.acquire().await.unwrap();
                    conn.id
                })
        }
    });
    let claimed_id = b.join().unwrap();

    assert_eq!(
        claimed_id, parked_id,
        "thread B should reuse thread A's connection"
    );
    assert_eq!(
        counts.connected.load(SeqCst),
        1,
        "exactly one dial across both threads"
    );
    assert_eq!(pool.metrics().unparked, 1);
}

/// Without an exchange, the same workload dials once per thread.
#[test]
fn without_an_exchange_threads_do_not_share() {
    let manager = MovableManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4));

    for _ in 0..2 {
        let pool = pool.clone();
        std::thread::spawn(move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move { pool.acquire().await.unwrap().id })
        })
        .join()
        .unwrap();
    }

    assert_eq!(counts.connected.load(SeqCst), 2);
    assert_eq!(pool.metrics().parked, 0);
}

/// `acquire` claims shard budget *before* awaiting `connect`, and `acquire` is
/// itself cancellable via the acquire timeout. If a cancelled attempt did not
/// give the budget back, a shard would silently lose capacity on every hung
/// dial until it deadlocked at `max_size` with zero real connections.
#[compio::test]
async fn a_timed_out_dial_gives_its_shard_budget_back() {
    let pool = Pool::new(
        LocalManager::new(),
        cfg().max_size(1).acquire_timeout(Duration::from_millis(30)),
    );
    pool.manager().set_hang_connect(true);

    for _ in 0..3 {
        assert!(matches!(pool.acquire().await, Err(Error::Timeout)));
        assert_eq!(pool.local_size(), 0, "cancelled dial leaked shard capacity");
    }

    // The shard must still be able to serve a connection afterwards.
    pool.manager().set_hang_connect(false);
    assert!(
        pool.acquire().await.is_ok(),
        "shard deadlocked after hung dials"
    );
}

/// The reservoir is bounded: surplus beyond its capacity is handed back and
/// stays on the shard rather than being silently dropped.
#[compio::test]
async fn reservoir_capacity_is_respected() {
    let _lock = common::detach_lock();
    let manager = MovableManager::new();
    let pool = Pool::builder(manager)
        .config(cfg().max_size(4).min_idle(0))
        .exchange(Reservoir::new(2))
        .build();

    // Hold all four at once so each one is a separate connection, then let
    // them all go back at the same time.
    let held: Vec<_> = {
        let mut v = Vec::new();
        for _ in 0..4 {
            v.push(pool.acquire().await.unwrap());
        }
        v
    };
    drop(held);

    let m = pool.metrics();
    assert_eq!(
        m.parked, 2,
        "the reservoir should fill to capacity and stop"
    );
    assert_eq!(pool.local_idle(), 2, "the refused two stay on this shard");
    assert_eq!(m.live, 2, "a parked connection belongs to no shard");
    assert_eq!(m.closed, 0, "refusing an offer must not close anything");
}

/// A connection that cannot be detached is consumed by `detach`, so the pool
/// has to account for it as closed rather than leak it out of the counters.
#[compio::test]
async fn a_connection_that_cannot_detach_is_closed() {
    let _lock = common::detach_lock();
    let manager = MovableManager::new();
    let counts = manager.counts();
    let pool = Pool::builder(manager)
        .config(cfg().max_size(2).min_idle(0))
        .exchange(Reservoir::new(8))
        .build();

    common::CAN_DETACH.store(false, SeqCst);

    let held: Vec<_> = {
        let mut v = Vec::new();
        for _ in 0..2 {
            v.push(pool.acquire().await.unwrap());
        }
        v
    };
    drop(held);

    let m = pool.metrics();
    assert_eq!(m.parked, 0, "nothing could be made portable");
    assert_eq!(m.live, 0, "and nothing was kept");
    assert_eq!(
        m.closed, 2,
        "a connection consumed by a failed detach must still be counted closed"
    );
    assert_eq!(m.created, m.closed, "created and closed must balance");
    assert_eq!(
        counts.disconnected.load(SeqCst),
        0,
        "detach consumed them, so `disconnect` never saw them"
    );
}

/// A parked connection that cannot be reattached is counted closed, and the
/// claiming thread falls back to dialling.
#[test]
fn a_failed_attach_is_counted_closed_and_redials() {
    let _lock = common::detach_lock();
    let manager = MovableManager::new();
    let counts = manager.counts();
    let pool = Pool::builder(manager)
        .config(cfg().max_size(2).min_idle(0))
        .exchange(Reservoir::new(8))
        .build();

    // Thread A parks one.
    std::thread::spawn({
        let pool = pool.clone();
        move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    pool.acquire().await.unwrap();
                })
        }
    })
    .join()
    .unwrap();
    assert_eq!(pool.metrics().parked, 1);
    assert_eq!(counts.connected.load(SeqCst), 1);

    // Thread B pops it, fails to reattach, and dials instead.
    common::CAN_ATTACH.store(false, SeqCst);
    std::thread::spawn({
        let pool = pool.clone();
        move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    pool.acquire().await.unwrap();
                })
        }
    })
    .join()
    .unwrap();

    let m = pool.metrics();
    assert_eq!(counts.connected.load(SeqCst), 2, "B had to dial its own");
    assert!(
        m.closed >= 1,
        "the connection lost to a failed attach must be counted closed, got {}",
        m.closed
    );
}

/// Many threads parking and stealing at once must not lose or duplicate a
/// connection: everything ever created is live, parked, or closed.
#[test]
fn concurrent_parking_and_stealing_conserves_connections() {
    let _lock = common::detach_lock();
    let manager = MovableManager::new();
    let pool = Pool::builder(manager)
        .config(cfg().max_size(4).min_idle(0))
        .exchange(Reservoir::new(16))
        .build();

    let threads: Vec<_> = (0..8)
        .map(|_| {
            let pool = pool.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        for _ in 0..200 {
                            let a = pool.acquire().await.unwrap();
                            let b = pool.acquire().await.unwrap();
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
    assert_eq!(m.live, 0, "every shard is gone, so nothing is held locally");
    assert_eq!(
        m.created,
        m.closed + m.parked,
        "created must equal closed + parked: {m:?}"
    );
    assert!(
        m.parked <= 16,
        "the reservoir must never exceed its capacity, got {}",
        m.parked
    );
    assert!(m.unparked > 0, "threads should have stolen from each other");
}
