//! Degenerate and extreme configurations — the ends of every range the public
//! API accepts.
//!
//! Sizes that cannot be represented, timeouts of zero, policies that retire a
//! connection the instant it is created, a reaper that never sleeps, hundreds of
//! waiters on one slot. None of these are sensible settings; all of them are
//! reachable, so none of them may hang, panic, spin forever or lose a
//! connection.

mod common;

use std::{
    sync::{Arc, Barrier},
    time::Duration,
};

use common::{LocalManager, MovableManager, cfg};
use compio_pool::{Config, Error, Pool, Reservoir};

// ---- Sizing at the extremes -----------------------------------------------

/// The largest shard the type system allows. `try_reserve` compares against it
/// on every checkout, so an overflow there would be a hard failure.
#[compio::test]
async fn an_unbounded_max_size_still_serves() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(usize::MAX));

    let held: Vec<_> = {
        let mut v = Vec::new();
        for _ in 0..64 {
            v.push(pool.acquire().await.unwrap());
        }
        v
    };
    assert_eq!(pool.local_size(), 64);
    assert_eq!(pool.metrics().live, 64);

    drop(held);
    assert_eq!(pool.local_idle(), 64);
}

/// `min_idle` beyond `max_size` is clamped rather than dialling forever.
#[compio::test]
async fn a_min_idle_larger_than_the_shard_is_clamped() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(3).min_idle(usize::MAX));

    pool.warm().await.unwrap();

    assert_eq!(counts.connected(), 3, "clamped to max_size, not unbounded");
    assert_eq!(pool.local_idle(), 3);
}

/// The smallest useful shard, with far more callers than slots. Every one has to
/// be served, and on the one connection.
#[compio::test]
async fn one_slot_serves_hundreds_of_waiters_in_turn() {
    const WAITERS: usize = 256;
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(1).acquire_timeout(None));

    let held = pool.acquire().await.unwrap();
    let tasks: Vec<_> = (0..WAITERS)
        .map(|_| {
            let pool = pool.clone();
            compio::runtime::spawn(async move { pool.acquire().await.map(|c| c.id) })
        })
        .collect();

    // Everything is queued behind this one guard; releasing it cascades.
    drop(held);
    for task in tasks {
        assert_eq!(
            task.await.unwrap().expect("every waiter must be served"),
            0,
            "all of them share the one connection"
        );
    }

    assert_eq!(counts.connected(), 1, "no waiter dialled its own");
    assert_eq!(pool.metrics().acquires as usize, WAITERS + 1);
    assert_eq!(pool.local_size(), 1);
}

// ---- Timeouts at the extremes ---------------------------------------------

/// A zero acquire timeout is degenerate but legal. Whichever way the timer
/// resolves it, the shard must not be left holding a claim.
#[compio::test]
async fn a_zero_acquire_timeout_leaks_nothing() {
    let pool = Pool::new(
        LocalManager::new(),
        cfg().max_size(2).acquire_timeout(Duration::ZERO),
    );

    for _ in 0..16 {
        match pool.acquire().await {
            // Either outcome is defensible; the accounting is what matters.
            Ok(conn) => drop(conn),
            Err(Error::Timeout) => {}
            Err(e) => panic!("unexpected error: {e:?}"),
        }
        let m = pool.metrics();
        assert!(pool.local_size() <= 2, "a claim leaked");
        assert_eq!(
            m.created,
            m.closed + m.live,
            "connections unaccounted for: {m:?}"
        );
    }

    // And a pool configured this way is still usable with a real timeout.
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1));
    assert!(pool.acquire().await.is_ok());
}

// ---- Retirement policies at the extremes ----------------------------------

/// `max_uses(0)` retires a connection before it is ever reused. The acquire loop
/// must fall through to dialling rather than spinning on the free list.
#[compio::test]
async fn a_zero_use_budget_dials_every_time_without_spinning() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2).max_uses(0u64));

    for i in 1..=8u64 {
        let conn = pool.acquire().await.unwrap();
        assert_eq!(conn.meta().uses, 0);
        drop(conn);
        assert_eq!(pool.local_idle(), 0, "nothing may survive a zero budget");
        assert_eq!(counts.connected(), i, "so every checkout is a fresh dial");
    }

    let m = pool.metrics();
    assert_eq!((m.created, m.closed, m.live), (8, 8, 0));
}

#[compio::test]
async fn an_unbounded_use_budget_never_retires() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(1).max_uses(u64::MAX));

    for _ in 0..32 {
        drop(pool.acquire().await.unwrap());
    }
    assert_eq!(counts.connected(), 1, "one connection for the whole run");
    assert_eq!(pool.acquire().await.unwrap().meta().uses, 32);
}

/// A lifetime of zero expires a connection the moment it exists. Both the return
/// path and the acquire path have to cope, and neither may loop.
#[compio::test]
async fn a_zero_lifetime_retires_everything_immediately() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2).max_lifetime(Duration::ZERO));

    for _ in 0..8 {
        drop(pool.acquire().await.unwrap());
        assert_eq!(pool.local_idle(), 0);
        assert_eq!(pool.local_size(), 0);
    }
    assert_eq!(counts.connected(), 8);
    assert_eq!(counts.disconnected(), 8);
}

#[compio::test]
async fn a_zero_idle_timeout_retires_everything_immediately() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2).idle_timeout(Duration::ZERO));

    for _ in 0..8 {
        drop(pool.acquire().await.unwrap());
    }
    assert_eq!(counts.disconnected(), 8);
    assert_eq!(pool.metrics().live, 0);
}

/// Every policy at its strictest, all at once.
#[compio::test]
async fn every_policy_at_its_tightest_still_serves() {
    let pool = Pool::new(
        LocalManager::new(),
        cfg()
            .max_size(1)
            .max_uses(0u64)
            .max_lifetime(Duration::ZERO)
            .idle_timeout(Duration::ZERO)
            .acquire_timeout(Duration::from_millis(100)),
    );

    for _ in 0..8 {
        assert!(
            pool.acquire().await.is_ok(),
            "a caller must still get a connection, however strict the policy"
        );
    }
    let m = pool.metrics();
    assert_eq!(m.created, m.closed, "and each one is closed again: {m:?}");
}

// ---- The reaper at the extremes -------------------------------------------

/// A reaper that effectively never sleeps must not leak, spin up connections, or
/// wedge the runtime it shares with the workload.
#[compio::test]
async fn a_reaper_with_a_one_nanosecond_interval_stays_harmless() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        Config::new()
            .max_size(4)
            .min_idle(1)
            .idle_timeout(None)
            .max_lifetime(Duration::from_secs(3600))
            .reap_interval(Duration::from_nanos(1))
            .acquire_timeout(Duration::from_secs(1)),
    );

    // Creating the shard starts the reaper; it then runs flat out.
    assert_eq!(pool.local_idle(), 0);
    compio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(
        counts.connected(),
        1,
        "the refill must stop at min_idle however often it runs"
    );
    assert_eq!(pool.local_idle(), 1);

    // The workload still gets served while the reaper hammers away.
    for _ in 0..16 {
        assert!(pool.acquire().await.is_ok());
    }
    let m = pool.metrics();
    assert_eq!(m.created, m.closed + m.live, "accounting held: {m:?}");

    pool.close();
    compio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(pool.local_idle(), 0, "and it stops when told to");
}

// ---- The reservoir at the extremes ----------------------------------------

/// One slot, eight threads. Every park and claim contends on the same admission
/// counter and the same ring entry.
#[test]
fn a_single_slot_reservoir_under_eight_threads() {
    let _lock = common::detach_lock();
    const THREADS: usize = 8;
    let pool = Pool::builder(MovableManager::new())
        .config(
            cfg()
                .max_size(2)
                .min_idle(0)
                .acquire_timeout(Duration::from_millis(500)),
        )
        .exchange(Reservoir::new(1))
        .build();

    let start = Arc::new(Barrier::new(THREADS));
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let pool = pool.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        start.wait();
                        for _ in 0..100 {
                            drop(pool.acquire().await.unwrap());
                        }
                    })
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    let m = pool.metrics();
    assert!(m.parked <= 1, "one slot means at most one parked: {m:?}");
    assert_eq!(m.live, 0, "every shard went with its thread");
    assert_eq!(
        m.created,
        m.closed + m.parked,
        "nothing lost under contention on a single slot: {m:?}"
    );
}

/// A reservoir far larger than anything that will use it, filled to the brim.
#[compio::test]
async fn a_large_reservoir_fills_to_capacity_and_stops() {
    let _lock = common::detach_lock();
    const CAPACITY: usize = 512;
    let pool = Pool::builder(MovableManager::new())
        .config(cfg().max_size(CAPACITY + 8).min_idle(0))
        .exchange(Reservoir::new(CAPACITY))
        .build();

    // Held all at once, so they really are distinct connections, then released
    // together so the reservoir is offered more than it can take.
    let held: Vec<_> = {
        let mut v = Vec::new();
        for _ in 0..CAPACITY + 8 {
            v.push(pool.acquire().await.unwrap());
        }
        v
    };
    drop(held);

    let m = pool.metrics();
    assert_eq!(m.parked as usize, CAPACITY, "filled exactly: {m:?}");
    assert_eq!(pool.local_idle(), 8, "and the surplus stayed local");
    assert_eq!(m.created, m.closed + m.live + m.parked, "conserved: {m:?}");
}

// ---- Many pools, one thread ----------------------------------------------

/// Shards live in one thread-local map keyed by pool id. A lot of pools on one
/// thread must stay independent, and none may pick up another's shard.
#[compio::test]
async fn many_pools_on_one_thread_stay_independent() {
    const POOLS: usize = 128;
    let pools: Vec<_> = (0..POOLS)
        .map(|_| Pool::new(LocalManager::new(), cfg().max_size(1)))
        .collect();

    let held: Vec<_> = {
        let mut v = Vec::new();
        for pool in &pools {
            v.push(pool.acquire().await.unwrap());
        }
        v
    };

    for pool in &pools {
        assert_eq!(pool.local_size(), 1, "each pool has exactly its own one");
        assert_eq!(pool.metrics().created, 1);
        assert_eq!(pool.manager().counts().connected(), 1);
    }
    drop(held);
    for pool in &pools {
        assert_eq!(pool.local_idle(), 1);
    }
}

// ---- Sustained churn -----------------------------------------------------

/// A long run on a single slot: the counters must not drift and the one
/// connection must be reused throughout.
#[compio::test]
async fn ten_thousand_checkouts_on_one_connection() {
    const ROUNDS: u64 = 10_000;
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(1));

    for _ in 0..ROUNDS {
        drop(pool.acquire().await.unwrap());
    }

    assert_eq!(counts.connected(), 1, "one dial for ten thousand checkouts");
    assert_eq!(
        counts.recycled(),
        ROUNDS - 1,
        "and a revalidation each reuse"
    );
    let m = pool.metrics();
    assert_eq!(m.acquires, ROUNDS);
    assert_eq!((m.created, m.closed, m.live, m.idle), (1, 0, 1, 1));
    assert_eq!(pool.acquire().await.unwrap().meta().uses, ROUNDS);
}

/// `take` removes a connection from the pool's control on every checkout, so the
/// shard has to keep granting fresh budget indefinitely.
#[compio::test]
async fn taking_every_connection_never_exhausts_the_shard() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(1));

    let escaped: Vec<_> = {
        let mut v = Vec::new();
        for _ in 0..64 {
            v.push(pool.acquire().await.unwrap().take());
        }
        v
    };

    assert_eq!(escaped.len(), 64);
    assert_eq!(counts.connected(), 64, "a fresh one every time");
    assert_eq!(
        counts.disconnected(),
        0,
        "and none of them closed by the pool"
    );
    let m = pool.metrics();
    assert_eq!((m.live, m.closed), (0, 0), "all 64 left the pool: {m:?}");
    assert_eq!(pool.local_size(), 0);
}

// ---- Resource lifetime ---------------------------------------------------

/// A thread's shard is never removed from the thread-local map that holds it, so
/// dropping the last `Pool` handle does **not** close that thread's connections.
/// They live until the thread exits.
///
/// This matters for a thread-per-core runtime, where threads are long-lived:
/// a pool built, used and dropped inside one of them leaves its sockets open.
/// `Pool::close` is what releases them early.
#[compio::test]
async fn dropping_the_last_handle_does_not_close_connections() {
    let manager = LocalManager::new();
    let counts = manager.counts();

    {
        let pool = Pool::new(manager, cfg().max_size(2));
        drop(pool.acquire().await.unwrap());
        assert_eq!(pool.local_idle(), 1);
    } // every handle is gone here

    assert_eq!(
        counts.disconnected(),
        0,
        "the shard outlives the handle, and so do its connections"
    );
}

/// Closing first is what actually releases them.
#[compio::test]
async fn closing_before_dropping_the_handle_releases_connections() {
    let manager = LocalManager::new();
    let counts = manager.counts();

    {
        let pool = Pool::new(manager, cfg().max_size(2));
        drop(pool.acquire().await.unwrap());
        pool.close();
    }

    assert_eq!(
        counts.disconnected(),
        1,
        "close reaches this thread's shard"
    );
}
