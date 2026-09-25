//! The per-shard background reaper.
//!
//! The reaper is what enforces `idle_timeout` and `max_lifetime` on connections
//! nobody is asking for, and what refills `min_idle`. It is spawned lazily, once
//! per thread, the first time that thread touches the pool.

mod common;

use std::{sync::mpsc, time::Duration};

use common::LocalManager;
use compio_pool::{Config, Pool};

/// Short enough to keep tests quick, long enough to survive a loaded CI box.
const TICK: Duration = Duration::from_millis(20);

/// Waits for several reap ticks.
async fn ticks(n: u32) {
    compio::time::sleep(TICK * n).await;
}

fn reaping(idle_timeout: Option<Duration>, max_lifetime: Option<Duration>) -> Config {
    Config::new()
        .max_size(4)
        .idle_timeout(idle_timeout)
        .max_lifetime(max_lifetime)
        .reap_interval(TICK)
        .acquire_timeout(Duration::from_secs(5))
}

#[compio::test]
async fn an_idle_connection_that_ages_out_is_reaped() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, reaping(Some(Duration::from_millis(1)), None));

    drop(pool.acquire().await.unwrap());
    assert_eq!(pool.local_idle(), 1);

    ticks(5).await;

    assert_eq!(
        pool.local_idle(),
        0,
        "nobody asked for it, so nobody else would have caught it"
    );
    assert_eq!(pool.local_size(), 0, "and its budget came back");
    assert_eq!(counts.disconnected(), 1, "closed through the manager");
    let m = pool.metrics();
    assert_eq!((m.live, m.idle, m.closed), (0, 0, 1));
}

#[compio::test]
async fn a_connection_past_its_max_lifetime_is_reaped() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, reaping(None, Some(Duration::from_millis(1))));

    drop(pool.acquire().await.unwrap());
    ticks(5).await;

    assert_eq!(pool.local_idle(), 0);
    assert_eq!(counts.disconnected(), 1);
}

#[compio::test]
async fn a_fresh_idle_connection_is_left_alone() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, reaping(Some(Duration::from_secs(3600)), None));

    drop(pool.acquire().await.unwrap());
    ticks(5).await;

    assert_eq!(
        pool.local_idle(),
        1,
        "the reaper must not close live capacity"
    );
    assert_eq!(counts.disconnected(), 0);
}

/// A checked-out connection is not in the free list, so the reaper cannot touch
/// it however old it is; expiry is applied when it comes back.
#[compio::test]
async fn a_checked_out_connection_is_not_reaped_under_the_caller() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, reaping(Some(Duration::from_millis(1)), None));

    let held = pool.acquire().await.unwrap();
    ticks(5).await;
    assert_eq!(counts.disconnected(), 0, "it is still in use");
    assert_eq!(pool.local_size(), 1);

    drop(held);
    ticks(5).await;
    assert_eq!(counts.disconnected(), 1, "and reaped once it is idle");
}

#[compio::test]
async fn the_reaper_reaps_only_what_has_aged_out() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, reaping(Some(TICK * 4), None));

    // Two connections held at once, so they really are two, then returned about
    // three ticks apart: the older one ages out first.
    let a = pool.acquire().await.unwrap();
    let b = pool.acquire().await.unwrap();
    drop(a);
    ticks(3).await;
    drop(b);

    ticks(3).await;
    assert_eq!(counts.disconnected(), 1, "one aged out, one did not");
    assert_eq!(pool.local_idle(), 1);
}

#[compio::test]
async fn the_reaper_refills_min_idle() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        Config::new()
            .max_size(4)
            .min_idle(2)
            .idle_timeout(None)
            .max_lifetime(None)
            .reap_interval(TICK),
    );

    // Touching the pool is what creates this thread's shard and its reaper.
    assert_eq!(pool.local_idle(), 0);
    ticks(4).await;

    assert_eq!(pool.local_idle(), 2, "min_idle should have been dialled up");
    assert_eq!(counts.connected(), 2);
    assert_eq!(pool.local_size(), 2);

    // And it stops at the target rather than dialling every tick.
    ticks(4).await;
    assert_eq!(counts.connected(), 2);
}

#[compio::test]
async fn the_reaper_tops_up_after_a_connection_is_reaped() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        Config::new()
            .max_size(4)
            .min_idle(1)
            // Every idle connection ages out immediately, so each tick reaps one
            // and dials a replacement.
            .idle_timeout(Duration::from_millis(1))
            .max_lifetime(None)
            .reap_interval(TICK),
    );

    assert_eq!(pool.local_idle(), 0);
    ticks(6).await;

    assert!(
        counts.connected() >= 2,
        "the reaper should keep replacing what it reaps, got {}",
        counts.connected()
    );
    assert_eq!(
        counts.connected(),
        counts.disconnected() + pool.local_idle() as u64
    );
}

/// The refill respects `max_size`, which it shares with `acquire`.
#[compio::test]
async fn the_refill_cannot_push_a_shard_past_max_size() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        Config::new()
            .max_size(2)
            .min_idle(2)
            .idle_timeout(None)
            .max_lifetime(None)
            .reap_interval(TICK),
    );

    let _held = pool.acquire().await.unwrap();
    ticks(4).await;

    assert_eq!(pool.local_size(), 2, "one held, one warmed, and no more");
    assert_eq!(pool.local_idle(), 1);
    assert_eq!(counts.connected(), 2);
}

/// A refill that cannot dial must give its budget back and stop for this tick,
/// not spin.
#[compio::test]
async fn a_refill_that_cannot_dial_gives_up_without_leaking() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        Config::new()
            .max_size(2)
            .min_idle(2)
            .idle_timeout(None)
            .max_lifetime(None)
            .reap_interval(TICK),
    );
    pool.manager().set_fail_connect(true);

    assert_eq!(pool.local_idle(), 0);
    ticks(4).await;

    assert_eq!(counts.connected(), 0);
    assert_eq!(pool.local_size(), 0, "each failed attempt freed its slot");

    // Once the backend recovers, the next tick fills the shard.
    pool.manager().set_fail_connect(false);
    ticks(4).await;
    assert_eq!(pool.local_idle(), 2);
}

/// The reaper must not dial while somebody on this thread is waiting: that
/// caller is about to be handed a connection anyway.
#[compio::test]
async fn the_refill_stands_aside_for_a_local_waiter() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        Config::new()
            .max_size(1)
            .min_idle(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .reap_interval(TICK)
            .acquire_timeout(None),
    );

    let held = pool.acquire().await.unwrap();
    let waiting = compio::runtime::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await.map(|c| c.id) }
    });
    ticks(4).await;

    assert_eq!(counts.connected(), 1, "the waiter gets the one coming back");
    drop(held);
    assert!(waiting.await.unwrap().is_ok());
}

/// After `close`, the reaper drains what is left and stops for good.
#[compio::test]
async fn the_reaper_stops_once_the_pool_is_closed() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        Config::new()
            .max_size(4)
            .min_idle(2)
            .idle_timeout(None)
            .max_lifetime(None)
            .reap_interval(TICK),
    );

    assert_eq!(pool.local_idle(), 0);
    ticks(4).await;
    assert_eq!(pool.local_idle(), 2);

    pool.close();
    assert_eq!(pool.local_idle(), 0, "close drains this thread itself");

    ticks(6).await;
    assert_eq!(pool.local_idle(), 0, "a closed pool is never refilled");
    assert_eq!(counts.connected(), 2, "and the reaper dialled nothing more");
    assert_eq!(counts.disconnected(), 2);
}

/// `close` only reaches the calling thread's shard. Another thread's idle
/// connections are closed by that thread's own reaper — the only thread allowed
/// to close them.
#[test]
fn a_reaper_closes_idle_connections_after_another_thread_closed_the_pool() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        Config::new()
            .max_size(4)
            .min_idle(0)
            // Nothing ages out on its own, but the config still wants a reaper.
            .max_lifetime(Duration::from_secs(3600))
            .idle_timeout(None)
            .reap_interval(TICK)
            .acquire_timeout(Duration::from_secs(5)),
    );

    let (ready_tx, ready_rx) = mpsc::channel();
    let (closed_tx, closed_rx) = mpsc::channel();

    let worker = std::thread::spawn({
        let pool = pool.clone();
        move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    drop(pool.acquire().await.unwrap());
                    assert_eq!(pool.local_idle(), 1);
                    ready_tx.send(()).unwrap();

                    // Blocks this thread, including its reaper, until the main
                    // thread has closed the pool.
                    closed_rx.recv().unwrap();
                    ticks(5).await;
                    pool.local_idle()
                })
        }
    });

    ready_rx.recv().unwrap();
    assert_eq!(pool.metrics().idle, 1);
    pool.close();
    closed_tx.send(()).unwrap();

    assert_eq!(
        worker.join().unwrap(),
        0,
        "the worker's reaper must close what close() could not reach"
    );
    assert_eq!(counts.disconnected(), 1);
    assert_eq!(pool.metrics().live, 0);
}

/// Outside a compio runtime there is nothing to spawn onto. The pool still has
/// to work; expiry is then enforced on the acquire path instead.
#[test]
fn a_pool_without_a_runtime_enforces_expiry_on_acquire_instead() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, reaping(Some(Duration::from_millis(1)), None));

    // Creating the shard here, off-runtime, is what skips the reaper spawn.
    assert_eq!(pool.local_size(), 0);

    compio::runtime::Runtime::new().unwrap().block_on(async {
        drop(pool.acquire().await.unwrap());
        assert_eq!(pool.local_idle(), 1);
        compio::time::sleep(TICK * 5).await;
        // No reaper ran, so the aged-out connection is still in the free list…
        assert_eq!(pool.local_idle(), 1);
        // …and is discarded on the way out of it instead.
        let conn = pool.acquire().await.unwrap();
        assert_eq!(conn.id, 1, "a replacement, not the stale one");
    });

    assert_eq!(counts.disconnected(), 1);
}
