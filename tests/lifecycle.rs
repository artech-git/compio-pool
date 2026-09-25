//! Retirement, closing, and the metric counters that record them.

mod common;

use std::time::Duration;

use common::{LocalManager, cfg};
use compio_pool::{Error, Pool};

// ---- Retirement policies --------------------------------------------------

/// `max_uses` is a budget of completed checkouts. It is spent in `release`,
/// which stamps `uses` and then tests expiry, so the connection is retired as it
/// comes back rather than on the next request for it.
#[compio::test]
async fn max_uses_allows_exactly_that_many_checkouts() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2).max_uses(2u64));

    let first = pool.acquire().await.unwrap();
    assert_eq!(first.meta().uses, 0, "a brand new connection");
    drop(first);
    assert_eq!(pool.local_idle(), 1, "one of two uses spent");
    assert_eq!(counts.disconnected(), 0);

    let second = pool.acquire().await.unwrap();
    assert_eq!(second.meta().uses, 1, "the same connection, once used");
    drop(second);

    assert_eq!(
        pool.local_idle(),
        0,
        "the budget is spent, so it is retired"
    );
    assert_eq!(counts.disconnected(), 1);
    assert_eq!(pool.local_size(), 0);

    // The next caller gets a replacement rather than waiting or failing.
    let third = pool.acquire().await.unwrap();
    assert_eq!(third.meta().uses, 0);
    assert_eq!(counts.connected(), 2);
}

#[compio::test]
async fn a_connection_at_max_uses_is_retired_when_it_is_returned() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2).max_uses(1u64));

    drop(pool.acquire().await.unwrap());

    // `release` stamps `uses` before checking expiry, so the connection never
    // even reaches the free list.
    assert_eq!(pool.local_idle(), 0);
    assert_eq!(counts.disconnected(), 1);
    assert_eq!(pool.local_size(), 0);
}

#[compio::test]
async fn max_lifetime_retires_a_connection_on_return() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        cfg().max_size(2).max_lifetime(Duration::from_millis(10)),
    );

    let conn = pool.acquire().await.unwrap();
    compio::time::sleep(Duration::from_millis(30)).await;
    drop(conn);

    assert_eq!(pool.local_idle(), 0, "too old to pool");
    assert_eq!(counts.disconnected(), 1);
}

#[compio::test]
async fn invalidate_retires_idle_connections_without_closing_the_pool() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4));

    let old_id = {
        let conn = pool.acquire().await.unwrap();
        conn.id
    };
    assert_eq!(pool.local_idle(), 1);

    pool.invalidate();
    assert!(!pool.is_closed(), "invalidate is not close");
    assert_eq!(
        pool.local_idle(),
        1,
        "the stale connection is still in the free list for now"
    );

    let conn = pool.acquire().await.unwrap();
    assert_ne!(conn.id, old_id, "but it is never handed out");
    assert_eq!(counts.connected(), 2);
    assert_eq!(counts.disconnected(), 1);
}

/// A checkout that was already live when `invalidate` ran keeps working, and is
/// closed rather than pooled when it comes back.
#[compio::test]
async fn invalidate_lets_a_live_checkout_finish() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4));

    let conn = pool.acquire().await.unwrap();
    pool.invalidate();
    assert_eq!(conn.id, 0, "still usable");

    drop(conn);
    assert_eq!(pool.local_idle(), 0);
    assert_eq!(counts.disconnected(), 1);
}

#[compio::test]
async fn invalidate_can_be_called_repeatedly() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    for _ in 0..3 {
        drop(pool.acquire().await.unwrap());
        pool.invalidate();
    }
    let m = pool.metrics();
    assert_eq!(m.created, 3);
    assert_eq!(m.live, m.created - m.closed);
}

/// Every retirement policy at once, on the same connection.
#[compio::test]
async fn the_strictest_policy_wins() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        cfg()
            .max_size(2)
            .max_uses(100u64)
            .max_lifetime(Duration::from_secs(3600))
            .idle_timeout(Duration::from_millis(5)),
    );

    drop(pool.acquire().await.unwrap());
    compio::time::sleep(Duration::from_millis(30)).await;

    // Well under `max_uses` and `max_lifetime`, but past `idle_timeout`.
    let conn = pool.acquire().await.unwrap();
    assert_eq!(conn.id, 1);
    assert_eq!(counts.disconnected(), 1);
}

// ---- Closing --------------------------------------------------------------

#[compio::test]
async fn close_closes_this_threads_idle_connections_at_once() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4));

    let held: Vec<_> = {
        let mut v = Vec::new();
        for _ in 0..3 {
            v.push(pool.acquire().await.unwrap());
        }
        v
    };
    drop(held);
    assert_eq!(pool.local_idle(), 3);

    pool.close();

    assert!(pool.is_closed());
    assert_eq!(pool.local_idle(), 0);
    assert_eq!(pool.local_size(), 0);
    assert_eq!(counts.disconnected(), 3);
    let m = pool.metrics();
    assert_eq!((m.live, m.idle, m.closed), (0, 0, 3));
}

#[compio::test]
async fn a_checkout_outstanding_at_close_is_closed_when_it_returns() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4));

    let conn = pool.acquire().await.unwrap();
    pool.close();
    assert_eq!(counts.disconnected(), 0, "it is still in use");

    drop(conn);
    assert_eq!(counts.disconnected(), 1);
    assert_eq!(pool.local_idle(), 0, "a closed pool pools nothing");
}

#[compio::test]
async fn close_is_idempotent() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2));

    drop(pool.acquire().await.unwrap());
    pool.close();
    pool.close();
    pool.close();

    assert_eq!(counts.disconnected(), 1);
    assert_eq!(pool.metrics().closed, 1);
}

#[compio::test]
async fn a_closed_pool_refuses_new_checkouts() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    pool.close();

    assert!(matches!(pool.acquire().await, Err(Error::Closed)));
    assert!(matches!(pool.acquire().await, Err(Error::Closed)));
    let m = pool.metrics();
    assert_eq!(m.created, 0, "and does not dial");
    assert_eq!(m.timeouts, 0, "failing closed is not a timeout");
}

#[compio::test]
async fn warm_on_a_closed_pool_does_not_leave_connections_behind() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4).min_idle(2));
    pool.close();

    // `warm` has no `closed` check of its own; what it opens is closed again the
    // moment it is offered back to the shard.
    pool.warm().await.unwrap();
    assert_eq!(
        pool.local_idle(),
        2,
        "warm pushes straight to the free list"
    );

    // Anything that touches the shard afterwards clears them out.
    pool.close();
    assert_eq!(pool.local_idle(), 0);
    assert_eq!(counts.disconnected(), 2);
    let m = pool.metrics();
    assert_eq!(m.created, m.closed);
}

// ---- Counter accounting ----------------------------------------------------

#[compio::test]
async fn a_fresh_pool_reports_nothing() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    assert_eq!(pool.metrics(), Default::default());
}

/// One pass over the whole surface, checking every counter moves exactly when it
/// should and never otherwise.
#[compio::test]
async fn every_counter_records_its_own_event() {
    let pool = Pool::new(
        LocalManager::new(),
        cfg().max_size(1).acquire_timeout(Duration::from_millis(20)),
    );

    // A successful checkout: created, live, acquires.
    let conn = pool.acquire().await.unwrap();
    let m = pool.metrics();
    assert_eq!((m.created, m.live, m.acquires), (1, 1, 1));
    assert_eq!((m.idle, m.closed, m.waits, m.timeouts), (0, 0, 0, 0));

    // A checkout that gives up at max_size: waits, timeouts.
    assert!(matches!(pool.acquire().await, Err(Error::Timeout)));
    let m = pool.metrics();
    assert_eq!((m.waits, m.timeouts, m.acquires), (1, 1, 1));

    // Returned: idle.
    drop(conn);
    assert_eq!(pool.metrics().idle, 1);

    // Reused: acquires again, and recycle ran.
    let conn = pool.acquire().await.unwrap();
    let m = pool.metrics();
    assert_eq!((m.acquires, m.idle, m.created), (2, 0, 1));
    assert_eq!(pool.manager().counts().recycled(), 1);

    // Poisoned on return: poisoned, closed.
    conn.poison();
    drop(conn);
    let m = pool.metrics();
    assert_eq!((m.poisoned, m.closed, m.live), (1, 1, 0));

    // A stale connection on the next acquire: recycle_failures.
    drop(pool.acquire().await.unwrap());
    pool.manager().set_fail_recycle(true);
    drop(pool.acquire().await.unwrap());
    let m = pool.metrics();
    assert_eq!(m.recycle_failures, 1);
    assert_eq!(m.created, m.closed + m.live, "conserved throughout: {m:?}");

    // Nothing parked: no exchange is installed.
    assert_eq!((m.parked, m.unparked), (0, 0));
}

#[compio::test]
async fn the_live_gauge_follows_checkouts_up_and_down() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(3));

    let held: Vec<_> = {
        let mut v = Vec::new();
        for i in 0..3 {
            v.push(pool.acquire().await.unwrap());
            assert_eq!(pool.metrics().live, i + 1);
        }
        v
    };
    assert_eq!(pool.metrics().idle, 0, "all three are checked out");

    drop(held);
    let m = pool.metrics();
    assert_eq!(m.live, 3, "still this shard's connections");
    assert_eq!(m.idle, 3, "but available again");
}

#[compio::test]
async fn counters_are_monotonic_while_gauges_are_not() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));

    for _ in 0..5 {
        let conn = pool.acquire().await.unwrap();
        conn.poison();
    }

    let m = pool.metrics();
    assert_eq!(m.created, 5, "counters only ever go up");
    assert_eq!(m.closed, 5);
    assert_eq!(m.acquires, 5);
    assert_eq!(m.poisoned, 5);
    assert_eq!((m.live, m.idle), (0, 0), "gauges came back to zero");
}

#[compio::test]
async fn metrics_are_shared_by_every_handle() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    let clone = pool.clone();

    drop(pool.acquire().await.unwrap());

    assert_eq!(clone.metrics(), pool.metrics());
    assert_eq!(clone.metrics().created, 1);
}
