//! The acquire path: reuse, capacity, waiting, warming and the error cases.

mod common;

use std::time::Duration;

use common::{LocalManager, cfg};
use compio_pool::{Error, Pool};

// ---- Basics ---------------------------------------------------------------

#[compio::test]
async fn the_first_checkout_dials_and_does_not_recycle() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2));

    let conn = pool.acquire().await.unwrap();

    assert_eq!(conn.id, 0);
    assert_eq!(counts.connected(), 1);
    assert_eq!(counts.recycled(), 0, "there was nothing to revalidate");
    assert_eq!(pool.local_size(), 1);
    assert_eq!(pool.local_idle(), 0, "it is checked out, not idle");
    assert_eq!(pool.metrics().acquires, 1);
}

#[compio::test]
async fn returning_a_connection_makes_it_idle_rather_than_closing_it() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2));

    drop(pool.acquire().await.unwrap());

    assert_eq!(pool.local_idle(), 1);
    assert_eq!(pool.local_size(), 1, "it is still this shard's connection");
    assert_eq!(counts.disconnected(), 0);
    let m = pool.metrics();
    assert_eq!((m.live, m.idle, m.closed), (1, 1, 0));
}

/// LIFO: the connection returned most recently is handed out first, because it
/// is the warmest.
#[compio::test]
async fn idle_connections_are_reused_newest_first() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(4));

    let a = pool.acquire().await.unwrap();
    let b = pool.acquire().await.unwrap();
    let (a_id, b_id) = (a.id, b.id);
    drop(a);
    drop(b); // returned last, so it sits on top

    // Held, not dropped: a guard returned to the free list would go back on top
    // and be handed straight out again.
    let first = pool.acquire().await.unwrap();
    let second = pool.acquire().await.unwrap();
    assert_eq!((first.id, second.id), (b_id, a_id));
}

#[compio::test]
async fn distinct_checkouts_get_distinct_connections() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(3));

    let a = pool.acquire().await.unwrap();
    let b = pool.acquire().await.unwrap();
    let c = pool.acquire().await.unwrap();

    let mut ids = [a.id, b.id, c.id];
    ids.sort();
    assert_eq!(ids, [0, 1, 2]);
    assert_eq!(counts.connected(), 3);
    assert_eq!(pool.local_size(), 3);
}

// ---- Failure modes --------------------------------------------------------

#[compio::test]
async fn a_refused_dial_surfaces_the_manager_error() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1));
    pool.manager().set_fail_connect(true);

    let err = pool.acquire().await.expect_err("connect was refused");
    assert_eq!(err.into_backend(), Some("connect refused"));

    // The budget claimed for the failed dial has to come back, or one refused
    // connection would cost the shard a slot for good.
    assert_eq!(pool.local_size(), 0);
    let m = pool.metrics();
    assert_eq!((m.created, m.live, m.acquires), (0, 0, 0));
}

#[compio::test]
async fn a_shard_recovers_from_a_run_of_refused_dials() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1));
    pool.manager().set_fail_connect(true);
    for _ in 0..5 {
        assert!(pool.acquire().await.is_err());
    }

    pool.manager().set_fail_connect(false);
    assert!(pool.acquire().await.is_ok(), "the shard wedged at max_size");
}

/// Every idle connection failing `recycle` must not leave the caller with
/// nothing: the pool drains the free list and then dials.
#[compio::test]
async fn a_whole_stale_free_list_is_discarded_and_replaced() {
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

    pool.manager().set_fail_recycle(true);
    pool.manager().set_fail_connect(true);
    let err = pool.acquire().await.expect_err("nothing usable is left");
    assert_eq!(err.into_backend(), Some("connect refused"));

    assert_eq!(counts.disconnected(), 3, "all three stale ones were closed");
    assert_eq!(pool.metrics().recycle_failures, 3);
    assert_eq!(pool.local_size(), 0, "and their budget came back");
}

#[compio::test]
async fn an_error_reports_which_failure_mode_it_was() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1));
    pool.manager().set_fail_connect(true);
    let err = pool.acquire().await.unwrap_err();
    assert!(err.to_string().contains("failed to establish connection"));

    pool.close();
    let err = pool.acquire().await.unwrap_err();
    assert!(matches!(err, Error::Closed));
    assert_eq!(err.into_backend(), None);
}

// ---- Capacity and waiting -------------------------------------------------

#[compio::test]
async fn a_waiter_is_counted_and_then_served() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1));
    let held = pool.acquire().await.unwrap();
    let id = held.id;

    let waiting = compio::runtime::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await.map(|c| c.id) }
    });
    compio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(pool.metrics().waits, 1);

    drop(held);
    assert_eq!(waiting.await.unwrap().unwrap(), id, "the same warm socket");
    assert_eq!(
        pool.manager().counts().connected(),
        1,
        "waiting beats dialling past max_size"
    );
}

/// A waiter blocked at `max_size` is woken by *any* freed budget, including a
/// connection destroyed rather than returned.
#[compio::test]
async fn a_waiter_is_woken_when_a_connection_is_destroyed_instead_of_returned() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(1));
    let held = pool.acquire().await.unwrap();
    held.poison();

    let waiting = compio::runtime::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await.map(|c| c.id) }
    });
    compio::time::sleep(Duration::from_millis(20)).await;
    drop(held);

    let id = waiting
        .await
        .unwrap()
        .expect("the freed slot must be usable");
    assert_ne!(id, 0, "a poisoned connection is never handed on");
    assert_eq!(counts.connected(), 2);
}

/// A closed pool must not hand a connection to someone already waiting for one.
#[compio::test]
async fn a_waiter_woken_after_close_is_told_the_pool_is_closed() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1).acquire_timeout(None));
    let held = pool.acquire().await.unwrap();

    let waiting = compio::runtime::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await.map(|c| c.id) }
    });
    compio::time::sleep(Duration::from_millis(20)).await;

    pool.close();
    drop(held); // frees the budget and wakes the waiter, which re-checks `closed`

    assert!(matches!(waiting.await.unwrap(), Err(Error::Closed)));
}

/// With no acquire timeout the caller waits as long as it takes rather than
/// giving up.
#[compio::test]
async fn without_a_timeout_a_checkout_waits_indefinitely() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1).acquire_timeout(None));
    let held = pool.acquire().await.unwrap();

    let waiting = compio::runtime::spawn({
        let pool = pool.clone();
        async move { pool.acquire().await.map(|c| c.id) }
    });
    // Comfortably longer than the 200ms default this config overrides.
    compio::time::sleep(Duration::from_millis(300)).await;
    drop(held);

    assert!(waiting.await.unwrap().is_ok());
    assert_eq!(pool.metrics().timeouts, 0);
}

#[compio::test]
async fn a_timeout_is_counted_and_leaves_the_shard_intact() {
    let pool = Pool::new(
        LocalManager::new(),
        cfg().max_size(1).acquire_timeout(Duration::from_millis(20)),
    );
    let held = pool.acquire().await.unwrap();

    assert!(matches!(pool.acquire().await, Err(Error::Timeout)));
    assert!(matches!(pool.acquire().await, Err(Error::Timeout)));
    assert_eq!(pool.metrics().timeouts, 2);

    // Both abandoned waiters must have left the queue, or the returned
    // connection goes to a future nobody is polling.
    drop(held);
    assert_eq!(pool.local_idle(), 1);
    assert!(pool.acquire().await.is_ok());
}

// ---- Warming --------------------------------------------------------------

#[compio::test]
async fn warm_is_a_no_op_without_min_idle() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4).min_idle(0));

    pool.warm().await.unwrap();
    assert_eq!(counts.connected(), 0);
    assert_eq!(pool.local_idle(), 0);
}

#[compio::test]
async fn warm_is_idempotent() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4).min_idle(2));

    pool.warm().await.unwrap();
    pool.warm().await.unwrap();
    assert_eq!(counts.connected(), 2, "the target was already met");
    assert_eq!(pool.local_idle(), 2);
}

#[compio::test]
async fn warm_clamps_min_idle_to_max_size() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2).min_idle(10));

    pool.warm().await.unwrap();
    assert_eq!(counts.connected(), 2);
    assert_eq!(pool.local_idle(), 2);
}

/// `warm` competes for the same per-shard budget as `acquire`, so it stops at
/// what is left rather than overshooting `max_size`.
#[compio::test]
async fn warm_stops_when_the_shard_has_no_budget_left() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2).min_idle(2));

    let _held = pool.acquire().await.unwrap();
    pool.warm().await.unwrap();

    assert_eq!(counts.connected(), 2, "one checked out, one warmed");
    assert_eq!(pool.local_idle(), 1);
    assert_eq!(pool.local_size(), 2);
}

#[compio::test]
async fn warm_reports_a_refused_dial_and_returns_the_budget() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4).min_idle(3));
    pool.manager().set_fail_connect(true);

    let err = pool.warm().await.expect_err("connect was refused");
    assert_eq!(err.into_backend(), Some("connect refused"));

    assert_eq!(counts.connected(), 0);
    assert_eq!(pool.local_size(), 0, "the failed attempt freed its slot");
    assert_eq!(pool.local_idle(), 0);
}

/// A dial that fails part-way through warming keeps what it already opened.
#[compio::test]
async fn warm_keeps_the_connections_it_opened_before_failing() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4).min_idle(1));

    pool.warm().await.unwrap();
    pool.manager().set_fail_connect(true);
    assert!(pool.warm().await.is_ok(), "min_idle is already satisfied");
    assert_eq!(counts.connected(), 1);
    assert_eq!(pool.local_idle(), 1);
}

#[compio::test]
async fn a_warmed_connection_is_handed_out_without_a_new_dial() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(4).min_idle(2));

    pool.warm().await.unwrap();
    let conn = pool.acquire().await.unwrap();

    assert_eq!(counts.connected(), 2, "no handshake on the first request");
    assert_eq!(counts.recycled(), 1, "but it is revalidated first");
    assert_eq!(conn.meta().uses, 0, "warming is not a checkout");
}
