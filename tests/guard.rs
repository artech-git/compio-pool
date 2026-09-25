//! The checkout guard: deref, metadata, poisoning and hand-off.

mod common;

use common::{DefaultsManager, LocalManager, cfg};
use compio_pool::Pool;

#[compio::test]
async fn a_guard_derefs_to_the_connection() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    let mut conn = pool.acquire().await.unwrap();

    assert_eq!(conn.id, 0, "shared access goes through Deref");
    conn.id = 99;
    assert_eq!(conn.id, 99, "and mutable access through DerefMut");
}

#[compio::test]
async fn metadata_starts_fresh_and_counts_completed_checkouts() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));

    {
        let conn = pool.acquire().await.unwrap();
        let meta = conn.meta();
        assert_eq!(meta.uses, 0, "a brand new connection has no history");
        assert_eq!(meta.generation, 0);
        assert_eq!(meta.created_at, meta.last_used);
    }

    // `uses` and `last_used` are stamped on the way back in, so they show up on
    // the next checkout rather than during the first.
    let conn = pool.acquire().await.unwrap();
    assert_eq!(conn.meta().uses, 1);
    assert!(conn.meta().last_used >= conn.meta().created_at);
    assert!(conn.meta().age() >= conn.meta().idle_for());
}

#[compio::test]
async fn a_guard_starts_unpoisoned() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    let conn = pool.acquire().await.unwrap();
    assert!(!conn.is_poisoned());
}

#[compio::test]
async fn poisoning_by_hand_closes_the_connection_on_drop() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2));

    {
        let conn = pool.acquire().await.unwrap();
        // The caller learned the protocol state is unknown: a partial write, an
        // unparseable response.
        conn.poison();
        assert!(conn.is_poisoned());
    }

    assert_eq!(counts.disconnected(), 1);
    assert_eq!(
        pool.local_idle(),
        0,
        "a poisoned connection is never pooled"
    );
    assert_eq!(pool.local_size(), 0, "and its budget is freed");
    assert_eq!(pool.metrics().poisoned, 1);
}

#[compio::test]
async fn poisoning_twice_counts_once() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    {
        let conn = pool.acquire().await.unwrap();
        conn.poison();
        conn.poison();
    }
    assert_eq!(
        pool.metrics().poisoned,
        1,
        "poison is a flag, not a counter"
    );
}

#[compio::test]
async fn a_completed_op_guard_leaves_the_connection_reusable() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2));

    {
        let conn = pool.acquire().await.unwrap();
        let op = conn.begin_op();
        // ... the operation ran to completion, so the protocol is in step ...
        op.complete_op();
        assert!(!conn.is_poisoned());
    }

    assert_eq!(pool.local_idle(), 1);
    assert_eq!(counts.disconnected(), 0);
    assert_eq!(pool.metrics().poisoned, 0);
}

/// The guard borrows nothing from the connection, so it can be armed for the
/// whole of an operation while the connection is still being used.
#[compio::test]
async fn an_op_guard_does_not_borrow_the_connection() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    let mut conn = pool.acquire().await.unwrap();

    let op = conn.begin_op();
    conn.id += 1; // a real caller would be reading and writing here
    assert_eq!(conn.id, 1);
    op.complete_op();
}

/// Several operations may be in flight from the caller's point of view; any one
/// of them failing to complete is enough to condemn the connection.
#[compio::test]
async fn one_dropped_guard_among_several_still_poisons() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    let conn = pool.acquire().await.unwrap();

    let first = conn.begin_op();
    let second = conn.begin_op();
    first.complete_op();
    assert!(!conn.is_poisoned());

    drop(second);
    assert!(conn.is_poisoned());
}

/// A guard armed after the connection was already poisoned cannot un-poison it.
#[compio::test]
async fn completing_an_op_never_clears_an_existing_poison() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    let conn = pool.acquire().await.unwrap();

    conn.poison();
    conn.begin_op().complete_op();
    assert!(conn.is_poisoned(), "poisoning is one-way");
}

#[compio::test]
async fn take_hands_the_connection_over_and_frees_the_slot() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    // max_size 1 so the freed budget is observable: without it the next acquire
    // would time out.
    let pool = Pool::new(manager, cfg().max_size(1));

    let conn = pool.acquire().await.unwrap();
    let taken = conn.take();
    assert_eq!(taken.id, 0, "the caller owns the connection now");

    let m = pool.metrics();
    assert_eq!(m.live, 0, "the pool no longer counts it");
    assert_eq!(m.closed, 0, "but it was not closed either");
    assert_eq!(counts.disconnected(), 0);
    assert_eq!(pool.local_size(), 0);
    assert_eq!(pool.local_idle(), 0);

    // The shard has room again, which is the point of freeing the budget early.
    let replacement = pool.acquire().await.unwrap();
    assert_ne!(replacement.id, taken.id);
    assert_eq!(counts.connected(), 2);
}

/// Dropping the connection `take` handed out must not reach the pool again.
#[compio::test]
async fn a_taken_connection_is_not_returned_when_it_is_dropped() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    drop(pool.acquire().await.unwrap().take());
    assert_eq!(pool.local_idle(), 0);
    assert_eq!(pool.metrics().live, 0);
}

#[compio::test]
async fn the_guards_debug_output_shows_poison_state_and_metadata() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));
    let conn = pool.acquire().await.unwrap();

    let s = format!("{conn:?}");
    assert!(s.contains("poisoned: false"), "got {s}");
    assert!(s.contains("meta"), "got {s}");

    conn.poison();
    assert!(format!("{conn:?}").contains("poisoned: true"));

    let op = conn.begin_op();
    assert!(format!("{op:?}").contains("armed: true"));
    op.complete_op();
}

/// `Manage::disconnect` defaults to a no-op — dropping the connection is what
/// closes it. The pool must still do its own accounting.
#[compio::test]
async fn a_manager_using_the_default_disconnect_still_balances_the_counters() {
    let pool = Pool::new(DefaultsManager, cfg().max_size(1));

    let conn = pool.acquire().await.unwrap();
    conn.poison();
    drop(conn);

    let m = pool.metrics();
    assert_eq!((m.live, m.created, m.closed), (0, 1, 1));
}
