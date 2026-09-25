//! Cancellation safety on the acquire path.
//!
//! With completion-based IO, dropping a future does not undo what the kernel is
//! already doing. `acquire` claims shard budget *before* awaiting `connect`,
//! `unpark` or `recycle`, and `acquire` is itself cancellable — by the acquire
//! timeout or by the caller's own `select!`. Every await in between therefore
//! has to return both the budget and, where one is riding along, the connection.

mod common;

use std::{
    future::Future,
    task::{Context, Poll, Waker},
    time::Duration,
};

use common::{LocalManager, MovableManager, cfg};
use compio_pool::{Error, Pool, Reservoir};

/// Cancelled inside `Manage::connect`, with budget claimed but no connection
/// behind it.
#[compio::test]
async fn a_cancelled_dial_returns_only_the_budget() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        cfg().max_size(1).acquire_timeout(Duration::from_millis(20)),
    );
    pool.manager().set_hang_connect(true);

    assert!(matches!(pool.acquire().await, Err(Error::Timeout)));

    assert_eq!(pool.local_size(), 0, "the claimed slot came back");
    let m = pool.metrics();
    assert_eq!(
        (m.live, m.created, m.closed),
        (0, 0, 0),
        "no connection ever existed, so nothing to account for: {m:?}"
    );
    assert_eq!(counts.disconnected(), 0);
}

/// Cancelled inside `Manage::recycle`, holding a connection that is out of the
/// free list but not yet inside a guard. Nobody else can return it, so the
/// cancelled future has to account for it.
#[compio::test]
async fn a_cancelled_recycle_accounts_for_the_connection_it_was_holding() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        cfg().max_size(1).acquire_timeout(Duration::from_millis(20)),
    );

    drop(pool.acquire().await.unwrap());
    assert_eq!(pool.local_idle(), 1);

    pool.manager().set_hang_recycle(true);
    assert!(matches!(pool.acquire().await, Err(Error::Timeout)));

    let m = pool.metrics();
    assert_eq!(m.live, 0, "the connection is gone");
    assert_eq!(m.closed, 1, "and counted as closed");
    assert_eq!(m.created, m.closed, "created and closed must balance");
    assert_eq!(pool.local_size(), 0, "the budget came back too");
    assert_eq!(pool.local_idle(), 0);
    assert_eq!(
        counts.disconnected(),
        0,
        "a cancelled future cannot await a graceful goodbye"
    );
}

#[compio::test]
async fn a_shard_survives_repeated_cancellations_inside_recycle() {
    let pool = Pool::new(
        LocalManager::new(),
        cfg().max_size(1).acquire_timeout(Duration::from_millis(20)),
    );

    for _ in 0..3 {
        // Each round: open one, return it, then lose it to a cancelled recycle.
        pool.manager().set_hang_recycle(false);
        drop(pool.acquire().await.unwrap());
        pool.manager().set_hang_recycle(true);
        assert!(matches!(pool.acquire().await, Err(Error::Timeout)));
        assert_eq!(pool.local_size(), 0, "capacity leaked");
    }

    pool.manager().set_hang_recycle(false);
    assert!(pool.acquire().await.is_ok());
    let m = pool.metrics();
    assert_eq!(m.created, m.closed + m.live);
}

/// Cancellation by the caller's own future, not by the acquire timeout: the
/// same guard has to cover both.
#[compio::test]
async fn a_checkout_abandoned_by_the_caller_returns_its_budget() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1).acquire_timeout(None));
    pool.manager().set_hang_connect(true);

    {
        // Polled once to get past `try_reserve` and into `connect`, then dropped
        // without ever completing — what a `select!` losing a race looks like.
        let mut fut = Box::pin(pool.acquire());
        assert!(matches!(
            fut.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        assert_eq!(pool.local_size(), 1, "the budget is claimed while dialling");
    }

    assert_eq!(pool.local_size(), 0, "and released when abandoned");
    pool.manager().set_hang_connect(false);
    assert!(pool.acquire().await.is_ok());
}

/// An `acquire` dropped while parked in the wait queue must leave the queue, or
/// the next returned connection is handed to a future nobody polls.
#[compio::test]
async fn an_abandoned_waiter_does_not_swallow_the_next_connection() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1).acquire_timeout(None));
    let held = pool.acquire().await.unwrap();

    {
        let mut fut = Box::pin(pool.acquire());
        assert!(matches!(
            fut.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }

    drop(held);
    assert_eq!(
        pool.local_idle(),
        1,
        "the connection came back to the shard"
    );
    assert!(pool.acquire().await.is_ok());
}

/// A claim from the reservoir that *does* complete must leave both the shard's
/// budget and the reservoir's own admission accounting square.
#[compio::test]
async fn a_completed_unpark_disarms_its_reservation() {
    let _lock = common::detach_lock();
    let pool = Pool::builder(MovableManager::new())
        .config(cfg().max_size(1).min_idle(0).acquire_timeout(None))
        .exchange(Reservoir::new(4))
        .build();

    // One connection parked, and the shard's budget free.
    drop(pool.acquire().await.unwrap());
    assert_eq!(pool.metrics().parked, 1);
    assert_eq!(pool.local_size(), 0);

    // `unpark` completes without yielding, so one poll runs it to the end.
    let mut fut = Box::pin(pool.acquire());
    let claimed = fut.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    assert!(matches!(claimed, Poll::Ready(Ok(_))));
    drop(claimed);
    drop(fut);

    let m = pool.metrics();
    assert_eq!(m.created, 1, "the parked socket was claimed, not replaced");
    assert_eq!(m.unparked, 1);
    assert_eq!(m.created, m.closed + m.live + m.parked, "conserved: {m:?}");
    assert_eq!(pool.local_size(), 0, "no budget leaked");
}

/// Cancelled *inside* `Detach::attach`, after the entry has been popped.
///
/// The shard's budget must come back and the shard must stay usable. Note what
/// is asserted below about the counters: the popped connection is dropped by the
/// cancelled future, and the pool does not count it closed, so `created` ends up
/// ahead of `closed + live + parked`. That is the behaviour as it stands, and
/// this test pins it so a future change to the accounting is a deliberate one.
#[compio::test]
async fn a_cancelled_reattach_returns_the_budget_but_loses_the_connection() {
    let _lock = common::detach_lock();
    let pool = Pool::builder(MovableManager::new())
        .config(
            cfg()
                .max_size(1)
                .min_idle(0)
                .acquire_timeout(Duration::from_millis(20)),
        )
        .exchange(Reservoir::new(4))
        .build();

    drop(pool.acquire().await.unwrap());
    assert_eq!(pool.metrics().parked, 1);

    common::HANG_ATTACH.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(matches!(pool.acquire().await, Err(Error::Timeout)));

    assert_eq!(pool.local_size(), 0, "the shard's budget came back");
    let m = pool.metrics();
    assert_eq!(m.parked, 0, "it was popped, not left in the reservoir");
    assert_eq!(m.live, 0);
    assert_eq!(
        (m.created, m.closed),
        (1, 0),
        "a connection lost inside `attach` is not counted closed: {m:?}"
    );

    // Whatever the counters say, the pool itself keeps working.
    common::HANG_ATTACH.store(false, std::sync::atomic::Ordering::SeqCst);
    assert!(pool.acquire().await.is_ok());
}

/// The timeout wraps the whole acquire, including the wait, so a caller blocked
/// at `max_size` gives up rather than hanging.
#[compio::test]
async fn the_timeout_covers_waiting_as_well_as_dialling() {
    let pool = Pool::new(
        LocalManager::new(),
        cfg().max_size(1).acquire_timeout(Duration::from_millis(20)),
    );
    let _held = pool.acquire().await.unwrap();

    let started = std::time::Instant::now();
    assert!(matches!(pool.acquire().await, Err(Error::Timeout)));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the wait was not bounded by the acquire timeout"
    );
    assert_eq!(pool.metrics().waits, 1);
    assert_eq!(pool.metrics().timeouts, 1);
}
