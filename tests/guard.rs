//! The checkout guard: deref, metadata, poisoning and hand-off.

mod common;

use std::{
    cell::Cell,
    future::Future,
    panic::AssertUnwindSafe,
    rc::Rc,
    task::{Context, Waker},
    time::Duration,
};

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

// ---------------------------------------------------------------------------
// `run` / `run_async`: the scoped forms of a checkout.
// ---------------------------------------------------------------------------

/// The happy path: the value comes back, the connection goes back.
#[compio::test]
async fn run_returns_the_closures_value_and_pools_the_connection() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2));

    let out = pool
        .acquire()
        .await
        .unwrap()
        .run(|conn| Ok::<_, &str>(conn.id + 100))
        .unwrap();

    assert_eq!(out, 100, "the closure's value is passed straight through");
    assert_eq!(pool.local_idle(), 1, "the connection was returned");
    assert_eq!(counts.disconnected(), 0);
    assert_eq!(pool.metrics().poisoned, 0);
}

/// Mutations through the `&mut` outlive the call, because it is the pooled
/// connection itself and not a copy.
#[compio::test]
async fn run_hands_over_the_real_connection() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1));

    pool.acquire()
        .await
        .unwrap()
        .run(|conn| {
            conn.id = 77;
            Ok::<_, &str>(())
        })
        .unwrap();

    let conn = pool.acquire().await.unwrap();
    assert_eq!(conn.id, 77, "the same connection came back out");
    assert_eq!(conn.meta().uses, 1, "and the checkout was counted");
}

/// An `Err` is the protocol working as designed — a `SELECT` on a missing table
/// says nothing about the socket. Poisoning stays the caller's decision.
#[compio::test]
async fn run_does_not_poison_on_a_returned_error() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2));

    let err = pool
        .acquire()
        .await
        .unwrap()
        .run(|_| Err::<(), _>("no such table"))
        .unwrap_err();

    assert_eq!(err, "no such table");
    assert_eq!(pool.local_idle(), 1, "an error alone does not condemn it");
    assert_eq!(counts.disconnected(), 0);
    assert_eq!(pool.metrics().poisoned, 0);
}

/// The closure is handed the connection, not the guard, so it cannot poison
/// from the inside. A caller that already knows the connection is suspect
/// poisons before calling `run`, and the flag survives: poisoning is one-way and
/// `complete_op` never clears it.
#[compio::test]
async fn a_poison_set_before_run_survives_a_successful_closure() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(1));

    let conn = pool.acquire().await.unwrap();
    conn.poison();
    let out = conn.run(|_| Ok::<_, &str>("last request on a doomed socket"));

    assert!(out.is_ok(), "poisoning does not change the return value");
    assert_eq!(pool.local_idle(), 0, "but the connection is not pooled");
    assert_eq!(counts.disconnected(), 1);
    assert_eq!(pool.metrics().poisoned, 1);
}

/// The same for the async form, including across an await.
#[compio::test]
async fn a_poison_set_before_run_async_survives() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(1));

    let conn = pool.acquire().await.unwrap();
    conn.poison();
    conn.run_async(async |_| {
        compio::time::sleep(Duration::from_millis(1)).await;
        Ok::<(), &str>(())
    })
    .await
    .unwrap();

    assert_eq!(pool.local_idle(), 0);
    assert_eq!(counts.disconnected(), 1);
    assert_eq!(pool.metrics().poisoned, 1);
}

/// The reason there is no `catch_unwind` in `run`: the destructors already
/// handle it. The panic must reach the caller *and* leave the pool consistent.
#[compio::test]
async fn a_panicking_closure_poisons_the_connection_and_frees_the_budget() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    // max_size 1 makes the budget observable: if the panic leaked the slot, the
    // acquire at the end of this test would time out instead.
    let pool = Pool::new(
        manager,
        cfg().max_size(1).acquire_timeout(Duration::from_millis(50)),
    );

    let conn = pool.acquire().await.unwrap();
    let caught = std::panic::catch_unwind(AssertUnwindSafe(move || {
        conn.run(|_| -> Result<(), &str> { panic!("closure blew up") })
    }));

    let payload = caught.expect_err("the panic must reach the caller, not be swallowed");
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("closure blew up"),
        "and it must be the original payload, not a re-raised copy"
    );

    assert_eq!(pool.local_idle(), 0, "a panicked connection is never pooled");
    assert_eq!(counts.disconnected(), 1, "it was closed instead");
    assert_eq!(pool.metrics().poisoned, 1);
    assert_eq!(pool.local_size(), 0, "and its budget came back");

    // The proof that the budget really is free.
    let replacement = pool.acquire().await.unwrap();
    assert_eq!(replacement.id, 1, "a fresh connection, not the poisoned one");
}

/// A panic *after* the connection was already touched is the dangerous case:
/// half a request is on the wire. Same handling, and the mutation is not
/// observable by anyone else because the connection never returns to the pool.
#[compio::test]
async fn a_panic_midway_through_the_closure_still_discards_the_connection() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1));

    let guard = pool.acquire().await.unwrap();
    let caught = std::panic::catch_unwind(AssertUnwindSafe(move || {
        guard.run(|conn| {
            conn.id = 4242; // a partial write
            panic!("lost the connection mid-frame");
            #[allow(unreachable_code)]
            Ok::<(), &str>(())
        })
    }));
    assert!(caught.is_err());

    let fresh = pool.acquire().await.unwrap();
    assert_ne!(fresh.id, 4242, "nobody ever sees the half-written connection");
}

/// `run` takes no `Send` bound, so a closure capturing thread-local state
/// compiles. This is a compile-time assertion as much as a runtime one.
#[compio::test]
async fn run_accepts_a_closure_that_is_not_send() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));

    let local: Rc<Cell<u64>> = Rc::new(Cell::new(5));
    let captured = local.clone();
    assert_not_send(&captured);

    let out = pool
        .acquire()
        .await
        .unwrap()
        .run(move |conn| {
            captured.set(captured.get() + conn.id);
            Ok::<_, &str>(captured.get())
        })
        .unwrap();

    assert_eq!(out, 5);
    assert_eq!(local.get(), 5, "the same Rc, not a copy sent elsewhere");
}

#[compio::test]
async fn run_async_returns_the_value_and_pools_the_connection() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(2));

    let out = pool
        .acquire()
        .await
        .unwrap()
        .run_async(async |conn| {
            compio::time::sleep(Duration::from_millis(1)).await;
            conn.id = 8;
            Ok::<_, &str>(conn.id)
        })
        .await
        .unwrap();

    assert_eq!(out, 8, "the value survives the awaits");
    assert_eq!(pool.local_idle(), 1);
    assert_eq!(counts.disconnected(), 0);
    assert_eq!(pool.metrics().poisoned, 0);
}

#[compio::test]
async fn run_async_does_not_poison_on_a_returned_error() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(2));

    let err = pool
        .acquire()
        .await
        .unwrap()
        .run_async(async |_| {
            compio::time::sleep(Duration::from_millis(1)).await;
            Err::<(), _>("query failed")
        })
        .await
        .unwrap_err();

    assert_eq!(err, "query failed");
    assert_eq!(pool.local_idle(), 1);
    assert_eq!(pool.metrics().poisoned, 0);
}

/// A panic inside an `async` closure unwinds through the `poll`, so the same
/// two destructors run. Spawning is how a real caller would see this: `compio`
/// catches a task's panic and parks it in the `JoinHandle`, so the runtime and
/// the pool both survive — which is exactly why the connection must be poisoned
/// rather than left to a dying process.
#[compio::test]
async fn a_panicking_async_closure_poisons_the_connection_and_frees_the_budget() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(
        manager,
        cfg().max_size(1).acquire_timeout(Duration::from_millis(50)),
    );

    let task_pool = pool.clone();
    let joined = compio::runtime::spawn(async move {
        task_pool
            .acquire()
            .await
            .unwrap()
            .run_async(async |_| -> Result<(), &str> {
                compio::time::sleep(Duration::from_millis(1)).await;
                panic!("async closure blew up")
            })
            .await
    })
    .await;

    assert!(joined.is_err(), "the task panicked rather than returning");

    assert_eq!(pool.local_idle(), 0);
    assert_eq!(counts.disconnected(), 1);
    assert_eq!(pool.metrics().poisoned, 1);
    assert_eq!(pool.local_size(), 0, "the budget came back");

    let replacement = pool.acquire().await.unwrap();
    assert_eq!(replacement.id, 1);
}

/// The cancellation case falls out of the same two destructors: dropping the
/// future mid-`await` is indistinguishable from a panic as far as the protocol
/// is concerned, and gets the same treatment without any extra code.
#[compio::test]
async fn a_run_async_future_dropped_mid_await_poisons_the_connection() {
    let manager = LocalManager::new();
    let counts = manager.counts();
    let pool = Pool::new(manager, cfg().max_size(1));

    {
        let conn = pool.acquire().await.unwrap();
        let mut fut = Box::pin(conn.run_async(async |_| {
            // An operation the kernel has already been told about, and whose
            // completion is still coming.
            std::future::pending::<()>().await;
            Ok::<(), &str>(())
        }));

        let mut cx = Context::from_waker(Waker::noop());
        assert!(
            fut.as_mut().poll(&mut cx).is_pending(),
            "the operation is in flight"
        );
        drop(fut); // the caller's `select!` picked the other branch
    }

    assert_eq!(pool.local_idle(), 0, "a cancelled connection is not reused");
    assert_eq!(counts.disconnected(), 1);
    assert_eq!(pool.metrics().poisoned, 1);

    let replacement = pool.acquire().await.unwrap();
    assert_eq!(replacement.id, 1);
}

/// Completing normally after an `await` must leave nothing armed behind.
#[compio::test]
async fn run_async_leaves_no_armed_guard_behind() {
    let pool = Pool::new(LocalManager::new(), cfg().max_size(1));

    for expected_uses in 0..3 {
        let conn = pool.acquire().await.unwrap();
        assert_eq!(conn.meta().uses, expected_uses);
        conn.run_async(async |_| {
            compio::time::sleep(Duration::from_millis(1)).await;
            Ok::<(), &str>(())
        })
        .await
        .unwrap();
        assert_eq!(pool.local_idle(), 1, "reusable every time round");
    }
    assert_eq!(pool.metrics().poisoned, 0);
    assert_eq!(pool.metrics().created, 1, "one connection did all the work");
}

fn assert_not_send<T>(_: &T) {}
