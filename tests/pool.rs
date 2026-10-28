//! Behaviour of the bb8-style pool, each test inside its own compio runtime.
//!
//! The connection is just its own `u32` id, so a test can tell a reused
//! connection from a fresh one. Thread-locals let each test steer the manager;
//! every `#[test]` runs on its own thread, so the state is naturally isolated.

use std::{
    cell::{Cell, RefCell},
    fmt,
    future::Future,
    rc::Rc,
    task::{Context, Waker},
    time::{Duration, Instant},
};

use compio_pool::{ManageConnection, Pool, RunError};

thread_local! {
    static CREATED: Cell<u32> = const { Cell::new(0) };
    static FAIL_CONNECT: Cell<bool> = const { Cell::new(false) };
    static INVALID_ONCE: Cell<bool> = const { Cell::new(false) };
    static BROKEN: Cell<bool> = const { Cell::new(false) };
    static SLOW_CONNECT: Cell<Option<Duration>> = const { Cell::new(None) };
}

fn reset() {
    CREATED.with(|c| c.set(0));
    FAIL_CONNECT.with(|c| c.set(false));
    INVALID_ONCE.with(|c| c.set(false));
    BROKEN.with(|c| c.set(false));
    SLOW_CONNECT.with(|c| c.set(None));
}

#[derive(Debug)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for TestError {}

struct Mgr;

impl ManageConnection for Mgr {
    type Connection = u32;
    type Error = TestError;

    async fn connect(&self) -> Result<u32, TestError> {
        if let Some(d) = SLOW_CONNECT.with(|c| c.get()) {
            compio::time::sleep(d).await;
        }
        if FAIL_CONNECT.with(|f| f.replace(false)) {
            return Err(TestError("connect failed"));
        }
        let id = CREATED.with(|c| {
            c.set(c.get() + 1);
            c.get()
        });
        Ok(id)
    }

    async fn is_valid(&self, _conn: &mut u32) -> Result<(), TestError> {
        if INVALID_ONCE.with(|f| f.replace(false)) {
            return Err(TestError("not valid"));
        }
        Ok(())
    }

    fn has_broken(&self, _conn: &mut u32) -> bool {
        BROKEN.with(|b| b.get())
    }
}

fn run<F: Future>(f: F) -> F::Output {
    compio::runtime::Runtime::new().unwrap().block_on(f)
}

// Compile-time: the blueprint crosses threads, the per-thread pool does not.
const _: fn() = || {
    fn is_send<T: Send + Sync + Clone>() {}
    is_send::<Pool<Mgr>>();
};

#[test]
fn reuses_an_idle_connection() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(2)
            .test_on_check_out(false)
            .build(Mgr);
        let local = pool.local();

        let first = *local.get().await.unwrap(); // dropped here → returned idle
        let second = *local.get().await.unwrap();

        assert_eq!(first, second, "the same connection should come back");
        assert_eq!(CREATED.with(|c| c.get()), 1, "only one was ever opened");
    });
}

#[test]
fn enforces_max_size_then_times_out() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_millis(50))
            .build(Mgr);
        let local = pool.local();

        let held = local.get().await.unwrap();
        #[cfg(feature = "metrics")]
        assert_eq!(local.state().connections, 1);

        // At capacity with the only connection checked out: the next get waits
        // and then times out.
        let blocked = local.get().await;
        assert!(matches!(blocked, Err(RunError::TimedOut)));

        drop(held);
        assert!(local.get().await.is_ok(), "a freed slot lets get succeed");
    });
}

#[test]
fn drops_a_broken_connection() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(2)
            .test_on_check_out(false)
            .build(Mgr);
        let local = pool.local();

        BROKEN.with(|b| b.set(true));
        drop(local.get().await.unwrap()); // opened id 1, broken on return → dropped
        #[cfg(feature = "metrics")]
        {
            assert_eq!(local.state().connections, 0);
            assert_eq!(local.state().idle_connections, 0);
        }

        BROKEN.with(|b| b.set(false));
        let fresh = *local.get().await.unwrap();
        assert_eq!(fresh, 2, "a new connection replaces the broken one");
    });
}

#[test]
fn replaces_a_connection_that_fails_validation() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(2)
            .test_on_check_out(true)
            .build(Mgr);
        let local = pool.local();

        drop(local.get().await.unwrap()); // id 1 → idle

        INVALID_ONCE.with(|f| f.set(true)); // idle id 1 fails is_valid once
        let fresh = *local.get().await.unwrap();
        assert_eq!(
            fresh, 2,
            "the stale idle connection is discarded, a fresh one opened"
        );
        assert_eq!(CREATED.with(|c| c.get()), 2);
    });
}

#[test]
fn surfaces_a_connect_error_and_frees_the_slot() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .build(Mgr);
        let local = pool.local();

        FAIL_CONNECT.with(|f| f.set(true));
        assert!(matches!(local.get().await, Err(RunError::User(_))));
        #[cfg(feature = "metrics")]
        assert_eq!(
            local.state().connections,
            0,
            "the reserved slot was released"
        );

        assert!(
            local.get().await.is_ok(),
            "the pool recovers after a connect error"
        );
    });
}

#[test]
fn warm_opens_min_idle_up_front() {
    reset();
    run(async {
        let pool = Pool::builder().max_size(5).min_idle(3).build(Mgr);
        let local = pool.local();

        assert_eq!(local.warm().await.unwrap(), 3);
        #[cfg(feature = "metrics")]
        {
            let state = local.state();
            assert_eq!(state.connections, 3);
            assert_eq!(state.idle_connections, 3);
        }
    });
}

#[test]
fn warm_times_out_on_a_hanging_connect() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(2)
            .min_idle(2)
            .connection_timeout(Duration::from_millis(100))
            .build(Mgr);
        let local = pool.local();

        // A backend that never answers must not wedge warm() forever: each dial
        // is bounded by connection_timeout.
        SLOW_CONNECT.with(|c| c.set(Some(Duration::from_secs(10))));
        let start = Instant::now();
        assert!(matches!(local.warm().await, Err(RunError::TimedOut)));
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "warm() ignored connection_timeout"
        );

        // The slot the timed-out dial reserved was freed, so a normal get()
        // succeeds once the backend recovers.
        SLOW_CONNECT.with(|c| c.set(None));
        assert!(local.get().await.is_ok());
    });
}

#[test]
fn warm_refills_the_idle_floor_after_checkouts() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(5)
            .min_idle(3)
            .test_on_check_out(false)
            .build(Mgr);
        let local = pool.local();

        // Three connections leased, none idle. The floor is three idle, and there
        // is room for two more before max_size, so warm() opens two — it refills
        // the idle floor rather than counting the three already-open slots as
        // meeting it.
        let a = local.get().await.unwrap();
        let b = local.get().await.unwrap();
        let c = local.get().await.unwrap();
        assert_eq!(local.warm().await.unwrap(), 2);
        drop((a, b, c));
    });
}

#[test]
fn state_reports_live_counts_without_the_metrics_feature() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(3)
            .test_on_check_out(false)
            .build(Mgr);
        let local = pool.local();

        let a = local.get().await.unwrap();
        let b = local.get().await.unwrap();
        let s = local.state();
        assert_eq!(s.connections, 2, "two leased");
        assert_eq!(s.idle_connections, 0);
        assert_eq!(s.pending_waiters, 0);

        drop(a); // returns to the idle list
        let s = local.state();
        assert_eq!(s.connections, 2, "still two open");
        assert_eq!(s.idle_connections, 1, "one of them now idle");
        drop(b);
    });
}

#[test]
fn state_counts_pending_waiters_without_the_metrics_feature() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_secs(5))
            .build(Mgr);
        let local = pool.local();

        let held = local.get().await.unwrap();
        let w = local.clone();
        let task = compio::runtime::spawn(async move { w.get().await.map(|c| *c) });
        compio::time::sleep(Duration::from_millis(20)).await; // parked

        assert_eq!(local.state().pending_waiters, 1);
        drop(held); // wakes the waiter
        let _ = task.await.unwrap();
        assert_eq!(local.state().pending_waiters, 0);
    });
}

#[cfg(feature = "metrics")]
#[test]
fn statistics_count_direct_reuse_and_creation() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(2)
            .test_on_check_out(false)
            .build(Mgr);
        let local = pool.local();

        drop(local.get().await.unwrap()); // opens id 1, returns it idle
        drop(local.get().await.unwrap()); // reuses id 1

        let s = local.statistics();
        assert_eq!(s.connections_created, 1, "only one was ever opened");
        assert_eq!(s.get_direct, 2, "neither get had to wait");
        assert_eq!(s.get_waited, 0);

        // On a single thread the process-wide roll-up equals this thread's.
        assert_eq!(pool.statistics(), local.statistics());
        assert_eq!(pool.state(), local.state());
    });
}

#[cfg(feature = "metrics")]
#[test]
fn statistics_count_timeouts() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_millis(50))
            .build(Mgr);
        let local = pool.local();

        let _held = local.get().await.unwrap();
        assert!(matches!(local.get().await, Err(RunError::TimedOut)));
        assert_eq!(local.statistics().get_timed_out, 1);
    });
}

#[test]
fn a_waiter_is_counted_and_then_served() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_secs(5))
            .build(Mgr);
        let local = pool.local();

        let held = local.get().await.unwrap(); // the only slot

        let waiter = local.clone();
        let task = compio::runtime::spawn(async move { *waiter.get().await.unwrap() });

        // Let the spawned task park on the full pool.
        compio::time::sleep(Duration::from_millis(20)).await;
        #[cfg(feature = "metrics")]
        assert_eq!(local.state().pending_waiters, 1, "the getter is parked");

        drop(held); // frees the slot and wakes the waiter
        let got = task.await.unwrap();

        assert_eq!(got, 1, "the waiter reused the freed connection");
        #[cfg(feature = "metrics")]
        {
            let s = local.statistics();
            assert_eq!(s.get_waited, 1);
            assert_eq!(local.state().pending_waiters, 0);
        }
    });
}

#[test]
fn reap_and_maintain_retire_then_refill() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(3)
            .min_idle(2)
            .test_on_check_out(false)
            .idle_timeout(None)
            .max_lifetime(Some(Duration::from_millis(10)))
            .build(Mgr);
        let local = pool.local();

        assert_eq!(local.warm().await.unwrap(), 2);
        compio::time::sleep(Duration::from_millis(25)).await;

        // Both idle connections are now past max_lifetime.
        assert_eq!(local.reap(), 2);
        #[cfg(feature = "metrics")]
        {
            assert_eq!(local.state().connections, 0);
            assert_eq!(local.statistics().connections_closed_max_lifetime, 2);
        }

        // maintain reaps (nothing left) then warms back up to min_idle.
        assert_eq!(local.maintain().await.unwrap(), 2);
        #[cfg(feature = "metrics")]
        {
            assert_eq!(local.state().idle_connections, 2);
            assert_eq!(local.statistics().connections_created, 4);
        }
    });
}

#[test]
fn clear_drains_idle_connections_and_frees_their_slots() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(3)
            .min_idle(3)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_millis(100))
            .build(Mgr);
        let local = pool.local();

        local.warm().await.unwrap();
        #[cfg(feature = "metrics")]
        assert_eq!(local.state().idle_connections, 3);

        assert_eq!(local.clear(), 3);
        #[cfg(feature = "metrics")]
        {
            assert_eq!(local.state().connections, 0);
            assert_eq!(local.state().idle_connections, 0);
        }

        // Nothing is open now, so every slot must be usable again: `get` opens
        // fresh connections rather than timing out on capacity never given back.
        // (Regression: `clear` used to drop the connections but keep their slots,
        // wedging the thread's pool for good.)
        let a = local.get().await.expect("first get after clear");
        let b = local.get().await.expect("second get after clear");
        let c = local.get().await.expect("third get after clear");
        assert_eq!((*a, *b, *c), (4, 5, 6), "all three are fresh connections");
    });
}

#[test]
fn clear_hands_the_freed_capacity_to_a_parked_waiter() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_millis(300))
            .build(Mgr);
        let local = pool.local();

        let held = local.get().await.unwrap();
        let waiter = local.clone();
        let task = compio::runtime::spawn(async move { waiter.get().await.map(|c| *c) });
        compio::time::sleep(Duration::from_millis(20)).await; // parked

        // Return the connection (it goes idle and the waiter is woken for it),
        // then drain the idle list before the waiter runs — a failover drain.
        // The waiter must open a fresh connection, not time out on a leaked slot.
        drop(held);
        assert_eq!(local.clear(), 1);
        let got = task.await.unwrap().expect("waiter served after clear");
        assert_eq!(got, 2, "a fresh connection replaced the cleared one");
    });
}

#[test]
fn clear_after_close_wakes_a_waiter_with_nothing_idle() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_secs(5))
            .build(Mgr);
        let local = pool.local();

        let held = local.get().await.unwrap(); // the one slot is taken
        let waiter = local.clone();
        let task = compio::runtime::spawn(async move {
            let start = Instant::now();
            let errored = waiter.get().await.is_err();
            (start.elapsed(), errored)
        });
        compio::time::sleep(Duration::from_millis(20)).await; // parked

        // `held` stays leased, so nothing is idle for clear() to free. After
        // close(), clear() must still wake the parked waiter so it fails fast
        // instead of sitting out the 5s connection_timeout.
        pool.close();
        assert_eq!(local.clear(), 0);
        let (elapsed, errored) = task.await.unwrap();
        assert!(errored, "a closed pool hands out nothing");
        assert!(
            elapsed < Duration::from_secs(1),
            "waiter sat out its timeout instead of being woken: {elapsed:?}"
        );
        drop(held);
    });
}

#[test]
fn waiters_are_served_in_arrival_order() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_secs(5))
            .build(Mgr);
        let local = pool.local();
        let held = local.get().await.unwrap();

        let order = Rc::new(RefCell::new(Vec::new()));
        let mut tasks = Vec::new();
        for i in 0..4 {
            let local = local.clone();
            let order = order.clone();
            tasks.push(compio::runtime::spawn(async move {
                let _conn = local.get().await.unwrap();
                order.borrow_mut().push(i);
            }));
            // Park them one at a time so the arrival order is unambiguous.
            compio::time::sleep(Duration::from_millis(5)).await;
        }

        // Each return wakes the oldest waiter only, so the one connection is
        // passed down the queue in FIFO order.
        drop(held);
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(*order.borrow(), vec![0, 1, 2, 3]);
        assert_eq!(
            CREATED.with(|c| c.get()),
            1,
            "one connection served them all"
        );
    });
}

#[test]
fn a_woken_waiter_that_gives_up_passes_the_wake_on() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_millis(300))
            .build(Mgr);
        let local = pool.local();
        let held = local.get().await.unwrap();

        // First in line: a `get` driven by hand with a waker that does nothing,
        // so it is woken but never runs — the shape of a waiter whose timeout
        // fires in the same instant a connection comes back.
        let first = local.clone();
        let mut first_get = Box::pin(first.get());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(first_get.as_mut().poll(&mut cx).is_pending(), "parked");

        // Second in line: a real task.
        let second = local.clone();
        let task = compio::runtime::spawn(async move { second.get().await.map(|c| *c) });
        compio::time::sleep(Duration::from_millis(20)).await; // parked behind

        drop(held); // the connection goes to the first waiter...
        drop(first_get); // ...which gives up without taking it.

        // The wake must be handed on, or the second waiter times out while a
        // connection sits idle.
        let got = task.await.unwrap().expect("second waiter served");
        assert_eq!(got, 1, "it got the returned connection");
    });
}

#[test]
fn a_task_that_panics_holding_a_lease_returns_it() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(1)
            .test_on_check_out(false)
            .connection_timeout(Duration::from_secs(5))
            .build(Mgr);
        let local = pool.local();

        let holder = local.clone();
        let panicking = compio::runtime::spawn(async move {
            let _lease = holder.get().await.unwrap();
            compio::time::sleep(Duration::from_millis(20)).await;
            panic!("handler bug while holding a pooled connection");
        });
        compio::time::sleep(Duration::from_millis(5)).await; // it holds the only slot

        let waiter = local.clone();
        let parked = compio::runtime::spawn(async move { waiter.get().await.map(|c| *c) });

        // The lease drops during the panic's unwind, on this thread, and wakes
        // the waiter from there. That must neither abort the process (the
        // executor's abort-on-unwinding guard, ADR 0006, is on the cross-thread
        // path) nor lose the slot.
        assert!(
            panicking.await.is_err(),
            "the panic surfaces on the join handle"
        );
        assert_eq!(parked.await.unwrap().expect("waiter served"), 1);
    });
}

#[cfg(feature = "metrics")]
#[test]
fn statistics_outlive_a_dropped_local_pool() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(2)
            .test_on_check_out(false)
            .build(Mgr);
        {
            let local = pool.local();
            drop(local.get().await.unwrap());
            assert_eq!(pool.state().connections, 1);
        }
        // The thread's pool is gone: its live gauges with it, its totals kept.
        assert_eq!(pool.state(), compio_pool::State::default());
        assert_eq!(pool.statistics().connections_created, 1);
        assert_eq!(pool.statistics().get_direct, 1);

        // A new pool on the thread adds to the same totals.
        let local = pool.local();
        drop(local.get().await.unwrap());
        assert_eq!(pool.statistics().connections_created, 2);
        assert_eq!(pool.state().connections, 1);
    });
}

#[test]
fn close_rejects_gets_and_drops_returns() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(2)
            .test_on_check_out(false)
            .build(Mgr);
        let local = pool.local();

        let held = local.get().await.unwrap();
        #[cfg(feature = "metrics")]
        assert_eq!(local.state().connections, 1);

        pool.close();
        assert!(pool.is_closed());
        assert!(matches!(local.get().await, Err(RunError::Closed)));
        assert_eq!(
            local.warm().await.unwrap(),
            0,
            "warm is a no-op once closed"
        );

        drop(held); // a closing pool keeps nothing
        #[cfg(feature = "metrics")]
        {
            assert_eq!(local.state().connections, 0);
            assert_eq!(local.state().idle_connections, 0);
        }
    });
}
