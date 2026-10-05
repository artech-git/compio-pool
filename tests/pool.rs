//! Behaviour of the bb8-style pool, each test inside its own compio runtime.
//!
//! The connection is just its own `u32` id, so a test can tell a reused
//! connection from a fresh one. Thread-locals let each test steer the manager;
//! every `#[test]` runs on its own thread, so the state is naturally isolated.

use std::{cell::Cell, fmt, future::Future, time::Duration};

use compio_pool::{ManageConnection, Pool, RunError};

thread_local! {
    static CREATED: Cell<u32> = const { Cell::new(0) };
    static FAIL_CONNECT: Cell<bool> = const { Cell::new(false) };
    static INVALID_ONCE: Cell<bool> = const { Cell::new(false) };
    static BROKEN: Cell<bool> = const { Cell::new(false) };
}

fn reset() {
    CREATED.with(|c| c.set(0));
    FAIL_CONNECT.with(|c| c.set(false));
    INVALID_ONCE.with(|c| c.set(false));
    BROKEN.with(|c| c.set(false));
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
        assert_eq!(local.state().connections, 0);
        assert_eq!(local.state().idle_connections, 0);

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
        let state = local.state();
        assert_eq!(state.connections, 3);
        assert_eq!(state.idle_connections, 3);
    });
}

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
        assert_eq!(local.state().pending_waiters, 1, "the getter is parked");

        drop(held); // frees the slot and wakes the waiter
        let got = task.await.unwrap();

        assert_eq!(got, 1, "the waiter reused the freed connection");
        let s = local.statistics();
        assert_eq!(s.get_waited, 1);
        assert_eq!(local.state().pending_waiters, 0);
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
        assert_eq!(local.state().connections, 0);
        assert_eq!(local.statistics().connections_closed_max_lifetime, 2);

        // maintain reaps (nothing left) then warms back up to min_idle.
        assert_eq!(local.maintain().await.unwrap(), 2);
        assert_eq!(local.state().idle_connections, 2);
        assert_eq!(local.statistics().connections_created, 4);
    });
}

#[test]
fn clear_drains_idle_connections() {
    reset();
    run(async {
        let pool = Pool::builder()
            .max_size(3)
            .min_idle(3)
            .test_on_check_out(false)
            .build(Mgr);
        let local = pool.local();

        local.warm().await.unwrap();
        assert_eq!(local.state().idle_connections, 3);

        assert_eq!(local.clear(), 3);
        assert_eq!(local.state().connections, 0);
        assert_eq!(local.state().idle_connections, 0);
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
        assert_eq!(local.state().connections, 0);
        assert_eq!(local.state().idle_connections, 0);
    });
}
