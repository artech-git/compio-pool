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
        assert_eq!(fresh, 2, "the stale idle connection is discarded, a fresh one opened");
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
        assert_eq!(local.state().connections, 0, "the reserved slot was released");

        assert!(local.get().await.is_ok(), "the pool recovers after a connect error");
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
