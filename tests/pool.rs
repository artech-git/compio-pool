//! `LocalPool` accounting, run inside a single compio runtime.

use std::{
    cell::Cell,
    future::Future,
    io,
    pin::pin,
    task::{Context, Poll, Waker},
};

use compio_pool::{CoreId, LocalPool, Resource, WorkerContext};

thread_local! {
    static CREATED: Cell<u32> = const { Cell::new(0) };
    static FAIL_NEXT: Cell<bool> = const { Cell::new(false) };
}

struct Item {
    id: u32,
    reusable: bool,
}

impl Resource for Item {
    async fn create(_cx: &WorkerContext) -> io::Result<Self> {
        if FAIL_NEXT.with(|f| f.replace(false)) {
            return Err(io::Error::other("boom"));
        }
        let id = CREATED.with(|c| {
            c.set(c.get() + 1);
            c.get()
        });
        Ok(Item { id, reusable: true })
    }

    fn recycle(&mut self) -> bool {
        self.reusable
    }
}

fn cx(capacity: usize) -> WorkerContext {
    WorkerContext {
        index: 0,
        core: CoreId { id: 0 },
        pinned: false,
        workers: 1,
        capacity,
    }
}

fn run<F: Future>(f: F) -> F::Output {
    compio::runtime::Runtime::new().unwrap().block_on(f)
}

/// Poll once with a no-op waker.
fn poll_once<F: Future>(f: &mut std::pin::Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    f.as_mut().poll(&mut cx)
}

#[test]
fn permits_count_against_capacity_and_refund_on_drop() {
    run(async {
        let pool = LocalPool::<Item>::new(cx(2), 2);
        assert!(pool.has_capacity());
        let a = pool.try_reserve().expect("slot 1");
        let b = pool.try_reserve().expect("slot 2");
        assert!(pool.try_reserve().is_none(), "capacity 2 means two permits");
        assert_eq!(pool.taken(), 2);
        assert_eq!(pool.available(), 0);
        drop(a);
        assert_eq!(pool.taken(), 1);
        let _c = pool.try_reserve().expect("refunded slot");
        drop(b);
        assert_eq!(pool.taken(), 1);
    });
}

#[test]
fn leases_create_lazily_and_reuse_idle_resources() {
    run(async {
        CREATED.with(|c| c.set(0));
        let pool = LocalPool::<Item>::new(cx(2), 2);
        assert_eq!(pool.created(), 0);

        let lease = pool.try_reserve().unwrap().acquire().await.unwrap();
        let first = lease.id;
        assert_eq!(pool.created(), 1);
        assert_eq!(pool.idle(), 0);
        drop(lease);
        assert_eq!(pool.idle(), 1, "recycled onto the idle list");
        assert_eq!(pool.taken(), 0);

        let lease = pool.try_reserve().unwrap().acquire().await.unwrap();
        assert_eq!(
            lease.id, first,
            "the idle resource is reused, not recreated"
        );
        assert_eq!(pool.created(), 1);
    });
}

#[test]
fn recycle_false_drops_the_resource() {
    run(async {
        let pool = LocalPool::<Item>::new(cx(1), 1);
        let mut lease = pool.try_reserve().unwrap().acquire().await.unwrap();
        lease.reusable = false;
        drop(lease);
        assert_eq!(pool.idle(), 0);
        assert_eq!(pool.taken(), 0);
    });
}

#[test]
fn discard_drops_the_resource_and_frees_the_slot() {
    run(async {
        let pool = LocalPool::<Item>::new(cx(1), 1);
        let lease = pool.try_reserve().unwrap().acquire().await.unwrap();
        lease.discard();
        assert_eq!(pool.idle(), 0);
        assert_eq!(pool.taken(), 0);
        assert!(pool.has_capacity());
    });
}

#[test]
fn create_failure_releases_the_permit() {
    run(async {
        let pool = LocalPool::<Item>::new(cx(1), 1);
        FAIL_NEXT.with(|f| f.set(true));
        let err = pool
            .try_reserve()
            .unwrap()
            .acquire()
            .await
            .expect_err("creation fails");
        assert_eq!(err.to_string(), "boom");
        assert_eq!(pool.taken(), 0, "the slot is not leaked");
        assert!(pool.try_reserve().is_some());
    });
}

#[test]
fn reserve_waits_until_a_slot_is_released() {
    run(async {
        let pool = LocalPool::<Item>::new(cx(1), 1);
        let held = pool.try_reserve().unwrap();

        let mut waiting = pin!(pool.reserve());
        assert!(
            poll_once(&mut waiting).is_pending(),
            "full pool: reserve waits"
        );

        drop(held);
        match poll_once(&mut waiting) {
            Poll::Ready(permit) => {
                assert_eq!(pool.taken(), 1);
                drop(permit);
            }
            Poll::Pending => panic!("released slot should satisfy the waiter"),
        }
    });
}

#[test]
fn wait_available_does_not_take_the_slot() {
    run(async {
        let pool = LocalPool::<Item>::new(cx(1), 1);
        let held = pool.try_reserve().unwrap();
        let mut waiting = pin!(pool.wait_available());
        assert!(poll_once(&mut waiting).is_pending());
        drop(held);
        assert!(poll_once(&mut waiting).is_ready());
        assert_eq!(pool.taken(), 0, "wait_available only observes");
        assert!(pool.has_capacity());
    });
}

#[test]
fn drained_resolves_when_nothing_is_outstanding() {
    run(async {
        let pool = LocalPool::<Item>::new(cx(2), 2);
        let mut drained = pin!(pool.drained());
        assert!(poll_once(&mut drained).is_ready(), "empty pool is drained");

        let lease = pool.try_reserve().unwrap().acquire().await.unwrap();
        let mut drained = pin!(pool.drained());
        assert!(poll_once(&mut drained).is_pending());
        drop(lease);
        assert!(poll_once(&mut drained).is_ready());
    });
}

#[test]
fn unbounded_reservation_goes_over_capacity_and_does_not_grow_the_idle_list() {
    run(async {
        let pool = LocalPool::<Item>::new(cx(1), 1);
        let a = pool.try_reserve().unwrap().acquire().await.unwrap();
        let b = pool.reserve_unbounded().acquire().await.unwrap();
        assert_eq!(pool.taken(), 2);
        assert_eq!(pool.available(), 0);
        assert!(!pool.has_capacity());

        // Returning the extra one: capacity is 1 and one lease is still out, so
        // there is no room on the idle list; it is dropped.
        drop(b);
        assert_eq!(pool.idle(), 0);
        assert_eq!(pool.taken(), 1);
        drop(a);
        assert_eq!(pool.idle(), 1);
        assert_eq!(pool.taken(), 0);
    });
}

#[test]
fn prewarm_is_capped_at_capacity() {
    run(async {
        CREATED.with(|c| c.set(0));
        let pool = LocalPool::<Item>::new(cx(3), 3);
        assert_eq!(pool.prewarm(10).await.unwrap(), 3);
        assert_eq!(pool.idle(), 3);
        assert_eq!(pool.prewarm(10).await.unwrap(), 0, "already full");
        let _lease = pool.try_reserve().unwrap().acquire().await.unwrap();
        assert_eq!(pool.created(), 3, "lease came from the warm list");
    });
}

#[test]
fn pool_handles_share_state() {
    run(async {
        let pool = LocalPool::<Item>::new(cx(1), 1);
        let other = pool.clone();
        let _p = pool.try_reserve().unwrap();
        assert!(other.try_reserve().is_none());
        assert_eq!(other.taken(), 1);
    });
}
