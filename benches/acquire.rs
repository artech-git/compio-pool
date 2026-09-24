//! Cost of the acquire fast path, and how much of it is the shard lookup.
//!
//! Run with: `cargo bench`

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use compio_pool::{Config, Manage, Pool, SlotMeta};

/// Does as little as possible, so what we measure is the pool, not the backend.
struct NullManager;

impl Manage for NullManager {
    type Connection = u64;
    type Error = ();

    async fn connect(&self) -> Result<u64, ()> {
        Ok(0)
    }

    async fn recycle(&self, _conn: &mut u64, _meta: &SlotMeta) -> Result<(), ()> {
        Ok(())
    }
}

fn bench(name: &str, iters: u32, mut f: impl FnMut()) {
    // Warm up so we time steady state, not first-touch shard creation.
    for _ in 0..iters / 10 {
        f();
    }
    let start = Instant::now();
    for _ in 0..iters {
        f();
    }
    let per = start.elapsed() / iters;
    println!("{name:<34} {:>7.1} ns/op", per.as_nanos() as f64);
}

#[compio::main]
async fn main() {
    const ITERS: u32 = 2_000_000;

    let base = Config::new()
        .max_size(8)
        .min_idle(4)
        .max_lifetime(None)
        .idle_timeout(None);

    // Default config: `acquire_timeout` is Some(30s), so every checkout arms a
    // timer even though the fast path never waits.
    let timed = Pool::new(
        NullManager,
        base.clone().acquire_timeout(Duration::from_secs(30)),
    );
    timed.warm().await.unwrap();

    // Same pool with the timeout off, so `acquire` calls `acquire_inner` directly.
    let untimed = Pool::new(NullManager, base.clone().acquire_timeout(None));
    untimed.warm().await.unwrap();

    bench("acquire, acquire_timeout=30s", ITERS, || {
        let conn = poll_once(timed.acquire());
        black_box(**conn.as_ref().unwrap());
    });

    bench("acquire, acquire_timeout=None", ITERS, || {
        let conn = poll_once(untimed.acquire());
        black_box(**conn.as_ref().unwrap());
    });

    // Shard lookup alone: SHARDS.with + RefCell + HashMap::get + downcast + Rc::clone.
    bench("shard lookup only (HashMap)", ITERS, || {
        black_box(untimed.local_idle());
    });

    // Decompose what is left, to see which parts of the fast path they are.
    bench("  Instant::now() (in release)", ITERS, || {
        black_box(Instant::now());
    });
    bench("  Rc<Cell<bool>> alloc + drop", ITERS, || {
        black_box(std::rc::Rc::new(std::cell::Cell::new(false)));
    });

    // Baseline: what a thread-local Vec pop costs, for scale.
    let mut v: Vec<u64> = (0..64).collect();
    bench("Vec push+pop (baseline)", ITERS, || {
        let x = v.pop().unwrap();
        v.push(black_box(x));
    });

    // The question that actually matters for a thread-per-core pool: does the
    // per-thread cost stay flat as threads are added? Anything shared and
    // mutable on the fast path shows up here as cache-line ping-pong.
    println!("\nscaling (per-thread cost of the same fast path):");
    for threads in [1usize, 2, 4, 8] {
        scaling(threads, 400_000);
    }
}

fn scaling(threads: usize, iters: u32) {
    use std::sync::{Arc, Barrier};

    let pool = Pool::new(
        NullManager,
        Config::new()
            .max_size(8)
            .min_idle(4)
            .max_lifetime(None)
            .idle_timeout(None)
            .acquire_timeout(None),
    );
    let barrier = Arc::new(Barrier::new(threads));

    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let pool = pool.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        pool.warm().await.unwrap();
                        for _ in 0..iters / 10 {
                            black_box(**poll_once(pool.acquire()).as_ref().unwrap());
                        }
                        barrier.wait();
                        let start = Instant::now();
                        for _ in 0..iters {
                            black_box(**poll_once(pool.acquire()).as_ref().unwrap());
                        }
                        start.elapsed()
                    })
            })
        })
        .collect();

    let times: Vec<Duration> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    let worst = times.iter().max().unwrap().as_nanos() as f64 / iters as f64;
    let mean = times.iter().map(|t| t.as_nanos() as f64).sum::<f64>()
        / (times.len() as f64 * iters as f64);
    println!("  {threads:>2} threads   mean {mean:>6.1} ns/op   worst {worst:>6.1} ns/op");
}

/// Drives a non-yielding future to completion with one poll.
fn poll_once<F: Future>(fut: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    // `pin!` not `Box::pin`: a heap allocation here would be measured as if it
    // were pool overhead.
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("fast path unexpectedly yielded"),
    }
}
