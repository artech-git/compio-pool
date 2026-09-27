//! Cost of the acquire fast path, and how much of it is the shard lookup.
//!
//! Run with: `cargo bench`, or
//! `BENCH_JSON=target/bench/acquire.jsonl cargo bench --bench acquire` to also
//! record every measurement as data. See `benches/support/report.rs`.

use std::{
    hint::black_box,
    time::{Duration, Instant},
};

use compio_pool::{Config, Manage, Pool, SlotMeta};

#[path = "support/report.rs"]
mod report;

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

/// How many times each case is measured. The total number of iterations is
/// unchanged — `iters` is split across the repetitions — so this costs no extra
/// runtime and turns one number into a distribution.
const REPS: u32 = 10;

fn bench(name: &str, iters: u32, mut f: impl FnMut()) {
    let per_rep = iters / REPS;

    // Warm up so we time steady state, not first-touch shard creation.
    for _ in 0..per_rep {
        f();
    }

    let mut samples = Vec::with_capacity(REPS as usize);
    for _ in 0..REPS {
        let start = Instant::now();
        for _ in 0..per_rep {
            f();
        }
        samples.push(start.elapsed().as_nanos() as f64 / per_rep as f64);
    }

    // The median rather than the mean of the whole run: one descheduled
    // repetition should not move the headline number.
    let stats = report::Stats::of(&samples);
    println!(
        "{name:<34} {:>7.1} ns/op   (p50 of {REPS}; {:.1}-{:.1})",
        stats.p50, stats.min, stats.max
    );
    report::stat("acquire", "fast path", name, None, "ns/op", &stats);
}

#[compio::main]
async fn main() {
    const ITERS: u32 = 2_000_000;

    report::meta("acquire");

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
    // Two series, because the interesting question is not "does it get slower"
    // but "is anything shared". `Work::Acquire` is the whole fast path;
    // `Work::LocalOnly` is the thread-local part of it and touches nothing any
    // other thread can see. If the first degrades with thread count while the
    // second stays flat, the difference is contention on something shared, not
    // the machine running out of cores.
    println!("\nscaling (per-thread cost, whole fast path vs its parts):");
    for threads in [1usize, 2, 4, 8] {
        scaling(Work::Acquire, threads, 400_000);
        scaling(Work::LocalOnly, threads, 400_000);
        scaling(Work::SharedCounter, threads, 400_000);
    }
}

/// Which part of a checkout the scaling measurement drives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Work {
    /// `Pool::acquire` — shard lookup, recycle, guard, and the shared metrics
    /// counters it bumps on the way out.
    Acquire,
    /// Only the thread-local half: the shard lookup. No atomics, nothing another
    /// thread shares a cache line with.
    LocalOnly,
    /// Neither: one `AtomicU64::fetch_add` on a counter every thread shares,
    /// which is what `Metrics` does on every checkout. Here to attribute the
    /// difference between the two series above, rather than guess at it — if
    /// this curve has the same shape as `Acquire`, the shape is coherence
    /// traffic on a shared cache line and not the pool's structure.
    SharedCounter,
}

impl Work {
    fn label(self) -> &'static str {
        match self {
            Work::Acquire => "acquire",
            Work::LocalOnly => "shard lookup only",
            Work::SharedCounter => "one shared atomic",
        }
    }
}

/// Measures the per-thread fast-path cost at `threads`, repeatedly.
///
/// The repetition is the point. A single pass at 4 threads has been seen to
/// report anything from 362 to 837 ns/op in one binary, so a lone figure says
/// more about scheduling luck than about the pool. `iters` is split across the
/// repetitions, so the work done is the same as one long pass.
fn scaling(work: Work, threads: usize, iters: u32) {
    const SCALING_REPS: u32 = 5;

    let mut samples = Vec::with_capacity((SCALING_REPS as usize) * threads);
    for _ in 0..SCALING_REPS {
        samples.extend(scaling_once(work, threads, iters / SCALING_REPS));
    }

    let stats = report::Stats::of(&samples);
    println!(
        "  {threads:>2} threads  {:<18} p50 {:>7.1} ns/op   worst {:>7.1} ns/op",
        work.label(),
        stats.p50,
        stats.max,
    );
    report::stat(
        "acquire",
        "scaling",
        work.label(),
        Some(threads),
        "ns/op",
        &stats,
    );
}

/// One pass: every thread hammers its own shard, and each reports its own
/// per-operation cost.
fn scaling_once(work: Work, threads: usize, iters: u32) -> Vec<f64> {
    use std::sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering::Relaxed},
    };

    // One counter for every thread in this pass, so the fetch_add lands on the
    // same cache line each time — the situation `Metrics` is in.
    let shared = Arc::new(AtomicU64::new(0));

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
            let shared = shared.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        pool.warm().await.unwrap();
                        let once = || match work {
                            Work::Acquire => {
                                black_box(**poll_once(pool.acquire()).as_ref().unwrap());
                            }
                            Work::LocalOnly => {
                                black_box(pool.local_idle());
                            }
                            Work::SharedCounter => {
                                black_box(shared.fetch_add(1, Relaxed));
                            }
                        };
                        for _ in 0..iters / 10 {
                            once();
                        }
                        barrier.wait();
                        let start = Instant::now();
                        for _ in 0..iters {
                            once();
                        }
                        start.elapsed()
                    })
            })
        })
        .collect();

    workers
        .into_iter()
        .map(|w| w.join().unwrap().as_nanos() as f64 / iters as f64)
        .collect()
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
