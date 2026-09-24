//! Cost of the cross-thread exchange: lock-free `ArrayQueue` against the
//! `Mutex<Vec<_>>` it replaced.
//!
//! Both designs are compiled into this one binary and measured back to back on
//! the same machine, so the comparison does not depend on run-to-run scheduling
//! variance the way an A/B across two builds would.
//!
//! Run with: `cargo bench --bench exchange`

use std::{
    hint::black_box,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use compio_pool::{
    Config, Detach, Exchange, Manage, Parked, Pool, Reservoir, SlotMeta, Unparked,
};

/// Does as little as possible, so what we measure is the exchange.
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

impl Detach for NullManager {
    type Parked = u64;

    fn detach(conn: u64) -> Option<u64> {
        Some(conn)
    }

    async fn attach(parked: u64) -> Result<u64, ()> {
        Ok(parked)
    }
}

// ------------------------------------------------- the design being replaced

struct Entry<P> {
    parked: P,
    meta: SlotMeta,
}

/// The previous `Reservoir`: a LIFO stack behind a `Mutex`, with the length
/// mirrored into an atomic so `parked()` does not have to take the lock.
struct MutexReservoir<M: Detach> {
    capacity: usize,
    stack: Mutex<Vec<Entry<M::Parked>>>,
    len: AtomicU64,
}

impl<M: Detach> MutexReservoir<M> {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            stack: Mutex::new(Vec::with_capacity(capacity)),
            len: AtomicU64::new(0),
        }
    }
}

impl<M: Detach> Exchange<M> for MutexReservoir<M> {
    fn park(&self, conn: M::Connection, meta: SlotMeta) -> Parked<M> {
        let mut stack = self.stack.lock().unwrap_or_else(|p| p.into_inner());
        if stack.len() >= self.capacity {
            return Parked::Refused(conn, meta);
        }
        let Some(parked) = M::detach(conn) else {
            return Parked::Destroyed;
        };
        stack.push(Entry { parked, meta });
        self.len.store(stack.len() as u64, Relaxed);
        Parked::Accepted
    }

    async fn unpark(&self) -> Unparked<M> {
        let entry = {
            let mut stack = self.stack.lock().unwrap_or_else(|p| p.into_inner());
            let Some(entry) = stack.pop() else {
                return Unparked::Empty;
            };
            self.len.store(stack.len() as u64, Relaxed);
            entry
        };
        match M::attach(entry.parked).await {
            Ok(conn) => Unparked::Claimed(conn, entry.meta),
            Err(_) => Unparked::Lost,
        }
    }

    fn parked(&self) -> u64 {
        self.len.load(Relaxed)
    }

    fn clear(&self) {
        let mut stack = self.stack.lock().unwrap_or_else(|p| p.into_inner());
        stack.clear();
        self.len.store(0, Relaxed);
    }
}

// ------------------------------------------------------------------ harness

fn poll_once<F: Future>(fut: F) -> F::Output {
    use std::task::{Context, Poll, Waker};
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("exchange unexpectedly yielded"),
    }
}

fn meta() -> SlotMeta {
    let now = Instant::now();
    SlotMeta {
        created_at: now,
        last_used: now,
        uses: 0,
        generation: 0,
    }
}

/// One park + one unpark, which is what a connection crossing a thread costs.
fn round_trip<X: Exchange<NullManager>>(exchange: &X) {
    match exchange.park(black_box(1u64), meta()) {
        Parked::Accepted => {}
        _ => panic!("park refused"),
    }
    match poll_once(exchange.unpark()) {
        Unparked::Claimed(conn, _) => {
            black_box(conn);
        }
        _ => panic!("unpark came back empty"),
    }
}

/// Drives `threads` threads through `iters` park/unpark round trips each and
/// returns the worst per-thread cost, which is what a caller actually feels.
fn contended<X: Exchange<NullManager>>(exchange: Arc<X>, threads: usize, iters: u32) -> f64 {
    let barrier = Arc::new(Barrier::new(threads));
    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let exchange = exchange.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                for _ in 0..iters / 10 {
                    round_trip(&*exchange);
                }
                barrier.wait();
                let start = Instant::now();
                for _ in 0..iters {
                    round_trip(&*exchange);
                }
                start.elapsed()
            })
        })
        .collect();
    let worst = workers
        .into_iter()
        .map(|w| w.join().unwrap())
        .max()
        .unwrap_or(Duration::ZERO);
    worst.as_nanos() as f64 / iters as f64
}

/// End-to-end: `min_idle` is 0, so *every* checkout returns through the
/// exchange and every acquire on an empty shard steals from it.
fn through_pool<X: Exchange<NullManager>>(exchange: X, threads: usize, iters: u32) -> f64 {
    let pool = Pool::builder(NullManager)
        .config(
            Config::new()
                .max_size(4)
                .min_idle(0)
                .acquire_timeout(None)
                .idle_timeout(None)
                .max_lifetime(None),
        )
        .exchange(exchange)
        .build();

    let barrier = Arc::new(Barrier::new(threads));
    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let pool = pool.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        for _ in 0..iters / 10 {
                            black_box(*poll_once(pool.acquire()).unwrap());
                        }
                        barrier.wait();
                        let start = Instant::now();
                        for _ in 0..iters {
                            black_box(*poll_once(pool.acquire()).unwrap());
                        }
                        start.elapsed()
                    })
            })
        })
        .collect();
    let worst = workers
        .into_iter()
        .map(|w| w.join().unwrap())
        .max()
        .unwrap_or(Duration::ZERO);
    worst.as_nanos() as f64 / iters as f64
}

/// Per-operation latency distribution, which is the real argument for a
/// lock-free structure: a mutex under contention convoys, so the unlucky
/// caller waits behind everyone else, while a queue that never blocks
/// degrades more evenly.
fn latency_profile<X: Exchange<NullManager>>(
    exchange: Arc<X>,
    threads: usize,
    iters: u32,
) -> Vec<u64> {
    let barrier = Arc::new(Barrier::new(threads));
    let workers: Vec<_> = (0..threads)
        .map(|_| {
            let exchange = exchange.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                for _ in 0..iters / 10 {
                    round_trip(&*exchange);
                }
                barrier.wait();
                let mut samples = Vec::with_capacity(iters as usize);
                for _ in 0..iters {
                    let t = Instant::now();
                    round_trip(&*exchange);
                    samples.push(t.elapsed().as_nanos() as u64);
                }
                samples
            })
        })
        .collect();
    let mut all = Vec::new();
    for w in workers {
        all.extend(w.join().unwrap());
    }
    all.sort_unstable();
    all
}

fn pct(sorted: &[u64], p: f64) -> u64 {
    sorted[(((sorted.len() - 1) as f64) * p).round() as usize]
}

fn main() {
    const ITERS: u32 = 500_000;
    const CAP: usize = 256;

    println!("park + unpark round trip (ns/op, worst thread)\n");
    println!(
        "{:>8}  {:>14}  {:>14}  {:>9}",
        "threads", "ArrayQueue", "Mutex<Vec>", "change"
    );

    for threads in [1usize, 2, 4, 8] {
        // Interleave the two so a thermal or scheduling drift during the run
        // hits both designs rather than whichever went second.
        // Best of two each, alternating, so neither design is charged for a
        // drift that happened while the other was running.
        let queue = contended(Arc::new(Reservoir::<NullManager>::new(CAP)), threads, ITERS);
        let mutex = contended(
            Arc::new(MutexReservoir::<NullManager>::new(CAP)),
            threads,
            ITERS,
        );
        let queue = queue.min(contended(
            Arc::new(Reservoir::<NullManager>::new(CAP)),
            threads,
            ITERS,
        ));
        let mutex = mutex.min(contended(
            Arc::new(MutexReservoir::<NullManager>::new(CAP)),
            threads,
            ITERS,
        ));
        println!(
            "{threads:>8}  {queue:>14.1}  {mutex:>14.1}  {:>8.2}x",
            mutex / queue
        );
    }

    println!("\npark + unpark latency distribution at 8 threads (ns)\n");
    println!(
        "{:>12}  {:>8} {:>8} {:>8} {:>10} {:>10}",
        "", "p50", "p90", "p99", "p99.9", "max"
    );
    for (name, samples) in [
        (
            "ArrayQueue",
            latency_profile(Arc::new(Reservoir::<NullManager>::new(CAP)), 8, 100_000),
        ),
        (
            "Mutex<Vec>",
            latency_profile(
                Arc::new(MutexReservoir::<NullManager>::new(CAP)),
                8,
                100_000,
            ),
        ),
    ] {
        println!(
            "{name:>12}  {:>8} {:>8} {:>8} {:>10} {:>10}",
            pct(&samples, 0.50),
            pct(&samples, 0.90),
            pct(&samples, 0.99),
            pct(&samples, 0.999),
            pct(&samples, 1.0),
        );
    }

    println!("\nfull acquire path, min_idle=0 so every checkout crosses the exchange\n");
    println!(
        "{:>8}  {:>14}  {:>14}  {:>9}",
        "threads", "ArrayQueue", "Mutex<Vec>", "change"
    );
    for threads in [1usize, 2, 4, 8] {
        let queue = through_pool(Reservoir::<NullManager>::new(CAP), threads, 200_000);
        let mutex = through_pool(MutexReservoir::<NullManager>::new(CAP), threads, 200_000);
        let queue = queue.min(through_pool(
            Reservoir::<NullManager>::new(CAP),
            threads,
            200_000,
        ));
        let mutex = mutex.min(through_pool(
            MutexReservoir::<NullManager>::new(CAP),
            threads,
            200_000,
        ));
        println!(
            "{threads:>8}  {queue:>14.1}  {mutex:>14.1}  {:>8.2}x",
            mutex / queue
        );
    }
}
