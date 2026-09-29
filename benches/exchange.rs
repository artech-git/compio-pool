//! Cost of the cross-thread exchange, across all three designs it has had.
//!
//! * **Mutex** — the current [`Reservoir`]: a FIFO `VecDeque` behind a
//!   `parking_lot::Mutex`, which barges.
//! * **FairMutex** — the same structure behind `parking_lot::FairMutex`, which
//!   hands off to the longest waiter on every unlock. The arm that shows why
//!   the obvious fix for the tail is not one.
//! * **ArrayQueue** — the lock-free ring it replaced, with the atomic admission
//!   counter that a fallible push needs. Kept compiled in (crossbeam-queue is a
//!   dev-dependency now) so this stays a measurement rather than a memory.
//! * **Mutex<Vec>** — the original: a LIFO stack behind a `std::sync::Mutex`.
//!
//! All three are compiled into this one binary and measured back to back on
//! the same machine, so the comparison does not depend on run-to-run scheduling
//! variance the way an A/B across two builds would.
//!
//! Run with: `cargo bench --bench exchange`, or
//! `BENCH_JSON=target/bench/exchange.jsonl cargo bench --bench exchange` to also
//! record every measurement as data. See `benches/support/report.rs`.

use std::{
    hint::black_box,
    sync::{
        Arc, Barrier, Mutex,
        atomic::{
            AtomicU64, AtomicUsize,
            Ordering::{AcqRel, Acquire, Relaxed},
        },
    },
    time::{Duration, Instant},
};

use crossbeam_queue::ArrayQueue;
use parking_lot::FairMutex;

use compio_pool::{Config, Detach, Exchange, Manage, Parked, Pool, Reservoir, SlotMeta, Unparked};

#[path = "support/report.rs"]
mod report;

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

// --------------------------------- the current design, with the other lock

use std::collections::VecDeque;

/// Structurally identical to the crate's [`Reservoir`] — same FIFO `VecDeque`,
/// same single critical section — differing *only* in which parking_lot lock
/// it takes. `FairMutex` hands the lock to the longest waiter on every unlock;
/// the `Mutex` the crate ships barges instead.
///
/// This arm exists to isolate that one choice, and it is kept because the
/// answer is so lopsided: a strict handoff wakes a thread through the OS per
/// unlock, so a ~25ns critical section buys a ~20µs park/unpark. It is the arm
/// that stops anyone "fixing" the tail by reaching for the obvious lock.
struct FairReservoir<M: Detach> {
    queue: FairMutex<VecDeque<Entry<M::Parked>>>,
    capacity: usize,
    len: AtomicU64,
}

impl<M: Detach> FairReservoir<M> {
    fn new(capacity: usize) -> Self {
        Self {
            queue: FairMutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            len: AtomicU64::new(0),
        }
    }
}

impl<M: Detach> Exchange<M> for FairReservoir<M> {
    fn park(&self, conn: M::Connection, meta: SlotMeta) -> Parked<M> {
        let mut queue = self.queue.lock();
        if queue.len() == self.capacity {
            return Parked::Refused(conn, meta);
        }
        let Some(parked) = M::detach(conn) else {
            return Parked::Destroyed;
        };
        queue.push_back(Entry { parked, meta });
        self.len.store(queue.len() as u64, Relaxed);
        Parked::Accepted
    }

    async fn unpark(&self) -> Unparked<M> {
        let entry = {
            let mut queue = self.queue.lock();
            let Some(entry) = queue.pop_front() else {
                return Unparked::Empty;
            };
            self.len.store(queue.len() as u64, Relaxed);
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
        let mut queue = self.queue.lock();
        queue.clear();
        self.len.store(0, Relaxed);
    }
}

// ------------------------------------------------------ the previous design

struct Entry<P> {
    parked: P,
    meta: SlotMeta,
}

/// The `Reservoir` as it stood between the `Mutex<Vec>` and the `FairMutex`: a
/// lock-free bounded ring.
///
/// `ArrayQueue::push` is fallible, and by the time it runs the connection has
/// already been detached with no synchronous way to rebuild it — so a slot has
/// to be claimed with a CAS *before* detaching. That `admitted` counter is the
/// price of not having a critical section, and reproducing it faithfully here
/// is the point: it is what the current design deletes.
struct QueueReservoir<M: Detach> {
    queue: ArrayQueue<Entry<M::Parked>>,
    admitted: AtomicUsize,
}

impl<M: Detach> QueueReservoir<M> {
    fn new(capacity: usize) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
            admitted: AtomicUsize::new(0),
        }
    }

    fn admit(&self) -> bool {
        self.admitted
            .fetch_update(AcqRel, Acquire, |n| {
                (n < self.queue.capacity()).then_some(n + 1)
            })
            .is_ok()
    }

    fn readmit(&self) {
        self.admitted.fetch_sub(1, AcqRel);
    }
}

impl<M: Detach> Exchange<M> for QueueReservoir<M> {
    fn park(&self, conn: M::Connection, meta: SlotMeta) -> Parked<M> {
        if !self.admit() {
            return Parked::Refused(conn, meta);
        }
        let Some(parked) = M::detach(conn) else {
            self.readmit();
            return Parked::Destroyed;
        };
        if self.queue.push(Entry { parked, meta }).is_err() {
            self.readmit();
            return Parked::Destroyed;
        }
        Parked::Accepted
    }

    async fn unpark(&self) -> Unparked<M> {
        let Some(entry) = self.queue.pop() else {
            return Unparked::Empty;
        };
        self.readmit();
        match M::attach(entry.parked).await {
            Ok(conn) => Unparked::Claimed(conn, entry.meta),
            Err(_) => Unparked::Lost,
        }
    }

    fn parked(&self) -> u64 {
        self.queue.len() as u64
    }

    fn clear(&self) {
        while self.queue.pop().is_some() {
            self.readmit();
        }
    }
}

// ------------------------------------------------------ the original design

/// The first `Reservoir`: a LIFO stack behind a barging `Mutex`, with the length
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

/// Per-operation latency distribution, which is the real argument for a *fair*
/// lock: a barging mutex under contention lets the incumbent reacquire while
/// everyone else queues, so the median flatters it and the unlucky caller pays
/// for all of it. Sorting every sample is the only way to see that.
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

    report::meta("exchange");

    let hdr = || {
        println!(
            "{:>8}  {:>10}  {:>10}  {:>10}  {:>10}",
            "threads", "Mutex", "FairMutex", "ArrayQueue", "Mutex<Vec>"
        )
    };

    println!("park + unpark round trip (ns/op, worst thread)\n");
    hdr();
    for threads in [1usize, 2, 4, 8] {
        // Interleave the arms so a thermal or scheduling drift during the run
        // hits every design rather than whichever went last. Best of two
        // alternating passes each, so these are the floor.
        let mut r = [f64::MAX; 4];
        for _ in 0..2 {
            let pass = [
                contended(Arc::new(Reservoir::<NullManager>::new(CAP)), threads, ITERS),
                contended(Arc::new(FairReservoir::<NullManager>::new(CAP)), threads, ITERS),
                contended(Arc::new(QueueReservoir::<NullManager>::new(CAP)), threads, ITERS),
                contended(Arc::new(MutexReservoir::<NullManager>::new(CAP)), threads, ITERS),
            ];
            for (slot, v) in r.iter_mut().zip(pass) {
                *slot = slot.min(v);
            }
        }
        println!(
            "{threads:>8}  {:>10.1}  {:>10.1}  {:>10.1}  {:>10.1}",
            r[0], r[1], r[2], r[3]
        );
        for (name, v) in ARMS.iter().zip(r) {
            report::value("exchange", "round trip", name, Some(threads), "ns/op", v);
        }
    }

    println!("\npark + unpark latency distribution at 8 threads (ns)\n");
    println!(
        "{:>12}  {:>8} {:>8} {:>8} {:>10} {:>10}",
        "", "p50", "p90", "p99", "p99.9", "max"
    );
    for (name, samples) in [
        ("Mutex", latency_profile(Arc::new(Reservoir::<NullManager>::new(CAP)), 8, 100_000)),
        ("FairMutex", latency_profile(Arc::new(FairReservoir::<NullManager>::new(CAP)), 8, 100_000)),
        ("ArrayQueue", latency_profile(Arc::new(QueueReservoir::<NullManager>::new(CAP)), 8, 100_000)),
        ("Mutex<Vec>", latency_profile(Arc::new(MutexReservoir::<NullManager>::new(CAP)), 8, 100_000)),
    ] {
        println!(
            "{name:>12}  {:>8} {:>8} {:>8} {:>10} {:>10}",
            pct(&samples, 0.50),
            pct(&samples, 0.90),
            pct(&samples, 0.99),
            pct(&samples, 0.999),
            pct(&samples, 1.0),
        );
        let as_f64: Vec<f64> = samples.iter().map(|ns| *ns as f64).collect();
        report::stat("exchange", "tail latency", name, Some(8), "ns", &report::Stats::of(&as_f64));
    }

    println!("\nfull acquire path, min_idle=0 so every checkout crosses the exchange\n");
    hdr();
    for threads in [1usize, 2, 4, 8] {
        const PITERS: u32 = 200_000;
        let mut r = [f64::MAX; 4];
        for _ in 0..2 {
            let pass = [
                through_pool(Reservoir::<NullManager>::new(CAP), threads, PITERS),
                through_pool(FairReservoir::<NullManager>::new(CAP), threads, PITERS),
                through_pool(QueueReservoir::<NullManager>::new(CAP), threads, PITERS),
                through_pool(MutexReservoir::<NullManager>::new(CAP), threads, PITERS),
            ];
            for (slot, v) in r.iter_mut().zip(pass) {
                *slot = slot.min(v);
            }
        }
        println!(
            "{threads:>8}  {:>10.1}  {:>10.1}  {:>10.1}  {:>10.1}",
            r[0], r[1], r[2], r[3]
        );
        for (name, v) in ARMS.iter().zip(r) {
            report::value("exchange", "acquire path", name, Some(threads), "ns/op", v);
        }
    }
}

/// Column order, shared by the tables and the recorded data.
const ARMS: [&str; 4] = ["Mutex", "FairMutex", "ArrayQueue", "Mutex<Vec>"];
