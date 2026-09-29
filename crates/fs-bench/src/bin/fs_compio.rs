//! The compio arm: `io_uring` file I/O, handles pooled per thread by
//! `compio-pool`.
//!
//! One OS thread per worker, each with its own runtime, its own ring and its own
//! shard — the topology the pool is designed for. A read is an SQE pushed on the
//! thread that wants it and a CQE collected on the same thread. No descriptor
//! crosses a thread, no operation crosses a thread, and the pool's acquire path
//! touches no atomics.
//!
//! # Maximum configuration
//!
//! Each ring is built with the largest submission queue the builder accepts
//! (`capacity`), and with `coop_taskrun` and `taskrun_flag` on: completions are
//! run cooperatively on the submitting task rather than through an IPI, and the
//! kernel sets a flag the driver can check instead of entering the kernel to
//! ask. Both are the right setting for a thread-per-core ring that is polled by
//! its own thread, and both reduce exactly the cost this arm is here to
//! demonstrate is absent.
//!
//! Each worker is pinned to its own core, so a 12-thread point is twelve rings
//! on twelve cores rather than twelve rings the scheduler is free to stack.
//!
//! Run with: `cargo run --profile maxopt --bin fs_compio`

use std::{
    collections::HashSet,
    io,
    sync::{
        Arc, Barrier,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use compio::{
    buf::BufResult,
    fs::{File, OpenOptions},
    io::{AsyncReadAt, AsyncWriteAt},
};
use compio_pool::{Config, Manage, NoExchange, Pool, Pooled, SlotMeta};
use fs_bench::{Case, Dataset, Outcome, Rng, Usage, drive};

/// Opens dataset handles, and does nothing on the way back.
///
/// `recycle` is deliberately a no-op, matching the microbenchmark manager in
/// `benches/acquire.rs` and the deadpool arm's manager: a liveness check would be
/// a syscall per checkout, and then the comparison would be between two health
/// checks rather than between two pools. `docs/performance.md` records the same
/// caveat.
struct Files {
    ds: Dataset,
    write: bool,
    /// Round-robins which dataset file the next handle opens. Shared with no one
    /// else, but shared across this arm's shards so the spread over files
    /// matches the deadpool arm's.
    next: Arc<AtomicUsize>,
}

impl Manage for Files {
    type Connection = File;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<File> {
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        if self.write {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(self.ds.write_path(i))
                .await
        } else {
            File::open(self.ds.read_path(i)).await
        }
    }

    async fn recycle(&self, _conn: &mut File, _meta: &SlotMeta) -> io::Result<()> {
        Ok(())
    }
}

/// A checked-out handle. `Pooled` takes the exchange as a parameter and does not
/// default it; this arm uses `NoExchange`, because a file descriptor has no
/// reason to migrate — every thread opens its own from the same dataset.
type Handle = Pooled<Files, NoExchange>;

fn main() -> io::Result<()> {
    drive("compio", &mut run)
}

fn run(
    case: Case,
    threads: usize,
    depth: usize,
    budget: Duration,
    ds: &Dataset,
) -> io::Result<Outcome> {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    // `threads + 1`: the parent stands at the start line too, so it learns the
    // exact moment the measured window opens and can take the resource snapshot
    // there instead of around the setup.
    let barrier = Arc::new(Barrier::new(threads + 1));
    let next = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::with_capacity(threads);

    for tid in 0..threads {
        let (ds, barrier, next) = (ds.clone(), barrier.clone(), next.clone());
        workers.push(
            std::thread::Builder::new()
                .name(format!("compio-{tid}"))
                .spawn(move || -> io::Result<Vec<f64>> {
                    let rt = build_runtime(tid % cores)?;
                    rt.block_on(worker(case, depth, budget, ds, tid, barrier, next))
                })?,
        );
    }

    barrier.wait();
    let before = Usage::now();
    let opened = Instant::now();
    // Sampled here, with every ring's thread up and running: after the join the
    // answer would be 1.
    let threads_at_start = fs_bench::os_threads();

    let mut lat_ns = Vec::with_capacity(threads);
    let mut failure = None;
    for w in workers {
        match w.join() {
            Ok(Ok(v)) => lat_ns.push(v),
            Ok(Err(e)) => failure = Some(e),
            // Joined before returning, so one worker failing does not leave the
            // rest detached and still hammering the disk under the next case.
            Err(_) => failure = Some(io::Error::other("compio worker panicked")),
        }
    }
    let window = opened.elapsed();
    let usage = Usage::now().since(before);
    if let Some(e) = failure {
        return Err(e);
    }
    Ok(Outcome {
        lat_ns,
        window,
        usage,
        os_threads: threads_at_start.max(fs_bench::os_threads()),
    })
}

/// One ring, on one core, configured as wide as the builder allows.
fn build_runtime(cpu: usize) -> io::Result<compio::runtime::Runtime> {
    let mut proactor = compio::driver::ProactorBuilder::new();
    proactor
        // 4096 SQEs: more than any depth this bench offers, so a full queue is
        // never what is being measured.
        .capacity(4096)
        .coop_taskrun(true)
        .taskrun_flag(true)
        // Demanded, not suggested. With compio's default features off the
        // `io-uring` feature goes with them, and what is left is a stub driver
        // that supports no operations; asking for the ring by name turns that
        // into a build error instead of a benchmark of nothing.
        .driver_type(compio::driver::DriverType::IoUring);
    let mut affinity = HashSet::new();
    affinity.insert(cpu);
    compio::runtime::Runtime::builder()
        .with_proactor(proactor)
        .thread_affinity(affinity)
        .build()
}

/// The per-thread half: build the shard, pre-open what the case holds, wait for
/// the other threads, then run `depth` operations in flight until the deadline.
async fn worker(
    case: Case,
    depth: usize,
    budget: Duration,
    ds: Dataset,
    tid: usize,
    barrier: Arc<Barrier>,
    next: Arc<AtomicUsize>,
) -> io::Result<Vec<f64>> {
    let pool = Pool::new(
        Files {
            ds: ds.clone(),
            write: case.writes(),
            next,
        },
        Config::new().max_size(depth.max(1)).min_idle(0),
    );

    // Pre-open every handle the run will use. For the held cases each slot keeps
    // one for the duration; for `acquire_read4k` they are opened and returned, so
    // the shard's free list is populated and the measured acquires hit the fast
    // path instead of `connect`.
    let mut held: Vec<Option<Handle>> = Vec::with_capacity(depth);
    if matches!(case, Case::Stat | Case::OpenClose) {
        held.resize_with(depth, || None);
    } else {
        for _ in 0..depth {
            held.push(Some(pool.acquire().await.map_err(io::Error::other)?));
        }
        if !case.holds_handle() {
            held.clear();
            held.resize_with(depth, || None);
        }
    }

    // Blocking, and that is fine: nothing is in flight yet and the deadline has
    // not started.
    barrier.wait();
    let deadline = Instant::now() + budget;

    let runs = held
        .into_iter()
        .enumerate()
        .map(|(slot, handle)| slot_loop(case, slot, depth, tid, deadline, handle, &pool, &ds));
    let results = futures_util::future::join_all(runs).await;

    let mut lat = Vec::new();
    for r in results {
        lat.extend(r?);
    }
    Ok(lat)
}

/// One in-flight stream of operations. `depth` of these run concurrently on one
/// thread, which is what gives the ring something to batch.
#[allow(clippy::too_many_arguments)]
async fn slot_loop(
    case: Case,
    slot: usize,
    depth: usize,
    tid: usize,
    deadline: Instant,
    mut handle: Option<Handle>,
    pool: &Pool<Files>,
    ds: &Dataset,
) -> io::Result<Vec<f64>> {
    let mut lat: Vec<f64> = Vec::with_capacity(8192);
    let mut rng = Rng::new(((tid as u64 + 1) << 32) ^ (slot as u64 + 1));
    let len = case.bytes_per_op() as usize;
    // Read buffers carry no initialised bytes: compio fills the spare capacity
    // and sets the length, so each iteration clears it back to empty.
    let mut rbuf: Vec<u8> = Vec::with_capacity(len.max(1));
    // Write buffers carry their payload, because `IoBuf` writes `buf[..len]`.
    let mut wbuf: Vec<u8> = vec![0xA5; len.max(1)];
    let mut path_i = slot;
    let mut seq = slot as u64 * (64 << 10);

    loop {
        let started = Instant::now();
        match case {
            Case::Stat => {
                let p = ds.read_path(path_i);
                path_i += depth;
                compio::fs::metadata(&p).await?;
            }
            Case::OpenClose => {
                let p = ds.read_path(path_i);
                path_i += depth;
                File::open(&p).await?.close().await?;
            }
            Case::AcquireRead4k => {
                let guard = pool.acquire().await.map_err(io::Error::other)?;
                let off = rng.offset(ds.read_file_size, len as u64);
                rbuf.clear();
                let BufResult(res, back) = guard.read_at(rbuf, off).await;
                rbuf = back;
                res?;
                drop(guard);
            }
            Case::Read4kRand => {
                let f = handle.as_mut().expect("held case has a handle");
                let off = rng.offset(ds.read_file_size, len as u64);
                rbuf.clear();
                let BufResult(res, back) = f.read_at(rbuf, off).await;
                rbuf = back;
                res?;
            }
            Case::Read64kSeq => {
                let f = handle.as_mut().expect("held case has a handle");
                let off = seq % (ds.read_file_size - len as u64);
                seq += len as u64;
                rbuf.clear();
                let BufResult(res, back) = f.read_at(rbuf, off).await;
                rbuf = back;
                res?;
            }
            Case::Write4kFsync => {
                let f = handle.as_mut().expect("held case has a handle");
                let off = write_offset(ds.write_region, len as u64, slot, seq);
                seq += len as u64;
                let BufResult(res, back) = f.write_at(wbuf, off).await;
                wbuf = back;
                res?;
                f.sync_data().await?;
            }
            Case::Write1mBuffered => {
                let f = handle.as_mut().expect("held case has a handle");
                let off = write_offset(ds.write_region, len as u64, slot, seq);
                seq += len as u64;
                let BufResult(res, back) = f.write_at(wbuf, off).await;
                wbuf = back;
                res?;
            }
        }
        let now = Instant::now();
        lat.push(now.duration_since(started).as_nanos() as f64);
        // The clock is read twice per operation already; reusing the second read
        // as the deadline check keeps it at two.
        if now >= deadline {
            break;
        }
    }
    Ok(lat)
}

/// Where slot `slot` writes its `seq`-th block.
///
/// Slots are spread a megabyte apart so concurrent writers on one thread are not
/// all hammering one block group, and the whole thing wraps inside the
/// preallocated region so the file never grows.
fn write_offset(region: u64, len: u64, slot: usize, seq: u64) -> u64 {
    let span = region - len;
    (((slot as u64).wrapping_mul(1 << 20).wrapping_add(seq)) % span) & !0xFFF
}
