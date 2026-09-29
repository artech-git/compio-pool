//! The naive tokio arm: `tokio::fs`, no pool, written the way the documentation
//! shows.
//!
//! This is the arm that says what the default costs. `tokio::fs` has no
//! asynchronous file I/O underneath it — every call is `spawn_blocking` around
//! the ordinary blocking syscall — so each operation is a handoff to the
//! blocking pool, the syscall, and a handoff back. The random-read cases pay it
//! twice, because `tokio::fs::File` has no positional read: reaching an offset
//! means `seek` and then `read`, and each is its own dispatch.
//!
//! It is included precisely because it is not tuned. `fs_deadpool` is the same
//! ecosystem with the obvious fixes applied — a pooled descriptor and one
//! `pread` — and the two together bracket what tokio can do, so the compio
//! column is not compared against a single unflattering configuration.
//!
//! # Maximum configuration
//!
//! `worker_threads` is the thread count of the point being measured, and
//! `max_blocking_threads` is 4096 — above the 3072 operations the deepest sweep
//! point puts in flight at once. That matters: the blocking pool is where this
//! arm's parallelism actually lives, so a cap below the offered concurrency
//! would be measuring the cap and calling it the filesystem.
//!
//! # One caveat, in tokio's favour
//!
//! Dropping a `tokio::fs::File` schedules the `close` on the blocking pool and
//! returns without waiting. The `open_close` case therefore measures the open and
//! not the close, while the compio arm awaits `File::close` and the deadpool arm
//! closes inside the blocking call. The bias is recorded rather than corrected,
//! because working around it would mean not using the API under test.
//!
//! Run with: `cargo run --profile maxopt --bin fs_tokio`

use std::{
    io::{self, SeekFrom},
    sync::Arc,
    time::{Duration, Instant},
};

use fs_bench::{Case, Dataset, Outcome, Rng, Usage, drive};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Barrier,
};

fn main() -> io::Result<()> {
    drive("tokio", &mut run)
}

fn run(
    case: Case,
    threads: usize,
    depth: usize,
    budget: Duration,
    ds: &Dataset,
) -> io::Result<Outcome> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        // The blocking pool is where every filesystem operation in this arm
        // actually runs, so it is given far more room than any point needs.
        .max_blocking_threads(4096)
        .enable_time()
        .build()?;
    let ds = ds.clone();
    rt.block_on(async move {
        // `threads + 1`: this task stands at the start line too, so the
        // resource snapshot is taken when the window opens rather than around the
        // setup.
        let barrier = Arc::new(Barrier::new(threads + 1));
        let mut tasks = Vec::with_capacity(threads);
        for tid in 0..threads {
            let (ds, barrier) = (ds.clone(), barrier.clone());
            tasks.push(tokio::spawn(async move {
                worker(case, depth, budget, ds, tid, barrier).await
            }));
        }
        barrier.wait().await;
        let before = Usage::now();
        let opened = Instant::now();
        let threads_at_start = fs_bench::os_threads();

        let mut lat_ns = Vec::with_capacity(threads);
        let mut failure = None;
        for t in tasks {
            match t.await {
                Ok(Ok(v)) => lat_ns.push(v),
                Ok(Err(e)) => failure = Some(e),
                Err(e) => failure = Some(io::Error::other(e)),
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
            // The blocking pool grows on demand, so its high-water mark is at the
            // end of the run rather than at the start.
            os_threads: threads_at_start.max(fs_bench::os_threads()),
        })
    })
}

/// One task's worth: open what the case holds, wait for the others, then run
/// `depth` operations in flight until the deadline.
async fn worker(
    case: Case,
    depth: usize,
    budget: Duration,
    ds: Dataset,
    tid: usize,
    barrier: Arc<Barrier>,
) -> io::Result<Vec<f64>> {
    // Every slot opens its own handle, because reaching an offset through
    // `tokio::fs::File` needs `&mut` for the seek — there is no sharing a file
    // between concurrent readers in this API.
    let mut held: Vec<Option<File>> = Vec::with_capacity(depth);
    for slot in 0..depth {
        held.push(if case.holds_handle() {
            Some(open_for(case, &ds, tid * depth + slot).await?)
        } else {
            None
        });
    }

    barrier.wait().await;
    let deadline = Instant::now() + budget;

    let runs = held
        .into_iter()
        .enumerate()
        .map(|(slot, handle)| slot_loop(case, slot, depth, tid, deadline, handle, &ds));
    let results = futures_util::future::join_all(runs).await;

    let mut lat = Vec::new();
    for r in results {
        lat.extend(r?);
    }
    Ok(lat)
}

/// The handle a held case runs on: read-only for the read cases, read-write for
/// the write cases, and never truncating — the region is preallocated.
async fn open_for(case: Case, ds: &Dataset, i: usize) -> io::Result<File> {
    if case.writes() {
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(ds.write_path(i))
            .await
    } else {
        File::open(ds.read_path(i)).await
    }
}

/// One in-flight stream of operations.
#[allow(clippy::too_many_arguments)]
async fn slot_loop(
    case: Case,
    slot: usize,
    depth: usize,
    tid: usize,
    deadline: Instant,
    mut handle: Option<File>,
    ds: &Dataset,
) -> io::Result<Vec<f64>> {
    let mut lat: Vec<f64> = Vec::with_capacity(8192);
    let mut rng = Rng::new(((tid as u64 + 1) << 32) ^ (slot as u64 + 1));
    let len = case.bytes_per_op() as usize;
    let mut rbuf = vec![0u8; len.max(1)];
    let wbuf = vec![0xA5u8; len.max(1)];
    let mut path_i = slot;
    let mut seq = slot as u64 * (64 << 10);

    loop {
        let started = Instant::now();
        match case {
            Case::Stat => {
                let p = ds.read_path(path_i);
                path_i += depth;
                tokio::fs::metadata(&p).await?;
            }
            Case::OpenClose => {
                let p = ds.read_path(path_i);
                path_i += depth;
                // The drop schedules the close on the blocking pool without
                // waiting for it. See the module docs.
                drop(File::open(&p).await?);
            }
            Case::AcquireRead4k => {
                let p = ds.read_path(path_i);
                path_i += depth;
                let mut f = File::open(&p).await?;
                let off = rng.offset(ds.read_file_size, len as u64);
                f.seek(SeekFrom::Start(off)).await?;
                f.read_exact(&mut rbuf).await?;
                drop(f);
            }
            Case::Read4kRand => {
                let f = handle.as_mut().expect("held case has a handle");
                let off = rng.offset(ds.read_file_size, len as u64);
                // Two dispatches, not one: the seek is a blocking-pool round trip
                // of its own before the read is even submitted.
                f.seek(SeekFrom::Start(off)).await?;
                f.read_exact(&mut rbuf).await?;
            }
            Case::Read64kSeq => {
                let f = handle.as_mut().expect("held case has a handle");
                let off = seq % (ds.read_file_size - len as u64);
                seq += len as u64;
                f.seek(SeekFrom::Start(off)).await?;
                f.read_exact(&mut rbuf).await?;
            }
            Case::Write4kFsync => {
                let f = handle.as_mut().expect("held case has a handle");
                let off = write_offset(ds.write_region, len as u64, slot, seq);
                seq += len as u64;
                f.seek(SeekFrom::Start(off)).await?;
                f.write_all(&wbuf).await?;
                // `write_all` queues into the file's own buffer; the flush is what
                // makes the syscall happen, and the sync is what makes it durable.
                f.flush().await?;
                f.sync_data().await?;
            }
            Case::Write1mBuffered => {
                let f = handle.as_mut().expect("held case has a handle");
                let off = write_offset(ds.write_region, len as u64, slot, seq);
                seq += len as u64;
                f.seek(SeekFrom::Start(off)).await?;
                f.write_all(&wbuf).await?;
                f.flush().await?;
            }
        }
        let now = Instant::now();
        lat.push(now.duration_since(started).as_nanos() as f64);
        if now >= deadline {
            break;
        }
    }
    Ok(lat)
}

/// Same offset schedule as the other two arms, so the three write to the same
/// blocks in the same order.
fn write_offset(region: u64, len: u64, slot: usize, seq: u64) -> u64 {
    let span = region - len;
    (((slot as u64).wrapping_mul(1 << 20).wrapping_add(seq)) % span) & !0xFFF
}
