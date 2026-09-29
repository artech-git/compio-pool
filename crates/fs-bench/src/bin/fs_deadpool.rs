//! The tuned tokio arm: tokio + `deadpool` over pooled descriptors, positional
//! I/O, one blocking dispatch per operation.
//!
//! Everything `fs_tokio` leaves on the table is picked up here, so that the
//! compio column is measured against tokio at its best rather than tokio at its
//! most obvious:
//!
//! * **The descriptor is pooled.** `deadpool` holds `Arc<std::fs::File>`, so
//!   `acquire_read4k` is a checkout rather than an `open`.
//! * **One dispatch, not two.** `std::os::unix::fs::FileExt::read_at` is a
//!   `pread`: it takes `&self` and an offset, so there is no seek, and reaching
//!   an offset costs nothing extra.
//! * **One dispatch for write and sync.** The write and the `fdatasync` happen
//!   inside a single `spawn_blocking`, so the durability case pays one handoff
//!   rather than two.
//!
//! What remains is the one thing that cannot be tuned away: the handoff itself.
//! Every operation still moves to a blocking thread and back, and that is what
//! the `vcsw/op` column in the report is counting.
//!
//! # Why `Arc<std::fs::File>` and not `std::fs::File`
//!
//! `spawn_blocking` needs an owned `'static` closure, and `pread`/`pwrite` take
//! `&self`. An `Arc` clone into the closure gives the blocking thread a handle
//! without taking it out of the pool, so the pooled object stays checked out for
//! the whole of a held case exactly as the compio arm's guard does.
//!
//! # Maximum configuration
//!
//! Same runtime settings as `fs_tokio` — the point's thread count for
//! `worker_threads`, 4096 `max_blocking_threads`, above the 3072 the deepest
//! sweep point puts in flight — and a pool sized so every in-flight slot can
//! hold its own descriptor, which means a checkout never waits.
//!
//! Run with: `cargo run --profile maxopt --bin fs_deadpool`

use std::{
    io,
    os::unix::fs::FileExt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use deadpool::{
    Runtime,
    managed::{Manager, Metrics, Object, Pool as DeadPool, RecycleResult},
};
use fs_bench::{Case, Dataset, Outcome, Rng, Usage, drive};
use tokio::{sync::Barrier, task::spawn_blocking};

/// Opens dataset handles, and does nothing on the way back.
///
/// `recycle` is a no-op for the same reason the compio arm's is: a liveness check
/// would be a syscall per checkout, and the comparison would become one between
/// two health checks.
struct Files {
    ds: Dataset,
    write: bool,
    /// Round-robins which dataset file the next descriptor opens, matching the
    /// compio arm's spread over files.
    next: Arc<AtomicUsize>,
}

impl Manager for Files {
    type Type = Arc<std::fs::File>;
    type Error = io::Error;

    async fn create(&self) -> io::Result<Arc<std::fs::File>> {
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        let (path, write) = if self.write {
            (self.ds.write_path(i), true)
        } else {
            (self.ds.read_path(i), false)
        };
        // On the blocking pool, because an `open` is a blocking syscall and this
        // arm's whole premise is that such things do not run on a worker thread.
        spawn_blocking(move || {
            std::fs::OpenOptions::new()
                .read(true)
                .write(write)
                .open(path)
                .map(Arc::new)
        })
        .await
        .map_err(io::Error::other)?
    }

    async fn recycle(&self, _: &mut Arc<std::fs::File>, _: &Metrics) -> RecycleResult<io::Error> {
        Ok(())
    }
}

fn main() -> io::Result<()> {
    drive("deadpool", &mut run)
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
        .max_blocking_threads(4096)
        .enable_time()
        .build()?;
    let ds = ds.clone();
    rt.block_on(async move {
        let pool: DeadPool<Files> = DeadPool::builder(Files {
            ds: ds.clone(),
            write: case.writes(),
            next: Arc::new(AtomicUsize::new(0)),
        })
        // Every in-flight slot can hold its own descriptor, so a checkout in the
        // held cases never blocks on another slot returning one.
        .max_size(threads * depth.max(1))
        .wait_timeout(Some(Duration::from_secs(30)))
        .runtime(Runtime::Tokio1)
        .build()
        .map_err(io::Error::other)?;

        // Warm the pool to its full size before the barrier, so a measured
        // checkout is a checkout and not an `open` in disguise.
        if !matches!(case, Case::Stat | Case::OpenClose) {
            let mut warm = Vec::with_capacity(threads * depth.max(1));
            for _ in 0..threads * depth.max(1) {
                warm.push(pool.get().await.map_err(io::Error::other)?);
            }
            drop(warm);
        }

        // `threads + 1`: this task stands at the start line too, so the
        // resource snapshot is taken when the window opens rather than around the
        // setup.
        let barrier = Arc::new(Barrier::new(threads + 1));
        let mut tasks = Vec::with_capacity(threads);
        for tid in 0..threads {
            let (ds, barrier, pool) = (ds.clone(), barrier.clone(), pool.clone());
            tasks.push(tokio::spawn(async move {
                worker(case, depth, budget, ds, tid, barrier, pool).await
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

async fn worker(
    case: Case,
    depth: usize,
    budget: Duration,
    ds: Dataset,
    tid: usize,
    barrier: Arc<Barrier>,
    pool: DeadPool<Files>,
) -> io::Result<Vec<f64>> {
    let mut held: Vec<Option<Object<Files>>> = Vec::with_capacity(depth);
    for _ in 0..depth {
        held.push(if case.holds_handle() {
            Some(pool.get().await.map_err(io::Error::other)?)
        } else {
            None
        });
    }

    barrier.wait().await;
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

/// One in-flight stream of operations. Each iteration is exactly one
/// `spawn_blocking`, which is the floor for this design.
#[allow(clippy::too_many_arguments)]
async fn slot_loop(
    case: Case,
    slot: usize,
    depth: usize,
    tid: usize,
    deadline: Instant,
    handle: Option<Object<Files>>,
    pool: &DeadPool<Files>,
    ds: &Dataset,
) -> io::Result<Vec<f64>> {
    let mut lat: Vec<f64> = Vec::with_capacity(8192);
    let mut rng = Rng::new(((tid as u64 + 1) << 32) ^ (slot as u64 + 1));
    let len = case.bytes_per_op() as usize;
    // Owned by the loop and moved through each blocking call, so no buffer is
    // allocated per operation.
    let mut rbuf = Some(vec![0u8; len.max(1)]);
    let mut wbuf = Some(vec![0xA5u8; len.max(1)]);
    // Cloned once, not per operation: the `Arc` is what the blocking closure
    // needs and cloning it inside the timed section would measure the clone.
    let fd: Option<Arc<std::fs::File>> = handle.as_deref().cloned();
    let mut path_i = slot;
    let mut seq = slot as u64 * (64 << 10);

    loop {
        let started = Instant::now();
        match case {
            Case::Stat => {
                let p = ds.read_path(path_i);
                path_i += depth;
                spawn_blocking(move || std::fs::metadata(p))
                    .await
                    .map_err(io::Error::other)??;
            }
            Case::OpenClose => {
                let p = ds.read_path(path_i);
                path_i += depth;
                // The close is inside the blocking call, so unlike the `fs_tokio`
                // arm this case pays for it.
                spawn_blocking(move || std::fs::File::open(p).map(drop))
                    .await
                    .map_err(io::Error::other)??;
            }
            Case::AcquireRead4k => {
                let obj = pool.get().await.map_err(io::Error::other)?;
                let f: Arc<std::fs::File> = (*obj).clone();
                let off = rng.offset(ds.read_file_size, len as u64);
                let buf = rbuf.take().expect("buffer returns every iteration");
                let (buf, res) = spawn_blocking(move || {
                    let mut buf = buf;
                    let r = f.read_exact_at(&mut buf, off);
                    (buf, r)
                })
                .await
                .map_err(io::Error::other)?;
                rbuf = Some(buf);
                res?;
                drop(obj);
            }
            Case::Read4kRand | Case::Read64kSeq => {
                let f = fd.clone().expect("held case has a descriptor");
                let off = if case == Case::Read4kRand {
                    rng.offset(ds.read_file_size, len as u64)
                } else {
                    let o = seq % (ds.read_file_size - len as u64);
                    seq += len as u64;
                    o
                };
                let buf = rbuf.take().expect("buffer returns every iteration");
                let (buf, res) = spawn_blocking(move || {
                    let mut buf = buf;
                    let r = f.read_exact_at(&mut buf, off);
                    (buf, r)
                })
                .await
                .map_err(io::Error::other)?;
                rbuf = Some(buf);
                res?;
            }
            Case::Write4kFsync | Case::Write1mBuffered => {
                let f = fd.clone().expect("held case has a descriptor");
                let off = write_offset(ds.write_region, len as u64, slot, seq);
                seq += len as u64;
                let sync = case == Case::Write4kFsync;
                let buf = wbuf.take().expect("buffer returns every iteration");
                let (buf, res) = spawn_blocking(move || {
                    // Write and sync in one handoff: two would be a cost of the
                    // harness rather than of the design.
                    let r = f
                        .write_all_at(&buf, off)
                        .and_then(|()| if sync { f.sync_data() } else { Ok(()) });
                    (buf, r)
                })
                .await
                .map_err(io::Error::other)?;
                wbuf = Some(buf);
                res?;
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

/// Same offset schedule as the other two arms.
fn write_offset(region: u64, len: u64, slot: usize, seq: u64) -> u64 {
    let span = region - len;
    (((slot as u64).wrapping_mul(1 << 20).wrapping_add(seq)) % span) & !0xFFF
}
