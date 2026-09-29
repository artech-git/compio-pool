//! A filesystem-only three-way comparison, on one machine, over one dataset.
//!
//! `docs/performance.md` measures this crate against tokio + deadpool over
//! *sockets*. Nothing there says what happens when the thing being pooled is a
//! file, and files are the case where the two designs differ most sharply:
//!
//! * `tokio::fs` is not asynchronous. Every call is `spawn_blocking` around the
//!   ordinary blocking syscall, so each operation costs a handoff to a thread
//!   pool and back — two context switches that a socket read never pays.
//! * `compio::fs` is asynchronous, because `io_uring` will do file I/O. A read
//!   is one SQE on the calling thread's ring; nothing is handed anywhere.
//!
//! So the interesting axis is not "which pool is faster". It is how much of a
//! filesystem operation is the operation, and how much is the machinery around
//! it — which is why every case here reports CPU time and context switches per
//! operation alongside the latency distribution.
//!
//! # The three arms
//!
//! | binary | what it is | handle per op | dispatches per op |
//! |---|---|---|---|
//! | `fs_tokio` | `tokio::fs`, no pool, as a user would write it | opened per op, or held | 1–3 |
//! | `fs_deadpool` | tokio + `deadpool` of `Arc<std::fs::File>`, positional I/O | pooled | 1 |
//! | `fs_compio` | compio + `compio-pool` of `compio::fs::File` | pooled | 0 |
//!
//! `fs_tokio` is the naive arm on purpose: it is what you get from the
//! documented API. `fs_deadpool` is the same ecosystem tuned as far as it goes —
//! a pooled descriptor and one `pread` per operation instead of a seek and a
//! read. The two bracket tokio, so the compio column is not being compared
//! against a strawman.
//!
//! # Reading the numbers
//!
//! Every record carries a distribution, for the reason
//! `benches/support/report.rs` gives: one number per case invites comparisons
//! the run-to-run variance will not support. Percentiles are over every
//! operation on every thread; `worst_p99` is the highest p99 of any single
//! thread, which is the figure that catches a pool that is fast on average and
//! terrible on one shard.
//!
//! Reads are served from the page cache unless the dataset is larger than RAM —
//! the default is deliberately smaller, because a cache-warm read is the case
//! where the dispatch machinery is the *entire* cost and therefore the case that
//! separates the arms. `write_4k_fsync` is the counterweight: it goes to the
//! device on every operation.

use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// One filesystem operation, measured end to end.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Case {
    /// `stat` by path. No descriptor, no data: pure metadata dispatch.
    Stat,
    /// `open` then `close`, no I/O. The descriptor churn a pool exists to remove.
    OpenClose,
    /// Get a usable handle, read 4 KiB, release it. The pool-effect headline:
    /// the tokio arm opens, the pooled arms check out.
    AcquireRead4k,
    /// 4 KiB at a random offset on a handle held for the whole run. Submission
    /// overhead with the open taken out of the picture.
    Read4kRand,
    /// 64 KiB sequential on a held handle. Bandwidth rather than IOPS.
    Read64kSeq,
    /// 4 KiB write followed by `fdatasync`. The one case that reaches the device
    /// on every operation.
    Write4kFsync,
    /// 1 MiB write, no sync. Page-cache write bandwidth.
    Write1mBuffered,
}

impl Case {
    /// Every case, in the order the report tables use.
    pub const ALL: [Case; 7] = [
        Case::Stat,
        Case::OpenClose,
        Case::AcquireRead4k,
        Case::Read4kRand,
        Case::Read64kSeq,
        Case::Write4kFsync,
        Case::Write1mBuffered,
    ];

    /// The name used in `BENCH_JSON` and in `FSB_CASES`.
    pub fn as_str(self) -> &'static str {
        match self {
            Case::Stat => "stat",
            Case::OpenClose => "open_close",
            Case::AcquireRead4k => "acquire_read4k",
            Case::Read4kRand => "read_4k_rand",
            Case::Read64kSeq => "read_64k_seq",
            Case::Write4kFsync => "write_4k_fsync",
            Case::Write1mBuffered => "write_1m_buffered",
        }
    }

    /// Inverse of [`Case::as_str`].
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == s)
    }

    /// How long one repetition of this case runs for, before scaling.
    ///
    /// A budget rather than an operation count, because the arms differ by more
    /// than an order of magnitude on some cases: a count that gives compio
    /// enough samples makes the tokio arm take minutes, and a count that is kind
    /// to tokio leaves compio with a few hundred.
    pub fn budget_ms(self) -> u64 {
        match self {
            // Slow per operation, and the tail is the point, so it needs the
            // samples.
            Case::Write4kFsync => 1_500,
            // Moves a megabyte per operation; 600 ms is already gigabytes.
            Case::Write1mBuffered => 600,
            _ => 400,
        }
    }

    /// Bytes moved by one operation, for the MiB/s column. Zero where the
    /// operation moves none.
    pub fn bytes_per_op(self) -> u64 {
        match self {
            Case::Stat | Case::OpenClose => 0,
            Case::AcquireRead4k | Case::Read4kRand | Case::Write4kFsync => 4 << 10,
            Case::Read64kSeq => 64 << 10,
            Case::Write1mBuffered => 1 << 20,
        }
    }

    /// True where the operation runs on a handle checked out once for the whole
    /// run, rather than acquired per operation.
    pub fn holds_handle(self) -> bool {
        matches!(
            self,
            Case::Read4kRand | Case::Read64kSeq | Case::Write4kFsync | Case::Write1mBuffered
        )
    }

    /// True where the operation writes, and so needs the write dataset.
    pub fn writes(self) -> bool {
        matches!(self, Case::Write4kFsync | Case::Write1mBuffered)
    }
}

/// The files every arm runs against.
///
/// Created once by `fs_setup` and then left alone, so the three arms are
/// measured over byte-identical data with identical block allocation. Writes go
/// to preallocated regions: a growing file would mix block allocation and
/// metadata updates into what is meant to be a data write.
#[derive(Clone, Debug)]
pub struct Dataset {
    /// Where it lives. Must be on the filesystem under test, not a VM share.
    pub dir: PathBuf,
    /// How many files the read cases spread over.
    pub read_files: usize,
    /// Size of each read file.
    pub read_file_size: u64,
    /// Size of each per-thread write file.
    pub write_region: u64,
    /// How many write files to make — one per worker thread at the widest point.
    pub write_files: usize,
}

impl Dataset {
    /// Reads the shape from the environment.
    ///
    /// `FSB_DIR` is the only one that usually needs setting, and it must point at
    /// the filesystem under test.
    pub fn from_env() -> Self {
        Self {
            dir: env_str("FSB_DIR", "/var/tmp/fsbench").into(),
            read_files: env_usize("FSB_READ_FILES", 64),
            read_file_size: env_u64("FSB_READ_FILE_MIB", 32) << 20,
            write_region: env_u64("FSB_WRITE_REGION_MIB", 128) << 20,
            write_files: env_usize("FSB_WRITE_FILES", 16),
        }
    }

    /// Path of read file `i`, wrapping.
    pub fn read_path(&self, i: usize) -> PathBuf {
        self.dir
            .join("r")
            .join(format!("{}.dat", i % self.read_files))
    }

    /// Path of write file `i`, wrapping.
    pub fn write_path(&self, i: usize) -> PathBuf {
        self.dir
            .join("w")
            .join(format!("{}.dat", i % self.write_files))
    }

    /// Creates anything missing or the wrong size, and leaves the rest alone.
    ///
    /// Idempotent so a re-run does not spend minutes rewriting gigabytes.
    pub fn ensure(&self) -> io::Result<()> {
        fs::create_dir_all(self.dir.join("r"))?;
        fs::create_dir_all(self.dir.join("w"))?;
        // A pattern rather than zeroes: a filesystem or device that
        // special-cases all-zero blocks would make the write cases lie.
        let mut chunk = vec![0u8; 1 << 20];
        for (i, b) in chunk.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        for i in 0..self.read_files {
            fill(&self.read_path(i), self.read_file_size, &chunk)?;
        }
        for i in 0..self.write_files {
            fill(&self.write_path(i), self.write_region, &chunk)?;
        }
        Ok(())
    }
}

/// Writes `path` to exactly `size` bytes, skipping the work if it already is.
fn fill(path: &Path, size: u64, chunk: &[u8]) -> io::Result<()> {
    if fs::metadata(path).is_ok_and(|m| m.len() == size) {
        return Ok(());
    }
    let mut f = fs::File::create(path)?;
    let mut left = size;
    while left > 0 {
        let n = left.min(chunk.len() as u64) as usize;
        f.write_all(&chunk[..n])?;
        left -= n as u64;
    }
    // Durably, so the first measured run is not competing with writeback of the
    // setup itself.
    f.sync_all()
}

/// What one repetition produced.
pub struct Outcome {
    /// One latency vector per worker thread, in nanoseconds per operation.
    ///
    /// Per-thread rather than pooled, so `worst_p99` can be computed. A pool
    /// that is fast on average and terrible on one shard is a pool with a
    /// latency problem — `docs/performance.md`.
    pub lat_ns: Vec<Vec<f64>>,
    /// The measured span: from every worker being at the start line to the last
    /// one stopping.
    ///
    /// Not the wall time of the repetition. Standing up an arm is not free and
    /// it is not equally expensive across arms — the deadpool arm opens
    /// `threads * depth` descriptors before it starts, and the tokio arms grow a
    /// blocking pool to match — so throughput measured over the wall clock would
    /// charge each arm for its own setup and call the difference I/O.
    pub window: Duration,
    /// Process resource usage over `window`, and not over the setup.
    pub usage: Usage,
    /// Live OS threads at the high-water mark of the run.
    ///
    /// Sampled by the arm while its threads are still up, because by the time
    /// the driver sees an `Outcome` the runtime has been dropped and the answer
    /// would always be 1. It is the column that makes the tokio arms' cost
    /// visible as a resource rather than as a latency.
    pub os_threads: u64,
}

impl Outcome {
    /// Total operations across all threads.
    pub fn ops(&self) -> usize {
        self.lat_ns.iter().map(Vec::len).sum()
    }
}

/// Summary of repeated measurements, in the shape `benches/support/report.rs`
/// established.
#[derive(Debug, Clone, Copy)]
pub struct Stats {
    pub n: usize,
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
    pub mean: f64,
}

impl Stats {
    /// Summarises `samples`, which need not be sorted.
    ///
    /// # Panics
    ///
    /// If `samples` is empty; a measurement with no samples is a bug in the
    /// bench, not a case to paper over with zeroes.
    pub fn of(samples: &[f64]) -> Self {
        assert!(
            !samples.is_empty(),
            "a measurement needs at least one sample"
        );
        let mut sorted = samples.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).expect("no NaN in a duration"));
        Self {
            n: sorted.len(),
            min: sorted[0],
            p50: percentile(&sorted, 0.50),
            p95: percentile(&sorted, 0.95),
            p99: percentile(&sorted, 0.99),
            p999: percentile(&sorted, 0.999),
            max: sorted[sorted.len() - 1],
            mean: sorted.iter().sum::<f64>() / sorted.len() as f64,
        }
    }
}

/// Nearest-rank percentile of an already-sorted slice.
pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[(((sorted.len() - 1) as f64) * p).round() as usize]
}

/// The CPU side of the story, from `getrusage(RUSAGE_SELF)`.
///
/// The context-switch counters are the ones that matter: they are the direct
/// measurement of the thread handoff `tokio::fs` performs and `compio::fs` does
/// not, and they explain a latency gap that a latency table can only assert.
#[derive(Clone, Copy, Debug, Default)]
pub struct Usage {
    pub user_ns: u64,
    pub sys_ns: u64,
    pub vcsw: u64,
    pub ivcsw: u64,
    pub max_rss_kib: u64,
}

impl Usage {
    /// Snapshot of this process, now.
    pub fn now() -> Self {
        // SAFETY: `getrusage` writes a fully-initialised `rusage` into the
        // pointer on success, and we pass a valid one.
        unsafe {
            let mut ru: libc::rusage = std::mem::zeroed();
            if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
                return Self::default();
            }
            Self {
                user_ns: tv_ns(ru.ru_utime),
                sys_ns: tv_ns(ru.ru_stime),
                vcsw: ru.ru_nvcsw as u64,
                ivcsw: ru.ru_nivcsw as u64,
                max_rss_kib: ru.ru_maxrss as u64,
            }
        }
    }

    /// `self - earlier`, saturating. `max_rss_kib` is a high-water mark, so it is
    /// carried through rather than subtracted.
    pub fn since(self, earlier: Self) -> Self {
        Self {
            user_ns: self.user_ns.saturating_sub(earlier.user_ns),
            sys_ns: self.sys_ns.saturating_sub(earlier.sys_ns),
            vcsw: self.vcsw.saturating_sub(earlier.vcsw),
            ivcsw: self.ivcsw.saturating_sub(earlier.ivcsw),
            max_rss_kib: self.max_rss_kib,
        }
    }
}

fn tv_ns(tv: libc::timeval) -> u64 {
    tv.tv_sec as u64 * 1_000_000_000 + tv.tv_usec as u64 * 1_000
}

/// Live OS threads in this process, from `/proc/self/status`.
///
/// The number is the point of the comparison on the tokio side: the blocking
/// pool grows to whatever the offered concurrency demands, and this is what it
/// grew to.
pub fn os_threads() -> u64 {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("Threads:")?.trim().parse::<u64>().ok())
        })
        .unwrap_or(0)
}

/// A cheap deterministic PRNG for read offsets.
///
/// Deterministic so every arm visits the same offsets in the same order, and
/// cheap so the generator is not part of what is being measured.
pub struct Rng(u64);

impl Rng {
    /// Seeded so that the same `(thread, slot)` pair gives the same stream in
    /// every arm.
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    /// xorshift64\*. Named `next_u64` rather than `next` so it cannot be
    /// mistaken for `Iterator::next` at a call site.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A block-aligned offset at which `len` bytes still fit inside `size`.
    pub fn offset(&mut self, size: u64, len: u64) -> u64 {
        let blocks = (size.saturating_sub(len)) / 4096;
        if blocks == 0 {
            return 0;
        }
        (self.next_u64() % blocks) * 4096
    }
}

/// Which points of the matrix to run.
#[derive(Clone, Debug)]
pub struct Matrix {
    /// Cases for the thread-scaling panel.
    pub cases: Vec<Case>,
    /// Thread counts for the thread-scaling panel.
    pub threads: Vec<usize>,
    /// Queue depth used by the thread-scaling panel.
    pub depth: usize,
    /// Cases for the queue-depth panel.
    pub sweep_cases: Vec<Case>,
    /// Depths for the queue-depth panel.
    pub sweep_depths: Vec<usize>,
    /// Thread count the queue-depth panel is taken at.
    pub sweep_threads: usize,
    /// Measured repetitions per point. One warm-up repetition runs first and is
    /// discarded.
    pub reps: usize,
    /// Label for the filesystem under test — `ext4`, `tmpfs`. Recorded, not
    /// detected, because only the operator knows what was mounted where.
    pub surface: String,
    /// Multiplies every case budget, for a quick smoke run.
    pub budget_scale: f64,
}

impl Matrix {
    /// Reads the matrix from the environment, with the defaults the report used.
    pub fn from_env() -> Self {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        Self {
            cases: env_cases("FSB_CASES", &Case::ALL),
            threads: env_list("FSB_THREADS", &[1, 2, 4, 8, cores]),
            depth: env_usize("FSB_DEPTH", 32),
            sweep_cases: env_cases("FSB_SWEEP_CASES", &[Case::Read4kRand, Case::AcquireRead4k]),
            sweep_depths: env_list("FSB_SWEEP_DEPTHS", &[1, 4, 16, 64, 256]),
            sweep_threads: env_usize("FSB_SWEEP_THREADS", cores),
            reps: env_usize("FSB_REPS", 5),
            surface: env_str("FSB_SURFACE", "unknown"),
            budget_scale: env_str("FSB_BUDGET_SCALE", "1.0").parse().unwrap_or(1.0),
        }
    }

    /// The budget for one repetition of `case`.
    pub fn budget(&self, case: Case) -> Duration {
        Duration::from_secs_f64(case.budget_ms() as f64 * self.budget_scale / 1000.0)
    }
}

/// Where one measurement sits in the matrix, for labelling its records.
#[derive(Clone, Copy)]
struct Ctx<'a> {
    arm: &'a str,
    panel: &'a str,
    case: Case,
    surface: &'a str,
    threads: usize,
    depth: usize,
    rep: usize,
}

/// What each arm supplies to [`drive`]: given a case, a thread count, a queue
/// depth, a time budget and the dataset, run it and report one latency vector
/// per worker thread.
pub type Runner<'a> = dyn FnMut(Case, usize, usize, Duration, &Dataset) -> io::Result<Outcome> + 'a;

/// Runs the whole matrix for one arm and emits every record.
///
/// `run` is the only thing that differs between the three binaries.
pub fn drive(arm: &str, run: &mut Runner<'_>) -> io::Result<()> {
    let m = Matrix::from_env();
    let ds = Dataset::from_env();
    ds.ensure()?;
    meta(arm, &m, &ds);

    let mut points: Vec<(&str, Case, usize, usize)> = Vec::new();
    for &case in &m.cases {
        for &t in &m.threads {
            points.push(("threads", case, t, m.depth));
        }
    }
    for &case in &m.sweep_cases {
        for &d in &m.sweep_depths {
            points.push(("depth", case, m.sweep_threads, d));
        }
    }

    for (panel, case, threads, depth) in points {
        let budget = m.budget(case);
        // One discarded repetition: first touch of a shard, of the blocking
        // pool, and of the ring's buffers should not land in the samples.
        let warm = Duration::from_secs_f64(budget.as_secs_f64() / 4.0);
        if let Err(e) = run(case, threads, depth, warm, &ds) {
            eprintln!("{arm}: warmup {} t={threads} d={depth}: {e}", case.as_str());
            continue;
        }
        for rep in 0..m.reps {
            let ctx = Ctx {
                arm,
                panel,
                case,
                surface: &m.surface,
                threads,
                depth,
                rep,
            };
            let started = Instant::now();
            let out = match run(case, threads, depth, budget, &ds) {
                Ok(o) => o,
                Err(e) => {
                    eprintln!("{arm}: {} t={threads} d={depth}: {e}", case.as_str());
                    continue;
                }
            };
            emit(ctx, &out, started.elapsed());
        }
        eprintln!(
            "{arm}: {panel} {} t={threads} d={depth} done",
            case.as_str()
        );
    }
    Ok(())
}

/// Emits every record for one measurement.
fn emit(ctx: Ctx<'_>, out: &Outcome, wall: Duration) {
    let all: Vec<f64> = out.lat_ns.iter().flatten().copied().collect();
    if all.is_empty() {
        eprintln!("{}: no samples for {}", ctx.arm, ctx.case.as_str());
        return;
    }
    let ops = all.len() as f64;
    let secs = out.window.as_secs_f64();
    let used = out.usage;
    stat(ctx, "ns/op", &Stats::of(&all));

    // The highest p99 of any one thread. Reported separately rather than folded
    // into the pooled distribution, because the pooled p99 hides a single
    // starved shard behind eleven healthy ones.
    let worst = out
        .lat_ns
        .iter()
        .filter(|v| !v.is_empty())
        .map(|v| Stats::of(v).p99)
        .fold(0.0f64, f64::max);
    value(ctx, "worst_p99_ns", worst);

    value(ctx, "ops/s", ops / secs);
    value(ctx, "window_ms", secs * 1000.0);
    // What it cost to get to the start line. Interesting in its own right: it is
    // where opening a few thousand descriptors, or growing a blocking pool to a
    // few thousand threads, actually shows up.
    value(
        ctx,
        "setup_ms",
        (wall.as_secs_f64() - secs).max(0.0) * 1000.0,
    );
    let bytes = ctx.case.bytes_per_op();
    if bytes > 0 {
        value(ctx, "MiB/s", ops * bytes as f64 / secs / (1024.0 * 1024.0));
    }
    // Per operation, so the columns are comparable across arms that did
    // different numbers of operations in the same budget.
    value(ctx, "user_ns/op", used.user_ns as f64 / ops);
    value(ctx, "sys_ns/op", used.sys_ns as f64 / ops);
    value(ctx, "cpu_ns/op", (used.user_ns + used.sys_ns) as f64 / ops);
    value(ctx, "vcsw/op", used.vcsw as f64 / ops);
    value(ctx, "ivcsw/op", used.ivcsw as f64 / ops);
    value(ctx, "max_rss_kib", used.max_rss_kib as f64);
    value(ctx, "os_threads", out.os_threads as f64);
}

/// The fields every record carries, so a reader can group without a schema.
fn labels(ctx: Ctx<'_>) -> String {
    format!(
        r#""suite":"fs-bench","arm":"{}","panel":"{}","case":"{}","surface":"{}","threads":{},"depth":{},"rep":{}"#,
        esc(ctx.arm),
        esc(ctx.panel),
        ctx.case.as_str(),
        esc(ctx.surface),
        ctx.threads,
        ctx.depth,
        ctx.rep,
    )
}

fn stat(ctx: Ctx<'_>, unit: &str, s: &Stats) {
    line(&format!(
        r#"{{"kind":"stat",{},"unit":"{}","n":{},"min":{:.4},"p50":{:.4},"p95":{:.4},"p99":{:.4},"p999":{:.4},"max":{:.4},"mean":{:.4}}}"#,
        labels(ctx),
        esc(unit),
        s.n,
        s.min,
        s.p50,
        s.p95,
        s.p99,
        s.p999,
        s.max,
        s.mean,
    ));
}

fn value(ctx: Ctx<'_>, unit: &str, v: f64) {
    line(&format!(
        r#"{{"kind":"value",{},"unit":"{}","value":{:.4}}}"#,
        labels(ctx),
        esc(unit),
        v,
    ));
}

/// Appends one reference constant from `fs_floor`.
///
/// Its own record kind: these are properties of the machine, not points in the
/// matrix, and a reader grouping by `case` should not find them mixed in with
/// the arms.
pub fn emit_floor(case: &str, unit: &str, s: &Stats) {
    line(&format!(
        r#"{{"kind":"floor","suite":"fs-bench","case":"{}","unit":"{}","n":{},"min":{:.4},"p50":{:.4},"p95":{:.4},"p99":{:.4},"p999":{:.4},"max":{:.4},"mean":{:.4}}}"#,
        esc(case),
        esc(unit),
        s.n,
        s.min,
        s.p50,
        s.p95,
        s.p99,
        s.p999,
        s.max,
        s.mean,
    ));
}

/// One record describing the run, so a chart can label it without being told.
fn meta(arm: &str, m: &Matrix, ds: &Dataset) {
    let kernel = fs::read_to_string("/proc/sys/kernel/osrelease")
        .unwrap_or_default()
        .trim()
        .to_string();
    let cpu = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("model name") || l.starts_with("Model"))
                .map(|l| l.split(':').nth(1).unwrap_or("").trim().to_string())
        })
        .unwrap_or_default();
    line(&format!(
        r#"{{"kind":"meta","suite":"fs-bench","arm":"{}","surface":"{}","profile":"{}","os":"{}","arch":"{}","cores":{},"kernel":"{}","cpu":"{}","reps":{},"depth":{},"read_files":{},"read_file_bytes":{},"write_region_bytes":{},"dir":"{}"}}"#,
        esc(arm),
        esc(&m.surface),
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        std::env::consts::OS,
        std::env::consts::ARCH,
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        esc(&kernel),
        esc(&cpu),
        m.reps,
        m.depth,
        ds.read_files,
        ds.read_file_size,
        ds.write_region,
        esc(&ds.dir.display().to_string()),
    ));
}

/// Appends one line to `BENCH_JSON`, if it is set.
fn line(record: &str) {
    let Some(path) = std::env::var_os("BENCH_JSON").filter(|v| !v.is_empty()) else {
        return;
    };
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let _ = fs::create_dir_all(parent);
    }
    let record: String = record.split('\n').collect();
    match fs::OpenOptions::new().create(true).append(true).open(&path) {
        Ok(mut f) => {
            if let Err(e) = writeln!(f, "{record}") {
                eprintln!("BENCH_JSON: cannot write {}: {e}", path.display());
            }
        }
        Err(e) => eprintln!("BENCH_JSON: cannot open {}: {e}", path.display()),
    }
}

/// The only characters a label here could plausibly contain that JSON forbids
/// bare.
fn esc(s: &str) -> String {
    s.replace('\\', r"\\").replace('"', r#"\""#)
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A comma-separated list of numbers, deduplicated and sorted so a chart's x
/// axis is monotonic whatever order it was given in.
fn env_list(key: &str, default: &[usize]) -> Vec<usize> {
    let mut v: Vec<usize> = match std::env::var(key) {
        Ok(s) if !s.is_empty() => s.split(',').filter_map(|p| p.trim().parse().ok()).collect(),
        _ => default.to_vec(),
    };
    v.sort_unstable();
    v.dedup();
    if v.is_empty() { default.to_vec() } else { v }
}

fn env_cases(key: &str, default: &[Case]) -> Vec<Case> {
    match std::env::var(key) {
        Ok(s) if !s.is_empty() => {
            let v: Vec<Case> = s.split(',').filter_map(|p| Case::parse(p.trim())).collect();
            if v.is_empty() { default.to_vec() } else { v }
        }
        _ => default.to_vec(),
    }
}
