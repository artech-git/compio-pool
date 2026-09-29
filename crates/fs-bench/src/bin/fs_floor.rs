//! The constants the rest of the report divides by.
//!
//! The three arms differ by more than an order of magnitude on some cases, and
//! that gap is worth nothing to a reader who cannot tell how much of it is the
//! design and how much is the machine. This binary measures the two quantities
//! that separate those:
//!
//! * **What the syscall costs.** A blocking `pread`, `stat` and `open`/`close` in
//!   a tight loop on one thread, no runtime anywhere. This is the floor: no
//!   design can beat it, and anything close to it is paying for nothing but the
//!   operation.
//! * **What a thread handoff costs.** An empty `spawn_blocking` — no syscall, no
//!   filesystem, nothing in the closure — awaited one at a time. This is the
//!   whole of what `tokio::fs` adds to a filesystem operation, measured on its
//!   own.
//!
//! Under virtualisation the second number is much larger than it would be on
//! bare metal, because a thread wakeup crosses the guest scheduler and then the
//! host's. Measuring it here rather than asserting it is what lets the report say
//! which part of its own results would travel to other hardware.
//!
//! Run with: `cargo run --profile maxopt --bin fs_floor`

use std::{
    io,
    os::unix::fs::FileExt,
    time::{Duration, Instant},
};

use fs_bench::{Dataset, Stats};

/// Long enough for the distribution to settle, short enough that four of these
/// is not a coffee break.
const BUDGET: Duration = Duration::from_millis(400);

fn main() -> io::Result<()> {
    let ds = Dataset::from_env();
    ds.ensure()?;

    let f = std::fs::File::open(ds.read_path(0))?;
    let mut buf = vec![0u8; 4096];
    let mut off = 0u64;
    let span = ds.read_file_size - 4096;

    report(
        "syscall_pread_4k",
        "ns/op",
        &loop_for(BUDGET, || {
            off = (off + 4096) % span;
            f.read_exact_at(&mut buf, off)
        })?,
    );

    let p = ds.read_path(0);
    report(
        "syscall_stat",
        "ns/op",
        &loop_for(BUDGET, || std::fs::metadata(&p).map(|_| ()))?,
    );

    report(
        "syscall_open_close",
        "ns/op",
        &loop_for(BUDGET, || std::fs::File::open(&p).map(drop))?,
    );

    // One worker thread and one operation in flight: the number wanted is the
    // round trip, not what happens to it under contention.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .max_blocking_threads(64)
        .build()?;
    let handoff = rt.block_on(async {
        let deadline = Instant::now() + BUDGET;
        let mut lat = Vec::with_capacity(1 << 16);
        loop {
            let started = Instant::now();
            // Deliberately empty. Anything in here would be measured alongside
            // the handoff, and the handoff is the entire point.
            tokio::task::spawn_blocking(|| {})
                .await
                .map_err(io::Error::other)?;
            let now = Instant::now();
            lat.push(now.duration_since(started).as_nanos() as f64);
            if now >= deadline {
                break;
            }
        }
        Ok::<_, io::Error>(Stats::of(&lat))
    })?;
    report("tokio_spawn_blocking_noop", "ns/op", &handoff);

    Ok(())
}

/// Runs `op` until the budget is spent, timing each call.
fn loop_for(budget: Duration, mut op: impl FnMut() -> io::Result<()>) -> io::Result<Stats> {
    // Warm first: the first `open` of a path pays for a cold dentry, and the
    // first `pread` for a cold page.
    for _ in 0..1000 {
        op()?;
    }
    let deadline = Instant::now() + budget;
    let mut lat = Vec::with_capacity(1 << 16);
    loop {
        let started = Instant::now();
        op()?;
        let now = Instant::now();
        lat.push(now.duration_since(started).as_nanos() as f64);
        if now >= deadline {
            break;
        }
    }
    Ok(Stats::of(&lat))
}

/// Prints a line and appends the same thing to `BENCH_JSON`.
fn report(case: &str, unit: &str, s: &Stats) {
    println!(
        "{case:28} n={:<9} min={:>9.0} p50={:>9.0} p99={:>9.0}",
        s.n, s.min, s.p50, s.p99
    );
    fs_bench::emit_floor(case, unit, s);
}
