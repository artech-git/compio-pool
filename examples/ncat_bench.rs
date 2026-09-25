//! Benchmarks the pool against a **real** TCP server — `ncat` running an echo
//! service — with **45 connections in flight at once**.
//!
//! Unlike `examples/tcp.rs`, nothing here is in-process: the peer is a separate
//! `ncat` process that forks `/bin/cat` per connection, so every round trip
//! crosses the kernel twice and every dial costs a real TCP handshake plus a
//! `fork`/`exec` on the server side. That expensive dial is exactly what a pool
//! is for, and phase C measures what it costs when you skip the pool.
//!
//! # Shape of the run
//!
//! 45 connections are spread over 5 compio threads as 5 shards of 9. `max_size`
//! is **per shard** (see [`compio_pool::Config`]), so 45 total is `5 x 9` — the
//! pool never applies a process-wide cap, by design.
//!
//! | phase | in-flight tasks | connections | what it shows |
//! |---|---|---|---|
//! | A `steady`  | 45 | 45 pooled | uncontended checkout: one task per connection |
//! | B `oversub` | 90 | 45 pooled | queueing: `waits` climbs, acquire latency grows a tail |
//! | C `no-pool` | 45 | new per request | the dial cost the pool amortises away |
//!
//! A and B do the same total number of requests, so their throughput numbers
//! are directly comparable.
//!
//! Phase C is where the argument for pooling stops being about latency. Each
//! request there leaves a socket in `TIME_WAIT` for 2*MSL, and the ephemeral
//! port range is finite (16384 ports on macOS, 49152-65535). Run the example a
//! few times in quick succession and phase C starts failing outright with
//! `EADDRNOTAVAIL` — "Can't assign requested address" — while the pooled phases,
//! which reuse 45 sockets for the entire run, are untouched. Check with
//! `netstat -an -p tcp | grep -c TIME_WAIT`.
//!
//! # Running
//!
//! ```text
//! cargo run --release --example ncat_bench
//! ```
//!
//! Needs `ncat` on `PATH` (`brew install nmap`, `apt install ncat`). The example
//! starts and stops the server itself; set `NCAT_ADDR=host:port` to point it at
//! a server you are already running instead.
//!
//! Tunables, all optional: `CONNS` (45), `THREADS` (5), `ROUNDS` (2000 requests
//! per connection), `PAYLOAD` (64 bytes), `NOPOOL_ROUNDS` (100).

use std::{
    io,
    mem::ManuallyDrop,
    net::{SocketAddr, TcpListener as StdListener},
    process::{Child, Command, Stdio},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use compio::{
    buf::BufResult,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use compio_pool::{Config, Manage, Metrics, Pool, SlotMeta};

// ---------------------------------------------------------------- parameters

struct Params {
    conns: usize,
    threads: usize,
    per_shard: usize,
    rounds: usize,
    payload: usize,
    nopool_rounds: usize,
}

impl Params {
    fn from_env() -> Self {
        let conns = env_usize("CONNS", 45);
        let threads = env_usize("THREADS", 5);
        assert!(
            conns.is_multiple_of(threads),
            "CONNS ({conns}) must divide evenly into THREADS ({threads}); \
             max_size is per shard, so the per-thread budget has to be a whole number"
        );
        Self {
            conns,
            threads,
            per_shard: conns / threads,
            rounds: env_usize("ROUNDS", 2_000),
            payload: env_usize("PAYLOAD", 64),
            nopool_rounds: env_usize("NOPOOL_ROUNDS", 100),
        }
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ------------------------------------------------------------------- manager

/// Borrows a pooled compio stream as a `std::net::TcpStream`, without letting
/// the borrow close the descriptor compio owns. See `examples/std_tcp.rs`.
#[cfg(unix)]
fn with_std<T>(conn: &TcpStream, f: impl FnOnce(&std::net::TcpStream) -> T) -> T {
    use std::os::fd::{AsFd, AsRawFd, FromRawFd};

    let raw = conn.as_fd().as_raw_fd();
    // SAFETY: `raw` is a live socket owned by `conn`, which outlives the
    // borrow; `ManuallyDrop` guarantees we never close it.
    let borrowed = ManuallyDrop::new(unsafe { std::net::TcpStream::from_raw_fd(raw) });
    f(&borrowed)
}

#[cfg(windows)]
fn with_std<T>(conn: &TcpStream, f: impl FnOnce(&std::net::TcpStream) -> T) -> T {
    use std::os::windows::io::{AsRawSocket, FromRawSocket};

    let raw = conn.as_raw_socket();
    // SAFETY: as above; `ManuallyDrop` keeps the socket open.
    let borrowed = ManuallyDrop::new(unsafe { std::net::TcpStream::from_raw_socket(raw) });
    f(&borrowed)
}

#[derive(Default)]
struct Instrumentation {
    dials: AtomicU64,
    dial_nanos: AtomicU64,
    recycles: AtomicU64,
    recycle_nanos: AtomicU64,
}

struct NcatManager {
    addr: SocketAddr,
    stats: Arc<Instrumentation>,
}

impl Manage for NcatManager {
    // `TcpStream` is `!Send`: it is bound to the driver of the thread that
    // opened it. No general-purpose pool can hold one.
    type Connection = TcpStream;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<TcpStream> {
        let start = Instant::now();
        let conn = TcpStream::connect(self.addr).await?;
        // Request/response over loopback is exactly the workload Nagle ruins:
        // without this the 40ms delayed-ack interaction shows up as a p99 cliff.
        conn.set_nodelay(true)?;
        self.stats.dials.fetch_add(1, Relaxed);
        self.stats
            .dial_nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
        Ok(conn)
    }

    async fn recycle(&self, conn: &mut TcpStream, _meta: &SlotMeta) -> io::Result<()> {
        let start = Instant::now();
        // `SO_ERROR` is the cheapest honest liveness check: it catches a peer
        // that sent an RST while the connection sat idle, which `peer_addr`
        // (pure local state) would happily miss. A protocol client would also
        // send a real ping here once `_meta.idle_for()` passed some threshold.
        let result = match with_std(conn, |s| s.take_error())? {
            Some(err) => Err(err),
            None => Ok(()),
        };
        self.stats.recycles.fetch_add(1, Relaxed);
        self.stats
            .recycle_nanos
            .fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
        result
    }
}

// -------------------------------------------------------------- the workload

/// One request/response exchange on an already-open connection.
///
/// The request carries its own identity so a mis-framed or crossed response is
/// caught rather than silently benchmarked.
async fn round_trip(
    conn: &mut TcpStream,
    req: Vec<u8>,
    resp: Vec<u8>,
    tag: u64,
    round: u64,
) -> (io::Result<()>, Vec<u8>, Vec<u8>) {
    let mut req = req;
    req[..8].copy_from_slice(&tag.to_le_bytes());
    req[8..16].copy_from_slice(&round.to_le_bytes());

    let BufResult(written, req) = conn.write_all(req).await;
    if let Err(e) = written {
        return (Err(e), req, resp);
    }

    let BufResult(read, resp) = conn.read_exact(resp).await;
    if let Err(e) = read {
        return (Err(e), req, resp);
    }

    if resp[..16] != req[..16] {
        let e = io::Error::new(io::ErrorKind::InvalidData, "echo did not match request");
        return (Err(e), req, resp);
    }
    (Ok(()), req, resp)
}

#[derive(Default)]
struct Samples {
    rtt: Vec<u64>,
    acquire: Vec<u64>,
    /// Failures getting a connection at all: a pool acquire error in the
    /// pooled phases, a failed `connect` in the unpooled one.
    connect_err: u64,
    /// Failures on an established connection.
    io_err: u64,
    /// Kept so the report can say *what* went wrong, not just how often.
    first_err: Option<String>,
}

impl Samples {
    fn with_capacity(n: usize) -> Self {
        Self {
            rtt: Vec::with_capacity(n),
            acquire: Vec::with_capacity(n),
            ..Self::default()
        }
    }

    fn record_err(&mut self, e: &dyn std::fmt::Display) {
        if self.first_err.is_none() {
            self.first_err = Some(e.to_string());
        }
    }

    fn errors(&self) -> u64 {
        self.connect_err + self.io_err
    }

    fn merge(&mut self, other: Samples) {
        self.rtt.extend_from_slice(&other.rtt);
        self.acquire.extend_from_slice(&other.acquire);
        self.connect_err += other.connect_err;
        self.io_err += other.io_err;
        if self.first_err.is_none() {
            self.first_err = other.first_err;
        }
    }
}

/// Pooled path: check a connection out per request, hand it straight back.
async fn pooled_task(pool: Pool<NcatManager>, rounds: usize, payload: usize, tag: u64) -> Samples {
    let mut s = Samples::with_capacity(rounds);
    let mut req = vec![0u8; payload];
    let mut resp = vec![0u8; payload];

    for round in 0..rounds as u64 {
        let t_acquire = Instant::now();
        let mut conn = match pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                s.connect_err += 1;
                s.record_err(&e);
                continue;
            }
        };
        let acquired = t_acquire.elapsed();

        let t_rtt = Instant::now();
        // Arm cancellation protection: with completion-based IO, dropping the
        // future below does not un-send the request, so the connection must be
        // destroyed rather than returned with half a response still in it.
        let op = conn.begin_op();
        let (result, r, p) = round_trip(&mut conn, req, resp, tag, round).await;
        req = r;
        resp = p;

        match result {
            Ok(()) => {
                op.complete_op();
                s.rtt.push(t_rtt.elapsed().as_nanos() as u64);
                s.acquire.push(acquired.as_nanos() as u64);
            }
            // No `op.complete_op()`: the guard poisons the connection on drop and
            // the pool closes it instead of handing it to the next caller.
            Err(e) => {
                s.io_err += 1;
                s.record_err(&e);
            }
        }
    }
    s
}

/// Unpooled path: a fresh TCP handshake (and a fresh `cat` on the server) per
/// request. This is the thing the pool is being compared against.
async fn unpooled_task(addr: SocketAddr, rounds: usize, payload: usize, tag: u64) -> Samples {
    let mut s = Samples::with_capacity(rounds);
    let mut req = vec![0u8; payload];
    let mut resp = vec![0u8; payload];

    for round in 0..rounds as u64 {
        let t_acquire = Instant::now();
        let mut conn = match TcpStream::connect(addr).await {
            Ok(c) => c,
            Err(e) => {
                s.connect_err += 1;
                s.record_err(&e);
                continue;
            }
        };
        let _ = conn.set_nodelay(true);
        let acquired = t_acquire.elapsed();

        let t_rtt = Instant::now();
        let (result, r, p) = round_trip(&mut conn, req, resp, tag, round).await;
        req = r;
        resp = p;

        match result {
            Ok(()) => {
                s.rtt.push(t_rtt.elapsed().as_nanos() as u64);
                s.acquire.push(acquired.as_nanos() as u64);
            }
            Err(e) => {
                s.io_err += 1;
                s.record_err(&e);
            }
        }
    }
    s
}

// --------------------------------------------------------------- phase driver

struct Phase {
    name: &'static str,
    /// Tasks per compio thread.
    tasks: usize,
    rounds: usize,
    pooled: bool,
}

struct PhaseResult {
    samples: Samples,
    /// Longest per-thread wall time; the barrier aligns the starts, so this is
    /// the window the whole phase actually occupied.
    elapsed: Duration,
    /// Counters attributable to this phase, from the snapshots either side of it.
    delta: Metrics,
    /// Gauges as of the end of the phase.
    after: Metrics,
}

/// Counter fields differenced; gauge fields left at zero (see [`PhaseResult`]).
fn delta(after: &Metrics, before: &Metrics) -> Metrics {
    Metrics {
        live: 0,
        idle: 0,
        parked: 0,
        created: after.created - before.created,
        closed: after.closed - before.closed,
        acquires: after.acquires - before.acquires,
        waits: after.waits - before.waits,
        timeouts: after.timeouts - before.timeouts,
        poisoned: after.poisoned - before.poisoned,
        recycle_failures: after.recycle_failures - before.recycle_failures,
        unparked: after.unparked - before.unparked,
    }
}

/// Runs every phase on the *same* set of compio threads.
///
/// That matters: shards are thread-local, so a fresh thread per phase would
/// throw away all 45 connections and re-dial them, and phase B would be
/// measuring a cold pool rather than a warm one.
fn run(
    params: &Params,
    addr: SocketAddr,
    pool: &Pool<NcatManager>,
    phases: &'static [Phase],
) -> (Vec<PhaseResult>, Metrics) {
    const WARMUP_ROUNDS: usize = 50;

    let barrier = Arc::new(Barrier::new(params.threads));
    // One snapshot before each phase plus one after the last: thread 0 takes
    // them while every other thread is parked on a barrier, so they are clean.
    let snapshots: Arc<std::sync::Mutex<Vec<Metrics>>> = Arc::default();
    let payload = params.payload;
    let params_per_shard = params.per_shard;

    let workers: Vec<_> = (0..params.threads)
        .map(|thread| {
            let pool = pool.clone();
            let barrier = barrier.clone();
            let snapshots = snapshots.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        // Dial this shard's share of the 45 up front, so phase A
                        // measures steady state and not the handshake.
                        pool.warm().await.expect("warm");

                        // Unreported warmup: touches every connection and forces
                        // the server to fork its `cat` per socket, so phase A is
                        // not the only one paying first-touch costs.
                        let warmup: Vec<_> = (0..params_per_shard)
                            .map(|i| {
                                compio::runtime::spawn(pooled_task(
                                    pool.clone(),
                                    WARMUP_ROUNDS,
                                    payload,
                                    (thread * 1_000 + i) as u64,
                                ))
                            })
                            .collect();
                        for h in warmup {
                            h.await.expect("warmup task panicked");
                        }

                        let mut out = Vec::new();
                        for phase in phases {
                            barrier.wait();
                            if thread == 0 {
                                snapshots.lock().unwrap().push(pool.metrics());
                            }
                            // Nothing runs between the two barriers, so the
                            // snapshot lands on a quiesced pool.
                            barrier.wait();
                            let start = Instant::now();

                            let handles: Vec<_> = (0..phase.tasks)
                                .map(|i| {
                                    let tag = (thread * 1_000 + i) as u64;
                                    let pool = pool.clone();
                                    if phase.pooled {
                                        compio::runtime::spawn(pooled_task(
                                            pool,
                                            phase.rounds,
                                            payload,
                                            tag,
                                        ))
                                    } else {
                                        compio::runtime::spawn(unpooled_task(
                                            addr,
                                            phase.rounds,
                                            payload,
                                            tag,
                                        ))
                                    }
                                })
                                .collect();

                            let mut samples = Samples::default();
                            for h in handles {
                                samples.merge(h.await.expect("task panicked"));
                            }
                            out.push((samples, start.elapsed()));
                        }

                        // Closing snapshot, still on a live thread: once these
                        // threads exit, the shards drop and `live` goes to zero.
                        barrier.wait();
                        if thread == 0 {
                            snapshots.lock().unwrap().push(pool.metrics());
                        }
                        barrier.wait();
                        out
                    })
            })
        })
        .collect();

    let per_thread: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    let snapshots = snapshots.lock().unwrap().clone();

    let results = (0..phases.len())
        .map(|i| {
            let mut samples = Samples::default();
            let mut elapsed = Duration::ZERO;
            for thread in &per_thread {
                let (s, e) = &thread[i];
                samples.rtt.extend_from_slice(&s.rtt);
                samples.acquire.extend_from_slice(&s.acquire);
                samples.connect_err += s.connect_err;
                samples.io_err += s.io_err;
                if samples.first_err.is_none() {
                    samples.first_err = s.first_err.clone();
                }
                elapsed = elapsed.max(*e);
            }
            PhaseResult {
                samples,
                elapsed,
                delta: delta(&snapshots[i + 1], &snapshots[i]),
                after: snapshots[i + 1],
            }
        })
        .collect();

    (results, *snapshots.last().unwrap())
}

// ------------------------------------------------------------------ reporting

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

fn us(nanos: u64) -> String {
    format!("{:.1}", nanos as f64 / 1_000.0)
}

fn latency_row(label: &str, samples: &mut [u64]) {
    samples.sort_unstable();
    let mean = if samples.is_empty() {
        0
    } else {
        samples.iter().sum::<u64>() / samples.len() as u64
    };
    println!(
        "  {label:<22} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        us(mean),
        us(percentile(samples, 0.50)),
        us(percentile(samples, 0.90)),
        us(percentile(samples, 0.99)),
        us(percentile(samples, 0.999)),
        us(percentile(samples, 1.0)),
    );
}

fn report_phase(phase: &Phase, result: &mut PhaseResult, params: &Params) {
    let ops = result.samples.rtt.len() as u64;
    let secs = result.elapsed.as_secs_f64();
    let in_flight = phase.tasks * params.threads;

    println!(
        "\n[{}]  {in_flight} tasks over {} threads, {} requests each",
        phase.name, params.threads, phase.rounds,
    );
    println!(
        "  {ops} ok / {} err in {:.3}s   ->  {:.0} req/s  ({:.0} req/s per connection)",
        result.samples.errors(),
        secs,
        ops as f64 / secs,
        ops as f64 / secs / params.conns as f64,
    );
    println!(
        "  {:<22} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "latency (us)", "mean", "p50", "p90", "p99", "p99.9", "max"
    );
    latency_row(
        if phase.pooled {
            "acquire"
        } else {
            "connect (no pool)"
        },
        &mut result.samples.acquire,
    );
    latency_row("round trip", &mut result.samples.rtt);

    if result.samples.errors() > 0 {
        println!(
            "  errors: {} getting a connection, {} on an open one; first was: {}",
            result.samples.connect_err,
            result.samples.io_err,
            result.samples.first_err.as_deref().unwrap_or("-"),
        );
    }

    let m = &result.delta;
    println!(
        "  pool: acquires={} waits={} timeouts={} created={} closed={} poisoned={} recycle_failures={} | live at end={}",
        m.acquires,
        m.waits,
        m.timeouts,
        m.created,
        m.closed,
        m.poisoned,
        m.recycle_failures,
        result.after.live,
    );
}

// ----------------------------------------------------------------- the server

/// Keeps the spawned `ncat` alive for the run and reaps it afterwards.
struct Server(Option<Child>);

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Starts `ncat` as an echo server: `--keep-open` so it accepts more than one
/// connection, `--exec /bin/cat` so each one gets its own echo process.
fn spawn_ncat(port: u16, max_conns: usize) -> io::Result<Server> {
    let child = Command::new("ncat")
        .args([
            "--listen",
            "127.0.0.1",
            &port.to_string(),
            "--keep-open",
            "--exec",
            "/bin/cat",
            "--max-conns",
            &max_conns.to_string(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "could not start `ncat` ({e}). Install it (brew install nmap / \
                     apt install ncat), or point the example at a server you are \
                     already running with NCAT_ADDR=host:port"
                ),
            )
        })?;
    Ok(Server(Some(child)))
}

/// Grabs a free port by binding and immediately dropping the listener.
fn free_port() -> io::Result<u16> {
    Ok(StdListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

/// `ncat` takes a few milliseconds to reach `listen(2)`; poll until it does.
fn wait_ready(addr: SocketAddr, limit: Duration) -> io::Result<()> {
    let deadline = Instant::now() + limit;
    loop {
        match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(_) => return Ok(()),
            Err(e) if Instant::now() >= deadline => return Err(e),
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

// ----------------------------------------------------------------------- main

fn main() -> io::Result<()> {
    let params = Params::from_env();

    // Phases are `'static` so the worker threads can borrow them.
    let phases: &'static [Phase] = Box::leak(Box::new([
        Phase {
            name: "A steady",
            tasks: params.per_shard,
            rounds: params.rounds,
            pooled: true,
        },
        Phase {
            name: "B oversubscribed",
            tasks: params.per_shard * 2,
            rounds: params.rounds / 2,
            pooled: true,
        },
        Phase {
            name: "C no pool",
            tasks: params.per_shard,
            rounds: params.nopool_rounds,
            pooled: false,
        },
    ]));

    let (addr, _server) = match std::env::var("NCAT_ADDR") {
        Ok(s) => {
            let addr: SocketAddr = s.parse().expect("NCAT_ADDR must be host:port");
            wait_ready(addr, Duration::from_secs(2))?;
            println!("using the server already listening on {addr}");
            (addr, None)
        }
        Err(_) => {
            let port = free_port()?;
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let server = spawn_ncat(port, params.conns * 4)?;
            wait_ready(addr, Duration::from_secs(5))?;
            println!("started `ncat --listen 127.0.0.1 {port} --keep-open --exec /bin/cat`");
            (addr, Some(server))
        }
    };

    let stats = Arc::new(Instrumentation::default());
    let pool = Pool::new(
        NcatManager {
            addr,
            stats: stats.clone(),
        },
        Config::new()
            // Per shard. 5 shards x 9 = the 45 connections we want live.
            .max_size(params.per_shard)
            // Same number, so `warm` opens the full set and the reaper keeps it.
            .min_idle(params.per_shard)
            .acquire_timeout(Duration::from_secs(5))
            // A benchmark must not have connections retired underneath it.
            .idle_timeout(None)
            .max_lifetime(None)
            .reap_interval(Duration::from_secs(60)),
    );

    println!(
        "\n{} connections = {} compio threads x {} per shard, {}-byte payload\n",
        params.conns, params.threads, params.per_shard, params.payload,
    );

    let warm_start = Instant::now();
    let (mut results, final_metrics) = run(&params, addr, &pool, phases);
    let wall = warm_start.elapsed();

    for (phase, result) in phases.iter().zip(results.iter_mut()) {
        report_phase(phase, result, &params);
    }

    let pooled_ops: u64 = phases
        .iter()
        .zip(results.iter())
        .filter(|(p, _)| p.pooled)
        .map(|(_, r)| r.samples.rtt.len() as u64)
        .sum();

    let dials = stats.dials.load(Relaxed);
    let dial_nanos = stats.dial_nanos.load(Relaxed);
    let recycles = stats.recycles.load(Relaxed);
    let recycle_nanos = stats.recycle_nanos.load(Relaxed);

    println!("\n--- pool counters (cumulative, all phases) ---");
    println!("{final_metrics:#?}");

    println!("\n--- what the numbers say ---");
    println!(
        "  live connections held        {} (expected {})",
        final_metrics.live, params.conns
    );
    println!(
        "  dials                        {dials}  ({} pooled requests served per dial)",
        pooled_ops / dials.max(1)
    );
    println!(
        "  mean dial cost               {} us  (paid once per connection, not per request)",
        us(dial_nanos / dials.max(1))
    );
    println!(
        "  mean recycle cost            {} us  ({recycles} checkouts validated; this is on the acquire hot path)",
        us(recycle_nanos / recycles.max(1))
    );
    println!(
        "  checkouts that had to wait   {} of {} ({:.2}%)",
        final_metrics.waits,
        final_metrics.acquires,
        100.0 * final_metrics.waits as f64 / final_metrics.acquires.max(1) as f64,
    );
    println!(
        "  timeouts / poisoned / recycle failures   {} / {} / {}",
        final_metrics.timeouts, final_metrics.poisoned, final_metrics.recycle_failures,
    );
    println!("  total wall time              {:.2}s", wall.as_secs_f64());

    assert_eq!(
        final_metrics.live as usize, params.conns,
        "the pool should still be holding exactly {} connections",
        params.conns
    );
    assert_eq!(
        final_metrics.timeouts, 0,
        "no checkout should have timed out"
    );

    // Close before `_server` is dropped, so the sockets go away before ncat does.
    pool.close();
    Ok(())
}
