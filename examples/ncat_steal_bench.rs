//! What the cross-thread exchange is worth, measured against a **real** TCP
//! server — `ncat` running an echo service — with the exchange on and off.
//!
//! `examples/ncat_bench.rs` asks "what does pooling buy over dialling per
//! request". This asks the next question: given a pool, what does letting
//! connections *migrate between threads* buy, and what does it cost? Both
//! answers are measured here in one process, against one server, so the
//! comparison does not rest on run-to-run variance.
//!
//! # Shape of the run
//!
//! Two arms, each on its own set of `THREADS` compio threads, split into two
//! equal halves. Every arm runs the same two phases:
//!
//! | phase | who works | what it shows |
//! |---|---|---|
//! | A `warm half` | first half, one task per pooled connection | steady state: what the exchange *costs* on the hot path |
//! | B `cold half` | second half, shards still empty | load migration: what the exchange *saves* |
//!
//! The arms differ only in the exchange:
//!
//! * **plain** — [`NoExchange`](compio_pool::NoExchange), the default. Phase
//!   A's connections stay in the first half's shards forever. The second half
//!   arrives in phase B with nothing and has to dial its own set: a TCP
//!   handshake plus a `fork`/`exec` of `/bin/cat` on the server, per
//!   connection.
//! * **reservoir** — [`Reservoir`], backed by a lock-free `ArrayQueue`. A
//!   shard whose free list is over `MIN_IDLE` parks the surplus there, so as
//!   phase A winds down its sockets go to the queue rather than sitting in a
//!   shard nobody is using, and phase B claims them instead of dialling.
//!   `Detach::attach` re-wraps each one in the claiming thread's driver.
//!
//! # The metrics that matter
//!
//! * **cold-start acquire** — the *first* checkout of each phase-B task, the
//!   one that either pays a handshake or steals a warm socket. Averaged over a
//!   whole phase this is invisible; on its own it is the entire point.
//! * **connections dialled in phase B** — `created`, straight from
//!   [`Metrics`]. The reservoir arm should barely move it.
//! * **phase A throughput and acquire percentiles** — the bill. With
//!   `MIN_IDLE=0` *every* return crosses the shared queue, so this is the
//!   worst case for the exchange, not a flattering one.
//! * **`detach`/`attach` cost against dial cost** — what a steal costs against
//!   what it replaces.
//!
//! # Two things the numbers will not tell you on their own
//!
//! * **A claimed connection skips `recycle`.** [`Pool::acquire`] validates a
//!   connection taken from the local free list, but one claimed from the
//!   exchange is handed straight to the caller. At `MIN_IDLE=0` every checkout
//!   comes from the queue, so the reservoir arm would do one fewer syscall per
//!   request than the baseline and its phase-A throughput would not be a
//!   like-for-like win. That is why the default keeps half the working set
//!   thread-local — and why the report prints the recycle counts side by side,
//!   so the gap is visible rather than flattering.
//! * **`ncat --exec /bin/cat` forks per connection.** Its accept path is
//!   deliberately expensive, which is what makes it a good stand-in for a
//!   database that authenticates on connect — but it does mean the
//!   cold-start ratio below is a property of this server, not a constant.
//!
//! # Running
//!
//! ```text
//! cargo run --release --example ncat_steal_bench
//! ```
//!
//! Needs `ncat` on `PATH` (`brew install nmap`, `apt install ncat`). The
//! example starts and stops the server itself; set `NCAT_ADDR=host:port` to
//! point it at one you are already running.
//!
//! Tunables, all optional: `THREADS` (6, must be even), `PER_SHARD` (6
//! connections and tasks per working thread), `ROUNDS` (1500 requests per
//! task), `WARMUP` (50), `PAYLOAD` (64 bytes), `MIN_IDLE` (half of
//! `PER_SHARD` — connections kept thread-local, above which a return is
//! treated as surplus and offered to the other threads; `MIN_IDLE=0` shares
//! everything and is the exchange's worst case on the hot path).

#[cfg(not(unix))]
fn main() {
    // A handle binds to one IOCP completion port for life, so a stolen socket
    // would keep delivering completions to the thread that opened it.
    eprintln!("this benchmark is unix-only; `Detach` is not sound on IOCP");
}

#[cfg(unix)]
fn main() -> std::io::Result<()> {
    unix::run()
}

#[cfg(unix)]
mod unix {
    use std::{
        io,
        mem::ManuallyDrop,
        net::{SocketAddr, TcpListener as StdListener},
        os::fd::{FromRawFd, IntoRawFd, OwnedFd},
        process::{Child, Command, Stdio},
        sync::{
            Arc, Barrier, Mutex,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
        time::{Duration, Instant},
    };

    use compio::{
        buf::BufResult,
        driver::ToSharedFd,
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };
    use compio_pool::{Config, Detach, Exchange, Manage, Metrics, Pool, Reservoir, SlotMeta};

    // ------------------------------------------------------------ parameters

    struct Params {
        threads: usize,
        /// Threads per half. The first half works in phase A, the second in B.
        half: usize,
        /// `max_size`, and the number of concurrent tasks, per working thread.
        per_shard: usize,
        /// Connections the working set needs: `half * per_shard`.
        conns: usize,
        rounds: usize,
        warmup: usize,
        payload: usize,
        min_idle: usize,
    }

    impl Params {
        fn from_env() -> Self {
            let threads = env_usize("THREADS", 6);
            assert!(
                threads >= 2 && threads.is_multiple_of(2),
                "THREADS ({threads}) must be even and at least 2: one half works \
                 in phase A, the other in phase B"
            );
            let half = threads / 2;
            let per_shard = env_usize("PER_SHARD", 6);
            assert!(per_shard > 0, "PER_SHARD must be greater than zero");
            Self {
                threads,
                half,
                per_shard,
                conns: half * per_shard,
                rounds: env_usize("ROUNDS", 1_500),
                warmup: env_usize("WARMUP", 50),
                payload: env_usize("PAYLOAD", 64),
                // Half the per-thread concurrency, which is roughly what
                // `Config::min_idle` is for: keep the steady-state working set
                // thread-local and share only what a thread is not using. At 0
                // nothing is kept local, every checkout crosses the queue, and
                // no checkout is validated — see the note the report prints.
                min_idle: env_usize("MIN_IDLE", per_shard / 2),
            }
        }

        fn config(&self) -> Config {
            Config::new()
                // Per shard: the second half can never exceed this, so it
                // cannot paper over a missing exchange by over-dialling.
                .max_size(self.per_shard)
                .min_idle(self.min_idle)
                .acquire_timeout(Duration::from_secs(5))
                // A benchmark must not have connections retired underneath it.
                .idle_timeout(None)
                .max_lifetime(None)
                // Far longer than any run: with min_idle > 0 the reaper would
                // otherwise wake on the *cold* half and dial it up to min_idle
                // before phase B ever asks for a connection, which is exactly
                // the thing being measured.
                .reap_interval(Duration::from_secs(60 * 60))
        }
    }

    fn env_usize(key: &str, default: usize) -> usize {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    // ----------------------------------------------------------------- probe

    /// Defines a counter block and the plain-`u64` snapshot that goes with it.
    ///
    /// The counters have to be `static`: [`Detach::detach`] and
    /// [`Detach::attach`] are associated functions with no `&self`, so there is
    /// no manager instance to hang them off.
    macro_rules! probe {
        ($($field:ident),+ $(,)?) => {
            #[derive(Debug, Default, Clone, Copy)]
            struct Counts { $($field: u64),+ }

            struct Probe { $($field: AtomicU64),+ }

            impl Probe {
                const fn new() -> Self {
                    Self { $($field: AtomicU64::new(0)),+ }
                }

                fn add(&self, which: &AtomicU64, n: u64) {
                    let _ = self;
                    which.fetch_add(n, Relaxed);
                }

                fn snapshot(&self) -> Counts {
                    Counts { $($field: self.$field.load(Relaxed)),+ }
                }
            }

            impl Counts {
                /// Field-wise difference, for per-phase attribution.
                fn since(self, earlier: Self) -> Self {
                    Self { $($field: self.$field - earlier.$field),+ }
                }
            }
        };
    }

    probe!(
        dials,
        dial_nanos,
        recycles,
        recycle_nanos,
        detaches,
        detach_refused,
        detach_nanos,
        attaches,
        attach_failed,
        attach_nanos,
    );

    static PROBE: Probe = Probe::new();

    /// Runs `f`, adding its duration to `nanos` and bumping `count`.
    fn timed<T>(count: &AtomicU64, nanos: &AtomicU64, start: Instant, value: T) -> T {
        PROBE.add(count, 1);
        PROBE.add(nanos, start.elapsed().as_nanos() as u64);
        value
    }

    // --------------------------------------------------------------- manager

    /// Borrows a pooled compio stream as a `std::net::TcpStream` without
    /// letting the borrow close the descriptor compio owns. See
    /// `examples/std_tcp.rs`.
    fn with_std<T>(conn: &TcpStream, f: impl FnOnce(&std::net::TcpStream) -> T) -> T {
        use std::os::fd::{AsFd, AsRawFd};

        let raw = conn.as_fd().as_raw_fd();
        // SAFETY: `raw` is a live socket owned by `conn`, which outlives the
        // borrow; `ManuallyDrop` guarantees we never close it.
        let borrowed = ManuallyDrop::new(unsafe { std::net::TcpStream::from_raw_fd(raw) });
        f(&borrowed)
    }

    struct StealManager {
        addr: SocketAddr,
    }

    impl Manage for StealManager {
        // `TcpStream` is `!Send`: it is bound to the driver of the thread that
        // opened it. Crossing threads is exactly what `Detach` below licenses.
        type Connection = TcpStream;
        type Error = io::Error;

        async fn connect(&self) -> io::Result<TcpStream> {
            let start = Instant::now();
            let conn = TcpStream::connect(self.addr).await?;
            // Request/response over loopback is the workload Nagle ruins:
            // without this the 40ms delayed-ack interaction is a p99 cliff.
            conn.set_nodelay(true)?;
            Ok(timed(&PROBE.dials, &PROBE.dial_nanos, start, conn))
        }

        async fn recycle(&self, conn: &mut TcpStream, _meta: &SlotMeta) -> io::Result<()> {
            let start = Instant::now();
            // `SO_ERROR` is the cheapest honest liveness check: it catches a
            // peer that sent an RST while the connection sat idle — or while
            // it sat in the reservoir, which is the case that matters here,
            // since a parked socket belongs to no thread and nobody is
            // watching it.
            let result = match with_std(conn, |s| s.take_error())? {
                Some(err) => Err(err),
                None => Ok(()),
            };
            timed(&PROBE.recycles, &PROBE.recycle_nanos, start, result)
        }
    }

    impl Detach for StealManager {
        /// `OwnedFd` is `Send` and owns the descriptor — everything the socket
        /// needs to exist between two drivers. A protocol client would carry
        /// its `Send` session state alongside it.
        type Parked = OwnedFd;

        fn detach(conn: TcpStream) -> Option<OwnedFd> {
            let start = Instant::now();
            // `to_shared_fd` hands out a *clone* of the handle, so drop our
            // stream first. `try_unwrap` then succeeds exactly when nothing
            // else holds a reference, which is the "no operation in flight"
            // precondition for moving an fd between drivers.
            let shared = conn.to_shared_fd();
            drop(conn);
            let Ok(socket) = shared.try_unwrap() else {
                PROBE.add(&PROBE.detach_refused, 1);
                return None;
            };
            // SAFETY: `try_unwrap` gave us sole ownership, and `into_raw_fd`
            // gives up the socket's claim, so this `OwnedFd` is its only owner.
            let fd = unsafe { OwnedFd::from_raw_fd(socket.into_raw_fd()) };
            Some(timed(&PROBE.detaches, &PROBE.detach_nanos, start, fd))
        }

        async fn attach(fd: OwnedFd) -> io::Result<TcpStream> {
            let start = Instant::now();
            // Runs on the claiming thread, so the socket is registered with
            // *that* thread's driver. Under io_uring and poll this is
            // bookkeeping; the fd table is process-wide.
            match TcpStream::from_std(std::net::TcpStream::from(fd)) {
                Ok(conn) => Ok(timed(&PROBE.attaches, &PROBE.attach_nanos, start, conn)),
                Err(e) => {
                    PROBE.add(&PROBE.attach_failed, 1);
                    Err(e)
                }
            }
        }
    }

    // ---------------------------------------------------------- the workload

    /// One request/response exchange on an already-open connection.
    ///
    /// The request carries its own identity, so a mis-framed or crossed
    /// response is caught rather than silently benchmarked — which matters
    /// more here than in `ncat_bench`, because a socket that changed threads
    /// mid-run is precisely the thing that could go wrong.
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
        /// The first checkout of each task: on a cold shard that is either a
        /// handshake or a steal, and it is the number this benchmark exists
        /// for. One entry per task, so it never drowns in the average.
        cold_start: Vec<u64>,
        /// Failures getting a connection at all.
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

        fn merge(&mut self, other: &Samples) {
            self.rtt.extend_from_slice(&other.rtt);
            self.acquire.extend_from_slice(&other.acquire);
            self.cold_start.extend_from_slice(&other.cold_start);
            self.connect_err += other.connect_err;
            self.io_err += other.io_err;
            if self.first_err.is_none() {
                self.first_err = other.first_err.clone();
            }
        }

        fn sort(&mut self) {
            self.rtt.sort_unstable();
            self.acquire.sort_unstable();
            self.cold_start.sort_unstable();
        }
    }

    /// Check a connection out per request, hand it straight back.
    ///
    /// Generic over the exchange, so both arms run byte-identical work.
    async fn pooled_task<X: Exchange<StealManager>>(
        pool: Pool<StealManager, X>,
        rounds: usize,
        payload: usize,
        tag: u64,
    ) -> Samples {
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
            // Arm cancellation protection: with completion-based IO, dropping
            // the future below does not un-send the request, so the connection
            // must be destroyed rather than returned — and above all never
            // parked, since another thread would inherit half a response.
            let op = conn.begin_op();
            let (result, r, p) = round_trip(&mut conn, req, resp, tag, round).await;
            req = r;
            resp = p;

            match result {
                Ok(()) => {
                    op.complete_op();
                    s.rtt.push(t_rtt.elapsed().as_nanos() as u64);
                    s.acquire.push(acquired.as_nanos() as u64);
                    if round == 0 {
                        s.cold_start.push(acquired.as_nanos() as u64);
                    }
                }
                // No `op.complete_op()`: the guard poisons the connection on drop
                // and the pool closes it instead of parking or reusing it.
                Err(e) => {
                    s.io_err += 1;
                    s.record_err(&e);
                }
            }
        }
        s
    }

    // ------------------------------------------------------------ the phases

    #[derive(Clone, Copy)]
    enum Group {
        First,
        Second,
    }

    impl Group {
        fn contains(self, thread: usize, half: usize) -> bool {
            match self {
                Group::First => thread < half,
                Group::Second => thread >= half,
            }
        }
    }

    struct Phase {
        name: &'static str,
        group: Group,
    }

    static PHASES: [Phase; 2] = [
        Phase {
            name: "A warm half",
            group: Group::First,
        },
        Phase {
            name: "B cold half",
            group: Group::Second,
        },
    ];

    struct PhaseResult {
        samples: Samples,
        /// Longest per-thread wall time. Idle threads contribute ~0, so this
        /// is the window the working half actually occupied.
        elapsed: Duration,
        /// Pool counters attributable to this phase.
        delta: Metrics,
        /// Manager counters attributable to this phase.
        probe: Counts,
        /// Gauges as of the end of the phase.
        after: Metrics,
    }

    struct ArmResult {
        label: &'static str,
        phases: Vec<PhaseResult>,
        final_metrics: Metrics,
        /// Manager counters for the whole arm, warmup included.
        probe: Counts,
        wall: Duration,
    }

    /// Counter fields differenced; gauge fields left at zero.
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

    /// Runs both phases of one arm on a fresh set of compio threads.
    ///
    /// The threads are fresh per arm and shared across phases, which is the
    /// whole experiment: shards are thread-local, so the second half's shards
    /// are cold in phase B precisely because those threads have existed all
    /// along without ever touching the pool.
    fn run_arm<X: Exchange<StealManager>>(
        params: &Params,
        label: &'static str,
        pool: &Pool<StealManager, X>,
    ) -> ArmResult {
        let barrier = Arc::new(Barrier::new(params.threads));
        // One snapshot before each phase plus one after the last. Thread 0
        // takes them while every other thread is parked on a barrier, so they
        // land on a quiesced pool.
        let snapshots: Arc<Mutex<Vec<(Metrics, Counts)>>> = Arc::default();
        let probe_start = PROBE.snapshot();
        let started = Instant::now();

        let (half, per_shard, rounds, warmup, payload) = (
            params.half,
            params.per_shard,
            params.rounds,
            params.warmup,
            params.payload,
        );

        let workers: Vec<_> = (0..params.threads)
            .map(|thread| {
                let pool = pool.clone();
                let barrier = barrier.clone();
                let snapshots = snapshots.clone();
                std::thread::spawn(move || {
                    compio::runtime::Runtime::new()
                        .unwrap()
                        .block_on(async move {
                            // Only the first half warms up. The second half's
                            // shards have to be genuinely cold when phase B
                            // starts or there is nothing to measure.
                            if thread < half {
                                pool.warm().await.expect("warm");
                                let tasks: Vec<_> = (0..per_shard)
                                    .map(|i| {
                                        compio::runtime::spawn(pooled_task(
                                            pool.clone(),
                                            warmup,
                                            payload,
                                            (thread * 1_000 + i) as u64,
                                        ))
                                    })
                                    .collect();
                                for t in tasks {
                                    t.await.expect("warmup task panicked");
                                }
                            }

                            let mut out = Vec::new();
                            for phase in PHASES.iter() {
                                barrier.wait();
                                if thread == 0 {
                                    snapshots
                                        .lock()
                                        .unwrap()
                                        .push((pool.metrics(), PROBE.snapshot()));
                                }
                                // Nothing runs between the two barriers.
                                barrier.wait();

                                let start = Instant::now();
                                let mut samples = Samples::default();
                                if phase.group.contains(thread, half) {
                                    let handles: Vec<_> = (0..per_shard)
                                        .map(|i| {
                                            compio::runtime::spawn(pooled_task(
                                                pool.clone(),
                                                rounds,
                                                payload,
                                                (thread * 1_000 + i) as u64,
                                            ))
                                        })
                                        .collect();
                                    for h in handles {
                                        samples.merge(&h.await.expect("task panicked"));
                                    }
                                }
                                out.push((samples, start.elapsed()));
                            }

                            // Closing snapshot, still on live threads: once
                            // they exit, the shards drop and `live` goes to 0.
                            barrier.wait();
                            if thread == 0 {
                                snapshots
                                    .lock()
                                    .unwrap()
                                    .push((pool.metrics(), PROBE.snapshot()));
                            }
                            barrier.wait();
                            out
                        })
                })
            })
            .collect();

        let per_thread: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        let wall = started.elapsed();
        let snapshots = snapshots.lock().unwrap().clone();

        let phases = (0..PHASES.len())
            .map(|i| {
                let mut samples = Samples::default();
                let mut elapsed = Duration::ZERO;
                for thread in &per_thread {
                    let (s, e) = &thread[i];
                    samples.merge(s);
                    elapsed = elapsed.max(*e);
                }
                samples.sort();
                PhaseResult {
                    samples,
                    elapsed,
                    delta: delta(&snapshots[i + 1].0, &snapshots[i].0),
                    probe: snapshots[i + 1].1.since(snapshots[i].1),
                    after: snapshots[i + 1].0,
                }
            })
            .collect();

        let (final_metrics, final_probe) = *snapshots.last().unwrap();
        ArmResult {
            label,
            phases,
            final_metrics,
            probe: final_probe.since(probe_start),
            wall,
        }
    }

    // ----------------------------------------------------------- reporting

    fn percentile(sorted: &[u64], p: f64) -> u64 {
        if sorted.is_empty() {
            return 0;
        }
        sorted[((sorted.len() - 1) as f64 * p).round() as usize]
    }

    fn us(nanos: u64) -> String {
        format!("{:.1}", nanos as f64 / 1_000.0)
    }

    fn mean(sorted: &[u64]) -> u64 {
        if sorted.is_empty() {
            0
        } else {
            sorted.iter().sum::<u64>() / sorted.len() as u64
        }
    }

    /// Mean of a `(count, total nanos)` pair, as microseconds.
    fn mean_us(nanos: u64, count: u64) -> String {
        if count == 0 {
            "-".to_string()
        } else {
            us(nanos / count)
        }
    }

    /// `"27000 @ 1.0us mean"`, or `"none"` — an operation that never ran has no
    /// mean, and printing one as `0.0` would invent a measurement.
    fn op_summary(count: u64, nanos: u64) -> String {
        if count == 0 {
            "none".to_string()
        } else {
            format!("{count} @ {}us", us(nanos / count))
        }
    }

    fn latency_row(label: &str, sorted: &[u64]) {
        println!(
            "  {label:<24} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            us(mean(sorted)),
            us(percentile(sorted, 0.50)),
            us(percentile(sorted, 0.90)),
            us(percentile(sorted, 0.99)),
            us(percentile(sorted, 0.999)),
            us(percentile(sorted, 1.0)),
        );
    }

    fn report_arm(arm: &ArmResult, params: &Params) {
        println!("\n=== arm: {} ===", arm.label);

        for (phase, result) in PHASES.iter().zip(arm.phases.iter()) {
            let ops = result.samples.rtt.len() as u64;
            let secs = result.elapsed.as_secs_f64();
            let tasks = params.half * params.per_shard;

            println!(
                "\n[{}]  {tasks} tasks over {} of {} threads, {} requests each",
                phase.name, params.half, params.threads, params.rounds,
            );
            println!(
                "  {ops} ok / {} err in {:.3}s   ->  {:.0} req/s  ({:.0} req/s per connection)",
                result.samples.errors(),
                secs,
                ops as f64 / secs,
                ops as f64 / secs / params.conns as f64,
            );
            println!(
                "  {:<24} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                "latency (us)", "mean", "p50", "p90", "p99", "p99.9", "max"
            );
            latency_row("acquire", &result.samples.acquire);
            latency_row("round trip", &result.samples.rtt);
            latency_row("cold-start acquire", &result.samples.cold_start);
            println!(
                "  {:<24} {} samples: the first checkout of each task",
                "",
                result.samples.cold_start.len()
            );

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
                "  pool:    acquires={} waits={} timeouts={} created={} closed={} poisoned={} recycle_failures={} unparked={}",
                m.acquires,
                m.waits,
                m.timeouts,
                m.created,
                m.closed,
                m.poisoned,
                m.recycle_failures,
                m.unparked,
            );
            println!(
                "  state:   live={} idle={} parked={}",
                result.after.live, result.after.idle, result.after.parked,
            );
            let p = &result.probe;
            println!(
                "  manager: dials {}  recycles {}  detach {} ({} refused)  attach {} ({} failed)",
                op_summary(p.dials, p.dial_nanos),
                op_summary(p.recycles, p.recycle_nanos),
                op_summary(p.detaches, p.detach_nanos),
                p.detach_refused,
                op_summary(p.attaches, p.attach_nanos),
                p.attach_failed,
            );
        }
    }

    fn throughput(result: &PhaseResult) -> f64 {
        result.samples.rtt.len() as f64 / result.elapsed.as_secs_f64()
    }

    fn row(label: &str, plain: String, steal: String, note: String) {
        println!("  {label:<38} {plain:>13} {steal:>13}   {note}");
    }

    fn ratio(plain: f64, steal: f64) -> String {
        if steal <= 0.0 || plain <= 0.0 {
            return String::new();
        }
        format!("{:.1}x", plain / steal)
    }

    fn summary(params: &Params, plain: &ArmResult, steal: &ArmResult) {
        let (pa, pb) = (&plain.phases[0], &plain.phases[1]);
        let (sa, sb) = (&steal.phases[0], &steal.phases[1]);

        println!("\n--- exchange off vs on ---\n");
        println!("  {:<38} {:>13} {:>13}", "", "no exchange", "reservoir");

        let (ta, tsa) = (throughput(pa), throughput(sa));
        row(
            "A  steady throughput (req/s)",
            format!("{ta:.0}"),
            format!("{tsa:.0}"),
            format!("{:+.1}% with the exchange", 100.0 * (tsa - ta) / ta),
        );
        row(
            "A  checkouts validated by recycle",
            pa.probe.recycles.to_string(),
            sa.probe.recycles.to_string(),
            "a claimed connection skips `recycle`".to_string(),
        );
        row(
            "A  acquire p50 / p99 (us)",
            format!(
                "{} / {}",
                us(percentile(&pa.samples.acquire, 0.50)),
                us(percentile(&pa.samples.acquire, 0.99))
            ),
            format!(
                "{} / {}",
                us(percentile(&sa.samples.acquire, 0.50)),
                us(percentile(&sa.samples.acquire, 0.99))
            ),
            "the bill: every return crosses the queue".to_string(),
        );

        println!();
        row(
            "B  connections dialled",
            pb.delta.created.to_string(),
            sb.delta.created.to_string(),
            format!(
                "{} handshakes avoided",
                pb.delta.created.saturating_sub(sb.delta.created)
            ),
        );
        row(
            "B  checkouts from the reservoir",
            pb.delta.unparked.to_string(),
            sb.delta.unparked.to_string(),
            // At min_idle=0 a thread parks on release and claims again on the
            // next checkout, so this counts every trip through the queue, not
            // only the cross-thread ones.
            if params.min_idle == 0 {
                "every return went through the queue".to_string()
            } else {
                "claimed from another thread instead of dialled".to_string()
            },
        );
        row(
            "B  cold-start acquire, mean (us)",
            us(mean(&pb.samples.cold_start)),
            us(mean(&sb.samples.cold_start)),
            ratio(
                mean(&pb.samples.cold_start) as f64,
                mean(&sb.samples.cold_start) as f64,
            ),
        );
        row(
            "B  cold-start acquire, max (us)",
            us(percentile(&pb.samples.cold_start, 1.0)),
            us(percentile(&sb.samples.cold_start, 1.0)),
            ratio(
                percentile(&pb.samples.cold_start, 1.0) as f64,
                percentile(&sb.samples.cold_start, 1.0) as f64,
            ),
        );
        let (tb, tsb) = (throughput(pb), throughput(sb));
        row(
            "B  steady throughput (req/s)",
            format!("{tb:.0}"),
            format!("{tsb:.0}"),
            format!("{:+.1}% with the exchange", 100.0 * (tsb - tb) / tb),
        );

        println!();
        row(
            "whole run: sockets opened",
            plain.final_metrics.created.to_string(),
            steal.final_metrics.created.to_string(),
            format!("working set is {} connections", params.conns),
        );
        row(
            "whole run: mean dial cost (us)",
            mean_us(plain.probe.dial_nanos, plain.probe.dials),
            mean_us(steal.probe.dial_nanos, steal.probe.dials),
            "what a steal replaces".to_string(),
        );
        row(
            "whole run: mean detach+attach (us)",
            "-".to_string(),
            format!(
                "{} + {}",
                mean_us(steal.probe.detach_nanos, steal.probe.detaches),
                mean_us(steal.probe.attach_nanos, steal.probe.attaches),
            ),
            "what a steal costs".to_string(),
        );
        row(
            "whole run: wall time (s)",
            format!("{:.2}", plain.wall.as_secs_f64()),
            format!("{:.2}", steal.wall.as_secs_f64()),
            String::new(),
        );

        println!("\n  Read phase A as the bill and phase B as the benefit.");
        if params.min_idle == 0 {
            println!(
                "    * at MIN_IDLE=0 the reservoir arm takes every checkout from\n\
                 \x20     the queue, and a claimed connection is never validated, so\n\
                 \x20     it is doing one fewer syscall per request than the\n\
                 \x20     baseline. The recycle row above is the tell."
            );
        } else {
            println!(
                "    * MIN_IDLE={} keeps the steady-state set thread-local, so\n\
                 \x20     phase A is like for like: both arms validate the same\n\
                 \x20     number of checkouts, and only the surplus is shared.",
                params.min_idle,
            );
        }
        println!(
            "    * the dial cost phase B avoids includes `ncat` forking /bin/cat,\n\
             \x20     so the cold-start ratio is a property of this server."
        );
        println!(
            "    * that cost is superlinear in *concurrent* dials: compare the two\n\
             \x20     arms' phase-B mean dial cost above. Halving the number of\n\
             \x20     threads that storm the accept queue at once makes the\n\
             \x20     handshakes that remain cheaper too, so the win is not simply\n\
             \x20     proportional to the handshakes skipped."
        );
    }

    // --------------------------------------------------------------- server

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

    /// Starts `ncat` as an echo server: `--keep-open` so it accepts more than
    /// one connection, `--exec /bin/cat` so each gets its own echo process.
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

    // ------------------------------------------------------------------ run

    pub fn run() -> io::Result<()> {
        let params = Params::from_env();

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
                // The no-exchange arm holds two full working sets at once:
                // phase A's, stranded in the first half's shards, plus phase
                // B's. Leave the server headroom for both, and for the `cat`
                // children it has not reaped yet.
                let server = spawn_ncat(port, params.conns * 6)?;
                wait_ready(addr, Duration::from_secs(5))?;
                println!("started `ncat --listen 127.0.0.1 {port} --keep-open --exec /bin/cat`");
                (addr, Some(server))
            }
        };

        println!(
            "\n{} threads = {} warm + {} cold, {} connections per shard, \
             working set {} connections, {}-byte payload, min_idle {}\n",
            params.threads,
            params.half,
            params.half,
            params.per_shard,
            params.conns,
            params.payload,
            params.min_idle,
        );

        // Baseline first, then the exchange. Both are built through the same
        // builder with the same config; `exchange` is the only difference, and
        // it is what changes the pool's type.
        let plain_pool = Pool::builder(StealManager { addr })
            .config(params.config())
            .build();
        let plain = run_arm(&params, "no exchange (NoExchange)", &plain_pool);
        plain_pool.close();
        drop(plain_pool);
        // Give `ncat` a moment to reap the `cat` children of the sockets that
        // closed when the arm's threads exited, so the next arm starts clean.
        std::thread::sleep(Duration::from_millis(250));

        let steal_pool = Pool::builder(StealManager { addr })
            .config(params.config())
            // Sized to the working set: phase A parks all of it at once.
            .exchange(Reservoir::new(params.conns))
            .build();
        let steal = run_arm(&params, "reservoir (cross-thread exchange)", &steal_pool);

        report_arm(&plain, &params);
        report_arm(&steal, &params);
        summary(&params, &plain, &steal);

        println!("\n--- checks ---");
        check(&params, &plain, &steal);
        println!("  all checks passed");

        // Close before `_server` drops, so the sockets go away before ncat does.
        steal_pool.close();
        Ok(())
    }

    /// The assertions that make this a test and not just a printout.
    fn check(params: &Params, plain: &ArmResult, steal: &ArmResult) {
        for arm in [plain, steal] {
            for (phase, result) in PHASES.iter().zip(arm.phases.iter()) {
                assert_eq!(
                    result.samples.errors(),
                    0,
                    "[{}] {}: {} request(s) failed; first was: {}",
                    arm.label,
                    phase.name,
                    result.samples.errors(),
                    result.samples.first_err.as_deref().unwrap_or("-"),
                );
                assert_eq!(
                    result.delta.timeouts, 0,
                    "[{}] {}: no checkout should have timed out",
                    arm.label, phase.name,
                );
                assert_eq!(
                    result.delta.poisoned, 0,
                    "[{}] {}: nothing was cancelled mid-operation",
                    arm.label, phase.name,
                );
            }
        }

        let (pa, pb) = (&plain.phases[0], &plain.phases[1]);
        let (sa, sb) = (&steal.phases[0], &steal.phases[1]);

        // Without an exchange, phase A's connections are stranded in the first
        // half's shards: nothing is parked, and the second half has to dial a
        // whole second working set of its own.
        assert_eq!(
            pa.after.parked, 0,
            "NoExchange must never park a connection"
        );
        assert_eq!(
            pa.after.live as usize, params.conns,
            "the warm half should be holding the whole working set"
        );
        assert_eq!(
            pb.delta.created as usize, params.conns,
            "every cold checkout without an exchange is a fresh dial"
        );
        assert_eq!(pb.delta.unparked, 0, "NoExchange cannot steal");

        if params.min_idle == 0 {
            // With nothing kept thread-local, every connection is surplus, so
            // by the end of phase A the whole working set belongs to the
            // reservoir and to no shard at all.
            assert_eq!(
                sa.after.live, 0,
                "at min_idle=0 every idle connection should have been parked"
            );
            assert_eq!(
                sa.after.parked,
                sa.after.created - sa.after.closed,
                "every connection that still exists should be in the reservoir"
            );
        }

        // What phase A left in the queue is what phase B can avoid dialling;
        // anything beyond that it has to open for itself. At min_idle=0 that
        // is the whole working set, but the bound holds for any min_idle.
        let stealable = sa.after.parked as usize;
        let unavoidable = params.conns.saturating_sub(stealable);
        // Slack, because the reservoir is not a global semaphore: a checkout
        // that lands in the window where every connection is either held or
        // mid-`attach` sees an empty queue and dials rather than waits. Rare,
        // but not impossible, so the bar is "close to", not "exactly".
        let allowed = unavoidable + stealable.div_ceil(4);
        assert!(
            sb.delta.created as usize <= allowed,
            "the cold half had {stealable} connections parked to claim and still \
             dialled {} (at most {allowed} expected; the no-exchange arm dialled {})",
            sb.delta.created,
            pb.delta.created,
        );
        assert!(
            sb.delta.unparked > 0,
            "the cold half served {} checkouts without claiming a single parked \
             connection",
            sb.delta.acquires,
        );
        assert_eq!(
            steal.probe.attach_failed, 0,
            "every parked socket should have re-attached to its new thread"
        );
        assert_eq!(
            steal.probe.detach_refused, 0,
            "no connection should have reached `detach` with an operation still \
             in flight"
        );
        println!(
            "  no exchange: {} sockets opened for a {}-connection working set; \
             the cold half dialled {} of its own",
            plain.final_metrics.created, params.conns, pb.delta.created,
        );
        println!(
            "  reservoir:   {} sockets opened, {} parked for the cold half to \
             claim, {} dialled there",
            steal.final_metrics.created, stealable, sb.delta.created,
        );
    }
}
