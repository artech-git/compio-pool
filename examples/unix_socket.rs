//! Pooling `compio::net::UnixStream` — the transport local services actually
//! run on: Postgres and MySQL on the same host, Redis with `unixsocket`, the
//! Docker daemon, anything started by systemd socket activation.
//!
//! The pool itself does not care that this is not TCP; [`Manage`] is shaped
//! around a protocol, not around a socket family. What earns Unix sockets an
//! example of their own are the three places they behave differently:
//!
//! * **A liveness check has to be a round trip.** `peer_addr()` — the cheap
//!   check `examples/tcp.rs` uses — reads back a path the kernel recorded at
//!   connect time and keeps succeeding long after the peer process has exited.
//!   Here [`Manage::recycle`] sends a real `PING`, and the report below prices
//!   it, because it runs at checkout and lands on the request's latency.
//! * **The address is a file, with a file's lifetime.** `bind` fails if the
//!   path already exists, and `compio`'s listener does not unlink it on drop,
//!   so a server that dies leaves a stale socket file behind. A client that
//!   dials one gets `ECONNREFUSED` rather than `ENOENT` — the file is there,
//!   nothing is listening.
//! * **Restarts are the failure mode you actually hit**, because the peer is a
//!   process on this machine that gets upgraded, not a load-balanced endpoint.
//!   When it goes, *every* pooled connection dies at once.
//!
//! The example also implements [`Manage::disconnect`], which no other example
//! does: a protocol goodbye that has to be sent without a runtime to await on.
//!
//! # Shape of the run
//!
//! | phase | what happens | what it shows |
//! |---|---|---|
//! | A `steady`  | 4 threads x 200 requests | the fast path: acquire is a thread-local pop, no IO, no probe |
//! | B `restart` | the server is killed and re-bound on the same path, then the same load runs again | `recycle` rejects each dead connection and the pool redials underneath the caller — visible as one outlier in phase B's acquire tail |
//!
//! Both phases do the same work, so their latency tables are directly
//! comparable: the only difference in B is the one checkout per shard that has
//! to pay for a failed `PING`, a close and a fresh dial.
//!
//! # Running
//!
//! ```text
//! cargo run --release --example unix_socket
//! ```
//!
//! Tunables, all optional: `THREADS` (4), `ROUNDS` (200 requests per worker per
//! phase), `MAX_SIZE` (4, per shard).
//!
//! Unix only. `compio` speaks AF_UNIX on Windows too, but the descriptor
//! borrowing in `disconnect` below does not.

#[cfg(unix)]
mod uds {
    use std::{
        io,
        mem::ManuallyDrop,
        net::Shutdown,
        os::fd::{AsFd, AsRawFd, FromRawFd, RawFd},
        path::{Path, PathBuf},
        sync::{
            Arc, Barrier, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        },
        thread::JoinHandle,
        time::{Duration, Instant},
    };

    use compio::{
        buf::BufResult,
        io::{AsyncRead, AsyncWriteExt},
        net::{UnixListener, UnixStream},
    };
    use compio_pool::{Config, Manage, Metrics, Pool, SlotMeta};

    // ------------------------------------------------------------ parameters

    /// How idle a connection must be before `recycle` pays for a `PING`.
    ///
    /// Every checkout below the threshold takes the fast path: pop, hand over,
    /// no IO at all. This is the knob that decides how much of `recycle` lands
    /// on the request's latency, since it runs at checkout rather than on
    /// return. The report prints the split both ways.
    ///
    /// Not tunable from the environment, because the run asserts on the number
    /// of probes the restart provokes.
    const PROBE_AFTER: Duration = Duration::from_millis(250);

    struct Params {
        threads: usize,
        rounds: usize,
        max_size: usize,
    }

    impl Params {
        fn from_env() -> Self {
            Self {
                threads: env_usize("THREADS", 4),
                rounds: env_usize("ROUNDS", 200),
                max_size: env_usize("MAX_SIZE", 4),
            }
        }
    }

    fn env_usize(key: &str, default: usize) -> usize {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    // ------------------------------------ reaching a socket without a runtime

    /// Borrows a live descriptor as a `std::os::unix::net::UnixStream` without
    /// taking ownership of it.
    ///
    /// Two places below need to touch a socket synchronously, with no runtime
    /// to await on: the manager's goodbye in [`Manage::disconnect`], and the
    /// server's shutdown path. Both go through here.
    ///
    /// `ManuallyDrop` is the whole trick, not an optimisation. A std
    /// `UnixStream` closes its descriptor on drop and this one does not own the
    /// descriptor. Letting it drop would close a socket its real owner still
    /// believes is live, and the fd number would later be handed out to
    /// something else entirely.
    ///
    /// # Safety
    ///
    /// `fd` must be a live socket whose owner outlives the call.
    unsafe fn with_borrowed<T>(
        fd: RawFd,
        f: impl FnOnce(&std::os::unix::net::UnixStream) -> T,
    ) -> T {
        // SAFETY: the caller guarantees `fd` is live; `ManuallyDrop` ensures
        // this borrow never closes it.
        let borrowed =
            ManuallyDrop::new(unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) });
        f(&borrowed)
    }

    // ------------------------------------------------------- the socket file

    /// Owns the socket file's lifetime, because nothing else does.
    ///
    /// `UnixListener::bind` fails with `EADDRINUSE` if the path exists, and
    /// `compio` does not unlink it when the listener drops. Both ends of that
    /// are the application's policy, so they live here rather than being
    /// assumed.
    struct SocketPath(PathBuf);

    impl SocketPath {
        fn new() -> Self {
            // `sun_path` holds 104 bytes on macOS/BSD and 108 on Linux,
            // including the NUL, and cannot be made longer. `$TMPDIR` on macOS
            // is already ~50 bytes, so there is less room here than it looks.
            Self(std::env::temp_dir().join(format!("compio-pool-{}.sock", std::process::id())))
        }

        fn as_path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for SocketPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // -------------------------- a line protocol, so `recycle` has real work

    /// Reads one `\n`-terminated line.
    ///
    /// The protocol is strictly one request, one response, so bytes after the
    /// newline mean the stream has desynchronised — which is exactly what an
    /// unguarded cancellation produces. A pipelining client would keep the tail
    /// in a per-connection buffer instead of rejecting it.
    async fn read_line(conn: &mut UnixStream) -> io::Result<String> {
        let mut line = Vec::new();
        loop {
            let BufResult(read, buf) = conn.read(vec![0u8; 64]).await;
            match read? {
                0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "peer closed the socket",
                    ));
                }
                n => line.extend_from_slice(&buf[..n]),
            }
            if let Some(end) = line.iter().position(|b| *b == b'\n') {
                if end + 1 != line.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "trailing bytes after the reply: this stream is out of step",
                    ));
                }
                line.truncate(end);
                return String::from_utf8(line)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "reply is not utf-8"));
            }
        }
    }

    /// One command, one reply.
    async fn request(conn: &mut UnixStream, command: &str) -> io::Result<String> {
        let BufResult(written, _) = conn.write_all(format!("{command}\n").into_bytes()).await;
        written?;
        read_line(conn).await
    }

    /// `PING` -> `PONG`, `ECHO <text>` -> `<text>`, `QUIT` -> close.
    async fn serve(mut stream: UnixStream, clients: Clients, fd: RawFd) {
        converse(&mut stream).await;
        // Off the registry *before* the socket closes, so a shutdown can never
        // land on a descriptor number that has already been recycled.
        clients.lock().unwrap().retain(|&client| client != fd);
    }

    async fn converse(stream: &mut UnixStream) {
        loop {
            let Ok(line) = read_line(stream).await else {
                return;
            };
            let reply = match line.split_once(' ') {
                Some(("ECHO", rest)) => rest.to_owned(),
                _ => match line.as_str() {
                    "PING" => "PONG".to_owned(),
                    // Sent by `UnixManager::disconnect` on the way out. There
                    // is nobody left to answer.
                    "QUIT" => return,
                    other => format!("ERR unknown command {other:?}"),
                },
            };
            let BufResult(written, _) = stream.write_all(format!("{reply}\n").into_bytes()).await;
            if written.is_err() {
                return;
            }
        }
    }

    // ------------------------------------------- the server, and how to kill it

    /// The server-side descriptors of the connections currently being served.
    ///
    /// The shutdown path closes these deliberately. Relying on the runtime drop
    /// to do it does not work: a handler parked on a read is not dropped when
    /// its runtime goes away, so its socket stays open for the life of the
    /// process and the client on the other end waits forever for a reply that
    /// is never coming. That is a good illustration of why a real `recycle`
    /// probe wants a deadline over it, but it makes for a non-deterministic
    /// example, so the kill here is explicit.
    type Clients = Arc<Mutex<Vec<RawFd>>>;

    struct Server {
        path: PathBuf,
        stop: Arc<AtomicBool>,
        clients: Clients,
        thread: Option<JoinHandle<()>>,
    }

    impl Server {
        fn start(path: &Path) -> io::Result<Self> {
            let path = path.to_path_buf();
            // A file left behind by a previous run would make `bind` fail with
            // `EADDRINUSE`. Unlinking first is what every UDS server does.
            let _ = std::fs::remove_file(&path);

            let stop = Arc::new(AtomicBool::new(false));
            let clients: Clients = Arc::new(Mutex::new(Vec::new()));
            let (ready_tx, ready_rx) = std::sync::mpsc::channel();
            let thread = std::thread::spawn({
                let path = path.clone();
                let stop = stop.clone();
                let clients = clients.clone();
                move || {
                    compio::runtime::Runtime::new()
                        .unwrap()
                        .block_on(async move {
                            let listener = match UnixListener::bind(&path).await {
                                Ok(listener) => {
                                    ready_tx.send(Ok(())).unwrap();
                                    listener
                                }
                                Err(e) => {
                                    ready_tx.send(Err(e)).unwrap();
                                    return;
                                }
                            };
                            loop {
                                let Ok((stream, _)) = listener.accept().await else {
                                    return;
                                };
                                // The wake-up dial from `stop`, not a client.
                                if stop.load(Relaxed) {
                                    return;
                                }
                                let fd = stream.as_fd().as_raw_fd();
                                clients.lock().unwrap().push(fd);
                                compio::runtime::spawn(serve(stream, clients.clone(), fd)).detach();
                            }
                        });
                }
            });
            ready_rx.recv().unwrap()?;
            Ok(Self {
                path,
                stop,
                clients,
                thread: Some(thread),
            })
        }

        /// Kills the server, taking every connection it is serving with it.
        ///
        /// Every socket the pool is holding goes dead at the same instant,
        /// which is exactly what a local service being restarted does to a
        /// client on the same box.
        fn stop(mut self) {
            for fd in self.clients.lock().unwrap().drain(..) {
                // SAFETY: a descriptor is on this list only while the handler
                // task that owns it is still running; `serve` removes it before
                // the socket drops.
                unsafe { with_borrowed(fd, |sock| sock.shutdown(Shutdown::Both)) }.ok();
            }
            self.stop.store(true, Relaxed);
            // The accept loop is parked in `accept()`. Rather than cancel that
            // operation — a submitted accept that is dropped can complete
            // anyway, and the connection it accepted is then closed under a
            // client that believes it connected — hand it one last dial to
            // return from. A blocking std connect is the right tool: one
            // syscall, from a thread with no compio driver on it.
            let _ = std::os::unix::net::UnixStream::connect(&self.path);
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
            let _ = std::fs::remove_file(&self.path);
        }
    }

    // --------------------------------------------------------- instrumentation

    /// What the manager did, as opposed to what the pool did.
    ///
    /// [`Metrics`] counts checkouts, waits and discards; it cannot know what a
    /// dial or a `recycle` *cost*, because those are the manager's code. Both
    /// numbers matter here: the dial is what pooling amortises away, and the
    /// probe is what `recycle` adds back on the checkout path.
    #[derive(Default)]
    struct Instrumentation {
        dials: AtomicU64,
        dial_nanos: AtomicU64,
        /// Checkouts where the connection was idle long enough to be probed.
        probes: AtomicU64,
        probe_nanos: AtomicU64,
        /// Probes that found a dead peer — the whole point of probing.
        probe_failures: AtomicU64,
        /// Checkouts handed over untouched: no syscall between pop and use.
        fast_path: AtomicU64,
        goodbyes: AtomicU64,
    }

    impl Instrumentation {
        fn mean_dial_nanos(&self) -> u64 {
            self.dial_nanos.load(Relaxed) / self.dials.load(Relaxed).max(1)
        }

        fn mean_probe_nanos(&self) -> u64 {
            self.probe_nanos.load(Relaxed) / self.probes.load(Relaxed).max(1)
        }
    }

    // ---------------------------------------------------------- the manager

    struct UnixManager {
        path: PathBuf,
        probe_after: Duration,
        stats: Arc<Instrumentation>,
    }

    impl Manage for UnixManager {
        // `UnixStream` is `!Send` for the same reason `TcpStream` is: it belongs
        // to the driver of the thread that opened it.
        type Connection = UnixStream;
        type Error = io::Error;

        async fn connect(&self) -> io::Result<UnixStream> {
            // Two failures with no TCP analogue, worth telling apart in a real
            // client's error message: `ENOENT` means there is no socket file at
            // all, so the server has never come up; `ECONNREFUSED` means the
            // file exists but nothing is listening on it — a stale file left by
            // a process that died without unlinking.
            let start = Instant::now();
            let conn = UnixStream::connect(&self.path).await;
            self.stats.dials.fetch_add(1, Relaxed);
            self.stats
                .dial_nanos
                .fetch_add(start.elapsed().as_nanos() as u64, Relaxed);
            conn
        }

        async fn recycle(&self, conn: &mut UnixStream, meta: &SlotMeta) -> io::Result<()> {
            // `conn.peer_addr()` is worthless as a liveness check here. It
            // reports the path the kernel recorded at connect time, out of
            // local state, and goes on reporting it after the peer process is
            // gone. On a Unix socket, liveness means a round trip or nothing.
            if meta.idle_for() < self.probe_after {
                self.stats.fast_path.fetch_add(1, Relaxed);
                return Ok(());
            }

            let start = Instant::now();
            let replied = request(conn, "PING").await;
            self.stats.probes.fetch_add(1, Relaxed);
            self.stats
                .probe_nanos
                .fetch_add(start.elapsed().as_nanos() as u64, Relaxed);

            // Returning `Err` discards this connection; the pool moves on to
            // the next idle one, or dials. The caller never sees it.
            let outcome = match replied {
                Ok(reply) if reply == "PONG" => Ok(()),
                Ok(other) => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("bad PING reply: {other:?}"),
                )),
                Err(e) => Err(e),
            };
            if outcome.is_err() {
                self.stats.probe_failures.fetch_add(1, Relaxed);
            }
            outcome
        }

        /// A best-effort protocol goodbye.
        ///
        /// `disconnect` runs from `Drop` and from thread teardown, so it cannot
        /// await — at teardown there may be no runtime left to await on. The
        /// way out is the descriptor: borrow it as a
        /// `std::os::unix::net::UnixStream` and do one blocking write. Nothing
        /// is in flight at this point, because the [`Pooled`] guard is already
        /// gone, so nothing races with it. If the socket is non-blocking and
        /// the peer is slow, the write fails with `EWOULDBLOCK` and we just
        /// close — which is what "best effort" has to mean here.
        ///
        /// [`Pooled`]: compio_pool::Pooled
        fn disconnect(&self, conn: UnixStream) {
            use std::io::Write;

            self.stats.goodbyes.fetch_add(1, Relaxed);
            // SAFETY: the descriptor is owned by `conn`, which is alive for the
            // whole call and closes it on the way out of this function.
            unsafe {
                with_borrowed(conn.as_fd().as_raw_fd(), |sock| {
                    let mut sock = sock;
                    let _ = sock.write_all(b"QUIT\n");
                })
            }
        }
    }

    // ---------------------------------------------------------------- samples

    #[derive(Default)]
    struct Samples {
        /// Time in `request`: write the command, read the reply.
        rtt: Vec<u64>,
        /// Time in `Pool::acquire`, which is where a probe and a redial land.
        acquire: Vec<u64>,
        /// Failures getting a connection at all.
        acquire_err: u64,
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
            self.acquire_err + self.io_err
        }

        fn merge(&mut self, other: Samples) {
            self.rtt.extend_from_slice(&other.rtt);
            self.acquire.extend_from_slice(&other.acquire);
            self.acquire_err += other.acquire_err;
            self.io_err += other.io_err;
            if self.first_err.is_none() {
                self.first_err = other.first_err;
            }
        }
    }

    // ----------------------------------------------------------- phase driver

    struct Phase {
        name: &'static str,
        what: &'static str,
        /// Printed under the phase's counters, for anything about the window
        /// they were measured over that the numbers do not say themselves.
        note: Option<&'static str>,
    }

    const PHASES: [Phase; 2] = [
        Phase {
            name: "A steady",
            what: "warm shards, no probes: acquire is a thread-local pop",
            note: None,
        },
        Phase {
            name: "B restart",
            what: "same load, but every pooled socket died between the phases",
            // The window has to close after the workers exit, because that is
            // when the last sample lands. Thread exit drops each shard, so half
            // of `closed` here is teardown rather than anything the phase did.
            note: Some(
                "closed = one per shard discarded by `recycle`, plus one per shard \
                 closed at thread exit",
            ),
        },
    ];

    struct PhaseResult {
        samples: Samples,
        /// Longest per-worker wall time; the barrier aligns the starts, so this
        /// is the window the phase actually occupied.
        elapsed: Duration,
        /// Counters attributable to this phase, from the snapshots either side.
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

    /// One worker's share of a phase: `rounds` sequential round trips, each on
    /// a freshly checked-out connection.
    async fn exercise(
        pool: &Pool<UnixManager>,
        worker: usize,
        phase: &str,
        rounds: usize,
    ) -> Samples {
        let mut samples = Samples::with_capacity(rounds);

        for round in 0..rounds {
            let checkout = Instant::now();
            let mut conn = match pool.acquire().await {
                Ok(conn) => conn,
                Err(e) => {
                    samples.acquire_err += 1;
                    samples.record_err(&e);
                    continue;
                }
            };
            samples.acquire.push(checkout.elapsed().as_nanos() as u64);

            // Arm cancellation protection. A completion-based read that is
            // dropped part-way leaves the reply in the socket for whoever reads
            // next, so a cancelled checkout must not go back in the pool. If
            // the await below is cancelled — or returns early on the error
            // path — `complete_op` is never reached and the connection is
            // poisoned instead of returned.
            let op = conn.begin_op();
            let sent = format!("{phase}/{worker}/{round}");
            let started = Instant::now();
            match request(&mut conn, &format!("ECHO {sent}")).await {
                Ok(echoed) => {
                    op.complete_op();
                    samples.rtt.push(started.elapsed().as_nanos() as u64);
                    assert_eq!(echoed, sent, "echo mismatch");
                }
                Err(e) => {
                    samples.io_err += 1;
                    samples.record_err(&e);
                }
            }
        }
        samples
    }

    /// What one worker thread reports back.
    struct WorkerResult {
        /// One entry per phase, in `PHASES` order.
        phases: Vec<(Samples, Duration)>,
        /// Connections this thread's shard still held at exit.
        held: usize,
    }

    // -------------------------------------------------------------- reporting

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

        println!(
            "\n[{}]  {} workers x {} requests — {}",
            phase.name, params.threads, params.rounds, phase.what,
        );
        println!(
            "  {ops} ok / {} err in {:.3}s   ->  {:.0} req/s",
            result.samples.errors(),
            secs,
            ops as f64 / secs,
        );
        println!(
            "  {:<22} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            "latency (us)", "mean", "p50", "p90", "p99", "p99.9", "max"
        );
        latency_row("acquire", &mut result.samples.acquire);
        latency_row("round trip", &mut result.samples.rtt);

        if result.samples.errors() > 0 {
            println!(
                "  errors: {} getting a connection, {} on an open one; first was: {}",
                result.samples.acquire_err,
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
        if let Some(note) = phase.note {
            println!("        {note}");
        }
    }

    // ------------------------------------------------------------------- run

    pub fn run() -> io::Result<()> {
        let params = Params::from_env();
        let socket = SocketPath::new();
        let server = Server::start(socket.as_path())?;

        let stats = Arc::new(Instrumentation::default());
        let pool = Pool::new(
            UnixManager {
                path: socket.as_path().to_owned(),
                probe_after: PROBE_AFTER,
                stats: stats.clone(),
            },
            Config::new()
                // Per shard: `threads` compio threads means up to
                // `threads * max_size` sockets against this server.
                .max_size(params.max_size)
                .min_idle(0)
                .acquire_timeout(Duration::from_secs(5))
                // A measured run must not have connections retired underneath
                // it, or the dial count stops meaning anything.
                .idle_timeout(None)
                .max_lifetime(None)
                .reap_interval(Duration::from_secs(60)),
        );

        println!(
            "socket {}\n{} workers x {} requests x {} phases, max_size {} per shard\n",
            socket.as_path().display(),
            params.threads,
            params.rounds,
            PHASES.len(),
            params.max_size,
        );

        // The workers, plus this thread, which snapshots the counters at every
        // phase boundary and drives the restart between the two phases.
        let gate = Arc::new(Barrier::new(params.threads + 1));
        let rounds = params.rounds;

        let workers: Vec<_> = (0..params.threads)
            .map(|worker| {
                let pool = pool.clone();
                let gate = gate.clone();
                std::thread::spawn(move || {
                    compio::runtime::Runtime::new()
                        .unwrap()
                        .block_on(async move {
                            let mut phases = Vec::with_capacity(PHASES.len());

                            // Blocking on a barrier inside `block_on` parks this
                            // thread's driver. Harmless here — there is one task
                            // on it — but not a pattern to copy into a server.
                            gate.wait(); // starts aligned, counters snapshotted

                            let started = Instant::now();
                            let samples = exercise(&pool, worker, "A", rounds).await;
                            phases.push((samples, started.elapsed()));

                            gate.wait(); // phase A done; the server is killed
                            gate.wait(); // …and is back up

                            // This shard's idle connection is now a socket whose
                            // peer no longer exists. Nothing below knows that:
                            // `recycle` catches it at checkout, and the cost
                            // shows up in phase B's acquire tail.
                            let started = Instant::now();
                            let samples = exercise(&pool, worker, "B", rounds).await;
                            phases.push((samples, started.elapsed()));

                            WorkerResult {
                                phases,
                                held: pool.local_size(),
                            }
                        })
                })
            })
            .collect();

        // Snapshots are taken here, on the coordinating thread, while every
        // worker is parked on the gate. Nothing is running, so the gauges and
        // counters are consistent with each other for once.
        let mut snapshots = vec![pool.metrics()];
        let wall = Instant::now();
        gate.wait(); // release phase A

        gate.wait(); // phase A finished
        snapshots.push(pool.metrics());

        println!("-- killing the server; every pooled connection dies with it");
        server.stop();
        let server = Server::start(socket.as_path())?;
        // Leave the idle connections sitting long enough that `recycle` decides
        // they are worth probing. Standing in for "idle for a while".
        std::thread::sleep(PROBE_AFTER + Duration::from_millis(150));
        println!("-- server back up on the same path");

        snapshots.push(pool.metrics());
        gate.wait(); // release phase B

        let finished: Vec<WorkerResult> = workers.into_iter().map(|w| w.join().unwrap()).collect();
        let wall = wall.elapsed();
        snapshots.push(pool.metrics());
        server.stop();

        // Which pair of snapshots bounds each phase. Phase B's lower bound is
        // the one taken *after* the restart, so the four closes the restart
        // itself caused are billed to neither phase.
        const WINDOWS: [(usize, usize); PHASES.len()] = [(0, 1), (2, 3)];

        let held: Vec<usize> = finished.iter().map(|w| w.held).collect();

        // Fold the per-worker samples into one result per phase.
        let mut folded: Vec<(Samples, Duration)> = (0..PHASES.len())
            .map(|_| (Samples::default(), Duration::ZERO))
            .collect();
        for worker in finished {
            for (phase, (samples, elapsed)) in worker.phases.into_iter().enumerate() {
                folded[phase].0.merge(samples);
                folded[phase].1 = folded[phase].1.max(elapsed);
            }
        }

        let mut results: Vec<PhaseResult> = folded
            .into_iter()
            .zip(WINDOWS)
            .map(|((samples, elapsed), (before, after))| PhaseResult {
                samples,
                elapsed,
                delta: delta(&snapshots[after], &snapshots[before]),
                after: snapshots[after],
            })
            .collect();

        for (phase, result) in PHASES.iter().zip(results.iter_mut()) {
            report_phase(phase, result, &params);
        }

        let metrics = *snapshots.last().unwrap();
        let requests = (params.threads * params.rounds * PHASES.len()) as u64;
        let dials = stats.dials.load(Relaxed);
        let probes = stats.probes.load(Relaxed);
        let fast_path = stats.fast_path.load(Relaxed);
        let mean_rtt = {
            let total: u64 = results
                .iter()
                .map(|r| r.samples.rtt.iter().sum::<u64>())
                .sum();
            let count: u64 = results.iter().map(|r| r.samples.rtt.len() as u64).sum();
            total / count.max(1)
        };

        println!("\n--- pool counters (cumulative, both phases) ---");
        println!("{metrics:#?}");

        println!("\n--- what the numbers say ---");
        println!(
            "  requests served per dial     {}  ({requests} requests over {dials} connections)",
            requests / dials.max(1),
        );
        println!(
            "  mean dial cost               {} us  ({:.1}x a round trip, paid once per connection)",
            us(stats.mean_dial_nanos()),
            stats.mean_dial_nanos() as f64 / mean_rtt.max(1) as f64,
        );
        println!(
            "  mean PING probe cost         {} us  (on the acquire path, when armed)",
            us(stats.mean_probe_nanos()),
        );
        println!(
            "  checkouts on the fast path   {fast_path} of {} ({:.2}%) — popped and handed over, no IO",
            metrics.acquires,
            100.0 * fast_path as f64 / metrics.acquires.max(1) as f64,
        );
        println!(
            "  checkouts that paid a probe  {probes} of {} ({:.2}%), of which {} found a dead peer",
            metrics.acquires,
            100.0 * probes as f64 / metrics.acquires.max(1) as f64,
            stats.probe_failures.load(Relaxed),
        );
        println!(
            "  checkouts that had to wait   {} of {} ({:.2}%)",
            metrics.waits,
            metrics.acquires,
            100.0 * metrics.waits as f64 / metrics.acquires.max(1) as f64,
        );
        println!(
            "  timeouts / poisoned / recycle failures   {} / {} / {}",
            metrics.timeouts, metrics.poisoned, metrics.recycle_failures,
        );
        println!(
            "  QUIT goodbyes sent           {}  (one per connection closed, from `disconnect`)",
            stats.goodbyes.load(Relaxed),
        );
        println!(
            "  connections held per worker  {held:?} at exit, {} live",
            metrics.live,
        );
        println!(
            "  total wall time              {:.2}s  (of which {:.2}s is the pause that lets \
             the pooled connections go stale)",
            wall.as_secs_f64(),
            (PROBE_AFTER + Duration::from_millis(150)).as_secs_f64(),
        );

        assert_eq!(metrics.acquires, requests, "every request checked out once");
        // Phase A dials once per shard and then reuses; phase B dials once per
        // shard again, because the restart invalidated every idle connection.
        assert_eq!(
            dials,
            (params.threads * 2) as u64,
            "{requests} requests should cost {} dials, not one per request",
            params.threads * 2
        );
        assert_eq!(
            metrics.recycle_failures, params.threads as u64,
            "each shard's dead connection should be caught by recycle"
        );
        assert_eq!(metrics.timeouts, 0, "no checkout should have timed out");
        assert_eq!(metrics.poisoned, 0, "no checkout was cancelled");
        // One per connection discarded at the restart, one per connection
        // closed when its thread exited.
        assert_eq!(
            stats.goodbyes.load(Relaxed),
            (params.threads * 2) as u64,
            "every connection should have been said goodbye to"
        );
        assert_eq!(metrics.live, 0, "every connection closed on its own thread");

        println!(
            "\nOK: {requests} requests over {} connections, across a server restart \
             no caller saw.",
            params.threads * 2
        );
        Ok(())
    }
}

#[cfg(unix)]
fn main() -> std::io::Result<()> {
    uds::run()
}

#[cfg(not(unix))]
fn main() {
    println!("examples/unix_socket: skipped, Unix domain sockets only.");
}
