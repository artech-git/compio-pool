//! Pooling `compio::net::UnixStream` — the transport local services actually
//! run on: Postgres and MySQL on the same host, Redis with `unixsocket`, the
//! Docker daemon, anything started by systemd socket activation.
//!
//! The pool does not care that this is not TCP; [`Manage`](compio_pool::Manage) is shaped around a
//! protocol, not a socket family. What earns Unix sockets an example of their
//! own is that they behave differently in three places:
//!
//! * **A liveness check has to be a round trip.** `peer_addr()` — the cheap
//!   check `examples/tcp.rs` uses — reads back a path the kernel recorded at
//!   connect time, and keeps succeeding long after the peer process is gone.
//!   Here [`Manage::recycle`](compio_pool::Manage::recycle) sends a real `PING`.
//! * **The address is a file, with a file's lifetime.** `bind` fails if the
//!   path exists and compio's listener does not unlink it on drop, so a server
//!   that dies leaves a stale socket file behind. A client that dials one gets
//!   `ECONNREFUSED` rather than `ENOENT`: the file is there, nothing is
//!   listening.
//! * **Restarts are the failure mode you actually hit**, because the peer is a
//!   process on this machine that gets upgraded, not a load-balanced endpoint.
//!   When it goes, *every* pooled connection dies at once.
//!
//! This is also the only example that implements [`Manage::disconnect`](compio_pool::Manage::disconnect): a
//! protocol goodbye that has to be sent with no runtime to await on.
//!
//! # Shape of the run
//!
//! | phase | what happens | what it shows |
//! |---|---|---|
//! | A `steady`  | `WORKERS` dispatcher threads, `ROUNDS` requests each | the fast path: acquire is a thread-local pop, no IO |
//! | B `restart` | the server is killed and re-bound on the same path, then the same load runs again | `recycle` rejects each dead connection and the pool redials underneath the caller |
//!
//! Run with: `cargo run --example unix_socket`

#[cfg(not(unix))]
fn main() {
    eprintln!("this example is unix-only");
}

#[cfg(unix)]
fn main() -> std::io::Result<()> {
    uds::run()
}

#[cfg(unix)]
mod uds {
    use std::{
        cell::RefCell,
        io,
        mem::ManuallyDrop,
        net::Shutdown,
        num::NonZeroUsize,
        os::fd::{AsFd, AsRawFd, FromRawFd, RawFd},
        path::{Path, PathBuf},
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
        time::Duration,
    };

    use compio::{
        buf::BufResult,
        dispatcher::Dispatcher,
        io::{AsyncRead, AsyncWriteExt},
        net::{UnixListener, UnixStream},
        runtime::JoinHandle,
    };
    use compio_pool::{Manage, Pool, SlotMeta};

    const WORKERS: usize = 4;
    const ROUNDS: usize = 50;

    /// How idle a connection must be before `recycle` pays for a `PING`.
    ///
    /// This is the knob that decides how much of `recycle` lands on a
    /// request's latency, since it runs at checkout rather than on return.
    /// Phase A stays under it and never probes; phase B starts after a
    /// deliberate pause and probes every connection it finds.
    const PROBE_AFTER: Duration = Duration::from_millis(50);

    // ------------------------------------ reaching a socket without a runtime

    /// Borrows a live descriptor as a `std::os::unix::net::UnixStream` without
    /// taking ownership of it.
    ///
    /// Two places below have to touch a socket synchronously, with no runtime
    /// to await on: the manager's goodbye in [`Manage::disconnect`], and the
    /// server's kill path.
    ///
    /// `ManuallyDrop` is the whole trick, not an optimisation. A std
    /// `UnixStream` closes its descriptor on drop and this one does not own the
    /// descriptor. Letting it drop would close a socket its real owner still
    /// believes is live, and the number would later be handed to something else.
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

    /// Owns the socket file's lifetime, because nothing else does.
    ///
    /// `sun_path` holds 104 bytes on macOS/BSD and 108 on Linux, including the
    /// NUL, and cannot be made longer. `$TMPDIR` on macOS is already ~50 bytes,
    /// so there is less room here than it looks.
    struct SocketPath(PathBuf);

    impl SocketPath {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("compio-pool-{}.sock", std::process::id())))
        }
    }

    impl Drop for SocketPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    // --------------------------------------------------------- a line protocol

    /// Reads one `\n`-terminated line.
    ///
    /// The protocol is strictly one request, one response, so bytes after the
    /// newline mean the stream has desynchronised — which is exactly what an
    /// unguarded cancellation produces.
    async fn read_line(conn: &mut UnixStream) -> io::Result<String> {
        let mut line = Vec::new();
        loop {
            let BufResult(read, buf) = conn.read(vec![0u8; 64]).await;
            match read? {
                0 => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed")),
                n => line.extend_from_slice(&buf[..n]),
            }
            if let Some(end) = line.iter().position(|b| *b == b'\n') {
                if end + 1 != line.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "trailing bytes: this stream is out of step",
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
    async fn converse(stream: &mut UnixStream) {
        while let Ok(line) = read_line(stream).await {
            let reply = match line.split_once(' ') {
                Some(("ECHO", rest)) => rest.to_owned(),
                // `QUIT` is sent by `Uds::disconnect` on the way out. There is
                // nobody left to answer.
                _ if line == "QUIT" => return,
                _ if line == "PING" => "PONG".to_owned(),
                _ => format!("ERR unknown command {line:?}"),
            };
            let BufResult(written, _) = stream.write_all(format!("{reply}\n").into_bytes()).await;
            if written.is_err() {
                return;
            }
        }
    }

    // ------------------------------------------- the server, and how to kill it

    /// The server-side descriptors of the connections being served.
    ///
    /// The kill path closes these deliberately. Letting the tasks be dropped
    /// does not work: a handler parked on a read keeps its socket open, and the
    /// client on the other end waits forever for a reply that is never coming.
    type Clients = Rc<RefCell<Vec<RawFd>>>;

    struct Server {
        /// Held, not detached: dropping the handle cancels the accept loop and
        /// with it the listener.
        accept: JoinHandle<()>,
        clients: Clients,
    }

    impl Server {
        /// Binds and starts accepting, as tasks on the current runtime.
        async fn start(path: &Path) -> io::Result<Self> {
            // A file left by a previous run would make `bind` fail with
            // `EADDRINUSE`. Unlinking first is what every UDS server does.
            let _ = std::fs::remove_file(path);
            let listener = UnixListener::bind(path).await?;
            let clients: Clients = Rc::new(RefCell::new(Vec::new()));

            let accept = compio::runtime::spawn({
                let clients = clients.clone();
                async move {
                    while let Ok((mut stream, _)) = listener.accept().await {
                        let fd = stream.as_fd().as_raw_fd();
                        clients.borrow_mut().push(fd);
                        compio::runtime::spawn({
                            let clients = clients.clone();
                            async move {
                                converse(&mut stream).await;
                                // Off the registry *before* the socket closes,
                                // so a kill can never land on a descriptor
                                // number that has already been recycled.
                                clients.borrow_mut().retain(|&c| c != fd);
                            }
                        })
                        .detach();
                    }
                }
            });

            Ok(Self { accept, clients })
        }

        /// Kills the server, taking every connection it is serving with it.
        ///
        /// This is what a local service being restarted does to a client on the
        /// same box: every socket the pool holds goes dead at the same instant.
        fn kill(self) {
            for fd in self.clients.borrow_mut().drain(..) {
                // SAFETY: a descriptor is on this list only while the handler
                // task that owns it is still running, and nothing can run
                // between the drain and here — this function does not await.
                unsafe { with_borrowed(fd, |sock| sock.shutdown(Shutdown::Both)) }.ok();
            }
            // Cancels the accept loop and drops the listener. Cancelling a
            // submitted `accept` is only safe because nothing is dialling right
            // now: an accept that completes anyway would close a connection the
            // client believes it opened.
            drop(self.accept);
        }
    }

    // ------------------------------------------------------------- the manager

    #[derive(Default)]
    struct Stats {
        dials: AtomicU64,
        /// Checkouts handed over untouched: no syscall between pop and use.
        fast_path: AtomicU64,
        /// Checkouts where the connection was idle long enough to be probed.
        probes: AtomicU64,
        /// Probes that found a dead peer — the whole point of probing.
        probe_failures: AtomicU64,
        goodbyes: AtomicU64,
    }

    struct Uds {
        path: PathBuf,
        stats: Arc<Stats>,
    }

    impl Manage for Uds {
        /// `!Send` for the same reason `TcpStream` is: it belongs to the driver
        /// of the thread that opened it.
        type Connection = UnixStream;
        type Error = io::Error;

        async fn connect(&self) -> io::Result<UnixStream> {
            // Two failures with no TCP analogue, worth telling apart in a real
            // client's error message: `ENOENT` means there is no socket file at
            // all, so the server never came up; `ECONNREFUSED` means the file
            // exists but nothing is listening — a stale file left by a process
            // that died without unlinking.
            self.stats.dials.fetch_add(1, Relaxed);
            UnixStream::connect(&self.path).await
        }

        async fn recycle(&self, conn: &mut UnixStream, meta: &SlotMeta) -> io::Result<()> {
            // `conn.peer_addr()` is worthless as a liveness check here. It
            // reports the path the kernel recorded at connect time, out of
            // local state, and goes on reporting it after the peer is gone. On
            // a Unix socket, liveness means a round trip or nothing.
            if meta.idle_for() < PROBE_AFTER {
                self.stats.fast_path.fetch_add(1, Relaxed);
                return Ok(());
            }

            self.stats.probes.fetch_add(1, Relaxed);
            // Returning `Err` discards this connection: the pool moves on to
            // the next idle one, or dials. The caller never sees it.
            let outcome = match request(conn, "PING").await {
                Ok(reply) if reply == "PONG" => Ok(()),
                Ok(other) => Err(io::Error::other(format!("bad PING reply: {other:?}"))),
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
        /// way out is the descriptor: borrow it as a std socket and do one
        /// blocking write. Nothing is in flight at this point, because the
        /// `Pooled` guard is already gone, so nothing races with it.
        fn disconnect(&self, conn: UnixStream) {
            use std::io::Write;

            self.stats.goodbyes.fetch_add(1, Relaxed);
            // SAFETY: the descriptor is owned by `conn`, which is alive for the
            // whole call and closes it on the way out.
            unsafe {
                with_borrowed(conn.as_fd().as_raw_fd(), |sock| {
                    let _ = { sock }.write_all(b"QUIT\n");
                })
            }
        }
    }

    // ------------------------------------------------------------- the run

    /// Dispatches one task per worker thread and waits for all of them.
    async fn phase(dispatcher: &Dispatcher, pool: &Pool<Uds>, tag: &'static str) -> io::Result<()> {
        let handles = (0..WORKERS)
            .map(|worker| {
                let pool = pool.clone();
                dispatcher
                    .dispatch(move || async move {
                        for round in 0..ROUNDS {
                            let mut conn = pool.acquire().await.expect("acquire");

                            // Arms cancellation protection. A cancelled request
                            // leaves an unknown amount of the reply in the
                            // socket — exactly the desynchronisation
                            // `read_line` rejects — so the pool destroys the
                            // connection instead of reusing it.
                            let op = conn.begin_op();
                            let sent = format!("{tag}-{worker}-{round}");
                            let got = request(&mut conn, &format!("ECHO {sent}"))
                                .await
                                .expect("request");
                            op.complete_op();

                            assert_eq!(got, sent, "echo mismatch");
                        }
                    })
                    .expect("dispatch")
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.await.expect("worker panicked");
        }
        Ok(())
    }

    #[compio::main]
    pub async fn run() -> io::Result<()> {
        let path = SocketPath::new();
        let stats = Arc::new(Stats::default());
        let mut server = Server::start(&path.0).await?;

        let pool = Pool::builder(Uds {
            path: path.0.clone(),
            stats: stats.clone(),
        })
        .max_size(2)
        .min_idle(1)
        .acquire_timeout(Duration::from_secs(5))
        .build();

        let dispatcher = Dispatcher::builder()
            .worker_threads(NonZeroUsize::new(WORKERS).unwrap())
            .concurrent(false)
            .build()?;

        // Phase A: warm shards, every checkout under `PROBE_AFTER`.
        phase(&dispatcher, &pool, "a").await?;
        let after_a = pool.metrics();
        println!(
            "phase A: {} requests, {} dials, {} fast-path checkouts, {} probes",
            WORKERS * ROUNDS,
            stats.dials.load(Relaxed),
            stats.fast_path.load(Relaxed),
            stats.probes.load(Relaxed),
        );
        assert_eq!(
            after_a.recycle_failures, 0,
            "the peer was up the whole time"
        );

        // The restart. Every pooled connection dies here, and the pool has no
        // idea: nothing tells a client its idle sockets went away.
        server.kill();
        server = Server::start(&path.0).await?;
        // Push every idle connection past `PROBE_AFTER`, so phase B probes
        // rather than handing out a corpse on the fast path.
        compio::time::sleep(PROBE_AFTER * 2).await;

        // Phase B: same load, and the pool repairs itself underneath it.
        phase(&dispatcher, &pool, "b").await?;
        let after_b = pool.metrics();
        println!(
            "phase B: {} requests, {} dials total, {} probes, {} of them found a dead peer",
            WORKERS * ROUNDS,
            stats.dials.load(Relaxed),
            stats.probes.load(Relaxed),
            stats.probe_failures.load(Relaxed),
        );
        println!("metrics: {after_b:#?}");

        assert!(
            after_b.recycle_failures > 0,
            "the restart should have been caught by `recycle`, not by a caller"
        );
        assert_eq!(
            after_b.recycle_failures,
            stats.probe_failures.load(Relaxed),
            "every rejection came from a failed PING"
        );

        // Stopping the workers drops their shards, which sends `QUIT` down
        // every connection they still hold — on the thread that owns it.
        dispatcher.join().await?;
        println!("goodbyes sent: {}", stats.goodbyes.load(Relaxed));

        server.kill();
        println!(
            "\nOK: {} requests survived a server restart.",
            2 * WORKERS * ROUNDS
        );
        Ok(())
    }
}
