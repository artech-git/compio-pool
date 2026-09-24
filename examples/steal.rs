//! The cross-thread overflow / steal pool, with real sockets.
//!
//! [`Reservoir`] is a bounded, lock-free `ArrayQueue` shared by every compio
//! thread. A shard whose free list has grown past `min_idle` **detaches** the
//! surplus socket — lifting the fd out of its driver — and pushes it there. A
//! shard whose free list is empty **pops** one and re-wraps it in its own
//! runtime before it will consider paying for a TCP handshake.
//!
//! This example implements [`Detach`] for a real [`compio::net::TcpStream`],
//! which is where the two interesting constraints live:
//!
//! * **No pending ops.** An fd may only change drivers when nothing is still
//!   submitted against it. `compio` exposes exactly that check:
//!   [`SharedFd::try_unwrap`](compio::driver::SharedFd::try_unwrap) succeeds
//!   only at a strong count of one, meaning no in-flight operation holds a
//!   reference. `detach` returns `None` otherwise.
//! * **Re-wrapping.** The claiming thread rebuilds the stream with
//!   [`TcpStream::from_std`], binding the socket to *its* driver. Which
//!   constructor to use differs across compio versions — `from_std` here, a
//!   `from_raw_fd` in others — which is why it lives behind [`Detach::attach`]
//!   rather than inside the pool.
//!
//! Run with: `cargo run --example steal`

#[cfg(not(unix))]
fn main() {
    // `Detach` is unsound under IOCP: a handle binds to one completion port
    // for life, so a stolen socket would keep delivering completions to the
    // thread that opened it. See the `Detach` docs.
    eprintln!("this example is unix-only; `Detach` is not sound on IOCP");
}

#[cfg(unix)]
fn main() -> std::io::Result<()> {
    unix::run()
}

#[cfg(unix)]
mod unix {
    use std::{
        io,
        net::SocketAddr,
        os::fd::{FromRawFd, IntoRawFd, OwnedFd},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
        time::Duration,
    };

    use compio::{
        buf::BufResult,
        driver::ToSharedFd,
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    use compio_pool::{Config, Detach, Manage, Pool, Reservoir, SlotMeta};

    /// How many threads park connections, and how many each opens.
    const PARKERS: usize = 4;
    const PER_THREAD: usize = 3;

    struct StealManager {
        addr: SocketAddr,
        dials: Arc<AtomicU64>,
    }

    impl Manage for StealManager {
        type Connection = TcpStream;
        type Error = io::Error;

        async fn connect(&self) -> io::Result<TcpStream> {
            self.dials.fetch_add(1, Relaxed);
            let conn = TcpStream::connect(self.addr).await?;
            conn.set_nodelay(true)?;
            Ok(conn)
        }

        async fn recycle(&self, conn: &mut TcpStream, _meta: &SlotMeta) -> io::Result<()> {
            conn.peer_addr().map(|_| ())
        }
    }

    impl Detach for StealManager {
        /// `OwnedFd` is `Send` and owns the descriptor — everything the socket
        /// needs to exist between two drivers. A protocol client would carry
        /// its `Send` session state alongside it here.
        type Parked = OwnedFd;

        fn detach(conn: TcpStream) -> Option<OwnedFd> {
            // `to_shared_fd` hands out a *clone* of the handle, so drop our
            // stream first. `try_unwrap` then succeeds exactly when nothing
            // else holds a reference — which is the "no operation in flight"
            // precondition for moving an fd between drivers. If a submission
            // is still outstanding it fails, `shared` drops, and the socket
            // closes once that operation completes.
            let shared = conn.to_shared_fd();
            drop(conn);
            let socket = shared.try_unwrap().ok()?;
            // SAFETY: `try_unwrap` gave us sole ownership, and `into_raw_fd`
            // gives up the socket's claim on the descriptor, so the `OwnedFd`
            // below is its only owner.
            Some(unsafe { OwnedFd::from_raw_fd(socket.into_raw_fd()) })
        }

        async fn attach(fd: OwnedFd) -> io::Result<TcpStream> {
            // Runs on the claiming thread, so the socket is registered with
            // *that* thread's driver. Under io_uring and poll this is
            // bookkeeping; the fd table is process-wide.
            TcpStream::from_std(std::net::TcpStream::from(fd))
        }
    }

    /// One echo round trip, to prove a stolen socket is still a live socket.
    async fn echo(conn: &mut TcpStream, msg: &str) -> io::Result<String> {
        let BufResult(written, _) = conn.write_all(msg.as_bytes().to_vec()).await;
        written?;
        let BufResult(read, buf) = conn.read_exact(vec![0u8; msg.len()]).await;
        read?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    fn spawn_echo_server() -> io::Result<SocketAddr> {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            compio::runtime::Runtime::new()
                .unwrap()
                .block_on(async move {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    tx.send(listener.local_addr().unwrap()).unwrap();
                    loop {
                        let Ok((mut stream, _)) = listener.accept().await else {
                            return;
                        };
                        compio::runtime::spawn(async move {
                            loop {
                                let BufResult(read, buf) = stream.read(vec![0u8; 64]).await;
                                match read {
                                    Ok(0) | Err(_) => return,
                                    Ok(n) => {
                                        let BufResult(w, _) =
                                            stream.write_all(buf[..n].to_vec()).await;
                                        if w.is_err() {
                                            return;
                                        }
                                    }
                                }
                            }
                        })
                        .detach();
                    }
                })
        });
        Ok(rx.recv().unwrap())
    }

    /// Runs `f` on its own compio thread and waits for it.
    fn on_compio_thread<T: Send + 'static>(
        f: impl FnOnce(Pool<StealManager, Reservoir<StealManager>>) -> T + Send + 'static,
        pool: &Pool<StealManager, Reservoir<StealManager>>,
    ) -> T {
        let pool = pool.clone();
        std::thread::spawn(move || f(pool)).join().unwrap()
    }

    pub fn run() -> io::Result<()> {
        let addr = spawn_echo_server()?;
        let dials = Arc::new(AtomicU64::new(0));

        let total = PARKERS * PER_THREAD;
        let pool = Pool::builder(StealManager {
            addr,
            dials: dials.clone(),
        })
        .config(
            Config::new()
                .max_size(PER_THREAD)
                // Keep nothing locally: every returned connection is surplus,
                // so it all goes to the shared queue where any thread can take
                // it. A real service sets this to its steady-state per-thread
                // concurrency and shares only the overflow.
                .min_idle(0)
                .acquire_timeout(Duration::from_secs(5))
                .idle_timeout(None)
                .max_lifetime(None),
        )
        .exchange(Reservoir::new(total))
        .build();

        println!("echo server on {addr}");
        println!("reservoir capacity {total}\n");

        // Stage 1: `PARKERS` threads each open `PER_THREAD` connections and
        // all hold them at the same time, so every one of them has to be
        // dialled — nobody can steal from a thread that is still using its
        // sockets. They are released only after the barrier.
        let barrier = Arc::new(std::sync::Barrier::new(PARKERS));
        let parkers: Vec<_> = (0..PARKERS)
            .map(|worker| {
                let pool = pool.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    compio::runtime::Runtime::new()
                        .unwrap()
                        .block_on(async move {
                            let mut held = Vec::new();
                            for _ in 0..PER_THREAD {
                                held.push(pool.acquire().await.expect("acquire"));
                            }
                            for (i, conn) in held.iter_mut().enumerate() {
                                let op = conn.begin_op();
                                let msg = format!("w{worker}c{i}");
                                assert_eq!(echo(conn, &msg).await.expect("echo"), msg);
                                op.complete();
                            }
                            // Everyone is holding their full share here, so
                            // `total` connections are live at once.
                            barrier.wait();
                            // Dropping them parks them: over min_idle, no
                            // waiters, so the shard offers each to the queue
                            // instead of closing it at thread exit.
                            drop(held);
                        })
                })
            })
            .collect();
        for p in parkers {
            p.join().unwrap();
        }

        let after_park = pool.metrics();
        println!("after {PARKERS} threads parked their connections:");
        println!("  dials    {}", dials.load(Relaxed));
        println!("  parked   {}", after_park.parked);
        println!("  live     {}  (a parked connection belongs to no shard)\n", after_park.live);
        assert_eq!(after_park.parked as usize, total, "all should be parked");
        assert_eq!(after_park.live, 0);

        // Stage 2: a thread that has never dialled anything. Its free list is
        // empty, so every checkout pops from the shared queue and re-wraps the
        // socket here rather than opening a new one.
        let dials_before = dials.load(Relaxed);
        on_compio_thread(
            |pool| {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        let mut held = Vec::new();
                        for i in 0..PER_THREAD {
                            let mut conn = pool.acquire().await.expect("acquire");
                            // A stolen socket has to still work: same fd, new
                            // driver, mid-stream.
                            let op = conn.begin_op();
                            let msg = format!("stolen{i}");
                            assert_eq!(echo(&mut conn, &msg).await.expect("echo"), msg);
                            op.complete();
                            held.push(conn);
                        }
                        drop(held);
                    })
            },
            &pool,
        );

        let after_steal = pool.metrics();
        println!("after a fresh thread served {PER_THREAD} requests:");
        println!(
            "  dials    {}  (+{} — it stole instead of dialling)",
            dials.load(Relaxed),
            dials.load(Relaxed) - dials_before,
        );
        println!("  unparked {}", after_steal.unparked);
        println!("  parked   {}\n", after_steal.parked);

        assert_eq!(
            dials.load(Relaxed),
            dials_before,
            "a thread with an empty free list must drain the queue before dialling"
        );
        assert_eq!(after_steal.unparked as usize, PER_THREAD);

        println!("metrics: {:#?}", pool.metrics());
        println!(
            "\nOK: {total} connections opened by {PARKERS} threads, \
             {PER_THREAD} of them later served requests on a thread that never dialled."
        );
        pool.close();
        Ok(())
    }
}
