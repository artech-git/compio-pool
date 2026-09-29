//! [`Detach`](compio_pool::Detach) for a real socket, moving between two real threads.
//!
//! A connection leaves one worker's free list, has its fd lifted out of that
//! worker's driver, waits in the shared [`Reservoir`](compio_pool::Reservoir), and is rebuilt as a
//! working stream on a *different* worker. That round trip is the subject
//! here, and it is worth isolating because it is the part with sharp edges:
//!
//! * **No pending ops.** An fd may only change drivers when nothing is still
//!   submitted against it. `compio` exposes exactly that check:
//!   [`SharedFd::try_unwrap`](compio::driver::SharedFd::try_unwrap) succeeds
//!   only at a strong count of one, meaning no in-flight operation holds a
//!   reference. [`Detach::detach`](compio_pool::Detach::detach) returns `None` otherwise.
//! * **Re-wrapping.** The claiming side rebuilds the stream with
//!   [`TcpStream::from_std`](compio::net::TcpStream::from_std), binding the socket to *its* driver. Which
//!   constructor to use differs across compio versions, which is why this
//!   lives behind [`Detach::attach`](compio_pool::Detach::attach) rather than inside the pool.
//!
//! # Getting the two halves onto two threads
//!
//! The pool shards per thread, so a parked socket is only interesting if some
//! *other* thread claims it. Two dispatched tasks on a two-worker dispatcher
//! guarantee that: with `concurrent(false)` a worker cannot take a second task
//! while its first is still running, so the parker and the claimer cannot land
//! on the same thread. Each reports the thread it ran on, and the run asserts
//! the two differ.
//!
//! They hand off through [`Pool::metrics`](compio_pool::Pool::metrics) rather than a channel: the parker
//! stays alive until its sockets have been claimed, and the claimer waits
//! until they have been parked.
//!
//! Run with: `cargo run --example steal`

#[cfg(not(unix))]
fn main() {
    // `Detach` is unsound under IOCP: a handle binds to one completion port
    // for life, so a claimed socket would keep delivering completions to the
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
        num::NonZeroUsize,
        os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        time::{Duration, Instant},
    };

    use compio::{
        buf::BufResult,
        dispatcher::Dispatcher,
        driver::ToSharedFd,
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    use compio_pool::{Detach, Manage, Pool, Reservoir, SlotMeta};

    /// Connections moved across. Holding them all at once is what forces the
    /// pool to dial more than one — a checkout already returned would just be
    /// reused.
    const CONNS: usize = 3;

    struct Echo(SocketAddr);

    impl Manage for Echo {
        type Connection = TcpStream;
        type Error = io::Error;

        async fn connect(&self) -> io::Result<TcpStream> {
            TcpStream::connect(self.0).await
        }

        async fn recycle(&self, conn: &mut TcpStream, _meta: &SlotMeta) -> io::Result<()> {
            conn.peer_addr().map(|_| ())
        }
    }

    impl Detach for Echo {
        /// `OwnedFd` is `Send` and owns the descriptor — everything the socket
        /// needs in order to exist in between two drivers. A protocol client
        /// would carry its `Send` session state alongside it here.
        type Parked = OwnedFd;

        fn detach(conn: TcpStream) -> Option<OwnedFd> {
            // `to_shared_fd` hands out a *clone* of the handle, so drop our
            // stream first. `try_unwrap` then succeeds exactly when nothing
            // else holds a reference — the "no operation in flight"
            // precondition for moving an fd between drivers. If a submission
            // is still outstanding it fails, `shared` drops, and the socket
            // closes once that operation completes.
            let shared = conn.to_shared_fd();
            drop(conn);
            let socket = shared.try_unwrap().ok()?;
            // SAFETY: `try_unwrap` gave us sole ownership, and `into_raw_fd`
            // gives up the socket's claim on the descriptor, so the `OwnedFd`
            // is its only owner.
            Some(unsafe { OwnedFd::from_raw_fd(socket.into_raw_fd()) })
        }

        async fn attach(fd: OwnedFd) -> io::Result<TcpStream> {
            // Runs on the claiming worker, so the socket registers with *that*
            // driver. Under io_uring and poll this is bookkeeping: the fd table
            // is process-wide and the descriptor number does not change, which
            // is what makes the fds printed below a usable identity.
            TcpStream::from_std(std::net::TcpStream::from(fd))
        }
    }

    /// One echo round trip, to prove a re-attached socket is still a live
    /// socket rather than just a number.
    async fn echo(conn: &mut TcpStream, msg: &str) -> io::Result<()> {
        let BufResult(written, msg) = conn.write_all(msg.as_bytes().to_vec()).await;
        written?;
        let BufResult(read, buf) = conn.read_exact(vec![0u8; msg.len()]).await;
        read?;
        assert_eq!(buf, msg, "echo mismatch");
        Ok(())
    }

    /// Waits for the other worker without blocking this one's driver.
    async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            compio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Starts an echo server as a task on the current runtime, and returns the
    /// address it bound.
    async fn serve() -> io::Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        compio::runtime::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                compio::runtime::spawn(async move {
                    loop {
                        let BufResult(read, buf) = stream.read(vec![0u8; 64]).await;
                        let Ok(n @ 1..) = read else { return };
                        let BufResult(written, _) = stream.write_all(buf[..n].to_vec()).await;
                        if written.is_err() {
                            return;
                        }
                    }
                })
                .detach();
            }
        })
        .detach();
        Ok(addr)
    }

    fn thread_name() -> String {
        std::thread::current().name().unwrap_or("?").to_owned()
    }

    #[compio::main]
    pub async fn run() -> io::Result<()> {
        let addr = serve().await?;

        let pool = Pool::builder(Echo(addr))
            .max_size(CONNS)
            // `min_idle` defaults to 0, so every returned connection is surplus
            // and gets offered to the reservoir rather than kept locally.
            .exchange(Reservoir::new(CONNS))
            .build();

        let dispatcher = Dispatcher::builder()
            .worker_threads(NonZeroUsize::new(2).unwrap())
            .concurrent(false)
            .thread_names(|i| format!("worker-{i}"))
            .build()?;

        // Dials `CONNS` sockets, uses them, then drops them so the shard hands
        // each to the reservoir.
        let park = dispatcher
            .dispatch({
                let pool = pool.clone();
                move || async move {
                    let mut held = Vec::new();
                    for i in 0..CONNS {
                        let mut conn = pool.acquire().await.expect("acquire");
                        // Arms cancellation protection: a checkout cancelled
                        // mid-operation is poisoned, and a poisoned connection
                        // is destroyed rather than parked. Nothing with unknown
                        // protocol state is ever handed to another driver.
                        let op = conn.begin_op();
                        echo(&mut conn, &format!("hello {i}")).await.expect("echo");
                        op.complete_op();
                        held.push(conn);
                    }
                    let fds: Vec<RawFd> = held.iter().map(|c| c.as_raw_fd()).collect();

                    // Over `min_idle`, nobody waiting: each one is detached out
                    // of this worker's driver and pushed to the reservoir.
                    drop(held);

                    // Stay on this thread until they have been claimed, so the
                    // claim cannot be this thread doing it.
                    wait_until("the claim", || pool.metrics().unparked as usize == CONNS).await;
                    (thread_name(), fds)
                }
            })
            .expect("dispatch");

        // Claims them back. This worker's free list is empty, so every checkout
        // pops from the reservoir and re-attaches rather than dialling.
        let claim = dispatcher
            .dispatch({
                let pool = pool.clone();
                move || async move {
                    wait_until("the park", || pool.metrics().parked as usize == CONNS).await;

                    let mut claimed = Vec::new();
                    let mut fds = Vec::new();
                    for i in 0..CONNS {
                        let mut conn = pool.acquire().await.expect("acquire");
                        fds.push(conn.as_raw_fd());
                        let op = conn.begin_op();
                        echo(&mut conn, &format!("again {i}")).await.expect("echo");
                        op.complete_op();
                        claimed.push(conn);
                    }
                    (thread_name(), fds)
                }
            })
            .expect("dispatch");

        let (parker, dialled) = park.await.expect("parker panicked");
        let (claimer, claimed) = claim.await.expect("claimer panicked");

        println!("{parker} dialled fds {dialled:?}");
        println!("{claimer} claimed fds {claimed:?}");

        let m = pool.metrics();
        println!(
            "created {}, parked {}, unparked {}",
            m.created, m.parked, m.unparked
        );

        assert_ne!(parker, claimer, "the two halves must be different threads");
        assert_eq!(m.created as usize, CONNS, "a claim must not cost a dial");
        assert_eq!(m.unparked as usize, CONNS);
        // The reservoir is FIFO, so they come back in the order they were
        // parked — and `attach` keeps the descriptor, so these are provably the
        // same sockets, not replacements.
        assert_eq!(claimed, dialled, "same sockets, same order, re-attached");

        dispatcher.join().await?;
        pool.close();
        assert_eq!(pool.metrics().parked, 0, "close empties the reservoir");

        println!("\nOK: {CONNS} sockets detached on {parker}, re-attached on {claimer}.");
        Ok(())
    }
}
