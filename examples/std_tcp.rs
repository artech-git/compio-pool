//! Pooling connections that start life as a `std::net::TcpStream`.
//!
//! Interop runs in both directions here:
//!
//! * **std → compio.** [`TcpStream::from_std`](compio::net::TcpStream::from_std) adopts a socket you already
//!   have and binds it to *the calling thread's* driver. It is the escape
//!   hatch for everything compio's own connector does not do: dialling with
//!   [`std::net::TcpStream::connect_timeout`], sockets handed over by systemd
//!   socket activation, a SOCKS crate that only speaks std.
//! * **compio → std.** A pooled stream can be *borrowed* back as a
//!   `std::net::TcpStream` to reach std-only APIs such as
//!   [`std::net::TcpStream::take_error`]. See `with_std` for how to do that
//!   without closing the socket out from under compio.
//!
//! The dial is blocking, so it runs on compio's blocking pool. Adoption then
//! happens on the worker thread that asked for the connection, which is the
//! thread that will drive its IO from then on.
//!
//! Run with: `cargo run --example std_tcp`

use std::{
    io,
    mem::ManuallyDrop,
    net::SocketAddr,
    num::NonZeroUsize,
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
    net::{TcpListener, TcpStream},
};
use compio_pool::{Manage, Pool, SlotMeta};

const WORKERS: usize = 4;
const ROUNDS: usize = 5;

/// Borrows a pooled compio stream as a `std::net::TcpStream`.
///
/// `ManuallyDrop` is not an optimisation, it is the whole trick: a
/// `std::net::TcpStream` closes its descriptor on drop, and this one does not
/// own the descriptor — compio does. Letting it drop would close a socket the
/// pool still believes is live, and the number would later be handed out to
/// something else entirely.
///
/// The borrow is read-only by construction, so it is fine for socket options
/// and diagnostics. Do not read or write through it: that races with whatever
/// compio has in flight.
#[cfg(unix)]
fn with_std<T>(conn: &TcpStream, f: impl FnOnce(&std::net::TcpStream) -> T) -> T {
    use std::os::fd::{AsFd, AsRawFd, FromRawFd};

    let raw = conn.as_fd().as_raw_fd();
    // SAFETY: `raw` is a live socket owned by `conn`, which outlives the
    // borrow. `ManuallyDrop` ensures we never close it.
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

struct AdoptedTcp {
    addr: SocketAddr,
    dials: Arc<AtomicU64>,
}

impl Manage for AdoptedTcp {
    type Connection = TcpStream;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<TcpStream> {
        let addr = self.addr;
        self.dials.fetch_add(1, Relaxed);

        // `connect_timeout` is blocking and has no compio equivalent, so it
        // goes to the blocking pool. A bare `std::net::TcpStream::connect`
        // here would stall this thread's driver, and with it every other
        // connection on this shard.
        let std_stream = compio::runtime::spawn_blocking(move || {
            let stream = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
            // Socket options are easier to set while it is still a std socket.
            stream.set_nodelay(true)?;
            io::Result::Ok(stream)
        })
        .await
        .map_err(|_| io::Error::other("dial task panicked"))??;

        // Adoption happens on *this* thread — the dispatcher worker that asked
        // for the connection — so the socket lands on the driver that will
        // drive it from now on.
        TcpStream::from_std(std_stream)
    }

    async fn recycle(&self, conn: &mut TcpStream, _meta: &SlotMeta) -> io::Result<()> {
        // `take_error` drains `SO_ERROR`: a reset that arrived while the socket
        // sat idle shows up here rather than as a mangled reply to the next
        // request. compio's wrapper does not expose it, which is a real reason
        // to reach for the std handle.
        match with_std(conn, |std_conn| std_conn.take_error())? {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

/// One request: write a line, read it back.
async fn round_trip(conn: &mut TcpStream, msg: String) -> io::Result<()> {
    let len = msg.len();
    let BufResult(written, msg) = conn.write_all(msg.into_bytes()).await;
    written?;
    let BufResult(read, buf) = conn.read(vec![0u8; len]).await;
    assert_eq!(&buf[..read?], &msg[..], "echo mismatch");
    Ok(())
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

#[compio::main]
async fn main() -> io::Result<()> {
    let addr = serve().await?;
    let dials = Arc::new(AtomicU64::new(0));

    let pool = Pool::builder(AdoptedTcp {
        addr,
        dials: dials.clone(),
    })
    .max_size(2)
    .min_idle(1)
    .acquire_timeout(Duration::from_secs(5))
    .build();

    let dispatcher = Dispatcher::builder()
        .worker_threads(NonZeroUsize::new(WORKERS).unwrap())
        .concurrent(false)
        .build()?;

    let handles = (0..WORKERS)
        .map(|worker| {
            let pool = pool.clone();
            dispatcher
                .dispatch(move || async move {
                    pool.warm().await.expect("warm");

                    for round in 0..ROUNDS {
                        let mut conn = pool.acquire().await.expect("acquire");

                        // The connection arrived as a `std::net::TcpStream`,
                        // but it is a full compio stream now: completion-based
                        // IO, owned buffers, this thread's driver.
                        let op = conn.begin_op();
                        round_trip(&mut conn, format!("worker {worker} round {round}"))
                            .await
                            .expect("round trip");
                        op.complete_op();
                    }

                    // The std view still works on a pooled socket, and the
                    // option set before adoption is still set.
                    let conn = pool.acquire().await.expect("acquire");
                    with_std(&conn, |s| s.nodelay()).expect("nodelay")
                })
                .expect("dispatch")
        })
        .collect::<Vec<_>>();

    for handle in handles {
        assert!(
            handle.await.expect("worker panicked"),
            "set_nodelay should have survived adoption"
        );
    }

    println!("dials: {}", dials.load(Relaxed));
    println!("metrics: {:#?}", pool.metrics());
    assert!(dials.load(Relaxed) as usize <= WORKERS);

    dispatcher.join().await?;
    println!(
        "\nOK: {WORKERS} std sockets adopted, {} requests over {WORKERS} threads.",
        WORKERS * (ROUNDS + 1)
    );
    Ok(())
}
