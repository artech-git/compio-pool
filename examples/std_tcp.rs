//! Pooling connections that come from an existing `std::net::TcpStream`.
//!
//! Two kinds of interop show up here, in both directions:
//!
//! * **std → compio.** [`compio::net::TcpStream::from_std`] adopts a socket you
//!   already have and attaches it to *the calling thread's* driver. That is the
//!   escape hatch for everything compio's async connector does not do: dialling
//!   with [`std::net::TcpStream::connect_timeout`], sockets handed to you by
//!   systemd socket activation, a `SOCKS`/proxy crate that only speaks std, or
//!   a legacy connector you are migrating away from.
//! * **compio → std.** A pooled `compio::net::TcpStream` can be *borrowed* as a
//!   `std::net::TcpStream` to reach std-only APIs such as
//!   [`std::net::TcpStream::take_error`]. See [`with_std`] for how to do that
//!   without closing the socket out from under compio.
//!
//! The dial itself is blocking, so it runs on compio's blocking pool rather
//! than stalling the driver thread.
//!
//! Run with: `cargo run --example std_tcp`

use std::{
    io,
    mem::ManuallyDrop,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use compio::{
    buf::BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use compio_pool::{Config, Manage, Pool, SlotMeta};

/// Borrows a pooled compio stream as a `std::net::TcpStream`.
///
/// `ManuallyDrop` is not an optimisation, it is the whole trick: a
/// `std::net::TcpStream` closes its descriptor on drop, and this one does not
/// own the descriptor — compio does. Letting it drop would close a socket the
/// pool still believes is live, and the fd number would later be reused by
/// something else entirely.
///
/// The borrow is read-only by construction (`f` gets `&std::net::TcpStream`),
/// so it is fine for socket options and diagnostics. Do not read or write
/// through it: that would race with whatever compio has in flight, and on the
/// poll driver `set_nonblocking` would corrupt the driver's own expectations
/// about the fd.
#[cfg(unix)]
fn with_std<T>(conn: &TcpStream, f: impl FnOnce(&std::net::TcpStream) -> T) -> T {
    use std::os::fd::{AsFd, AsRawFd, FromRawFd};

    let raw = conn.as_fd().as_raw_fd();
    // SAFETY: `raw` is a live TCP socket owned by `conn`, which outlives this
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

struct StdTcpManager {
    addr: SocketAddr,
    connect_timeout: Duration,
    dials: Arc<AtomicU64>,
}

impl Manage for StdTcpManager {
    type Connection = TcpStream;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<TcpStream> {
        let addr = self.addr;
        let connect_timeout = self.connect_timeout;
        self.dials.fetch_add(1, Relaxed);

        // `connect_timeout` is blocking and has no compio equivalent, so it
        // goes to the blocking pool. A bare `std::net::TcpStream::connect`
        // here would block this thread's driver — and with it every other
        // connection on this shard.
        let std_stream = compio::runtime::spawn_blocking(move || {
            let stream = std::net::TcpStream::connect_timeout(&addr, connect_timeout)?;
            // Socket options are easier to set while it is still a std socket.
            stream.set_nodelay(true)?;
            io::Result::Ok(stream)
        })
        .await
        .map_err(|_| io::Error::other("dial task panicked"))??;

        // Adoption happens on *this* thread, so the socket is attached to this
        // thread's driver — the one that will drive its IO from now on.
        TcpStream::from_std(std_stream)
    }

    async fn recycle(&self, conn: &mut TcpStream, meta: &SlotMeta) -> io::Result<()> {
        // `take_error` drains SO_ERROR: a connection reset while the socket sat
        // idle in the pool shows up here rather than as a mangled response to
        // the next request. compio's wrapper does not expose it, so this is a
        // real reason to reach for the std handle.
        if let Some(err) = with_std(conn, |std_conn| std_conn.take_error())
            .ok()
            .flatten()
        {
            return Err(err);
        }

        // Anything idle long enough to be doubtful gets a round-trip check.
        // Here that is just `peer_addr`; a real client would send a PING.
        if meta.idle_for() > Duration::from_secs(30) {
            conn.peer_addr()?;
        }
        Ok(())
    }
}

/// An echo server, so the example has something to talk to.
fn spawn_echo_server() -> SocketAddr {
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
                                    let BufResult(write, _) =
                                        stream.write_all(buf[..n].to_vec()).await;
                                    if write.is_err() {
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
    rx.recv().unwrap()
}

fn main() -> io::Result<()> {
    let addr = spawn_echo_server();
    let dials = Arc::new(AtomicU64::new(0));

    let pool = Pool::new(
        StdTcpManager {
            addr,
            connect_timeout: Duration::from_secs(5),
            dials: dials.clone(),
        },
        Config::new()
            .max_size(2) // per thread
            .min_idle(1)
            .acquire_timeout(Duration::from_secs(5)),
    );

    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let pool = pool.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        // Pre-dial `min_idle` so the first request skips the
                        // handshake. Each thread warms its own shard.
                        pool.warm().await.expect("warm");

                        for round in 0..5u8 {
                            let mut conn = pool.acquire().await.expect("acquire");

                            // The connection arrived as a `std::net::TcpStream` but
                            // is a full compio stream now: completion-based IO,
                            // owned buffers, driven by this thread's driver.
                            let op = conn.begin_op();

                            let msg = format!("worker {worker} round {round}");
                            let BufResult(written, msg) = conn.write_all(msg.into_bytes()).await;
                            written.expect("write");

                            let BufResult(read, buf) = conn.read(vec![0u8; msg.len()]).await;
                            let n = read.expect("read");
                            op.complete();

                            assert_eq!(&buf[..n], &msg[..], "echo mismatch");
                        }

                        // Show that the std view still works on a pooled socket.
                        let conn = pool.acquire().await.expect("acquire");
                        let nodelay = with_std(&conn, |s| s.nodelay()).expect("nodelay");
                        assert!(nodelay, "set_nodelay should have survived adoption");

                        pool.local_size()
                    })
            })
        })
        .collect();

    let held: Vec<usize> = workers.into_iter().map(|w| w.join().unwrap()).collect();

    println!("connections held per worker at exit: {held:?}");
    println!("total dials: {}", dials.load(Relaxed));
    println!("metrics: {:#?}", pool.metrics());

    assert_eq!(
        dials.load(Relaxed),
        4,
        "each thread should dial exactly once"
    );
    println!("\nOK: 4 std sockets adopted, 24 operations across 4 compio threads.");
    Ok(())
}
