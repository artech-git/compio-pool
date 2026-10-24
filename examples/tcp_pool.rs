//! The pool doing its day job: pooling **upstream TCP client connections**.
//!
//! `echo.rs` pools plain buffers to show the mechanics. This example shows the
//! canonical bb8 use — a bounded, reused set of connections to a backend. The
//! front is a thread-per-core `SO_REUSEPORT` TCP server; for each client request
//! it leases an upstream [`TcpStream`] from the pool, round-trips the bytes, and
//! returns the lease so the next request can reuse the same upstream. A handful
//! of upstreams per worker therefore serves many client connections.
//!
//! The upstream is just an echo backend, so from a client's point of view the
//! front echoes too — drive it with `examples/load.rs` and you measure the pool,
//! the forwarding, and the round-trip to the backend together.
//!
//! ```text
//! # 1. a backend to pool connections to (any echo server on one address):
//! cargo run --release --example echo -- 127.0.0.1:7001
//!
//! # 2. the pooling front:
//! UPSTREAM=127.0.0.1:7001 \
//! cargo run --release --example tcp_pool -- [ADDR] [CAPACITY]
//!   ADDR       front bind address              default 0.0.0.0:7000
//!   CAPACITY   upstream connections per worker default 64
//!
//! # 3. load the front as if it were the echo server:
//! cargo run --release --example load -- 127.0.0.1:7000 --conns 64 --seconds 5
//! ```

use std::{io, net::SocketAddr, sync::LazyLock, thread};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};
use compio_pool::{LocalPool, ManageConnection, Pool, bind_reuseport, cpu};

/// The backend every worker's pool connects to.
static UPSTREAM: LazyLock<SocketAddr> = LazyLock::new(|| {
    std::env::var("UPSTREAM")
        .unwrap_or_else(|_| "127.0.0.1:7001".into())
        .parse()
        .expect("UPSTREAM must be host:port")
});

/// One pooled upstream connection, plus a flag the handler trips when a round
/// trip fails so the pool discards it instead of handing out a dead socket.
struct Upstream {
    stream: TcpStream,
    healthy: bool,
}

/// Opens and vets upstream connections for the pool. This — not the buffer in
/// `echo.rs` — is what a real pool manages: a Redis, Postgres or HTTP backend
/// would connect and ping here exactly the same way.
struct Backend;

impl ManageConnection for Backend {
    type Connection = Upstream;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<Upstream> {
        let stream = TcpStream::connect(*UPSTREAM).await?;
        stream.set_nodelay(true)?;
        Ok(Upstream {
            stream,
            healthy: true,
        })
    }

    /// Checked out of the idle list when `test_on_check_out` is on. A real
    /// backend would send a cheap health probe (Redis `PING`, SQL `SELECT 1`)
    /// here; an opaque echo backend has no such probe, so we trust the
    /// `has_broken` flag instead.
    async fn is_valid(&self, conn: &mut Upstream) -> io::Result<()> {
        if conn.healthy {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "upstream marked dead",
            ))
        }
    }

    fn has_broken(&self, conn: &mut Upstream) -> bool {
        !conn.healthy
    }
}

fn main() -> io::Result<()> {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:7000".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let capacity: u32 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    // Build the pool once; every worker gets a clone of the `Send` handle.
    let pool = Pool::builder().max_size_per_thread(capacity).build(Backend);

    let cores = cpu::cores();
    if cores.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no cores to run on",
        ));
    }
    println!(
        "tcp_pool on {addr} -> upstream {}: {} workers, {capacity} upstreams/worker",
        *UPSTREAM,
        cores.len()
    );

    let mut handles = Vec::new();
    for (index, core) in cores.into_iter().enumerate() {
        let pool = pool.clone();
        let handle = thread::Builder::new()
            .name(format!("worker/{index}"))
            .spawn(move || worker(core, addr, pool))?;
        handles.push(handle);
    }
    for handle in handles {
        let _ = handle.join();
    }
    Ok(())
}

/// One pinned thread: its own runtime, its own `SO_REUSEPORT` listener, and its
/// own [`LocalPool`] of upstream connections carved from the shared [`Pool`].
fn worker(core: cpu::CoreId, addr: SocketAddr, pool: Pool<Backend>) {
    cpu::pin_current_core(core);

    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        let local = pool.local();

        let std_listener = bind_reuseport(addr, 1024, None).expect("bind SO_REUSEPORT");
        let listener = TcpListener::from_std(std_listener).expect("wrap listener in ring");

        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                continue;
            };
            compio::runtime::spawn(serve(local.clone(), stream)).detach();
        }
    });
}

/// Forward each client request through a freshly leased upstream and hand the
/// upstream straight back, so a small pool fans out across many clients.
async fn serve(local: LocalPool<Backend>, mut client: TcpStream) {
    let mut buf = Vec::with_capacity(16 * 1024);
    loop {
        buf.clear();
        let BufResult(read, b) = client.read(buf).await;
        buf = b;
        let n = match read {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };

        // Lease one upstream for just this round trip.
        let mut up = match local.get().await {
            Ok(up) => up,
            Err(_) => return, // pool exhausted past the timeout, or connect failed
        };

        // Request out to the upstream.
        let BufResult(written, b) = up.stream.write_all(buf).await;
        buf = b;
        if written.is_err() {
            up.healthy = false;
            return;
        }

        // Echo backend replies with exactly what it got, so read n back.
        let BufResult(got, reply) = up.stream.read_exact(Vec::with_capacity(n)).await;
        if got.is_err() {
            up.healthy = false;
            return;
        }
        drop(up); // return the upstream to the pool before touching the client

        let BufResult(written, _) = client.write_all(reply).await;
        if written.is_err() {
            return;
        }
    }
}
