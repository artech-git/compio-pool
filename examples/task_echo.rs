//! The thread-free counterpart of `echo.rs`: a TCP echo server on **one** compio
//! runtime, where concurrency is compio *tasks*, not OS threads.
//!
//! `echo.rs` spawns a pinned OS thread per core and gives each its own runtime,
//! listener and [`LocalPool`]. This example does none of that: it builds a single
//! [`Runtime`], binds one ordinary [`TcpListener`], and for each accepted
//! connection calls [`compio::runtime::spawn`] — a task on this same thread and
//! this same `io_uring` ring. There is no [`std::thread::spawn`], no core pinning
//! and no `SO_REUSEPORT`; thousands of connections are served concurrently by
//! thousands of cheap tasks cooperatively scheduled on one ring.
//!
//! The [`Pool`] is used exactly as everywhere else — one [`LocalPool`] for the
//! thread, a buffer leased per connection — which is the point: the pool's model
//! is *per runtime*, so a single-runtime server is just the one-thread case of
//! it, with nothing extra to set up.
//!
//! ```text
//! cargo run --release --example task_echo -- [ADDR] [CAPACITY]
//!   ADDR       bind address               default 127.0.0.1:7000
//!   CAPACITY   max connections (buffers)  default 1024
//!
//! # talk to it:
//! echo -n hello | nc -w1 127.0.0.1 7000
//! ```

use std::{io, net::SocketAddr};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};
use compio_pool::{LocalPool, ManageConnection, Pool};

/// Hands out reusable 16 KiB read buffers, one per in-flight connection.
struct Buffers;

impl ManageConnection for Buffers {
    type Connection = Vec<u8>;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<Vec<u8>> {
        Ok(Vec::with_capacity(16 * 1024))
    }

    async fn is_valid(&self, _buf: &mut Vec<u8>) -> io::Result<()> {
        Ok(())
    }

    fn has_broken(&self, _buf: &mut Vec<u8>) -> bool {
        false
    }
}

fn main() -> io::Result<()> {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7000".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let capacity: u32 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024);

    let pool = Pool::builder().max_size_per_thread(capacity).build(Buffers);

    // One runtime, one ring. Everything below runs on this single thread.
    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        // This runtime's pool. `!Send`, so it and its buffers stay on this thread.
        let local = pool.local();

        let listener = TcpListener::bind(addr).await?;
        println!("task_echo on {addr}: 1 runtime, capacity {capacity}");

        // Accept forever; hand each connection to its own task on this same ring.
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                continue;
            };
            let _ = stream.set_nodelay(true);
            compio::runtime::spawn(serve(local.clone(), stream)).detach();
        }
    })
}

/// Borrow a buffer from this runtime's pool for the life of the connection and
/// echo until the peer closes. Dropping the lease returns the buffer.
async fn serve(local: LocalPool<Buffers>, mut stream: TcpStream) {
    let mut lease = match local.get().await {
        Ok(lease) => lease,
        Err(_) => return,
    };
    loop {
        let mut buf = std::mem::take(&mut *lease);
        buf.clear();
        let BufResult(read, buf) = stream.read(buf).await;
        match read {
            Ok(0) | Err(_) => {
                *lease = buf;
                return;
            }
            Ok(_) => {}
        }
        let BufResult(written, buf) = stream.write_all(buf).await;
        *lease = buf;
        if written.is_err() {
            return;
        }
    }
}
