//! The reference server for the bb8-style pool. Unlike the old `Server`, the
//! crate owns none of the runtime here: *this example* spawns one pinned thread
//! per core, builds a `compio` runtime on each, binds its own `SO_REUSEPORT`
//! listener, and borrows a per-connection buffer from the crate's [`Pool`].
//!
//! That is the whole point of the redesign — the pool is a component you pull
//! into your own thread-per-core loop, not a framework that runs the loop for
//! you.
//!
//! ```text
//! cargo run --release --example echo -- [ADDR] [CAPACITY]
//!   ADDR       bind address               default 0.0.0.0:7000
//!   CAPACITY   max connections per worker default 1024
//! ```
//!
//! Drive it with `examples/load.rs`.

use std::{io, net::SocketAddr, thread};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};
use compio_pool::{LocalPool, ManageConnection, Pool, bind_reuseport, cpu};

/// Hands out reusable 16 KiB read buffers, one per in-flight connection. A real
/// manager would open a backend connection (Redis, Postgres, an upstream socket)
/// in `connect` instead.
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
        .unwrap_or_else(|| "0.0.0.0:7000".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let capacity: u32 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024);

    // Build the pool once. `Pool` is `Send + Clone`; every worker gets a clone.
    let pool = Pool::builder().max_size(capacity).build(Buffers);

    let cores = cpu::cores();
    if cores.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no cores to run on",
        ));
    }
    println!(
        "echo on {addr}: {} workers, capacity {capacity}/worker",
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

/// One pinned thread: its own `compio` runtime, its own `SO_REUSEPORT` listener,
/// and its own [`LocalPool`] carved from the shared [`Pool`].
fn worker(core: cpu::CoreId, addr: SocketAddr, pool: Pool<Buffers>) {
    cpu::pin_current(core);

    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        // This thread's pool. `!Send`, so it — and its ring-bound buffers —
        // cannot escape the thread.
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

/// Borrow a buffer from this thread's pool for the life of the connection and
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
