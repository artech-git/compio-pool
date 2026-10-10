//! An **AF_UNIX** (Unix domain socket) echo server — the local-IPC cousin of the
//! TCP `echo.rs`.
//!
//! A filesystem socket path can only be bound once, so there is no `SO_REUSEPORT`
//! fan-out here: this example runs a single pinned worker with one
//! [`UnixListener`], and still borrows a per-connection buffer from the crate's
//! [`Pool`] exactly as the thread-per-core examples do. It is the example to copy
//! when the thing you are pooling talks to a backend over a Unix socket — a local
//! Postgres, Redis or a sidecar — rather than TCP.
//!
//! ```text
//! cargo run --release --example unix_echo -- [PATH] [CAPACITY]
//!   PATH       socket path                 default /tmp/compio-pool-echo.sock
//!   CAPACITY   buffers (connections)       default 1024
//!
//! # talk to it:
//! echo -n hello | socat - UNIX-CONNECT:/tmp/compio-pool-echo.sock
//! # ...or interactively:  nc -U /tmp/compio-pool-echo.sock
//! ```

use std::io;

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    runtime::Runtime,
};
use compio_pool::{LocalPool, ManageConnection, Pool, cpu};

/// Hands out reusable 16 KiB read buffers, one per connection.
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
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/compio-pool-echo.sock".into());
    let capacity: u32 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024);

    // A stale socket file from a previous run would make `bind` fail with
    // EADDRINUSE, so clear it first.
    let _ = std::fs::remove_file(&path);

    let pool = Pool::builder().max_size_per_thread(capacity).build(Buffers);

    // One listener, so one worker. Pin it to the first permitted core so the ring
    // and the socket share a CPU, the same discipline the fan-out examples apply
    // per thread.
    if let Some(core) = cpu::cores().into_iter().next() {
        cpu::pin_current_core(core);
    }
    println!("unix_echo on {path}: 1 worker, capacity {capacity}");

    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        let local = pool.local();
        let listener = UnixListener::bind(&path).await?;

        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                continue;
            };
            compio::runtime::spawn(serve(local.clone(), stream)).detach();
        }
    })
}

/// Borrow a buffer for the life of the connection and echo until the peer closes.
async fn serve(local: LocalPool<Buffers>, mut stream: UnixStream) {
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
