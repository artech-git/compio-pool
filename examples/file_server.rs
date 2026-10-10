//! Disk **file I/O**: a thread-per-core static-file server. Each request names a
//! file; the reply is its size then its bytes, read straight off disk through the
//! ring and written to the socket.
//!
//! This is the example where `io_uring` earns its keep: the open, the positional
//! `read_at`s and the socket writes are all submissions from one pinned thread,
//! never a blocking-threadpool hop. The crate's [`Pool`] supplies the reusable
//! 64 KiB transfer buffer each connection streams through.
//!
//! Protocol, one request per line on a persistent connection:
//!   client:  `<filename>\n`
//!   server:  `<size>\n` then `<size>` raw bytes   — or   `ERR <why>\n`
//!
//! Filenames are resolved inside `FILE_DIR` and may not contain a path separator,
//! so a request cannot escape that directory.
//!
//! ```text
//! FILE_DIR=/tmp/bench-files \
//! cargo run --release --example file_server -- [ADDR] [CAPACITY]
//!   ADDR       bind address               default 0.0.0.0:7000
//!   CAPACITY   buffers (connections)      default 1024
//! ```
//!
//! Drive it with `examples/load_file.rs`, which creates the test files first.

use std::{io, net::SocketAddr, path::PathBuf, sync::LazyLock, thread};

use compio::{
    BufResult,
    fs::File,
    io::{AsyncRead, AsyncReadAt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};
use compio_pool::{LocalPool, ManageConnection, Pool, bind_reuseport, cpu};

/// The directory requested files are resolved inside.
static BASE_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    PathBuf::from(std::env::var("FILE_DIR").unwrap_or_else(|_| "/tmp/bench-files".into()))
});

/// Hands out reusable 64 KiB transfer buffers, one per connection in flight.
struct Buffers;

impl ManageConnection for Buffers {
    type Connection = Vec<u8>;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<Vec<u8>> {
        Ok(Vec::with_capacity(64 * 1024))
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

    let pool = Pool::builder().max_size(capacity).build(Buffers);

    let cores = cpu::cores();
    if cores.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "no cores to run on",
        ));
    }
    println!(
        "file_server on {addr} serving {}: {} workers, capacity {capacity}/worker",
        BASE_DIR.display(),
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

fn worker(core: cpu::CoreId, addr: SocketAddr, pool: Pool<Buffers>) {
    cpu::pin_current(core);

    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        let local = pool.local();

        let std_listener = bind_reuseport(addr, 1024, None).expect("bind SO_REUSEPORT");
        let listener = TcpListener::from_std(std_listener).expect("wrap listener in ring");

        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                continue;
            };
            // The reply is a size header followed by the body — two writes. Without
            // TCP_NODELAY, Nagle holds the body waiting for the header's ACK, which
            // the peer delays ~40 ms, so disable it on every accepted connection.
            let _ = stream.set_nodelay(true);
            compio::runtime::spawn(serve(local.clone(), stream)).detach();
        }
    });
}

/// Serve requests on one connection until it closes. Opening the file is per
/// request; the large transfer buffer is pooled and reused across every file.
async fn serve(local: LocalPool<Buffers>, mut stream: TcpStream) {
    let mut req = Vec::with_capacity(1024);
    loop {
        req.clear();
        let BufResult(read, b) = stream.read(req).await;
        req = b;
        match read {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }

        // Parse the filename and keep it inside BASE_DIR.
        let name = match std::str::from_utf8(&req) {
            Ok(s) => s.trim_end_matches(['\r', '\n']).to_string(),
            Err(_) => {
                if reply_err(&mut stream, "utf8").await.is_err() {
                    return;
                }
                continue;
            }
        };
        if name.is_empty() || name.contains('/') || name.contains("..") {
            if reply_err(&mut stream, "bad name").await.is_err() {
                return;
            }
            continue;
        }

        let path = BASE_DIR.join(&name);
        let file = match File::open(&path).await {
            Ok(f) => f,
            Err(e) => {
                if reply_err(&mut stream, &e.to_string()).await.is_err() {
                    return;
                }
                continue;
            }
        };
        let size = match file.metadata().await {
            Ok(m) => m.len(),
            Err(e) => {
                if reply_err(&mut stream, &e.to_string()).await.is_err() {
                    return;
                }
                continue;
            }
        };

        // Size header, then the body streamed through the pooled buffer.
        let BufResult(w, _) = stream.write_all(format!("{size}\n").into_bytes()).await;
        if w.is_err() {
            return;
        }

        let mut lease = match local.get().await {
            Ok(lease) => lease,
            Err(_) => return,
        };
        let mut data = std::mem::take(&mut *lease);
        let mut offset = 0u64;
        while offset < size {
            data.clear();
            let BufResult(r, d) = file.read_at(data, offset).await;
            data = d;
            let n = match r {
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => {
                    *lease = data;
                    return;
                }
            };
            offset += n as u64;
            let BufResult(w, d) = stream.write_all(data).await;
            data = d;
            if w.is_err() {
                *lease = data;
                return;
            }
        }
        *lease = data; // return the transfer buffer to the pool
    }
}

/// Send a one-line `ERR <why>` response. Returns `Err` if the socket is gone.
async fn reply_err(stream: &mut TcpStream, why: &str) -> io::Result<()> {
    let BufResult(w, _) = stream.write_all(format!("ERR {why}\n").into_bytes()).await;
    w
}
