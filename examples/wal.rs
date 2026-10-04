//! Durable **file I/O**: a write-ahead-log server. Every request is appended to a
//! log file and `fdatasync`'d before the ack — the socket-read -> file-append ->
//! fdatasync -> socket-write path of a database or a message queue.
//!
//! The append and the `sync_data` are both `io_uring` submissions from the pinned
//! worker, so the whole durable-commit path stays on one ring. This is also where
//! the pool's `!Send` guarantee matters most: each pooled resource owns its own
//! open log segment (an fd bound to this thread's ring), so appends from different
//! connections never contend and a segment can never be touched from another
//! thread.
//!
//! Protocol: persistent connection, one payload per request; the server appends
//! it and replies `OK\n`. One socket read is treated as one record — fine for the
//! loopback benchmark driver.
//!
//! ```text
//! WAL_DIR=/tmp/bench-wal WAL_SYNC=1 \
//! cargo run --release --example wal -- [ADDR] [CAPACITY]
//!   ADDR       bind address               default 0.0.0.0:7000
//!   CAPACITY   log segments per worker    default 1024
//!   WAL_SYNC=0 skips the fdatasync, isolating raw append throughput
//! ```
//!
//! Drive it with `examples/load_wal.rs`.

use std::{
    io,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    thread,
};

use compio::{
    BufResult,
    fs::{File, OpenOptions},
    io::{AsyncRead, AsyncWriteAtExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};
use compio_pool::{LocalPool, ManageConnection, Pool, bind_reuseport, cpu};

/// Where log segments are written.
static WAL_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    PathBuf::from(std::env::var("WAL_DIR").unwrap_or_else(|_| "/tmp/bench-wal".into()))
});

/// Whether to `fdatasync` after every append. On by default; the whole point of
/// a WAL is that the ack means "on disk".
static SYNC: LazyLock<bool> =
    LazyLock::new(|| std::env::var("WAL_SYNC").map(|v| v != "0").unwrap_or(true));

/// Names segment files uniquely across every worker thread.
static NEXT_SEGMENT: AtomicU64 = AtomicU64::new(0);

/// One append-only log segment and the offset of its tail.
struct Wal {
    file: File,
    offset: u64,
}

/// Opens a fresh log segment per pooled slot. Writing at an explicit offset
/// (rather than relying on `O_APPEND`) keeps the append a single positional
/// submission.
struct WalManager;

impl ManageConnection for WalManager {
    type Connection = Wal;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<Wal> {
        let id = NEXT_SEGMENT.fetch_add(1, Ordering::Relaxed);
        let path = WAL_DIR.join(format!("seg-{id}.log"));
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        Ok(Wal { file, offset: 0 })
    }

    async fn is_valid(&self, _wal: &mut Wal) -> io::Result<()> {
        Ok(())
    }

    fn has_broken(&self, _wal: &mut Wal) -> bool {
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

    std::fs::create_dir_all(&*WAL_DIR)?;
    let pool = Pool::builder().max_size(capacity).build(WalManager);

    let cores = cpu::cores();
    if cores.is_empty() {
        return Err(io::Error::new(io::ErrorKind::Unsupported, "no cores to run on"));
    }
    println!(
        "wal on {addr} -> {} (fdatasync: {}): {} workers, capacity {capacity}/worker",
        WAL_DIR.display(),
        if *SYNC { "on" } else { "off" },
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

fn worker(core: cpu::CoreId, addr: SocketAddr, pool: Pool<WalManager>) {
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
            compio::runtime::spawn(serve(local.clone(), stream)).detach();
        }
    });
}

/// Lease one log segment for the life of the connection; append and sync every
/// record before acking it.
async fn serve(local: LocalPool<WalManager>, mut stream: TcpStream) {
    let mut lease = match local.get().await {
        Ok(lease) => lease,
        Err(_) => return,
    };
    let mut buf = Vec::with_capacity(64 * 1024);
    loop {
        buf.clear();
        let BufResult(read, b) = stream.read(buf).await;
        buf = b;
        let n = match read {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };

        // Append the record at the segment tail. Read `offset` into a local so the
        // expression does not borrow `lease` twice.
        let offset = lease.offset;
        let BufResult(w, b) = lease.file.write_all_at(buf, offset).await;
        buf = b;
        if w.is_err() {
            return;
        }
        lease.offset += n as u64;

        if *SYNC && lease.file.sync_data().await.is_err() {
            return;
        }

        let BufResult(w, _) = stream.write_all(b"OK\n".to_vec()).await;
        if w.is_err() {
            return;
        }
    }
}
