//! The tokio + deadpool counterpart to compio-pool's `examples/wal.rs`.
//!
//! Same job, same pooling shape: every request is appended to a log file and
//! fdatasynced before the `OK\n` ack; each inbound connection leases a pooled
//! log segment (file + offset + buffer) for its lifetime. The difference is
//! the backend: every `tokio::fs` write and sync_data is a handoff to the
//! blocking thread pool, where compio submits both on the worker's io_uring.
//!
//! Two modes:
//!   percore (default) — one pinned `current_thread` runtime per core, each
//!                       with its own SO_REUSEPORT listener and its own pool.
//!   default           — one multi-threaded work-stealing runtime, one shared
//!                       listener, one shared pool.
//!
//! ```text
//! WAL_DIR=/tmp/bench-wal WAL_SYNC=1 \
//! cargo run --release -p deadpool-baseline --example wal -- [ADDR] [CAPACITY] [WORKERS] [MODE]
//! ```
//! `WAL_SYNC=0` skips the fdatasync. Drive it with compio-pool's
//! `examples/load_wal.rs`.

use std::{
    io,
    net::SocketAddr,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use deadpool::managed::{Manager, Metrics, Pool, RecycleResult};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::LocalSet,
};

fn wal_dir() -> PathBuf {
    PathBuf::from(std::env::var("WAL_DIR").unwrap_or_else(|_| "/tmp/bench-wal".into()))
}

fn sync_enabled() -> bool {
    std::env::var("WAL_SYNC").map(|v| v != "0").unwrap_or(true)
}

static NEXT_SEGMENT: AtomicU64 = AtomicU64::new(0);

/// One pooled log segment, the analogue of the compio example's `Wal`
/// resource. Appends are sequential writes, so the file cursor is the offset.
struct Segment {
    file: tokio::fs::File,
    buf: Vec<u8>,
}

struct SegmentManager {
    dir: PathBuf,
    label: &'static str,
}

impl Manager for SegmentManager {
    type Type = Segment;
    type Error = io::Error;

    async fn create(&self) -> Result<Segment, io::Error> {
        let id = NEXT_SEGMENT.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{}-{id}.wal", self.label));
        let file = tokio::fs::File::create(&path).await?;
        Ok(Segment {
            file,
            buf: vec![0u8; 16 * 1024],
        })
    }

    async fn recycle(&self, seg: &mut Segment, _: &Metrics) -> RecycleResult<io::Error> {
        if seg.buf.len() != 16 * 1024 {
            seg.buf.resize(16 * 1024, 0);
        }
        Ok(())
    }
}

type WalPool = Pool<SegmentManager>;

fn reuseport_listener(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    let s = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    s.set_reuse_address(true)?;
    s.set_reuse_port(true)?;
    s.set_nonblocking(true)?;
    s.bind(&addr.into())?;
    s.listen(1024)?;
    Ok(s.into())
}

/// Serve one inbound connection: lease a segment for its lifetime, then loop
/// read payload → append → fdatasync → ack.
async fn handle(mut stream: TcpStream, pool: WalPool, sync: bool) {
    let _ = stream.set_nodelay(true);
    let mut seg = match pool.get().await {
        Ok(s) => s,
        Err(_) => return,
    };
    // One deref so the borrow checker can split `file` from `buf`.
    let seg: &mut Segment = &mut seg;

    loop {
        let n = match stream.read(&mut seg.buf[..]).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if seg.file.write_all(&seg.buf[..n]).await.is_err() {
            return;
        }
        if sync && seg.file.sync_data().await.is_err() {
            return;
        }
        if stream.write_all(b"OK\n").await.is_err() {
            return;
        }
    }
}

fn run_percore(addr: SocketAddr, cap: usize, workers: usize) -> io::Result<()> {
    let cores = core_ids();
    let sync = sync_enabled();
    println!(
        "tokio+deadpool wal on {addr} writing {} (sync {}) — {workers} workers (percore: epoll, SO_REUSEPORT, per-worker pool cap {cap})",
        wal_dir().display(),
        if sync { "every record" } else { "off" },
    );
    let handles: Vec<_> = (0..workers)
        .map(|i| {
            let core = cores.get(i % cores.len().max(1)).copied();
            std::thread::spawn(move || -> io::Result<()> {
                if let Some(c) = core {
                    pin(c);
                    println!("  worker {i:>2} -> cpu {c:>3}");
                }
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let local = LocalSet::new();
                local.block_on(&rt, async move {
                    let pool: WalPool = Pool::builder(SegmentManager {
                        dir: wal_dir(),
                        label: "tokio-pc",
                    })
                    .max_size(cap.max(1))
                    .build()
                    .map_err(|e| io::Error::other(format!("pool build: {e}")))?;
                    let l = TcpListener::from_std(reuseport_listener(addr)?)?;
                    loop {
                        let (stream, _) = l.accept().await?;
                        let pool = pool.clone();
                        tokio::task::spawn_local(handle(stream, pool, sync));
                    }
                })
            })
        })
        .collect();
    for h in handles {
        h.join().expect("worker panicked")?;
    }
    Ok(())
}

fn run_default(addr: SocketAddr, cap: usize, workers: usize) -> io::Result<()> {
    let sync = sync_enabled();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    println!(
        "tokio+deadpool wal on {addr} writing {} (sync {}) — {workers} worker threads (default: shared listener + shared pool cap {})",
        wal_dir().display(),
        if sync { "every record" } else { "off" },
        cap.saturating_mul(workers).max(1)
    );
    rt.block_on(async move {
        let pool: WalPool = Pool::builder(SegmentManager {
            dir: wal_dir(),
            label: "tokio-def",
        })
        .max_size(cap.saturating_mul(workers).max(1))
        .build()
        .map_err(|e| io::Error::other(format!("pool build: {e}")))?;
        let l = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = l.accept().await?;
            let pool = pool.clone();
            tokio::spawn(handle(stream, pool, sync));
        }
    })
}

fn main() -> io::Result<()> {
    std::fs::create_dir_all(wal_dir())?;

    let mut args = std::env::args().skip(1);
    let addr: SocketAddr = args
        .next()
        .unwrap_or_else(|| "0.0.0.0:7000".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let cap: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
        .unwrap_or(1024);
    let workers: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));
    let mode = args.next().unwrap_or_else(|| "percore".into());

    match mode.as_str() {
        "percore" => run_percore(addr, cap, workers),
        "default" => run_default(addr, cap, workers),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("mode must be percore or default, got {other:?}"),
        )),
    }
}

/// CPUs this process may run on, in id order. Copied from `echo_tpc.rs` so
/// every tokio server pins identically.
fn core_ids() -> Vec<usize> {
    // SAFETY: sched_getaffinity fills a zeroed cpu_set_t of the correct size.
    let mask = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        (libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0)
            .then_some(set)
    };
    match mask {
        Some(set) => (0..libc::CPU_SETSIZE as usize)
            // SAFETY: `i` is below CPU_SETSIZE and `set` is initialised.
            .filter(|&i| unsafe { libc::CPU_ISSET(i, &set) })
            .collect(),
        None => (0..std::thread::available_parallelism().map_or(1, |n| n.get())).collect(),
    }
}

fn pin(cpu: usize) {
    // SAFETY: sched_setaffinity on the calling thread with a populated cpu_set_t.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}
