//! The tokio + deadpool counterpart to compio-pool's `examples/file_server.rs`.
//!
//! Same protocol, same pooling shape: each request names a file, the response
//! is its length and bytes; each inbound connection leases a pooled 64 KiB
//! read buffer for its lifetime. The difference is the backend: every
//! `tokio::fs` open/metadata/read bounces through tokio's blocking thread
//! pool, where compio submits it on the worker's io_uring.
//!
//! Two modes, to separate architecture from backend:
//!   percore (default) — one pinned `current_thread` runtime per core, each
//!                       with its own SO_REUSEPORT listener and its own pool.
//!   default           — one multi-threaded work-stealing runtime, one shared
//!                       listener, one shared pool.
//!
//! ```text
//! FILE_DIR=/tmp/bench-files \
//! cargo run --release -p deadpool-baseline --example file_server -- [ADDR] [CAPACITY] [WORKERS] [MODE]
//! ```
//! Drive it with compio-pool's `examples/load_file.rs`.

use std::{io, net::SocketAddr, path::PathBuf};

use deadpool::managed::{Manager, Metrics, Pool, RecycleResult};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::LocalSet,
};

fn base_dir() -> PathBuf {
    PathBuf::from(std::env::var("FILE_DIR").unwrap_or_else(|_| "/tmp/bench-files".into()))
}

/// The pooled resource: one 64 KiB read buffer per in-flight connection, the
/// direct analogue of the compio example's `Bufs`.
struct BufManager;

impl Manager for BufManager {
    type Type = Vec<u8>;
    type Error = io::Error;

    async fn create(&self) -> Result<Vec<u8>, io::Error> {
        Ok(Vec::with_capacity(64 * 1024))
    }

    async fn recycle(&self, buf: &mut Vec<u8>, _: &Metrics) -> RecycleResult<io::Error> {
        buf.clear();
        Ok(())
    }
}

type BufPool = Pool<BufManager>;

fn reuseport_listener(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    let s = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    s.set_reuse_address(true)?;
    s.set_reuse_port(true)?;
    s.set_nonblocking(true)?;
    s.bind(&addr.into())?;
    s.listen(1024)?;
    Ok(s.into())
}

/// Serve one inbound connection: lease a buffer for its lifetime, then loop
/// request → open → read → respond.
async fn handle(stream: TcpStream, pool: BufPool) {
    let _ = stream.set_nodelay(true);
    let mut buf = match pool.get().await {
        Ok(b) => b,
        Err(_) => return,
    };
    buf.resize(64 * 1024, 0);
    let dir = base_dir();

    let (r, mut w) = stream.into_split();
    let mut br = BufReader::new(r);
    let mut line = String::new();
    loop {
        line.clear();
        match br.read_line(&mut line).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
        let name = line.trim_end_matches(['\r', '\n']);
        let path = dir.join(name);

        let mut file = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(e) => {
                if w.write_all(format!("ERR {e}\n").as_bytes()).await.is_err() {
                    return;
                }
                continue;
            }
        };
        let size = match file.metadata().await {
            Ok(m) => m.len(),
            Err(_) => return,
        };
        if w.write_all(format!("{size}\n").as_bytes()).await.is_err() {
            return;
        }

        let mut sent = 0u64;
        while sent < size {
            let n = match file.read(&mut buf[..]).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(_) => return,
            };
            sent += n as u64;
            if w.write_all(&buf[..n]).await.is_err() {
                return;
            }
        }
    }
}

fn run_percore(addr: SocketAddr, cap: usize, workers: usize) -> io::Result<()> {
    let cores = core_ids();
    println!(
        "tokio+deadpool file_server on {addr} serving {} — {workers} workers (percore: epoll, SO_REUSEPORT, per-worker pool cap {cap})",
        base_dir().display()
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
                    let pool: BufPool = Pool::builder(BufManager)
                        .max_size(cap.max(1))
                        .build()
                        .map_err(|e| io::Error::other(format!("pool build: {e}")))?;
                    let l = TcpListener::from_std(reuseport_listener(addr)?)?;
                    loop {
                        let (stream, _) = l.accept().await?;
                        let pool = pool.clone();
                        tokio::task::spawn_local(handle(stream, pool));
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
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    println!(
        "tokio+deadpool file_server on {addr} serving {} — {workers} worker threads (default: shared listener + shared pool cap {})",
        base_dir().display(),
        cap.saturating_mul(workers).max(1)
    );
    rt.block_on(async move {
        let pool: BufPool = Pool::builder(BufManager)
            .max_size(cap.saturating_mul(workers).max(1))
            .build()
            .map_err(|e| io::Error::other(format!("pool build: {e}")))?;
        let l = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = l.accept().await?;
            let pool = pool.clone();
            tokio::spawn(handle(stream, pool));
        }
    })
}

fn main() -> io::Result<()> {
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
