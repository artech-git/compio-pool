//! The tokio + deadpool counterpart to compio-pool's `examples/churn.rs`.
//!
//! Accept, read one byte, echo it, close — the minimal per-connection
//! workload that isolates the accept path and the per-connection pool lease.
//! The compio server leases its one-byte buffer from a thread-local pool;
//! here every connection takes the deadpool mutex once, which is exactly the
//! shared-state cost the comparison exists to show.
//!
//! Two modes:
//!   percore (default) — one pinned `current_thread` runtime per core, each
//!                       with its own SO_REUSEPORT listener and its own pool.
//!   default           — one multi-threaded work-stealing runtime, one shared
//!                       listener, one shared pool.
//!
//! ```text
//! cargo run --release -p deadpool-baseline --example churn -- [ADDR] [CAPACITY] [WORKERS] [MODE]
//! ```
//! Drive it with compio-pool's `examples/load_churn.rs`.

use std::{io, net::SocketAddr};

use deadpool::managed::{Manager, Metrics, Pool, RecycleResult};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::LocalSet,
};

/// The pooled resource: one byte of buffer, as in the compio example.
struct TinyBufManager;

impl Manager for TinyBufManager {
    type Type = Vec<u8>;
    type Error = io::Error;

    async fn create(&self) -> Result<Vec<u8>, io::Error> {
        Ok(vec![0u8; 1])
    }

    async fn recycle(&self, buf: &mut Vec<u8>, _: &Metrics) -> RecycleResult<io::Error> {
        if buf.len() != 1 {
            buf.resize(1, 0);
        }
        Ok(())
    }
}

type TinyPool = Pool<TinyBufManager>;

fn reuseport_listener(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    let s = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    s.set_reuse_address(true)?;
    s.set_reuse_port(true)?;
    s.set_nonblocking(true)?;
    s.bind(&addr.into())?;
    s.listen(1024)?;
    Ok(s.into())
}

async fn handle(mut stream: TcpStream, pool: TinyPool) {
    let mut buf = match pool.get().await {
        Ok(b) => b,
        Err(_) => return,
    };
    match stream.read(&mut buf[..]).await {
        Ok(0) | Err(_) => return,
        Ok(n) => {
            let _ = stream.write_all(&buf[..n]).await;
        }
    }
}

fn run_percore(addr: SocketAddr, cap: usize, workers: usize) -> io::Result<()> {
    let cores = core_ids();
    println!(
        "tokio+deadpool churn on {addr} — {workers} workers (percore: epoll, SO_REUSEPORT, per-worker pool cap {cap})"
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
                    let pool: TinyPool = Pool::builder(TinyBufManager)
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
        "tokio+deadpool churn on {addr} — {workers} worker threads (default: shared listener + shared pool cap {})",
        cap.saturating_mul(workers).max(1)
    );
    rt.block_on(async move {
        let pool: TinyPool = Pool::builder(TinyBufManager)
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
