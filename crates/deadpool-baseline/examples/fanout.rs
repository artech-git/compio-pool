//! The tokio + deadpool counterpart to compio-pool's `examples/fanout.rs`.
//!
//! Same job, same pooling shape: every client request is forwarded to N
//! upstream echo servers (scatter), their responses drained (gather), then
//! the payload echoed back. Each inbound connection leases a pooled set of N
//! upstream connections for its lifetime. Writes and reads walk the upstreams
//! in the same order as the compio example, so the per-request backend I/O is
//! identical and only the runtime + pool differ.
//!
//! Two modes:
//!   percore (default) — one pinned `current_thread` runtime per core, each
//!                       with its own SO_REUSEPORT listener and its own pool.
//!   default           — one multi-threaded work-stealing runtime, one shared
//!                       listener, one shared pool.
//!
//! ```text
//! UPSTREAM_ADDRS=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 \
//! cargo run --release -p deadpool-baseline --example fanout -- [ADDR] [CAPACITY] [WORKERS] [MODE]
//! ```
//! Drive it with compio-pool's `examples/load.rs` pointed at the fanout.

use std::{io, net::SocketAddr};

use deadpool::managed::{Manager, Metrics, Pool, RecycleResult};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::LocalSet,
};

fn upstream_addrs() -> Vec<SocketAddr> {
    std::env::var("UPSTREAM_ADDRS")
        .unwrap_or_else(|_| "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003".into())
        .split(',')
        .map(|a| a.trim().parse().expect("bad address in UPSTREAM_ADDRS"))
        .collect()
}

/// The pooled resource: N upstream connections plus request and response
/// buffers, the direct analogue of the compio example's `Upstreams`.
struct Upstreams {
    streams: Vec<TcpStream>,
    buf: Vec<u8>,
    resp: Vec<u8>,
}

struct UpstreamsManager {
    addrs: Vec<SocketAddr>,
}

impl Manager for UpstreamsManager {
    type Type = Upstreams;
    type Error = io::Error;

    async fn create(&self) -> Result<Upstreams, io::Error> {
        let mut streams = Vec::with_capacity(self.addrs.len());
        for addr in &self.addrs {
            let s = TcpStream::connect(*addr).await?;
            s.set_nodelay(true)?;
            streams.push(s);
        }
        Ok(Upstreams {
            streams,
            buf: vec![0u8; 16 * 1024],
            resp: vec![0u8; 16 * 1024],
        })
    }

    async fn recycle(&self, up: &mut Upstreams, _: &Metrics) -> RecycleResult<io::Error> {
        if up.buf.len() != 16 * 1024 {
            up.buf.resize(16 * 1024, 0);
        }
        if up.resp.len() != 16 * 1024 {
            up.resp.resize(16 * 1024, 0);
        }
        Ok(())
    }
}

type FanPool = Pool<UpstreamsManager>;

fn reuseport_listener(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    let s = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    s.set_reuse_address(true)?;
    s.set_reuse_port(true)?;
    s.set_nonblocking(true)?;
    s.bind(&addr.into())?;
    s.listen(1024)?;
    Ok(s.into())
}

/// Serve one inbound connection: lease the upstream set for its lifetime,
/// then loop read → scatter → gather → echo.
async fn handle(mut stream: TcpStream, pool: FanPool) {
    let _ = stream.set_nodelay(true);
    let mut up = match pool.get().await {
        Ok(u) => u,
        Err(_) => return,
    };
    // One deref so the borrow checker can split `streams` from the buffers.
    let up: &mut Upstreams = &mut up;

    loop {
        let n = match stream.read(&mut up.buf[..]).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };

        for s in up.streams.iter_mut() {
            if s.write_all(&up.buf[..n]).await.is_err() {
                return;
            }
        }
        for s in up.streams.iter_mut() {
            match s.read(&mut up.resp[..]).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }

        if stream.write_all(&up.buf[..n]).await.is_err() {
            return;
        }
    }
}

fn run_percore(addr: SocketAddr, cap: usize, workers: usize) -> io::Result<()> {
    let cores = core_ids();
    println!(
        "tokio+deadpool fanout on {addr} -> {} upstreams — {workers} workers (percore: epoll, SO_REUSEPORT, per-worker pool cap {cap})",
        upstream_addrs().len()
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
                    let pool: FanPool = Pool::builder(UpstreamsManager {
                        addrs: upstream_addrs(),
                    })
                    .max_size(cap.max(1))
                    .build()
                    .map_err(|e| io::Error::other(format!("pool build: {e}")))?;
                    if let Err(e) = pool.get().await {
                        eprintln!("worker {i}: cannot reach upstreams: {e}");
                        std::process::exit(1);
                    }
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
        "tokio+deadpool fanout on {addr} -> {} upstreams — {workers} worker threads (default: shared listener + shared pool cap {})",
        upstream_addrs().len(),
        cap.saturating_mul(workers).max(1)
    );
    rt.block_on(async move {
        let pool: FanPool = Pool::builder(UpstreamsManager {
            addrs: upstream_addrs(),
        })
        .max_size(cap.saturating_mul(workers).max(1))
        .build()
        .map_err(|e| io::Error::other(format!("pool build: {e}")))?;
        if let Err(e) = pool.get().await {
            eprintln!("cannot reach upstreams: {e}");
            std::process::exit(1);
        }
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
