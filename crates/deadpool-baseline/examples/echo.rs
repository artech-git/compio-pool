//! The tokio + deadpool counterpart to compio-pool's [`examples/echo.rs`].
//!
//! It is the server you would actually reach for on tokio: one multi-threaded
//! runtime, one `TcpListener`, a task per accepted connection, and a
//! `deadpool` pool standing in for compio-pool's thread-local `Resource` pool
//! — one reusable 16 KiB read buffer per in-flight connection, leased for the
//! life of the connection and recycled (cleared) on return.
//!
//! Where compio-pool shards per core (one ring, one `SO_REUSEPORT` listener and
//! a thread-local pool each, no shared state), this has one accept loop feeding
//! tokio's work-stealing scheduler and one shared, mutex-guarded pool. That is
//! the comparison: the same echo handler and the same load generator against
//! the two architectures.
//!
//! ```text
//! cargo run --release -p deadpool-baseline --example echo -- [ADDR] [CAPACITY] [WORKERS]
//!   ADDR      bind address                           default 0.0.0.0:7000
//!   CAPACITY  pool buffers per worker (total = CAPACITY * WORKERS)
//!                                                     default 1024
//!   WORKERS   tokio worker threads                   default: one per core
//! ```
//!
//! Drive it with compio-pool's `examples/load.rs`; the CLI and the per-second
//! stats line mirror `echo.rs` so the same client measures both unchanged.
//! [`examples/echo.rs`]: ../../../examples/echo.rs

use std::{
    io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicI64, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use deadpool::managed::{Manager, Metrics, Pool, RecycleResult};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

/// The resource each in-flight connection leases: one read buffer, reused for
/// every read on that connection and cleared when it returns to the pool. The
/// direct analogue of the `Buf(Vec<u8>)` resource in compio-pool's `echo.rs`.
struct BufManager;

impl Manager for BufManager {
    type Type = Vec<u8>;
    type Error = io::Error;

    async fn create(&self) -> Result<Vec<u8>, io::Error> {
        Ok(Vec::with_capacity(16 * 1024))
    }

    async fn recycle(&self, buf: &mut Vec<u8>, _: &Metrics) -> RecycleResult<io::Error> {
        buf.clear();
        Ok(())
    }
}

type BufPool = Pool<BufManager>;

/// The counters the stats line prints, mirroring the ones compio-pool's `echo`
/// exposes that have a meaning here. `handed_off`/`claimed`/`bounced` have no
/// analogue — there are no shards to move a connection between — so they are
/// not tracked.
#[derive(Default)]
struct Stats {
    active: AtomicI64,
    accepted: AtomicU64,
    completed: AtomicU64,
    errors: AtomicU64,
}

async fn serve(stats: Arc<Stats>, pool: BufPool, mut stream: tokio::net::TcpStream) {
    stats.active.fetch_add(1, Ordering::Relaxed);
    // Lease a buffer for the whole connection, exactly as the compio handler
    // holds its `Buf` for the life of the connection.
    let mut buf = match pool.get().await {
        Ok(b) => b,
        Err(_) => {
            stats.errors.fetch_add(1, Ordering::Relaxed);
            stats.active.fetch_sub(1, Ordering::Relaxed);
            return;
        }
    };
    buf.resize(16 * 1024, 0);

    let result: io::Result<()> = async {
        loop {
            let n = stream.read(&mut buf).await?;
            if n == 0 {
                return Ok(());
            }
            stream.write_all(&buf[..n]).await?;
        }
    }
    .await;

    if result.is_err() {
        stats.errors.fetch_add(1, Ordering::Relaxed);
    }
    stats.completed.fetch_add(1, Ordering::Relaxed);
    stats.active.fetch_sub(1, Ordering::Relaxed);
    // `buf` drops here, returning to the pool to be recycled.
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr: SocketAddr = args
        .next()
        .unwrap_or_else(|| "0.0.0.0:7000".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let capacity: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .map_err(invalid)?
        .unwrap_or(1024);
    let workers: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .map_err(invalid)?
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        });

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;

    rt.block_on(async move {
        // One shared pool, sized to match compio-pool's total slots
        // (CAPACITY per worker * WORKERS) so neither server blocks on buffers
        // in the no-overflow runs.
        let pool: BufPool = Pool::builder(BufManager)
            .max_size(capacity.saturating_mul(workers).max(1))
            .build()
            .map_err(|e| io::Error::other(format!("pool build: {e}")))?;
        let stats = Arc::new(Stats::default());

        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        println!(
            "tokio+deadpool echo on {local} — {workers} workers, pool {} buffers",
            capacity.saturating_mul(workers).max(1)
        );
        println!();

        // The stats printer, mirroring echo.rs's once-a-second line.
        {
            let stats = stats.clone();
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tick.tick().await;
                    let s = pool.status();
                    println!(
                        "active {:>5}  pool {:>4}/{:<4} (avail {:>4})  accepted {:>8}  done {:>8}  errs {}",
                        stats.active.load(Ordering::Relaxed),
                        s.size,
                        s.max_size,
                        s.available,
                        stats.accepted.load(Ordering::Relaxed),
                        stats.completed.load(Ordering::Relaxed),
                        stats.errors.load(Ordering::Relaxed),
                    );
                }
            });
        }

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("accept: {e}");
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            stats.accepted.fetch_add(1, Ordering::Relaxed);
            tokio::spawn(serve(stats.clone(), pool.clone(), stream));
        }
    })
}

fn invalid(e: std::num::ParseIntError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, e)
}
