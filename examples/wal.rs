//! Write-ahead-log benchmark: every request is appended to a log file and
//! fsynced before the ack. Socket read → file append → fdatasync → socket
//! write: the durability path of a database or message queue.
//!
//! On compio the append and the fdatasync are io_uring submissions from the
//! pinned worker thread. The tokio baseline must push both through its
//! blocking thread pool — two thread handoffs per request — which is the
//! cost this example exists to measure.
//!
//! Protocol, persistent connections: client sends one payload per request,
//! server appends it and replies `OK\n`. Each pooled resource owns its own
//! log file (a per-connection segment), so appends never contend.
//!
//! ```text
//! WAL_DIR=/tmp/bench-wal WAL_SYNC=1 \
//! cargo run --release --example wal -- [ADDR] [CAPACITY] [WORKERS] [FLAGS]
//! ```
//! `WAL_SYNC=0` skips the fdatasync, isolating pure append throughput.
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
    time::Duration,
};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteAtExt, AsyncWriteExt},
};
use compio_pool::{Connection, Resource, Server, Service, UringConfig, WorkerContext, Workers};

static WAL_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    PathBuf::from(std::env::var("WAL_DIR").unwrap_or_else(|_| "/tmp/bench-wal".into()))
});
static SYNC: LazyLock<bool> =
    LazyLock::new(|| std::env::var("WAL_SYNC").map(|v| v != "0").unwrap_or(true));
static NEXT_SEGMENT: AtomicU64 = AtomicU64::new(0);

struct Wal {
    file: compio::fs::File,
    offset: u64,
    buf: Vec<u8>,
}

impl Resource for Wal {
    async fn create(cx: &WorkerContext) -> io::Result<Self> {
        let id = NEXT_SEGMENT.fetch_add(1, Ordering::Relaxed);
        let path = WAL_DIR.join(format!("w{}-{id}.wal", cx.index));
        let file = compio::fs::File::create(&path).await?;
        Ok(Wal {
            file,
            offset: 0,
            buf: Vec::with_capacity(16 * 1024),
        })
    }

    fn recycle(&mut self) -> bool {
        if self.buf.capacity() == 0 {
            self.buf = Vec::with_capacity(16 * 1024);
        }
        true
    }
}

#[derive(Clone)]
struct WalService;

impl Service for WalService {
    type Resource = Wal;

    async fn handle(&self, mut conn: Connection, wal: &mut Wal) -> io::Result<()> {
        loop {
            // One payload from the client.
            let mut b = std::mem::take(&mut wal.buf);
            b.clear();
            let BufResult(n, b) = conn.stream.read(b).await;
            match n {
                Ok(0) => {
                    wal.buf = b;
                    return Ok(());
                }
                Ok(_) => {}
                Err(e) => {
                    wal.buf = b;
                    return Err(e);
                }
            }
            let len = b.len() as u64;

            // Append at the current offset, then make it durable.
            let BufResult(w, b) = wal.file.write_all_at(b, wal.offset).await;
            wal.buf = b;
            w?;
            wal.offset += len;
            if *SYNC {
                wal.file.sync_data().await?;
            }

            let BufResult(w, _) = conn.stream.write_all(b"OK\n".to_vec()).await;
            w?;
        }
    }
}

fn main() -> io::Result<()> {
    std::fs::create_dir_all(&*WAL_DIR)?;

    let (flags, positional): (Vec<String>, Vec<String>) =
        std::env::args().skip(1).partition(|a| a.starts_with("--"));
    const KNOWN: [&str; 3] = ["--incoming-cpu", "--defer-taskrun", "--no-coop-taskrun"];
    if let Some(unknown) = flags.iter().find(|f| !KNOWN.contains(&f.as_str())) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown flag {unknown}"),
        ));
    }
    let has = |name: &str| flags.iter().any(|f| f == name);
    let incoming_cpu = has("--incoming-cpu");
    let uring = UringConfig {
        defer_taskrun: has("--defer-taskrun"),
        coop_taskrun: !has("--no-coop-taskrun"),
        ..UringConfig::default()
    };
    let mut args = positional.into_iter();
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
    let workers = match args
        .next()
        .map(|s| s.parse::<usize>())
        .transpose()
        .map_err(invalid)?
    {
        Some(n) => Workers::Count(n),
        None => Workers::AllCores,
    };

    let server = Server::builder(WalService)
        .bind(addr)
        .workers(workers)
        .capacity(capacity)
        .handoff_capacity(4096)
        .incoming_cpu(incoming_cpu)
        .uring(uring.clone())
        .start()?;

    println!(
        "wal on {} writing {} (sync {}) — {} workers, capacity {} each",
        server.local_addr(),
        WAL_DIR.display(),
        if *SYNC { "every record" } else { "off" },
        server.workers(),
        capacity
    );
    for (i, core) in server.cores().iter().enumerate() {
        println!(
            "  worker {i:>2} -> cpu {:>3}  smp_affinity {}",
            core.id,
            compio_pool::cpu::affinity_mask(core.id)
        );
    }
    println!();

    loop {
        std::thread::sleep(Duration::from_secs(1));
        let s = server.stats();
        let t = s.totals;
        println!(
            "active {:>5}  queued {:>4}/{:<4}  accepted {:>8}  local {:>8}  handed_off {:>6}  claimed {:>6}  bounced {:>4}  oversub {:>4}  rejected {:>4}  done {:>8}  errs {}",
            t.active,
            s.queued,
            s.handoff_capacity,
            t.accepted,
            t.served_local,
            t.handed_off,
            t.claimed,
            t.bounced,
            t.oversubscribed,
            t.rejected,
            t.completed,
            t.handler_errors + t.resource_errors + t.accept_errors
        );
    }
}

fn invalid(e: std::num::ParseIntError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, e)
}
