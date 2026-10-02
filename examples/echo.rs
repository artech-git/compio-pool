//! The reference server: one pinned worker per core, each with its own ring,
//! its own `SO_REUSEPORT` listener and a thread-local pool of buffers. Echoes
//! whatever it is sent, and prints the counters once a second so the handoff
//! path can be watched under load.
//!
//! ```text
//! cargo run --release --example echo -- [ADDR] [CAPACITY] [WORKERS] [FLAGS]
//!   ADDR              bind address        default 0.0.0.0:7000
//!   CAPACITY          connections/worker  default 1024
//!   WORKERS           worker count        default: one per core
//!   --incoming-cpu    set SO_INCOMING_CPU on each listener. Only after
//!                     scripts/tune-nic.sh has steered the NIC queues; on an
//!                     untuned host it sends every connection to one worker.
//!   --defer-taskrun   IORING_SETUP_DEFER_TASKRUN on each ring (Linux 6.1+):
//!                     completions are processed only when the worker asks.
//!   --no-coop-taskrun turn off IORING_SETUP_COOP_TASKRUN, which is on by
//!                     default, to see what it buys.
//! ```
//!
//! Drive it with `examples/load.rs`. A small CAPACITY (say 2) with many
//! concurrent clients is what makes `handed_off`, `claimed` and `bounced` move.

use std::{io, net::SocketAddr, time::Duration};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
};
use compio_pool::{Connection, Resource, Server, Service, UringConfig, WorkerContext, Workers};

/// The resource each in-flight connection leases: one read buffer, allocated
/// on the core that will use it and reused for every connection it serves.
struct Buf(Vec<u8>);

impl Resource for Buf {
    async fn create(_cx: &WorkerContext) -> io::Result<Self> {
        Ok(Buf(Vec::with_capacity(16 * 1024)))
    }
}

#[derive(Clone)]
struct Echo;

impl Service for Echo {
    type Resource = Buf;

    async fn handle(&self, mut conn: Connection, buf: &mut Buf) -> io::Result<()> {
        loop {
            // compio reads into the spare capacity after `len`, so the buffer
            // is cleared before every read and comes back with `len == n`.
            let mut b = std::mem::take(&mut buf.0);
            b.clear();
            let BufResult(read, b) = conn.stream.read(b).await;
            match read {
                Ok(0) => {
                    buf.0 = b;
                    return Ok(());
                }
                Ok(_) => {}
                Err(e) => {
                    buf.0 = b;
                    return Err(e);
                }
            }
            let BufResult(written, b) = conn.stream.write_all(b).await;
            buf.0 = b;
            written?;
        }
    }
}

fn main() -> io::Result<()> {
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

    let server = Server::builder(Echo)
        .bind(addr)
        .workers(workers)
        .capacity(capacity)
        .handoff_capacity(4096)
        .incoming_cpu(incoming_cpu)
        .uring(uring.clone())
        .start()?;

    println!(
        "echo on {} — {} workers, capacity {} each",
        server.local_addr(),
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
    if incoming_cpu {
        println!("SO_INCOMING_CPU is on: each listener prefers flows that arrive on its core");
    }
    if uring != UringConfig::default() {
        println!("io_uring setup: {uring:?}");
    }
    println!("tune the NIC for this layout with:");
    println!(
        "  sudo scripts/tune-nic.sh --cpus {}",
        server
            .cores()
            .iter()
            .map(|c| c.id.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
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
