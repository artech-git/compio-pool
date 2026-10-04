//! Connection-churn benchmark: accept, read one byte, echo it, close.
//!
//! The minimal per-connection workload isolates the accept path. io_uring's
//! multishot accept vs epoll + accept4, fd allocation, ring overhead for the
//! smallest possible I/O — all of it shows up here.
//!
//! ```text
//! cargo run --release --example churn -- [ADDR] [CAPACITY] [WORKERS] [FLAGS]
//! ```
//!
//! Drive it with `examples/load_churn.rs`.

use std::{io, net::SocketAddr, time::Duration};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
};
use compio_pool::{Connection, Resource, Server, Service, UringConfig, WorkerContext, Workers};

struct SmallBuf(Vec<u8>);

impl Resource for SmallBuf {
    async fn create(_cx: &WorkerContext) -> io::Result<Self> {
        Ok(SmallBuf(Vec::with_capacity(1)))
    }
}

#[derive(Clone)]
struct Churn;

impl Service for Churn {
    type Resource = SmallBuf;

    async fn handle(&self, mut conn: Connection, buf: &mut SmallBuf) -> io::Result<()> {
        let mut b = std::mem::take(&mut buf.0);
        b.clear();
        let BufResult(n, b) = conn.stream.read(b).await;
        match n {
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
        let BufResult(w, b) = conn.stream.write_all(b).await;
        buf.0 = b;
        w?;
        Ok(())
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

    let server = Server::builder(Churn)
        .bind(addr)
        .workers(workers)
        .capacity(capacity)
        .handoff_capacity(4096)
        .incoming_cpu(incoming_cpu)
        .uring(uring.clone())
        .start()?;

    println!(
        "churn on {} — {} workers, capacity {} each",
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
        println!("SO_INCOMING_CPU is on");
    }
    if uring != UringConfig::default() {
        println!("io_uring setup: {uring:?}");
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
