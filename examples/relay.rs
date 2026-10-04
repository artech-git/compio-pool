//! TCP relay benchmark: transparent proxy to one upstream echo server.
//!
//! Each accepted connection leases a pooled upstream `TcpStream`. The handler
//! runs a half-duplex loop: read from client → write to upstream → read from
//! upstream → write to client. From `load.rs`'s perspective this is an echo
//! server, so no separate load generator is needed — just point `load` at the
//! relay and start an echo server as the upstream.
//!
//! ```text
//! # start an echo backend first:
//! cargo run --release --example echo -- 127.0.0.1:7001 1024 2
//!
//! UPSTREAM_ADDR=127.0.0.1:7001 \
//! cargo run --release --example relay -- [ADDR] [CAPACITY] [WORKERS] [FLAGS]
//! ```
//!
//! Drive it with `examples/load.rs` pointed at the relay's ADDR.

use std::{io, net::SocketAddr, sync::LazyLock, time::Duration};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
};
use compio_pool::{Connection, Resource, Server, Service, UringConfig, WorkerContext, Workers};

static UPSTREAM_ADDR: LazyLock<SocketAddr> = LazyLock::new(|| {
    std::env::var("UPSTREAM_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:7001".into())
        .parse()
        .expect("bad UPSTREAM_ADDR")
});

struct Upstream {
    stream: compio::net::TcpStream,
    buf: Vec<u8>,
}

impl Resource for Upstream {
    async fn create(_cx: &WorkerContext) -> io::Result<Self> {
        let stream = compio::net::TcpStream::connect(*UPSTREAM_ADDR).await?;
        stream.set_nodelay(true)?;
        Ok(Upstream {
            stream,
            buf: Vec::with_capacity(16 * 1024),
        })
    }

    fn recycle(&mut self) -> bool {
        true
    }
}

#[derive(Clone)]
struct Relay;

impl Service for Relay {
    type Resource = Upstream;

    async fn handle(&self, mut conn: Connection, up: &mut Upstream) -> io::Result<()> {
        loop {
            let mut b = std::mem::take(&mut up.buf);
            b.clear();
            let BufResult(n, b) = conn.stream.read(b).await;
            match n {
                Ok(0) => {
                    up.buf = b;
                    return Ok(());
                }
                Ok(_) => {}
                Err(e) => {
                    up.buf = b;
                    return Err(e);
                }
            }

            let BufResult(w, mut b) = up.stream.write_all(b).await;
            if let Err(e) = w {
                up.buf = b;
                return Err(e);
            }

            b.clear();
            let BufResult(n, b) = up.stream.read(b).await;
            match n {
                Ok(0) => {
                    up.buf = b;
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "upstream closed",
                    ));
                }
                Ok(_) => {}
                Err(e) => {
                    up.buf = b;
                    return Err(e);
                }
            }

            let BufResult(w, b) = conn.stream.write_all(b).await;
            up.buf = b;
            w?;
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

    let server = Server::builder(Relay)
        .bind(addr)
        .workers(workers)
        .capacity(capacity)
        .handoff_capacity(4096)
        .incoming_cpu(incoming_cpu)
        .uring(uring.clone())
        .start()?;

    println!(
        "relay on {} -> {} — {} workers, capacity {} each",
        server.local_addr(),
        *UPSTREAM_ADDR,
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
