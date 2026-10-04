//! Scatter-gather benchmark: each client request is forwarded to N upstream
//! echo servers, their responses collected, then the payload echoed back.
//!
//! Each accepted connection leases a pooled set of upstream `TcpStream`s —
//! N round-trips of backend I/O per client request, the shape of a service
//! that aggregates several backends. From `load.rs`'s perspective this is an
//! echo server, so no separate load generator is needed.
//!
//! ```text
//! # start echo backends first:
//! cargo run --release --example echo -- 127.0.0.1:7001 1024 1
//! cargo run --release --example echo -- 127.0.0.1:7002 1024 1
//! cargo run --release --example echo -- 127.0.0.1:7003 1024 1
//!
//! UPSTREAM_ADDRS=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 \
//! cargo run --release --example fanout -- [ADDR] [CAPACITY] [WORKERS] [FLAGS]
//! ```
//!
//! Drive it with `examples/load.rs` pointed at the fanout's ADDR.

use std::{io, net::SocketAddr, sync::LazyLock, time::Duration};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
};
use compio_pool::{Connection, Resource, Server, Service, UringConfig, WorkerContext, Workers};

static UPSTREAM_ADDRS: LazyLock<Vec<SocketAddr>> = LazyLock::new(|| {
    std::env::var("UPSTREAM_ADDRS")
        .unwrap_or_else(|_| "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003".into())
        .split(',')
        .map(|a| a.trim().parse().expect("bad address in UPSTREAM_ADDRS"))
        .collect()
});

struct Upstreams {
    streams: Vec<compio::net::TcpStream>,
    buf: Vec<u8>,
    resp: Vec<u8>,
}

impl Resource for Upstreams {
    async fn create(_cx: &WorkerContext) -> io::Result<Self> {
        let mut streams = Vec::with_capacity(UPSTREAM_ADDRS.len());
        for addr in UPSTREAM_ADDRS.iter() {
            let s = compio::net::TcpStream::connect(*addr).await?;
            s.set_nodelay(true)?;
            streams.push(s);
        }
        Ok(Upstreams {
            streams,
            buf: Vec::with_capacity(16 * 1024),
            resp: Vec::with_capacity(16 * 1024),
        })
    }

    fn recycle(&mut self) -> bool {
        true
    }
}

#[derive(Clone)]
struct Fanout;

impl Service for Fanout {
    type Resource = Upstreams;

    async fn handle(&self, mut conn: Connection, up: &mut Upstreams) -> io::Result<()> {
        loop {
            // Read one request from the client.
            let mut b = std::mem::take(&mut up.buf);
            b.clear();
            let BufResult(n, mut b) = conn.stream.read(b).await;
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

            // Scatter: the payload buffer passes through each upstream write
            // and comes back unchanged, so no clone per backend.
            for i in 0..up.streams.len() {
                let BufResult(w, ret) = up.streams[i].write_all(b).await;
                b = ret;
                if let Err(e) = w {
                    up.buf = b;
                    return Err(e);
                }
            }

            // Gather: drain one echo from each upstream into a scratch buffer.
            let mut r = std::mem::take(&mut up.resp);
            for i in 0..up.streams.len() {
                r.clear();
                let BufResult(n, ret) = up.streams[i].read(r).await;
                r = ret;
                match n {
                    Ok(0) => {
                        up.resp = r;
                        up.buf = b;
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "upstream closed",
                        ));
                    }
                    Ok(_) => {}
                    Err(e) => {
                        up.resp = r;
                        up.buf = b;
                        return Err(e);
                    }
                }
            }
            up.resp = r;

            // Echo the original payload back to the client.
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

    let server = Server::builder(Fanout)
        .bind(addr)
        .workers(workers)
        .capacity(capacity)
        .handoff_capacity(4096)
        .incoming_cpu(incoming_cpu)
        .uring(uring.clone())
        .start()?;

    println!(
        "fanout on {} -> {} upstreams — {} workers, capacity {} each",
        server.local_addr(),
        UPSTREAM_ADDRS.len(),
        server.workers(),
        capacity
    );
    for a in UPSTREAM_ADDRS.iter() {
        println!("  upstream {a}");
    }
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
