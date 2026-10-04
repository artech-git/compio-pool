//! Load generator for `examples/churn.rs`. Plain `std`, one blocking thread
//! per "connection slot", like `load.rs` — but every request is a fresh TCP
//! connection: connect → send 1 byte → read 1 byte → close.
//!
//! What it measures is the server's accept path and fd lifecycle: each
//! latency sample covers the 3-way handshake, one round-trip, and the close.
//!
//! ```text
//! cargo run --release --example load_churn -- ADDR [--conns N] [--seconds S]
//! ```
//!
//! Prints the same requests/s and latency lines as `load.rs` (a "request"
//! here is one full connection).

use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

struct Opts {
    addr: SocketAddr,
    conns: usize,
    seconds: u64,
}

fn parse() -> io::Result<Opts> {
    let mut args = std::env::args().skip(1);
    let addr: SocketAddr = args
        .next()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: load_churn ADDR [--conns N] [--seconds S]",
            )
        })?
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let mut opts = Opts {
        addr,
        conns: 64,
        seconds: 5,
    };
    while let Some(flag) = args.next() {
        let value = args.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{flag} needs a value"))
        })?;
        let n: u64 = value
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{flag}: {e}")))?;
        match flag.as_str() {
            "--conns" => opts.conns = n as usize,
            "--seconds" => opts.seconds = n,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown flag {flag}"),
                ));
            }
        }
    }
    Ok(opts)
}

fn main() -> io::Result<()> {
    let opts = Arc::new(parse()?);
    let barrier = Arc::new(Barrier::new(opts.conns));
    let started = Instant::now();

    let workers: Vec<_> = (0..opts.conns)
        .map(|i| {
            let opts = opts.clone();
            let barrier = barrier.clone();
            thread::spawn(move || -> io::Result<Vec<u64>> {
                barrier.wait();
                let byte = [i as u8];
                let mut back = [0u8; 1];
                let mut latencies = Vec::with_capacity(64 * 1024);
                let deadline = started + Duration::from_secs(opts.seconds);
                while Instant::now() < deadline {
                    let t = Instant::now();
                    let mut stream = TcpStream::connect(opts.addr)?;
                    stream.set_nodelay(true)?;
                    stream.write_all(&byte)?;
                    stream.read_exact(&mut back)?;
                    drop(stream);
                    latencies.push(t.elapsed().as_nanos() as u64);
                    if back != byte {
                        return Err(io::Error::other("echo mismatch"));
                    }
                }
                Ok(latencies)
            })
        })
        .collect();

    let mut all = Vec::new();
    let mut failed = 0usize;
    for w in workers {
        match w.join().expect("client thread panicked") {
            Ok(l) => all.extend(l),
            Err(e) => {
                failed += 1;
                eprintln!("client failed: {e}");
            }
        }
    }
    all.sort_unstable();

    let secs = opts.seconds as f64;
    let n = all.len();
    let pct = |p: f64| -> f64 {
        if n == 0 {
            return 0.0;
        }
        let idx = ((n as f64 - 1.0) * p).round() as usize;
        all[idx] as f64 / 1000.0
    };
    println!(
        "conns {}  seconds {}  failed {}",
        opts.conns, opts.seconds, failed
    );
    println!("requests {n}  ({:.0} req/s)", n as f64 / secs);
    println!(
        "latency us  p50 {:.1}  p90 {:.1}  p99 {:.1}  p99.9 {:.1}  max {:.1}",
        pct(0.50),
        pct(0.90),
        pct(0.99),
        pct(0.999),
        pct(1.0)
    );
    Ok(())
}
