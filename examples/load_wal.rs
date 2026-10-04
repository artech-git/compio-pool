//! Load generator for `examples/wal.rs`. Plain `std`, one blocking thread per
//! connection, like `load.rs`.
//!
//! Each request sends a `--bytes` payload and waits for the server's `OK\n`
//! ack — which, with WAL_SYNC=1 on the server, means the record is on disk.
//! The latency distribution is therefore the fdatasync-inclusive commit
//! latency.
//!
//! ```text
//! cargo run --release --example load_wal -- ADDR [--conns N] [--seconds S] [--bytes B]
//! ```
//!
//! Prints the same requests/s and latency lines as `load.rs`.

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
    bytes: usize,
}

fn parse() -> io::Result<Opts> {
    let mut args = std::env::args().skip(1);
    let addr: SocketAddr = args
        .next()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: load_wal ADDR [--conns N] [--seconds S] [--bytes B]",
            )
        })?
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let mut opts = Opts {
        addr,
        conns: 64,
        seconds: 5,
        bytes: 512,
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
            "--bytes" => opts.bytes = n as usize,
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
                let mut stream = TcpStream::connect(opts.addr)?;
                stream.set_nodelay(true)?;
                barrier.wait();

                let payload: Vec<u8> = (0..opts.bytes).map(|j| (i + j) as u8).collect();
                let mut ack = [0u8; 3];
                let mut latencies = Vec::with_capacity(64 * 1024);
                let deadline = started + Duration::from_secs(opts.seconds);
                while Instant::now() < deadline {
                    let t = Instant::now();
                    stream.write_all(&payload)?;
                    stream.read_exact(&mut ack)?;
                    latencies.push(t.elapsed().as_nanos() as u64);
                    if &ack != b"OK\n" {
                        return Err(io::Error::other(format!("bad ack {ack:?}")));
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
        "conns {}  bytes {}  seconds {}  failed {}",
        opts.conns, opts.bytes, opts.seconds, failed
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
