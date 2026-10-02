//! A plain-`std` load generator for `examples/echo.rs`. No async runtime: one
//! blocking thread per connection, so what it measures is the server.
//!
//! ```text
//! cargo run --release --example load -- ADDR [--conns N] [--seconds S] [--bytes B] [--hold-ms MS]
//!   --conns    concurrent connections           default 64
//!   --seconds  how long to run                   default 5
//!   --bytes    payload per request               default 512
//!   --hold-ms  connect everything first, idle this long, then start sending.
//!              With a server capacity smaller than --conns this forces the
//!              surplus through the handoff channel.  default 0
//!   --reconnect 1  open a fresh connection for every request instead of
//!              keeping one open. With a small server capacity this is what
//!              measures the handoff path: parked connections are served as
//!              soon as a slot frees, so every request includes connect, hash,
//!              possibly detach + channel + attach, and close.
//! ```
//!
//! Prints requests/s and the latency distribution over every request made.

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
    hold: Duration,
    reconnect: bool,
}

fn parse() -> io::Result<Opts> {
    let mut args = std::env::args().skip(1);
    let addr: SocketAddr = args
        .next()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: load ADDR [--conns N] [--seconds S] [--bytes B] [--hold-ms MS]",
            )
        })?
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let mut opts = Opts {
        addr,
        conns: 64,
        seconds: 5,
        bytes: 512,
        hold: Duration::ZERO,
        reconnect: false,
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
            "--hold-ms" => opts.hold = Duration::from_millis(n),
            "--reconnect" => opts.reconnect = n != 0,
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
                let connect = || -> io::Result<TcpStream> {
                    let s = TcpStream::connect(opts.addr)?;
                    s.set_nodelay(true)?;
                    Ok(s)
                };
                let mut stream = connect()?;
                // Everyone connects, then everyone waits, so a small-capacity
                // server sees the whole burst at once and has to hand off.
                barrier.wait();
                thread::sleep(opts.hold);

                let payload: Vec<u8> = (0..opts.bytes).map(|j| (i + j) as u8).collect();
                let mut back = vec![0u8; opts.bytes];
                let mut latencies = Vec::with_capacity(64 * 1024);
                let deadline = started + opts.hold + Duration::from_secs(opts.seconds);
                while Instant::now() < deadline {
                    let t = Instant::now();
                    if opts.reconnect {
                        stream = connect()?;
                    }
                    stream.write_all(&payload)?;
                    stream.read_exact(&mut back)?;
                    latencies.push(t.elapsed().as_nanos() as u64);
                    if back != payload {
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
        "conns {}  bytes {}  seconds {}  hold {:?}  reconnect {}  failed {}",
        opts.conns, opts.bytes, opts.seconds, opts.hold, opts.reconnect, failed
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
