//! A plain-`std` load generator for the Redis-backed KV servers
//! (`compio-redis`'s `examples/server.rs` and `deadpool-baseline`'s
//! `examples/redis_proxy.rs`). No async runtime: one blocking thread per
//! connection, so what it measures is the server + its Redis backend, not a
//! client runtime.
//!
//! It speaks the servers' line protocol, one request and one reply per line:
//!
//! ```text
//! PING            -> PONG
//! SET <k> <v>     -> OK
//! GET <k>         -> <value>  (or  (nil))
//! INCR <k>        -> <n>
//! DEL <k>         -> <count>
//! ```
//!
//! ```text
//! cargo run --release --example kvload -- ADDR[,ADDR...] [flags]
//!   --conns N       concurrent connections                 default 64
//!   --seconds S     how long to run                        default 5
//!   --workload W    get | set | incr | ping | mix (1:1)    default get
//!   --value B       value size in bytes (set/get/mix)      default 16
//!   --keys K        per-connection key space               default 128
//!   --pipeline P    requests sent before reading replies   default 1
//! ```
//!
//! ADDR may be a comma list; connection `i` uses `addr[i % n]`, which fans a
//! single run across a sharded set of servers (one Redis per shard).
//!
//! The output block matches `examples/load.rs` so the same parser reads both:
//! a `conns ... failed F` line, a `requests N (R req/s)` line, and a
//! `latency us  p50 .. p90 .. p99 .. p99.9 .. max ..` line. With `--pipeline P`
//! > 1 a latency sample is one batch of `P` requests, not one request.

use std::{
    io::{self, BufRead, BufReader, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, PartialEq)]
enum Workload {
    Get,
    Set,
    Incr,
    Ping,
    Mix,
}

struct Opts {
    addrs: Vec<SocketAddr>,
    conns: usize,
    seconds: u64,
    workload: Workload,
    value: usize,
    keys: usize,
    pipeline: usize,
}

fn parse() -> io::Result<Opts> {
    let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidInput, m.to_string());
    let mut args = std::env::args().skip(1);
    let addr_arg = args.next().ok_or_else(|| {
        bad("usage: kvload ADDR[,ADDR...] [--conns N] [--seconds S] [--workload W] [--value B] [--keys K] [--pipeline P]")
    })?;
    let mut addrs = Vec::new();
    for a in addr_arg.split(',').filter(|s| !s.is_empty()) {
        let sa = a
            .to_socket_addrs()
            .map_err(|e| bad(&format!("bad address {a}: {e}")))?
            .next()
            .ok_or_else(|| bad(&format!("address {a} resolved to nothing")))?;
        addrs.push(sa);
    }
    if addrs.is_empty() {
        return Err(bad("no address given"));
    }
    let mut opts = Opts {
        addrs,
        conns: 64,
        seconds: 5,
        workload: Workload::Get,
        value: 16,
        keys: 128,
        pipeline: 1,
    };
    while let Some(flag) = args.next() {
        let val = args
            .next()
            .ok_or_else(|| bad(&format!("{flag} needs a value")))?;
        match flag.as_str() {
            "--conns" => opts.conns = val.parse().map_err(|e| bad(&format!("--conns: {e}")))?,
            "--seconds" => {
                opts.seconds = val.parse().map_err(|e| bad(&format!("--seconds: {e}")))?
            }
            "--value" => opts.value = val.parse().map_err(|e| bad(&format!("--value: {e}")))?,
            "--keys" => opts.keys = val.parse().map_err(|e| bad(&format!("--keys: {e}")))?,
            "--pipeline" => {
                opts.pipeline = val.parse().map_err(|e| bad(&format!("--pipeline: {e}")))?
            }
            "--workload" => {
                opts.workload = match val.as_str() {
                    "get" => Workload::Get,
                    "set" => Workload::Set,
                    "incr" => Workload::Incr,
                    "ping" => Workload::Ping,
                    "mix" => Workload::Mix,
                    other => return Err(bad(&format!("unknown workload {other}"))),
                }
            }
            _ => return Err(bad(&format!("unknown flag {flag}"))),
        }
    }
    opts.conns = opts.conns.max(1);
    opts.keys = opts.keys.max(1);
    opts.pipeline = opts.pipeline.max(1);
    Ok(opts)
}

/// Per-thread xorshift — cheap, no locking, good enough to spread keys.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

struct Conn {
    w: TcpStream,
    r: BufReader<TcpStream>,
}

impl Conn {
    fn open(addr: SocketAddr) -> io::Result<Self> {
        let w = TcpStream::connect(addr)?;
        w.set_nodelay(true)?;
        let r = BufReader::new(w.try_clone()?);
        Ok(Conn { w, r })
    }
    /// Write all of `batch`, then read exactly `n` reply lines into `scratch`.
    /// Returns an error on EOF (server hung up) so the caller can fail the conn.
    fn round(&mut self, batch: &[u8], n: usize, scratch: &mut String) -> io::Result<()> {
        self.w.write_all(batch)?;
        for _ in 0..n {
            scratch.clear();
            let got = self.r.read_line(scratch)?;
            if got == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed",
                ));
            }
        }
        Ok(())
    }
}

struct ConnResult {
    latencies: Vec<u64>,
    ops: u64,
    mismatches: u64,
}

fn run_conn(id: usize, opts: &Opts, barrier: &Barrier) -> io::Result<ConnResult> {
    let addr = opts.addrs[id % opts.addrs.len()];
    let mut c = Conn::open(addr)?;
    let mut rng = Rng(0x9e3779b97f4a7c15 ^ (id as u64).wrapping_mul(0xd1b54a32d192) ^ 1);
    let value: String = "x".repeat(opts.value);
    let key = |i: u64| format!("{id}:{}", i % opts.keys as u64);

    // Warm the key space so GET/MIX hit, untimed. INCR and SET create on write;
    // PING never touches a key.
    let mut scratch = String::new();
    if matches!(opts.workload, Workload::Get | Workload::Mix) {
        for i in 0..opts.keys as u64 {
            let line = format!("SET {} {}\n", key(i), value);
            c.round(line.as_bytes(), 1, &mut scratch)?;
        }
    }

    // Everyone connected and warmed; start the clock together.
    barrier.wait();

    let deadline = Instant::now() + Duration::from_secs(opts.seconds);
    let mut latencies: Vec<u64> = Vec::with_capacity(128 * 1024);
    let mut ops: u64 = 0;
    let mut mismatches: u64 = 0;
    let mut batch = String::with_capacity(opts.pipeline * (opts.value + 32));

    while Instant::now() < deadline {
        batch.clear();
        // Build a pipeline of `opts.pipeline` commands.
        for p in 0..opts.pipeline {
            let k = key(rng.next());
            match opts.workload {
                Workload::Get => {
                    batch.push_str("GET ");
                    batch.push_str(&k);
                    batch.push('\n');
                }
                Workload::Set => {
                    batch.push_str("SET ");
                    batch.push_str(&k);
                    batch.push(' ');
                    batch.push_str(&value);
                    batch.push('\n');
                }
                Workload::Incr => {
                    batch.push_str("INCR ");
                    batch.push_str(&k);
                    batch.push('\n');
                }
                Workload::Ping => batch.push_str("PING\n"),
                Workload::Mix => {
                    // 1:1 set/get, deterministic within the batch.
                    if p % 2 == 0 {
                        batch.push_str("SET ");
                        batch.push_str(&k);
                        batch.push(' ');
                        batch.push_str(&value);
                        batch.push('\n');
                    } else {
                        batch.push_str("GET ");
                        batch.push_str(&k);
                        batch.push('\n');
                    }
                }
            }
        }

        let t = Instant::now();
        if c.round(batch.as_bytes(), opts.pipeline, &mut scratch)
            .is_err()
        {
            // Hard failure mid-run: report what we have, flag the conn failed.
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "round failed"));
        }
        // Cheap sanity on the last reply only (keeps the hot loop lean): a GET
        // reply should carry the value we stored, a PING a PONG.
        match opts.workload {
            Workload::Get if scratch.trim_end().len() != opts.value => mismatches += 1,
            Workload::Ping if scratch.trim_end() != "PONG" => mismatches += 1,
            _ => {}
        }
        latencies.push(t.elapsed().as_nanos() as u64);
        ops += opts.pipeline as u64;
    }

    Ok(ConnResult {
        latencies,
        ops,
        mismatches,
    })
}

fn main() -> io::Result<()> {
    let opts = Arc::new(parse()?);
    let barrier = Arc::new(Barrier::new(opts.conns));

    let workers: Vec<_> = (0..opts.conns)
        .map(|i| {
            let opts = opts.clone();
            let barrier = barrier.clone();
            thread::spawn(move || run_conn(i, &opts, &barrier))
        })
        .collect();

    let mut all: Vec<u64> = Vec::new();
    let mut ops: u64 = 0;
    let mut failed = 0usize;
    let mut mismatches: u64 = 0;
    for w in workers {
        match w.join().expect("client thread panicked") {
            Ok(res) => {
                all.extend(res.latencies);
                ops += res.ops;
                mismatches += res.mismatches;
            }
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
    let wl = match opts.workload {
        Workload::Get => "get",
        Workload::Set => "set",
        Workload::Incr => "incr",
        Workload::Ping => "ping",
        Workload::Mix => "mix",
    };
    println!(
        "conns {}  workload {}  value {}  keys {}  pipeline {}  seconds {}  shards {}  mismatches {}  failed {}",
        opts.conns,
        wl,
        opts.value,
        opts.keys,
        opts.pipeline,
        opts.seconds,
        opts.addrs.len(),
        mismatches,
        failed
    );
    println!("requests {ops}  ({:.0} req/s)", ops as f64 / secs);
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
