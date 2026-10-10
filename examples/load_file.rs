//! Load generator for `examples/file_server.rs`. Plain `std`, one blocking
//! thread per connection, like `load.rs`.
//!
//! Creates `--files` test files of `--bytes` each in FILE_DIR (default
//! /tmp/bench-files) before the run, then requests them round-robin over
//! persistent connections and verifies the full body arrives.
//!
//! ```text
//! FILE_DIR=/tmp/bench-files \
//! cargo run --release --example load_file -- ADDR [--conns N] [--seconds S] [--bytes B] [--files F]
//! ```
//!
//! Prints the same requests/s and latency lines as `load.rs`.

use std::{
    io::{self, BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

struct Opts {
    addr: SocketAddr,
    conns: usize,
    seconds: u64,
    bytes: usize,
    files: usize,
    dir: PathBuf,
}

fn parse() -> io::Result<Opts> {
    let mut args = std::env::args().skip(1);
    let addr: SocketAddr = args
        .next()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "usage: load_file ADDR [--conns N] [--seconds S] [--bytes B] [--files F]",
            )
        })?
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let mut opts = Opts {
        addr,
        conns: 64,
        seconds: 5,
        bytes: 4096,
        files: 64,
        dir: PathBuf::from(std::env::var("FILE_DIR").unwrap_or_else(|_| "/tmp/bench-files".into())),
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
            "--files" => opts.files = n as usize,
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

/// Create the test files the run will request. Content is a repeating pattern
/// so a partially-read body would be caught by the length check alone.
fn setup_files(opts: &Opts) -> io::Result<()> {
    std::fs::create_dir_all(&opts.dir)?;
    let body: Vec<u8> = (0..opts.bytes).map(|i| (i % 251) as u8).collect();
    for i in 0..opts.files {
        let path = opts.dir.join(format!("f{}-{}.bin", opts.bytes, i));
        // Rewrite only when missing or the wrong size, so repeat runs are warm.
        if std::fs::metadata(&path).ok().map(|m| m.len() as usize) != Some(opts.bytes) {
            std::fs::write(&path, &body)?;
        }
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let opts = Arc::new(parse()?);
    setup_files(&opts)?;
    let barrier = Arc::new(Barrier::new(opts.conns));
    let started = Instant::now();

    let workers: Vec<_> = (0..opts.conns)
        .map(|i| {
            let opts = opts.clone();
            let barrier = barrier.clone();
            thread::spawn(move || -> io::Result<Vec<u64>> {
                let stream = TcpStream::connect(opts.addr)?;
                stream.set_nodelay(true)?;
                let mut reader = BufReader::new(stream.try_clone()?);
                let mut stream = stream;
                barrier.wait();

                let mut body = vec![0u8; opts.bytes];
                let mut header = Vec::with_capacity(32);
                let mut latencies = Vec::with_capacity(64 * 1024);
                let mut file_idx = i % opts.files;
                let deadline = started + Duration::from_secs(opts.seconds);
                while Instant::now() < deadline {
                    let t = Instant::now();
                    let req = format!("f{}-{}.bin\n", opts.bytes, file_idx);
                    file_idx = (file_idx + 1) % opts.files;
                    stream.write_all(req.as_bytes())?;

                    header.clear();
                    reader.read_until(b'\n', &mut header)?;
                    let line = std::str::from_utf8(&header)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
                        .trim_end();
                    if line.starts_with("ERR") {
                        return Err(io::Error::other(line.to_string()));
                    }
                    let size: usize = line.parse().map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("bad size {line:?}: {e}"),
                        )
                    })?;
                    if size != opts.bytes {
                        return Err(io::Error::other(format!(
                            "size mismatch: sent {}B file, got {size}",
                            opts.bytes
                        )));
                    }
                    reader.read_exact(&mut body)?;
                    latencies.push(t.elapsed().as_nanos() as u64);
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
        "conns {}  bytes {}  seconds {}  files {}  failed {}",
        opts.conns, opts.bytes, opts.seconds, opts.files, failed
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
