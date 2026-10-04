//! The tokio + deadpool counterpart to compio-redis's `examples/server.rs`.
//!
//! Same job, same wire protocol, same pooling shape — different runtime and pool.
//! A TCP server that fronts one `redis-server`, speaking the identical line
//! protocol (`PING` / `SET` / `GET` / `INCR` / `DEL` / `QUIT`) so the *same*
//! `kvload` generator drives both, and the only variables are the runtime
//! (tokio/epoll vs compio/io_uring) and the pool (deadpool vs compio-pool's
//! thread-local `Resource`).
//!
//! Like the compio server, a pooled upstream Redis connection is leased for the
//! whole lifetime of an inbound connection (not per command), so with persistent
//! clients the pool holds ~one upstream connection per inbound connection — the
//! apples-to-apples match. Size the pool (`CAPACITY`) at or above the peak
//! connection count, exactly as the compio server's capacity is sized.
//!
//! Two modes, to separate architecture from backend:
//!   percore (default) — one pinned `current_thread` runtime per core, each with
//!                       its own SO_REUSEPORT listener and its own deadpool pool.
//!                       The direct architectural match to compio-pool.
//!   default           — tokio's normal model: one multi-threaded work-stealing
//!                       runtime, one shared listener, one shared deadpool pool.
//!
//! ```text
//! # needs a redis:  REDIS_URL=redis://127.0.0.1:6399 is read from the env
//! cargo run --release -p deadpool-baseline --example redis_proxy -- [ADDR] [CAPACITY] [WORKERS] [MODE]
//! ```
//! Positional args match compio-redis's server (ADDR CAPACITY WORKERS); MODE is a
//! 4th positional, `percore` or `default`.

use std::{io, net::SocketAddr};

use deadpool_redis::{
    redis::{cmd, AsyncCommands, RedisError},
    Config, Connection, Pool, Runtime,
};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    task::LocalSet,
};

fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
}

/// Build a deadpool-redis pool sized to `max` upstream connections.
fn build_pool(max: usize) -> Pool {
    let cfg = Config::from_url(redis_url());
    let pool = cfg
        .create_pool(Some(Runtime::Tokio1))
        .expect("create deadpool-redis pool");
    pool.resize(max);
    pool
}

/// A SO_REUSEPORT, non-blocking listener — identical to `echo_tpc.rs`, so the
/// per-core accept path is the same one the tokio echo control uses.
fn reuseport_listener(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    let s = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    s.set_reuse_address(true)?;
    s.set_reuse_port(true)?;
    s.set_nonblocking(true)?;
    s.bind(&addr.into())?;
    s.listen(1024)?;
    Ok(s.into())
}

/// One request line against Redis → one reply line. `true` means the error is
/// connection-fatal and the inbound connection should end (mirrors the compio
/// server's `soft`/`is_connection_fatal` split).
async fn run(line: &str, conn: &mut Connection) -> (String, bool) {
    let mut parts = line.split_whitespace();
    let Some(command) = parts.next() else {
        return ("ERR empty command\n".into(), false);
    };

    let res: Result<String, RedisError> = match command.to_ascii_uppercase().as_str() {
        "PING" => cmd("PING").query_async::<String>(&mut *conn).await,
        "GET" => match parts.next() {
            Some(key) => conn
                .get::<_, Option<String>>(key)
                .await
                .map(|v| v.unwrap_or_else(|| "(nil)".into())),
            None => Ok("ERR GET needs a key".into()),
        },
        "SET" => match (parts.next(), parts.next()) {
            (Some(key), Some(value)) => conn.set::<_, _, ()>(key, value).await.map(|_| "OK".into()),
            _ => Ok("ERR SET needs a key and a value".into()),
        },
        "INCR" => match parts.next() {
            Some(key) => conn.incr::<_, _, i64>(key, 1).await.map(|n| n.to_string()),
            None => Ok("ERR INCR needs a key".into()),
        },
        "DEL" => match parts.next() {
            Some(key) => conn.del::<_, i64>(key).await.map(|n| n.to_string()),
            None => Ok("ERR DEL needs a key".into()),
        },
        "QUIT" => return ("BYE\n".into(), true),
        other => Ok(format!("ERR unknown command {other:?}")),
    };

    match res {
        Ok(reply) => (format!("{reply}\n"), false),
        Err(e) if e.is_io_error() || e.is_connection_dropped() || e.is_connection_refusal() => {
            (format!("ERR {e}\n"), true)
        }
        Err(e) => (format!("ERR {e}\n"), false),
    }
}

/// Serve one inbound connection: lease one upstream Redis connection for its
/// whole lifetime, then loop request→reply.
async fn handle(stream: TcpStream, pool: Pool) {
    let _ = stream.set_nodelay(true);
    let mut conn = match pool.get().await {
        Ok(c) => c,
        Err(_) => return, // pool/connect failure: drop the inbound connection
    };
    let (r, mut w) = stream.into_split();
    let mut br = BufReader::new(r);
    let mut line = String::new();
    loop {
        line.clear();
        match br.read_line(&mut line).await {
            Ok(0) | Err(_) => break, // clean EOF or read error
            Ok(_) => {}
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        let (reply, fatal) = run(trimmed, &mut conn).await;
        if w.write_all(reply.as_bytes()).await.is_err() {
            break;
        }
        if fatal {
            break;
        }
    }
}

fn run_percore(addr: SocketAddr, cap: usize, workers: usize) -> io::Result<()> {
    let cores = core_ids();
    println!(
        "tokio+deadpool redis proxy on {addr} backed by {} — {workers} workers (percore: epoll, SO_REUSEPORT, per-worker pool cap {cap})",
        redis_url()
    );
    let handles: Vec<_> = (0..workers)
        .map(|i| {
            let core = cores.get(i % cores.len().max(1)).copied();
            std::thread::spawn(move || -> io::Result<()> {
                if let Some(c) = core {
                    pin(c);
                    println!("  worker {i:>2} -> cpu {c:>3}");
                }
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let local = LocalSet::new();
                local.block_on(&rt, async move {
                    let pool = build_pool(cap);
                    // Prewarm one upstream connection so a dead Redis fails loudly
                    // at startup, like the compio server's prewarm(1).
                    if let Err(e) = pool.get().await {
                        eprintln!("worker {i}: cannot reach Redis at {}: {e}", redis_url());
                        std::process::exit(1);
                    }
                    let l = TcpListener::from_std(reuseport_listener(addr)?)?;
                    loop {
                        let (stream, _) = l.accept().await?;
                        let pool = pool.clone();
                        tokio::task::spawn_local(handle(stream, pool));
                    }
                })
            })
        })
        .collect();
    for h in handles {
        h.join().expect("worker panicked")?;
    }
    Ok(())
}

fn run_default(addr: SocketAddr, cap: usize, workers: usize) -> io::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?;
    println!(
        "tokio+deadpool redis proxy on {addr} backed by {} — {workers} worker threads (default: multi-thread work-stealing, shared listener + shared pool cap {cap})",
        redis_url()
    );
    rt.block_on(async move {
        let pool = build_pool(cap);
        if let Err(e) = pool.get().await {
            eprintln!("cannot reach Redis at {}: {e}", redis_url());
            std::process::exit(1);
        }
        let l = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = l.accept().await?;
            let pool = pool.clone();
            tokio::spawn(handle(stream, pool));
        }
    })
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr: SocketAddr = args
        .next()
        .unwrap_or_else(|| "0.0.0.0:7000".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let cap: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
        .unwrap_or(1024);
    let workers: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get()));
    let mode = args.next().unwrap_or_else(|| "percore".into());

    match mode.as_str() {
        "percore" => run_percore(addr, cap, workers),
        "default" => run_default(addr, cap, workers),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("mode must be percore or default, got {other:?}"),
        )),
    }
}

/// CPUs this process may run on, in id order (respects `taskset` / cgroup
/// cpusets). Copied from `echo_tpc.rs` so the two tokio servers pin identically.
fn core_ids() -> Vec<usize> {
    // SAFETY: sched_getaffinity fills a zeroed cpu_set_t of the correct size.
    let mask = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        (libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0)
            .then_some(set)
    };
    match mask {
        Some(set) => (0..libc::CPU_SETSIZE as usize)
            // SAFETY: `i` is below CPU_SETSIZE and `set` is initialised.
            .filter(|&i| unsafe { libc::CPU_ISSET(i, &set) })
            .collect(),
        None => (0..std::thread::available_parallelism().map_or(1, |n| n.get())).collect(),
    }
}

fn pin(cpu: usize) {
    // SAFETY: sched_setaffinity on the calling thread with a populated cpu_set_t.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}
