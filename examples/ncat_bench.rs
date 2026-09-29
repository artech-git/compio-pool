//! What pooling is worth, measured against a **real** server: `ncat` running
//! an echo service in a separate process.
//!
//! Nothing here is in-process. Every round trip crosses the kernel twice, and
//! every dial costs a TCP handshake plus a `fork`/`exec` of `/bin/cat` on the
//! server side. That expensive dial is what a pool exists to amortise, and the
//! second arm below measures what it costs when you do not.
//!
//! | arm | connections | what it shows |
//! |---|---|---|
//! | `pooled`   | `THREADS` reused for the whole run | acquire is a thread-local pop; the handshake is paid once |
//! | `unpooled` | one per request, closed after | the dial cost, per request |
//!
//! `max_size` is **per shard**, so the pool never applies a process-wide cap.
//! `THREADS` dispatcher workers means `THREADS` shards.
//!
//! The unpooled arm runs far fewer requests on purpose. Each one leaves a
//! socket in `TIME_WAIT` for 2*MSL and the ephemeral port range is finite, so
//! a long unpooled run starts failing outright with `EADDRNOTAVAIL` — while the
//! pooled arm, reusing a handful of sockets, is untouched. Check with
//! `netstat -an -p tcp | grep -c TIME_WAIT`.
//!
//! # Running
//!
//! ```text
//! cargo run --release --example ncat_bench
//! ```
//!
//! Needs `ncat` on `PATH` (`brew install nmap`, `apt install ncat`). The
//! example starts and stops the server itself; set `NCAT_ADDR=host:port` to
//! point it at one you are already running.
//!
//! Tunables: `THREADS` (4), `ROUNDS` (2000), `UNPOOLED_ROUNDS` (100),
//! `PAYLOAD` (64 bytes).

use std::{
    io,
    net::{SocketAddr, TcpListener as StdListener},
    num::NonZeroUsize,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use compio::{
    buf::BufResult,
    dispatcher::Dispatcher,
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use compio_pool::{Manage, Pool, SlotMeta};

// ------------------------------------------------------------------ parameters

struct Params {
    threads: usize,
    rounds: usize,
    unpooled_rounds: usize,
    payload: usize,
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// --------------------------------------------------------------- the manager

struct Ncat {
    addr: SocketAddr,
    dials: Arc<AtomicU64>,
}

impl Manage for Ncat {
    type Connection = TcpStream;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<TcpStream> {
        self.dials.fetch_add(1, Relaxed);
        let conn = TcpStream::connect(self.addr).await?;
        // Without this, every request waits on Nagle for a partner that is
        // never coming, and the numbers below measure the delayed-ACK timer
        // rather than the pool.
        conn.set_nodelay(true)?;
        Ok(conn)
    }

    async fn recycle(&self, conn: &mut TcpStream, _meta: &SlotMeta) -> io::Result<()> {
        conn.peer_addr().map(|_| ())
    }
}

/// One request: write `payload` bytes, read the same bytes back.
async fn round_trip(conn: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    let BufResult(written, payload) = conn.write_all(payload.to_vec()).await;
    written?;
    let BufResult(read, echoed) = conn.read_exact(vec![0u8; payload.len()]).await;
    read?;
    assert_eq!(echoed, payload, "echo mismatch");
    Ok(())
}

// ------------------------------------------------------------------ reporting

struct Arm {
    label: &'static str,
    requests: usize,
    wall: Duration,
    /// Summed per-request latency across every worker.
    latency: Duration,
    dials: u64,
}

impl Arm {
    fn per_request(&self) -> Duration {
        self.latency / self.requests as u32
    }

    fn per_second(&self) -> f64 {
        self.requests as f64 / self.wall.as_secs_f64()
    }

    fn report(&self) {
        println!(
            "{:<10} {:>7} requests  {:>9.0} req/s  {:>9.1?} per request  {:>5} dials",
            self.label,
            self.requests,
            self.per_second(),
            self.per_request(),
            self.dials,
        );
    }
}

// ----------------------------------------------------------------- the arms

/// Every worker checks out of its own shard, so the handshake is paid once per
/// thread and never again.
async fn pooled(dispatcher: &Dispatcher, pool: &Pool<Ncat>, params: &Params) -> Duration {
    let mut handles = Vec::new();
    for _ in 0..params.threads {
        let pool = pool.clone();
        let (rounds, payload) = (params.rounds, params.payload);
        handles.push(
            dispatcher
                .dispatch(move || async move {
                    let mut spent = Duration::ZERO;
                    for _ in 0..rounds {
                        let start = Instant::now();
                        let mut conn = pool.acquire().await.expect("acquire");
                        let op = conn.begin_op();
                        round_trip(&mut conn, &vec![b'x'; payload])
                            .await
                            .expect("round trip");
                        op.complete_op();
                        drop(conn);
                        spent += start.elapsed();
                    }
                    spent
                })
                .expect("dispatch"),
        );
    }
    let mut total = Duration::ZERO;
    for handle in handles {
        total += handle.await.expect("worker panicked");
    }
    total
}

/// The same work with no pool at all: a fresh socket, and therefore a fresh
/// handshake and a fresh `fork`/`exec` on the server, for every request.
async fn unpooled(dispatcher: &Dispatcher, addr: SocketAddr, params: &Params) -> Duration {
    let mut handles = Vec::new();
    for _ in 0..params.threads {
        let (rounds, payload) = (params.unpooled_rounds, params.payload);
        handles.push(
            dispatcher
                .dispatch(move || async move {
                    let mut spent = Duration::ZERO;
                    for _ in 0..rounds {
                        let start = Instant::now();
                        let mut conn = TcpStream::connect(addr).await.expect("connect");
                        conn.set_nodelay(true).expect("nodelay");
                        round_trip(&mut conn, &vec![b'x'; payload])
                            .await
                            .expect("round trip");
                        drop(conn);
                        spent += start.elapsed();
                    }
                    spent
                })
                .expect("dispatch"),
        );
    }
    let mut total = Duration::ZERO;
    for handle in handles {
        total += handle.await.expect("worker panicked");
    }
    total
}

// ------------------------------------------------------------- the ncat server

struct Server(Option<Child>);

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Starts `ncat` as an echo server: `--keep-open` so it accepts more than one
/// connection, `--exec /bin/cat` so each one gets its own echo process.
fn spawn_ncat(port: u16, max_conns: usize) -> io::Result<Server> {
    let child = Command::new("ncat")
        .args([
            "--listen",
            "127.0.0.1",
            &port.to_string(),
            "--keep-open",
            "--exec",
            "/bin/cat",
            "--max-conns",
            &max_conns.to_string(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "could not start `ncat` ({e}). Install it (brew install nmap / \
                     apt install ncat), or point the example at a server you are \
                     already running with NCAT_ADDR=host:port"
                ),
            )
        })?;
    Ok(Server(Some(child)))
}

/// Grabs a free port by binding and immediately dropping the listener.
fn free_port() -> io::Result<u16> {
    Ok(StdListener::bind("127.0.0.1:0")?.local_addr()?.port())
}

/// `ncat` takes a few milliseconds to reach `listen(2)`; poll until it does.
fn wait_ready(addr: SocketAddr) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            Ok(_) => return Ok(()),
            Err(e) if Instant::now() >= deadline => return Err(e),
            Err(_) => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

// ----------------------------------------------------------------------- main

#[compio::main]
async fn main() -> io::Result<()> {
    let params = Params {
        threads: env_usize("THREADS", 4),
        rounds: env_usize("ROUNDS", 2000),
        unpooled_rounds: env_usize("UNPOOLED_ROUNDS", 100),
        payload: env_usize("PAYLOAD", 64),
    };

    // `_server` is kept alive by the binding; dropping it kills `ncat`.
    let (addr, _server) = match std::env::var("NCAT_ADDR") {
        Ok(addr) => (addr.parse().expect("NCAT_ADDR is not host:port"), None),
        Err(_) => {
            let port = free_port()?;
            let server = spawn_ncat(port, params.threads * 8)?;
            (SocketAddr::from(([127, 0, 0, 1], port)), Some(server))
        }
    };
    wait_ready(addr)?;

    let dials = Arc::new(AtomicU64::new(0));
    let pool = Pool::builder(Ncat {
        addr,
        dials: dials.clone(),
    })
    .max_size(2)
    .min_idle(1)
    .acquire_timeout(Duration::from_secs(10))
    .build();

    let dispatcher = Dispatcher::builder()
        .worker_threads(NonZeroUsize::new(params.threads).unwrap())
        .concurrent(false)
        .build()?;

    println!(
        "{} threads, {} byte payload, against ncat at {addr}\n",
        params.threads, params.payload
    );

    let start = Instant::now();
    let latency = pooled(&dispatcher, &pool, &params).await;
    let pooled_arm = Arm {
        label: "pooled",
        requests: params.threads * params.rounds,
        wall: start.elapsed(),
        latency,
        dials: dials.load(Relaxed),
    };

    let before = dials.load(Relaxed);
    let start = Instant::now();
    let latency = unpooled(&dispatcher, addr, &params).await;
    let unpooled_arm = Arm {
        label: "unpooled",
        requests: params.threads * params.unpooled_rounds,
        wall: start.elapsed(),
        latency,
        // The pool dialled nothing during this arm; every dial was the arm's own.
        dials: (params.threads * params.unpooled_rounds) as u64,
    };
    assert_eq!(
        dials.load(Relaxed),
        before,
        "the unpooled arm bypassed the pool"
    );

    pooled_arm.report();
    unpooled_arm.report();
    println!(
        "\na request costs {:.1}x less pooled; the pool dialled {} times for {} requests",
        unpooled_arm.per_request().as_secs_f64() / pooled_arm.per_request().as_secs_f64(),
        pooled_arm.dials,
        pooled_arm.requests,
    );
    println!("\nmetrics: {:#?}", pool.metrics());

    dispatcher.join().await?;
    Ok(())
}
