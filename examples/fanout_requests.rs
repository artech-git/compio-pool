//! The pool's canonical job — pooling **upstream connections** — shown entirely
//! on one compio runtime, with the concurrency coming from compio *tasks* rather
//! than threads.
//!
//! Everything runs in a single [`Runtime`] on one thread:
//!
//!  1. an in-process echo **backend** on an ephemeral loopback port, spawned as a
//!     task (so the example is self-contained — no second process to start);
//!  2. a [`Pool`] of upstream [`TcpStream`]s to that backend, capped at
//!     `CAPACITY`;
//!  3. `CLIENTS` **client tasks**, all [`compio::runtime::spawn`]ed on this same
//!     runtime, sharing one [`LocalPool`] and racing to drain a shared work
//!     counter of `REQUESTS` round trips.
//!
//! The lesson is backpressure: however many client tasks you spawn, the pool only
//! ever opens up to `CAPACITY` upstreams, and [`LocalPool::get`] makes a task
//! *wait* for a free one when they are all busy. The program proves it by printing
//! how many upstreams were ever opened and the peak number in use at once — both
//! bounded by `CAPACITY`, no matter how large `CLIENTS` is.
//!
//! Because it is all one thread, the shared counters are plain [`Rc`]`<`[`Cell`]`>`
//! — no atomics, no locks — which is exactly the `!Send`, single-thread world the
//! pool is built for.
//!
//! ```text
//! cargo run --release --example fanout_requests -- [REQUESTS] [CAPACITY] [CLIENTS] [SIZE]
//!   REQUESTS   total round trips          default 50000
//!   CAPACITY   pooled upstreams (max)     default 8
//!   CLIENTS    concurrent client tasks    default 128
//!   SIZE       bytes per request          default 256
//! ```

use std::{
    cell::Cell,
    io,
    net::SocketAddr,
    rc::Rc,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};
use compio_pool::{LocalPool, ManageConnection, Pool};

/// How many upstream connections the pool has ever opened. A real pool's whole
/// value is keeping this far below the number of concurrent callers.
static OPENS: AtomicU64 = AtomicU64::new(0);

/// Opens pooled upstream connections to the in-process backend. This is the real
/// job of a pool — a Redis, Postgres or HTTP backend would connect here the same
/// way; here the backend is just a loopback echo so the example needs nothing
/// external.
struct Upstreams {
    addr: SocketAddr,
}

impl ManageConnection for Upstreams {
    type Connection = TcpStream;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<TcpStream> {
        let stream = TcpStream::connect(self.addr).await?;
        stream.set_nodelay(true)?;
        OPENS.fetch_add(1, Ordering::Relaxed);
        Ok(stream)
    }

    async fn is_valid(&self, _conn: &mut TcpStream) -> io::Result<()> {
        Ok(())
    }

    fn has_broken(&self, _conn: &mut TcpStream) -> bool {
        false
    }
}

fn main() -> io::Result<()> {
    let requests: usize = arg(1).unwrap_or(50_000);
    let capacity: u32 = arg(2).unwrap_or(8);
    let clients: usize = arg(3).unwrap_or(128);
    let size: usize = arg(4).unwrap_or(256);

    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        // 1. An in-process echo backend on an ephemeral port, run as a task.
        let listen: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let backend = TcpListener::bind(listen).await?;
        let backend_addr = backend.local_addr()?;
        compio::runtime::spawn(backend_loop(backend)).detach();

        // 2. One pool of upstreams to the backend, and this thread's LocalPool.
        let pool = Pool::builder()
            .max_size_per_thread(capacity)
            .build(Upstreams { addr: backend_addr });
        let local = pool.local();

        println!(
            "fanout_requests -> {backend_addr}: {clients} client tasks over {capacity} pooled \
             upstreams, {requests} requests x {size} B"
        );

        // Shared, single-thread state: plain Rc<Cell>, no atomics needed.
        let next = Rc::new(Cell::new(0usize)); // next request index to claim
        let bytes = Rc::new(Cell::new(0u64)); // total bytes echoed back
        let inflight = Rc::new(Cell::new(0u32)); // upstreams in use right now
        let peak = Rc::new(Cell::new(0u32)); // high-water mark of `inflight`

        // 3. Fan out: spawn the client tasks and await them all.
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(clients);
        for _ in 0..clients {
            tasks.push(compio::runtime::spawn(client(
                local.clone(),
                next.clone(),
                requests,
                size,
                bytes.clone(),
                inflight.clone(),
                peak.clone(),
            )));
        }
        for task in tasks {
            let _ = task.await;
        }
        let elapsed = started.elapsed();

        println!(
            "done: {requests} requests in {:.3}s ({:.0} req/s), {} B echoed",
            elapsed.as_secs_f64(),
            requests as f64 / elapsed.as_secs_f64(),
            bytes.get(),
        );
        println!(
            "pool: opened {} upstreams total, peak {} in use at once (cap {capacity}); final {:?}",
            OPENS.load(Ordering::Relaxed),
            peak.get(),
            local.state(),
        );
        Ok(())
    })
}

/// One client task: claim request indices from the shared counter until they run
/// out, leasing a pooled upstream for each round trip and handing it straight
/// back. Many of these share `CAPACITY` upstreams.
async fn client(
    local: LocalPool<Upstreams>,
    next: Rc<Cell<usize>>,
    total: usize,
    size: usize,
    bytes: Rc<Cell<u64>>,
    inflight: Rc<Cell<u32>>,
    peak: Rc<Cell<u32>>,
) {
    loop {
        // Claim the next index. There is no `.await` between the read and the
        // write, so on this single thread no other task can observe or take the
        // same index — a lock-free counter that needs no lock.
        let i = next.get();
        if i >= total {
            break;
        }
        next.set(i + 1);

        // Wait for a free upstream. `get` only returns once one is available, so
        // `inflight` can never climb past `CAPACITY` — that is the backpressure.
        let mut up = match local.get().await {
            Ok(up) => up,
            Err(_) => return,
        };
        let now = inflight.get() + 1;
        inflight.set(now);
        if now > peak.get() {
            peak.set(now);
        }

        // One round trip: send `size` bytes, read exactly that many back.
        let BufResult(written, _) = up.write_all(vec![b'x'; size]).await;
        if written.is_ok() {
            let BufResult(read, reply) = up.read_exact(Vec::with_capacity(size)).await;
            if read.is_ok() {
                bytes.set(bytes.get() + reply.len() as u64);
            }
        }

        inflight.set(inflight.get() - 1);
        // `up` drops here, returning the upstream to the pool for the next task.
    }
}

/// The in-process echo backend: accept forever, echo each connection on its own
/// task. Stands in for whatever real service the pool would front.
async fn backend_loop(listener: TcpListener) {
    loop {
        let Ok((mut stream, _peer)) = listener.accept().await else {
            continue;
        };
        let _ = stream.set_nodelay(true);
        compio::runtime::spawn(async move {
            let mut buf = Vec::with_capacity(16 * 1024);
            loop {
                buf.clear();
                let BufResult(read, b) = stream.read(buf).await;
                buf = b;
                match read {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
                let BufResult(written, b) = stream.write_all(buf).await;
                buf = b;
                if written.is_err() {
                    return;
                }
            }
        })
        .detach();
    }
}

/// Parse the nth CLI argument, if present and valid.
fn arg<T: std::str::FromStr>(n: usize) -> Option<T> {
    std::env::args().nth(n).and_then(|s| s.parse().ok())
}
