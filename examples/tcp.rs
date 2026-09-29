//! Pooling `compio::net::TcpStream` across the threads of a
//! [`compio::dispatcher::Dispatcher`].
//!
//! A `TcpStream` belongs to the driver of the thread that opened it. It is
//! `!Send`, so it cannot go behind an `Arc<Mutex<_>>`, and `bb8`/`deadpool`
//! cannot hold one at all. This crate's answer is a shard per thread; the
//! dispatcher is where those threads come from.
//!
//! # The shape every example here uses
//!
//! * The peer — an echo server — is a task on the **main** runtime, so its
//!   address comes straight off the listener, with no channel to hand it back.
//! * The load runs on a `Dispatcher`, one dispatched task per worker thread.
//! * `concurrent(false)` makes a worker run its task to completion before it
//!   takes another, so dispatching `WORKERS` tasks to `WORKERS` threads puts
//!   exactly one task on each — and therefore one shard behind each.
//! * `dispatcher.join()` stops the worker threads. Each shard is dropped on
//!   the thread that owns it, so every connection closes under its own driver.
//!
//! Run with: `cargo run --example tcp`

use std::{
    io,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use compio::{
    buf::BufResult,
    dispatcher::Dispatcher,
    io::{AsyncRead, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use compio_pool::{Manage, Pool, SlotMeta};

const WORKERS: usize = 4;
const ROUNDS: usize = 5;

struct Echo {
    addr: SocketAddr,
    dials: Arc<AtomicU64>,
}

impl Manage for Echo {
    /// Note the absence of `Send`. That is the whole point.
    type Connection = TcpStream;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<TcpStream> {
        self.dials.fetch_add(1, Relaxed);
        TcpStream::connect(self.addr).await
    }

    async fn recycle(&self, conn: &mut TcpStream, _meta: &SlotMeta) -> io::Result<()> {
        // A cheap local check. A real manager would send a protocol ping once
        // `meta.idle_for()` passed some threshold — see `examples/unix_socket.rs`.
        conn.peer_addr().map(|_| ())
    }
}

/// One request: write a line, read it back.
async fn round_trip(conn: &mut TcpStream, msg: String) -> io::Result<()> {
    let len = msg.len();
    let BufResult(written, msg) = conn.write_all(msg.into_bytes()).await;
    written?;
    let BufResult(read, buf) = conn.read(vec![0u8; len]).await;
    assert_eq!(&buf[..read?], &msg[..], "echo mismatch");
    Ok(())
}

/// Starts an echo server as a task on the current runtime, and returns the
/// address it bound.
async fn serve() -> io::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    compio::runtime::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            compio::runtime::spawn(async move {
                loop {
                    let BufResult(read, buf) = stream.read(vec![0u8; 64]).await;
                    let Ok(n @ 1..) = read else { return };
                    let BufResult(written, _) = stream.write_all(buf[..n].to_vec()).await;
                    if written.is_err() {
                        return;
                    }
                }
            })
            .detach();
        }
    })
    .detach();
    Ok(addr)
}

#[compio::main]
async fn main() -> io::Result<()> {
    let addr = serve().await?;
    let dials = Arc::new(AtomicU64::new(0));

    // `Pool` is `Send + Sync` and cheap to clone, but holds no connections:
    // those live in a thread-local shard, so `max_size` is per thread.
    let pool = Pool::builder(Echo {
        addr,
        dials: dials.clone(),
    })
    .max_size(2)
    .min_idle(1)
    .acquire_timeout(Duration::from_secs(5))
    .build();

    let dispatcher = Dispatcher::builder()
        .worker_threads(NonZeroUsize::new(WORKERS).unwrap())
        .concurrent(false)
        .build()?;

    // A dispatched closure must be `Send`, but the future it returns need not
    // be — it is polled on one thread only. That is exactly the shape a `!Send`
    // connection needs, and why the pool fits the dispatcher so neatly.
    let handles = (0..WORKERS)
        .map(|worker| {
            let pool = pool.clone();
            dispatcher
                .dispatch(move || async move {
                    // Each worker warms its own shard: `min_idle` dials up
                    // front, so the first request skips the handshake.
                    pool.warm().await.expect("warm");

                    for round in 0..ROUNDS {
                        let mut conn = pool.acquire().await.expect("acquire");

                        // Arms cancellation protection. If either await inside
                        // `round_trip` is dropped, the connection is left with
                        // an unknown amount of the reply still in it, so the
                        // pool destroys it instead of handing it to the next
                        // caller.
                        let op = conn.begin_op();
                        round_trip(&mut conn, format!("worker {worker} round {round}"))
                            .await
                            .expect("round trip");
                        op.complete_op();
                    }

                    pool.local_size()
                })
                .expect("dispatch")
        })
        .collect::<Vec<_>>();

    let mut held = Vec::new();
    for handle in handles {
        held.push(handle.await.expect("worker panicked"));
    }

    println!("connections held per worker: {held:?}");
    println!(
        "dials: {} for {} requests",
        dials.load(Relaxed),
        WORKERS * ROUNDS
    );
    println!("metrics: {:#?}", pool.metrics());

    // Every worker reuses its own shard's connection, so the dial count tracks
    // the number of threads, not the number of requests.
    assert!(dials.load(Relaxed) as usize <= WORKERS);

    // Stops the worker threads; each shard closes its connections on the
    // thread that opened them.
    dispatcher.join().await?;
    println!(
        "\nOK: {} requests over {WORKERS} threads.",
        WORKERS * ROUNDS
    );
    Ok(())
}
