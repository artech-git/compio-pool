//! Pools real `compio::net::TcpStream`s across several compio threads.
//!
//! `TcpStream` is `!Send` — it cannot go in an `Arc<Mutex<_>>`, so `bb8` and
//! `deadpool` cannot hold one. This is the case the crate exists for.
//!
//! Run with: `cargo run --example tcp`

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};

use compio::{
    buf::BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use compio_pool::{Config, Manage, Pool, SlotMeta};

struct TcpManager {
    addr: SocketAddr,
    dials: Arc<AtomicU64>,
}

impl Manage for TcpManager {
    // Note: no `Send` anywhere. This is the whole point.
    type Connection = TcpStream;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<TcpStream> {
        self.dials.fetch_add(1, Relaxed);
        TcpStream::connect(self.addr).await
    }

    async fn recycle(&self, conn: &mut TcpStream, _meta: &SlotMeta) -> io::Result<()> {
        // Cheap liveness check. A real manager would send a protocol-level
        // ping when `meta.idle_for()` exceeds some threshold.
        conn.peer_addr().map(|_| ())
    }
}

/// An echo server, so the example has something to talk to.
fn spawn_echo_server() -> io::Result<SocketAddr> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        compio::runtime::Runtime::new()
            .unwrap()
            .block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                tx.send(listener.local_addr().unwrap()).unwrap();
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        return;
                    };
                    compio::runtime::spawn(async move {
                        loop {
                            let BufResult(read, buf) = stream.read(vec![0u8; 64]).await;
                            match read {
                                Ok(0) | Err(_) => return,
                                Ok(n) => {
                                    let BufResult(write, _) =
                                        stream.write_all(buf[..n].to_vec()).await;
                                    if write.is_err() {
                                        return;
                                    }
                                }
                            }
                        }
                    })
                    .detach();
                }
            })
    });
    Ok(rx.recv().unwrap())
}

fn main() -> io::Result<()> {
    let addr = spawn_echo_server()?;
    let dials = Arc::new(AtomicU64::new(0));

    let pool = Pool::new(
        TcpManager {
            addr,
            dials: dials.clone(),
        },
        Config::new()
            .max_size(2) // per thread
            .min_idle(1)
            .acquire_timeout(Duration::from_secs(5)),
    );

    // Four compio threads, each with its own driver and its own shard.
    let workers: Vec<_> = (0..4)
        .map(|worker| {
            let pool = pool.clone();
            std::thread::spawn(move || {
                compio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(async move {
                        pool.warm().await.expect("warm");

                        for round in 0..5u8 {
                            let mut conn = pool.acquire().await.expect("acquire");

                            // Arm cancellation protection: if either await below is
                            // cancelled, the connection is poisoned and closed
                            // rather than returned with a half-read response in it.
                            let op = conn.begin_op();

                            let msg = format!("worker {worker} round {round}");
                            let BufResult(written, msg) = conn.write_all(msg.into_bytes()).await;
                            written.expect("write");

                            let BufResult(read, buf) = conn.read(vec![0u8; msg.len()]).await;
                            let n = read.expect("read");
                            op.complete_op();

                            assert_eq!(&buf[..n], &msg[..], "echo mismatch");
                        }

                        pool.local_size()
                    })
            })
        })
        .collect();

    let held: Vec<usize> = workers.into_iter().map(|w| w.join().unwrap()).collect();

    println!("connections held per worker at exit: {held:?}");
    println!("total dials: {}", dials.load(Relaxed));
    println!("metrics: {:#?}", pool.metrics());

    // 4 threads x 20 operations, but each thread reuses its own connection, so
    // dials should be 4 (one per shard) rather than 20.
    assert_eq!(
        dials.load(Relaxed),
        4,
        "each thread should dial exactly once"
    );
    println!("\nOK: 20 operations across 4 threads on 4 connections.");
    Ok(())
}
