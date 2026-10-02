//! Shared by the integration tests: an echo service that records how each
//! connection reached it, and blocking `std::net` clients to drive it.

#![allow(dead_code)]

use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
};
use compio_pool::{Builder, Connection, Resource, Route, Server, Service, WorkerContext, Workers};

pub struct Buf(pub Vec<u8>);

impl Resource for Buf {
    async fn create(_cx: &WorkerContext) -> io::Result<Self> {
        Ok(Buf(Vec::with_capacity(4096)))
    }
}

/// Echoes, and counts connections by route so a test can prove the handoff
/// path was taken and still produced a working stream.
#[derive(Clone, Default)]
pub struct Echo {
    pub local: Arc<AtomicUsize>,
    pub claimed: Arc<AtomicUsize>,
    pub oversubscribed: Arc<AtomicUsize>,
    pub max_hops: Arc<AtomicUsize>,
}

impl Service for Echo {
    type Resource = Buf;

    async fn handle(&self, mut conn: Connection, buf: &mut Buf) -> io::Result<()> {
        match conn.route {
            Route::Local => self.local.fetch_add(1, Relaxed),
            Route::Claimed { hops, .. } => {
                self.max_hops.fetch_max(hops as usize, Relaxed);
                self.claimed.fetch_add(1, Relaxed)
            }
        };
        if conn.oversubscribed {
            self.oversubscribed.fetch_add(1, Relaxed);
        }
        loop {
            let mut b = std::mem::take(&mut buf.0);
            b.clear();
            let BufResult(read, b) = conn.stream.read(b).await;
            match read {
                Ok(0) => {
                    buf.0 = b;
                    return Ok(());
                }
                Ok(_) => {}
                Err(e) => {
                    buf.0 = b;
                    return Err(e);
                }
            }
            let BufResult(written, b) = conn.stream.write_all(b).await;
            buf.0 = b;
            written?;
        }
    }
}

/// A server on 127.0.0.1 with an OS-assigned port and no CPU pinning (the
/// tests may run under a restricted affinity mask, and pinning is not what
/// they are checking). `SO_INCOMING_CPU` stays off too: on loopback the
/// "incoming" CPU is the client thread's, which would steer every test
/// connection to whichever worker happens to share it.
pub fn builder(service: Echo, workers: usize, capacity: usize) -> Builder<Echo> {
    Server::builder(service)
        .bind("127.0.0.1:0".parse().unwrap())
        .workers(Workers::Count(workers))
        .capacity(capacity)
        .pin(false)
        .incoming_cpu(false)
        .drain_timeout(Duration::from_secs(2))
}

pub fn connect(addr: SocketAddr) -> TcpStream {
    let s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    s.set_nodelay(true).unwrap();
    s
}

pub fn roundtrip(stream: &mut TcpStream, payload: &[u8]) -> Vec<u8> {
    stream.write_all(payload).expect("write");
    let mut back = vec![0u8; payload.len()];
    stream.read_exact(&mut back).expect("read echo");
    back
}

pub fn echo_once(addr: SocketAddr, payload: &[u8]) -> Vec<u8> {
    let mut s = connect(addr);
    roundtrip(&mut s, payload)
}

/// Poll `cond` until it holds or `timeout` passes. Counters are updated by
/// worker threads after the client has already seen its bytes, so a test that
/// reads them must allow for that.
pub fn eventually(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if cond() {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
