//! A **scatter/gather aggregator** on a single compio runtime: each client query
//! is fanned out to several backend *shards* at once, and their replies are
//! merged into one answer. One [`Pool`] per shard supplies the connections.
//!
//! This is the other shape a connection pool takes in a real service. The HTTP
//! proxy reuses *one* upstream per request; here every request touches *all* the
//! shards, concurrently, and each shard has its own small pool of kept-alive
//! connections. When a request arrives the aggregator [`compio::runtime::spawn`]s
//! one task per shard — each leasing from that shard's [`LocalPool`] — then awaits
//! them and stitches the results together. Several pools, many in-flight leases,
//! structured fan-out per request, all cooperatively scheduled on one thread with
//! no [`std::thread::spawn`] anywhere.
//!
//! To make the concurrency legible each shard sleeps `DELAY_MS` before replying
//! (standing in for real backend work). With N shards the gather still finishes in
//! roughly one `DELAY_MS`, not N of them — the reported per-request time proves the
//! scatter runs the shards in parallel rather than one after another.
//!
//! Everything is in-process: the shard backends are tasks on ephemeral loopback
//! ports, so the example needs nothing external.
//!
//! ```text
//! cargo run --release --example scatter_gather -- [LISTEN] [SHARDS] [CAPACITY]
//!   LISTEN     aggregator bind address     default 127.0.0.1:9000
//!   SHARDS     number of backend shards    default 4
//!   CAPACITY   pooled conns per shard       default 8
//!   DELAY_MS=<n>  simulated per-shard latency (default 5)
//!
//! # send a query line, read the merged answer:
//! printf 'needle\n' | nc -q1 127.0.0.1 9000
//! ```

use std::{io, net::SocketAddr, rc::Rc, time::Duration, time::Instant};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};
use compio_pool::{LocalPool, ManageConnection, Pool};

/// A newline-framed connection: one `io_uring`-bound stream, a read buffer, and a
/// health flag for the pool. Used for the client, the shard clients, and the
/// shard backends alike.
struct Line {
    stream: TcpStream,
    buf: Vec<u8>,
    healthy: bool,
}

impl Line {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: Vec::with_capacity(1024),
            healthy: true,
        }
    }

    /// Read one `\n`-terminated line, trimmed of its line ending. `Ok(None)` means
    /// the peer closed.
    async fn read_line(&mut self) -> io::Result<Option<String>> {
        loop {
            if let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = self.buf.drain(..=pos).collect();
                let text = String::from_utf8_lossy(&line[..line.len() - 1])
                    .trim_end_matches('\r')
                    .to_string();
                return Ok(Some(text));
            }
            // compio reads into spare capacity and grows the length; keep room.
            if self.buf.len() == self.buf.capacity() {
                self.buf.reserve(1024);
            }
            let taken = std::mem::take(&mut self.buf);
            let BufResult(res, b) = self.stream.read(taken).await;
            self.buf = b;
            match res {
                Ok(0) => return Ok(None),
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        }
    }

    /// Write one line, appending the newline.
    async fn write_line(&mut self, text: &str) -> io::Result<()> {
        let mut bytes = text.as_bytes().to_vec();
        bytes.push(b'\n');
        let BufResult(res, _) = self.stream.write_all(bytes).await;
        res
    }
}

/// Opens pooled client connections to one shard backend.
struct Shard {
    addr: SocketAddr,
}

impl ManageConnection for Shard {
    type Connection = Line;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<Line> {
        let stream = TcpStream::connect(self.addr).await?;
        stream.set_nodelay(true)?;
        Ok(Line::new(stream))
    }

    async fn is_valid(&self, conn: &mut Line) -> io::Result<()> {
        if conn.healthy {
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "shard conn marked dead"))
        }
    }

    fn has_broken(&self, conn: &mut Line) -> bool {
        !conn.healthy
    }
}

fn main() -> io::Result<()> {
    let listen: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9000".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad listen addr: {e}")))?;
    let shard_count: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let capacity: u32 = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let delay_ms: u64 = std::env::var("DELAY_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        // Start the in-process shard backends and build one pool per shard.
        let mut shard_pools = Vec::with_capacity(shard_count);
        for id in 0..shard_count {
            let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let listener = TcpListener::bind(any).await?;
            let addr = listener.local_addr()?;
            compio::runtime::spawn(shard_loop(listener, id, delay_ms)).detach();

            let pool = Pool::builder().max_size(capacity).build(Shard { addr });
            shard_pools.push(pool.local());
        }
        let shards = Rc::new(shard_pools);

        let server = TcpListener::bind(listen).await?;
        println!(
            "scatter_gather on {listen}: {shard_count} shards x up to {capacity} pooled conns \
             each, {delay_ms}ms/shard work"
        );
        println!("try: printf 'needle\\n' | nc -q1 {listen}");

        loop {
            let Ok((stream, _peer)) = server.accept().await else {
                continue;
            };
            let _ = stream.set_nodelay(true);
            compio::runtime::spawn(handle_client(shards.clone(), stream)).detach();
        }
    })
}

/// Serve one client connection: for each query line, scatter it to every shard
/// concurrently, gather the replies, and send back the merged line.
async fn handle_client(shards: Rc<Vec<LocalPool<Shard>>>, client_stream: TcpStream) {
    let mut client = Line::new(client_stream);
    loop {
        let query = match client.read_line().await {
            Ok(Some(q)) if !q.is_empty() => q,
            Ok(Some(_)) => continue, // blank line, wait for the next
            _ => return,             // closed or errored
        };

        // Scatter: one task per shard, each leasing from that shard's own pool.
        // They run concurrently on this one runtime while each awaits its shard.
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(shards.len());
        for pool in shards.iter() {
            let pool = pool.clone();
            let q = query.clone();
            tasks.push(compio::runtime::spawn(query_shard(pool, q)));
        }

        // Gather: await the tasks in shard order so the merged line is stable.
        let mut parts = Vec::with_capacity(tasks.len());
        for (id, task) in tasks.into_iter().enumerate() {
            match task.await {
                Ok(Some(reply)) => parts.push(reply),
                _ => parts.push(format!("shard{id}=ERR")),
            }
        }
        let elapsed = started.elapsed();

        let merged = format!(
            "{} | {} shards in {:.1}ms",
            parts.join(" "),
            parts.len(),
            elapsed.as_secs_f64() * 1000.0,
        );
        if client.write_line(&merged).await.is_err() {
            return;
        }
    }
}

/// Lease one connection from a shard's pool, send the query, read the reply, and
/// return the connection to the pool. `None` on any failure (and the connection
/// is marked dead so the pool discards it).
async fn query_shard(pool: LocalPool<Shard>, query: String) -> Option<String> {
    let mut conn = match pool.get().await {
        Ok(conn) => conn,
        Err(_) => return None,
    };
    if conn.write_line(&query).await.is_err() {
        conn.healthy = false;
        return None;
    }
    match conn.read_line().await {
        Ok(Some(reply)) => Some(reply),
        _ => {
            conn.healthy = false;
            None
        }
    }
}

/// One shard backend: accept forever, serve each connection on its own task.
async fn shard_loop(listener: TcpListener, id: usize, delay_ms: u64) {
    loop {
        let Ok((stream, _peer)) = listener.accept().await else {
            continue;
        };
        let _ = stream.set_nodelay(true);
        compio::runtime::spawn(shard_conn(stream, id, delay_ms)).detach();
    }
}

/// Answer each query with this shard's contribution after a simulated delay.
async fn shard_conn(stream: TcpStream, id: usize, delay_ms: u64) {
    let mut conn = Line::new(stream);
    loop {
        let query = match conn.read_line().await {
            Ok(Some(q)) => q,
            _ => return,
        };
        if delay_ms > 0 {
            compio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        // A deterministic, shard-specific "result" so the merged answer varies by
        // shard: the query's byte sum folded into this shard's bucket count.
        let buckets = 10 + id as u64;
        let hits = query.bytes().map(u64::from).sum::<u64>() % buckets;
        if conn.write_line(&format!("shard{id}={hits}")).await.is_err() {
            return;
        }
    }
}
