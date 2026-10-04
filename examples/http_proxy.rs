//! An **HTTP/1.1 reverse proxy** on a single compio runtime, pooling and reusing
//! keep-alive connections to the origin.
//!
//! This is the pool's headline use made concrete. The proxy accepts client
//! connections (a [`compio::runtime::spawn`] task each — no OS threads), and for
//! every request it leases one upstream connection from a [`Pool`], forwards the
//! request, reads the response, and *returns the upstream to the pool* instead of
//! closing it. A handful of pooled, kept-alive upstreams therefore serve an
//! unbounded stream of client requests — which is the entire reason a connection
//! pool exists.
//!
//! To make the reuse visible without any external service, the example also runs
//! an in-process **origin** on an ephemeral port (unless `ORIGIN=host:port` is
//! set). That origin stamps every response with two headers:
//!
//! * `x-upstream-conn:` — an id unique to each upstream TCP connection, and
//! * `x-upstream-reqs:` — how many requests that one connection has served.
//!
//! Hit the proxy repeatedly and you will see only a few distinct `x-upstream-conn`
//! ids, each with a climbing `x-upstream-reqs` — the pool handing the same warm
//! connections back out, request after request, across *different* client
//! connections.
//!
//! The message framing here is deliberately small: request/response heads are
//! read to the blank line, bodies are taken from `Content-Length` (the bundled
//! origin always sets it). That is enough to forward real `curl` traffic and to
//! show the pool working; it is not a conformant HTTP implementation.
//!
//! ```text
//! cargo run --release --example http_proxy -- [LISTEN] [CAPACITY]
//!   LISTEN     proxy bind address          default 127.0.0.1:8080
//!   CAPACITY   max pooled upstreams         default 16
//!   ORIGIN=host:port  proxy to a real origin instead of the built-in one
//!
//! # watch the same upstream connections get reused:
//! for i in $(seq 5); do curl -si http://127.0.0.1:8080/ | grep -i x-upstream; echo; done
//! ```

use std::{
    io,
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};
use compio_pool::{LocalPool, ManageConnection, Pool};

/// Gives each in-process origin connection a distinct id, so reuse is visible.
static ORIGIN_CONN_ID: AtomicU64 = AtomicU64::new(0);

/// A TCP stream plus a read buffer and a health flag: one `io_uring`-bound
/// connection framed as HTTP messages. Used for both ends and, when pooled, as
/// the upstream the pool hands out.
struct Framed {
    stream: TcpStream,
    buf: Vec<u8>,
    healthy: bool,
}

impl Framed {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: Vec::with_capacity(8 * 1024),
            healthy: true,
        }
    }

    /// Append one `read` into the buffer. `Ok(false)` means the peer closed.
    async fn read_more(&mut self) -> io::Result<bool> {
        // compio reads into the buffer's spare capacity and advances its length,
        // so a read with no room would come back as 0 bytes — reserve first.
        if self.buf.len() == self.buf.capacity() {
            self.buf.reserve(8 * 1024);
        }
        let taken = std::mem::take(&mut self.buf);
        let BufResult(res, b) = self.stream.read(taken).await;
        self.buf = b;
        match res {
            Ok(0) => Ok(false),
            Ok(_) => Ok(true),
            Err(e) => Err(e),
        }
    }

    /// Read one whole HTTP message — head to the blank line, then `Content-Length`
    /// bytes of body — and drain it out of the buffer. `Ok(None)` at a clean
    /// message boundary means the peer is done.
    async fn read_message(&mut self) -> io::Result<Option<Vec<u8>>> {
        // 1. Read until the end of the header block.
        let head_end = loop {
            if let Some(pos) = find(&self.buf, b"\r\n\r\n") {
                break pos + 4;
            }
            if !self.read_more().await? {
                // Clean close at a boundary -> done; mid-head close -> give up too.
                return Ok(None);
            }
        };

        // 2. Read the body named by Content-Length (0 when the header is absent).
        let total = head_end + content_length(&self.buf[..head_end]);
        while self.buf.len() < total {
            if !self.read_more().await? {
                return Ok(None); // truncated body
            }
        }

        // 3. Hand back exactly one message; anything after it stays for the next.
        Ok(Some(self.buf.drain(..total).collect()))
    }

    /// Write a full buffer of bytes.
    async fn write_all_bytes(&mut self, bytes: Vec<u8>) -> io::Result<()> {
        let BufResult(res, _) = self.stream.write_all(bytes).await;
        res
    }
}

/// Opens pooled upstream connections to the origin. A real deployment would point
/// this at an app server; `is_valid`/`has_broken` retire an upstream the proxy
/// flagged after a failed round trip.
struct Origin {
    addr: SocketAddr,
}

impl ManageConnection for Origin {
    type Connection = Framed;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<Framed> {
        let stream = TcpStream::connect(self.addr).await?;
        stream.set_nodelay(true)?;
        Ok(Framed::new(stream))
    }

    async fn is_valid(&self, conn: &mut Framed) -> io::Result<()> {
        if conn.healthy {
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "upstream marked dead"))
        }
    }

    fn has_broken(&self, conn: &mut Framed) -> bool {
        !conn.healthy
    }
}

fn main() -> io::Result<()> {
    let listen: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad listen addr: {e}")))?;
    let capacity: u32 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(16);

    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        // The origin: an external one via ORIGIN, or a built-in loopback one.
        let origin_addr = match std::env::var("ORIGIN") {
            Ok(s) => s.parse().map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("bad ORIGIN: {e}"))
            })?,
            Err(_) => {
                let any: SocketAddr = "127.0.0.1:0".parse().unwrap();
                let origin = TcpListener::bind(any).await?;
                let addr = origin.local_addr()?;
                compio::runtime::spawn(origin_loop(origin)).detach();
                addr
            }
        };

        let pool = Pool::builder()
            .max_size(capacity)
            .build(Origin { addr: origin_addr });
        let local = pool.local();

        let proxy = TcpListener::bind(listen).await?;
        println!(
            "http_proxy on {listen} -> origin {origin_addr}: 1 runtime, up to {capacity} pooled \
             upstreams"
        );
        println!("try: for i in $(seq 5); do curl -si http://{listen}/ | grep -i x-upstream; done");

        loop {
            let Ok((stream, _peer)) = proxy.accept().await else {
                continue;
            };
            let _ = stream.set_nodelay(true);
            compio::runtime::spawn(handle_client(local.clone(), stream)).detach();
        }
    })
}

/// Serve one client connection: for each request, lease an upstream, forward the
/// bytes, read the response, hand the upstream back, and reply — keeping the
/// client connection open for the next request.
async fn handle_client(local: LocalPool<Origin>, client_stream: TcpStream) {
    let mut client = Framed::new(client_stream);
    loop {
        let request = match client.read_message().await {
            Ok(Some(req)) => req,
            _ => return, // client closed or sent a malformed head
        };

        // Lease one upstream for this round trip. The same few upstreams are
        // reused across every request and every client — the point of the pool.
        let mut up = match local.get().await {
            Ok(up) => up,
            Err(_) => {
                let _ = client.write_all_bytes(bad_gateway("no upstream available")).await;
                return;
            }
        };

        // Forward the request and read one response back.
        let response = if up.write_all_bytes(request).await.is_ok() {
            up.read_message().await
        } else {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "upstream write failed"))
        };

        match response {
            Ok(Some(resp)) => {
                drop(up); // healthy: return it to the pool before touching the client
                if client.write_all_bytes(resp).await.is_err() {
                    return;
                }
            }
            _ => {
                up.healthy = false; // dropping now discards it instead of pooling it
                let _ = client.write_all_bytes(bad_gateway("upstream error")).await;
                return;
            }
        }
    }
}

/// The built-in origin: accept forever, each connection served on its own task
/// with a distinct id so the proxy's clients can see which upstream served them.
async fn origin_loop(listener: TcpListener) {
    loop {
        let Ok((stream, _peer)) = listener.accept().await else {
            continue;
        };
        let _ = stream.set_nodelay(true);
        let id = ORIGIN_CONN_ID.fetch_add(1, Ordering::Relaxed);
        compio::runtime::spawn(origin_conn(stream, id)).detach();
    }
}

/// One origin connection: reply to every request, reporting its id and how many
/// requests it has now served so reuse is visible end to end.
async fn origin_conn(stream: TcpStream, id: u64) {
    let mut conn = Framed::new(stream);
    let mut served: u64 = 0;
    loop {
        match conn.read_message().await {
            Ok(Some(_req)) => {
                served += 1;
                let body = format!("served by upstream connection #{id}, request #{served}\n");
                let resp = format!(
                    "HTTP/1.1 200 OK\r\n\
                     content-type: text/plain\r\n\
                     content-length: {}\r\n\
                     x-upstream-conn: {id}\r\n\
                     x-upstream-reqs: {served}\r\n\
                     connection: keep-alive\r\n\
                     \r\n\
                     {body}",
                    body.len()
                );
                if conn.write_all_bytes(resp.into_bytes()).await.is_err() {
                    return;
                }
            }
            _ => return,
        }
    }
}

/// A minimal `502` response for when the upstream is unavailable or fails.
fn bad_gateway(why: &str) -> Vec<u8> {
    let body = format!("502 Bad Gateway: {why}\n");
    format!(
        "HTTP/1.1 502 Bad Gateway\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

/// The `Content-Length` of an HTTP head, or 0 if it has none. Header names are
/// case-insensitive, so match on a lowercased copy.
fn content_length(head: &[u8]) -> usize {
    let text = String::from_utf8_lossy(head).to_ascii_lowercase();
    for line in text.split("\r\n") {
        if let Some(value) = line.strip_prefix("content-length:") {
            return value.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// Find the first occurrence of `needle` in `hay`.
fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || hay.len() < needle.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}
