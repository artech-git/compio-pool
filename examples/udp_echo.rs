//! A connectionless sibling of `echo.rs`: a thread-per-core **UDP** echo server.
//!
//! `SO_REUSEPORT` works for datagram sockets too, so each pinned worker binds the
//! same address and the kernel load-balances incoming datagrams across the group
//! by flow hash — the same trick the TCP examples use, one layer down. There is
//! no accept loop and no connection; each worker just loops `recv_from` ->
//! `send_to` on its own socket, borrowing a datagram buffer from this thread's
//! [`Pool`] for each packet.
//!
//! The crate's `bind_reuseport` is TCP-only, so the few lines of `socket2` for a
//! `SO_REUSEPORT` *datagram* socket live here in the example.
//!
//! ```text
//! cargo run --release --example udp_echo -- [ADDR] [CAPACITY]
//!   ADDR       bind address               default 0.0.0.0:7000
//!   CAPACITY   datagram buffers per worker default 1024
//!
//! # try it (sends one datagram, prints the echo):
//! echo -n hello | nc -u -w1 127.0.0.1 7000
//! ```

use std::{io, net::SocketAddr, thread};

use compio::{BufResult, net::UdpSocket, runtime::Runtime};
use compio_pool::{ManageConnection, Pool, cpu};
use socket2::{Domain, Protocol, Socket, Type};

/// Hands out reusable 64 KiB datagram buffers — one large enough for any single
/// UDP payload — one per packet in flight.
struct Buffers;

impl ManageConnection for Buffers {
    type Connection = Vec<u8>;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<Vec<u8>> {
        Ok(Vec::with_capacity(64 * 1024))
    }

    async fn is_valid(&self, _buf: &mut Vec<u8>) -> io::Result<()> {
        Ok(())
    }

    fn has_broken(&self, _buf: &mut Vec<u8>) -> bool {
        false
    }
}

/// A `SO_REUSEPORT` UDP socket, the datagram counterpart of
/// [`compio_pool::bind_reuseport`]. Both reuse flags go on before `bind`, which
/// is what lets every worker bind the same address and have the kernel split
/// datagrams across them.
fn bind_reuseport_udp(addr: SocketAddr) -> io::Result<std::net::UdpSocket> {
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

fn main() -> io::Result<()> {
    let addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:7000".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let capacity: u32 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024);

    let pool = Pool::builder().max_size(capacity).build(Buffers);

    let cores = cpu::cores();
    if cores.is_empty() {
        return Err(io::Error::new(io::ErrorKind::Unsupported, "no cores to run on"));
    }
    println!(
        "udp_echo on {addr}: {} workers, capacity {capacity}/worker",
        cores.len()
    );

    let mut handles = Vec::new();
    for (index, core) in cores.into_iter().enumerate() {
        let pool = pool.clone();
        let handle = thread::Builder::new()
            .name(format!("worker/{index}"))
            .spawn(move || worker(core, addr, pool))?;
        handles.push(handle);
    }
    for handle in handles {
        let _ = handle.join();
    }
    Ok(())
}

/// One pinned thread: its own runtime, its own `SO_REUSEPORT` datagram socket,
/// and its own [`LocalPool`] of datagram buffers.
fn worker(core: cpu::CoreId, addr: SocketAddr, pool: Pool<Buffers>) {
    cpu::pin_current(core);

    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        let local = pool.local();

        let std_socket = bind_reuseport_udp(addr).expect("bind SO_REUSEPORT UDP");
        let socket = UdpSocket::from_std(std_socket).expect("wrap socket in ring");

        // No accept loop for a datagram socket: one task owns the socket and
        // echoes each packet. Borrow a buffer per datagram so the working set is
        // bounded and reused rather than reallocated.
        loop {
            let mut lease = match local.get().await {
                Ok(lease) => lease,
                Err(_) => continue,
            };

            let mut buf = std::mem::take(&mut *lease);
            buf.clear();
            let BufResult(res, buf) = socket.recv_from(buf).await;
            let (buf, peer) = match res {
                // After `recv_from` the buffer's length is the datagram size, so
                // sending it straight back echoes exactly those bytes.
                Ok((_n, peer)) => (buf, peer),
                Err(_) => {
                    *lease = buf;
                    continue;
                }
            };

            let BufResult(_sent, buf) = socket.send_to(buf, peer).await;
            *lease = buf; // return the buffer to the pool on drop
        }
    });
}
