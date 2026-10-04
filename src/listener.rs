//! A `SO_REUSEPORT` listener helper, for building your own thread-per-core
//! accept loop around a [`Pool`](crate::Pool).

use std::{io, net::SocketAddr};

use socket2::{Domain, Protocol, Socket, Type};

/// Create, configure, bind and listen a raw TCP socket that shares its address
/// with the other threads' listeners.
///
/// `SO_REUSEADDR` and `SO_REUSEPORT` are set **before** `bind`, which is what
/// lets every thread bind the same address and makes the kernel split incoming
/// connections across the group by flow hash. With `incoming_cpu`, the socket
/// also carries `SO_INCOMING_CPU = cpu`, so when packet steering delivers a flow
/// on that CPU the kernel prefers this listener for it.
///
/// The result is a plain [`std::net::TcpListener`]: wrap it in the thread's
/// runtime with [`compio::net::TcpListener::from_std`], which attaches it to that
/// thread's ring.
pub fn bind_reuseport(
    addr: SocketAddr,
    backlog: i32,
    incoming_cpu: Option<usize>,
) -> io::Result<std::net::TcpListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    if let Some(cpu) = incoming_cpu {
        set_incoming_cpu(&socket, cpu);
    }
    socket.bind(&addr.into())?;
    socket.listen(backlog)?;
    Ok(socket.into())
}

/// `SO_INCOMING_CPU` is a hint, and an old kernel or a non-Linux build has no
/// way to honour it, so a failure here is not a reason to refuse to start.
#[cfg(target_os = "linux")]
fn set_incoming_cpu(socket: &Socket, cpu: usize) {
    let _ = socket.set_cpu_affinity(cpu);
}

#[cfg(not(target_os = "linux"))]
fn set_incoming_cpu(_socket: &Socket, _cpu: usize) {}
