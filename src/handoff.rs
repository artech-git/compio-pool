//! Moving a connection between rings (recipe steps 6, 14, 15 and 17).
//!
//! A [`compio::net::TcpStream`] is bound to the `io_uring` of the thread that
//! created it and is `!Send`. What *can* cross threads is the file descriptor
//! underneath. [`detach`] takes the stream apart and gives back the
//! [`OwnedFd`]; [`attach`] does the reverse on another thread, whose runtime
//! then owns it. Between the two, the fd rides in an [`Overflow`] on one
//! bounded [`flume`] channel shared by every worker.

use std::{future::Future, io, net::SocketAddr, os::fd::OwnedFd};

use compio::{driver::ToSharedFd, net::TcpStream};

/// A connection in transit between workers. Dropping it closes the socket.
#[derive(Debug)]
pub struct Overflow {
    /// The socket, detached from any ring.
    pub fd: OwnedFd,
    /// Peer address, carried along so the claimer does not have to ask again.
    pub peer: SocketAddr,
    /// Index of the worker that accepted it.
    pub from: usize,
    /// Times it has been pushed back after a failed claim.
    pub hops: u8,
}

/// The sending half every worker holds.
pub type Sender = flume::Sender<Overflow>;
/// The receiving half every worker holds.
pub type Receiver = flume::Receiver<Overflow>;

/// The single bounded MPMC channel (step 6). `capacity` is how many accepted
/// connections can wait for a free core at once, process-wide.
pub fn channel(capacity: usize) -> (Sender, Receiver) {
    flume::bounded(capacity)
}

/// Strip a stream of its runtime binding and return the owned fd (step 14).
///
/// `None` means another handle to the same socket is still alive and the fd
/// could not be taken; the socket is then closed when that handle goes away.
/// For a freshly accepted stream with nothing submitted against it, this
/// resolves immediately and never fails.
pub fn detach(stream: TcpStream) -> impl Future<Output = Option<OwnedFd>> {
    // The stream's own reference is dropped first, so the shared fd we hold is
    // the only one left unless an operation is still in flight.
    let shared = stream.to_shared_fd();
    drop(stream);
    async move {
        match shared.try_unwrap() {
            Ok(socket) => Some(OwnedFd::from(socket)),
            // An operation still holds a reference: wait for it to finish.
            Err(shared) => shared.take().await.map(OwnedFd::from),
        }
    }
}

/// Rebuild a stream from a raw fd on the **current** runtime (step 17).
///
/// Must be called on a worker thread, inside its runtime: that is what makes
/// the socket belong to this thread's ring from here on.
pub fn attach(fd: OwnedFd) -> io::Result<TcpStream> {
    TcpStream::from_std(std::net::TcpStream::from(fd))
}
