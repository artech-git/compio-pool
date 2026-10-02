//! What the user plugs in: the per-connection handler.

use std::{future::Future, io, net::SocketAddr};

use compio::net::TcpStream;

use crate::pool::Resource;

/// How a connection reached the worker that is serving it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    /// Accepted by this worker's own listener and served here (step 13).
    Local,
    /// Accepted by worker `from`, handed off through the channel because that
    /// worker was full, and claimed here (steps 14–17). `hops` counts how many
    /// times it was pushed back onto the channel after the first handoff.
    Claimed {
        /// Index of the worker whose listener accepted it.
        from: usize,
        /// Re-pushes after the first handoff. Usually 0.
        hops: u8,
    },
}

/// An accepted connection, attached to the ring of the worker it is handed to.
#[derive(Debug)]
pub struct Connection {
    /// The stream. It belongs to the current worker's runtime and must not
    /// leave the thread.
    pub stream: TcpStream,
    /// The peer address, as reported by `accept`.
    pub peer: SocketAddr,
    /// Index of the worker serving it.
    pub worker: usize,
    /// Whether it was served where it was accepted or claimed from the channel.
    pub route: Route,
    /// True when it was admitted over `capacity` because the handoff channel
    /// was full and the policy is
    /// [`OverflowPolicy::ServeLocally`](crate::OverflowPolicy::ServeLocally).
    pub oversubscribed: bool,
}

/// The per-connection handler.
///
/// One clone lives on each worker, so `Clone + Send` is required of the service
/// value itself; the futures it returns run on one thread and are not.
pub trait Service: Clone + Send + 'static {
    /// What each in-flight connection borrows from the worker's pool.
    type Resource: Resource;

    /// Serve one connection to completion. The resource is leased for the
    /// duration and returned to the pool when this future ends, however it ends.
    fn handle(
        &self,
        conn: Connection,
        resource: &mut Self::Resource,
    ) -> impl Future<Output = io::Result<()>>;
}
