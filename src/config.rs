//! Server configuration.

use std::{io, net::SocketAddr, time::Duration};

/// Which cores get a worker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Workers {
    /// One worker per core in this process's affinity mask (`taskset`,
    /// cgroup cpusets and the like are respected).
    AllCores,
    /// Exactly this many workers, assigned to cores round-robin from the
    /// affinity mask. More workers than cores shares cores.
    Count(usize),
    /// Workers on exactly these CPU ids, in this order. This is the list to
    /// pass to `scripts/tune-nic.sh --cpus` so queue *i* feeds worker *i*.
    Cores(Vec<usize>),
}

/// What to do with a connection when the handoff channel is full, which means
/// every worker is saturated *and* the queue of waiting connections is full.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Serve it on the accepting core anyway, over `capacity`. The pool creates
    /// an extra resource for it and drops that resource when the connection
    /// ends. Counted in [`Counters::oversubscribed`](crate::Counters::oversubscribed).
    #[default]
    ServeLocally,
    /// Close it. Counted in [`Counters::rejected`](crate::Counters::rejected).
    Reject,
}

/// `io_uring` setup for each worker's ring. Every field maps to a flag of
/// `io_uring_setup(2)` through compio's `ProactorBuilder`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UringConfig {
    /// Submission queue entries. `None` keeps compio's default.
    pub entries: Option<u32>,
    /// `IORING_SETUP_COOP_TASKRUN` + `IORING_SETUP_TASKRUN_FLAG` (Linux 5.19+):
    /// run completion work when the worker enters the kernel instead of
    /// interrupting it. Ignored when `sqpoll_idle` is set, since the kernel
    /// rejects the combination.
    pub coop_taskrun: bool,
    /// `IORING_SETUP_SINGLE_ISSUER` (Linux 6.0+). True by definition here: the
    /// ring is created and used by one pinned thread.
    pub single_issuer: bool,
    /// `IORING_SETUP_DEFER_TASKRUN` (Linux 6.1+). Needs `single_issuer`.
    pub defer_taskrun: bool,
    /// Enable `IORING_SETUP_SQPOLL` with this idle time. Off by default: a
    /// polling kernel thread per worker is a different trade-off than
    /// thread-per-core, and it steals the core the worker was pinned to.
    pub sqpoll_idle: Option<Duration>,
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            entries: None,
            coop_taskrun: true,
            single_issuer: true,
            defer_taskrun: false,
            sqpoll_idle: None,
        }
    }
}

/// Everything a [`Server`](crate::Server) is built from. Build one with
/// [`Builder`](crate::Builder) or fill the fields directly.
#[derive(Clone, Debug)]
pub struct Config {
    /// Address every worker's listener binds. Port 0 is resolved once, by the
    /// first worker, and the rest bind the port it was given.
    pub addr: SocketAddr,
    /// Which cores get a worker.
    pub workers: Workers,
    /// Connections one worker serves at a time. **Per worker**, not per
    /// process: the global ceiling is `capacity × workers`, plus the handoff
    /// queue, plus whatever [`OverflowPolicy::ServeLocally`] admits.
    pub capacity: usize,
    /// Resources to create on each worker before it starts accepting, so the
    /// first `prewarm` connections do not pay for creation. Capped at `capacity`.
    pub prewarm: usize,
    /// Slots in the handoff channel: how many accepted-but-unserved connections
    /// can wait for a free core across the whole process.
    pub handoff_capacity: usize,
    /// Times a connection may be pushed back onto the channel after a worker
    /// took it and then lost its free slot to a local accept. At the limit it is
    /// served over capacity (or rejected, per `overflow`) instead of bouncing again.
    pub max_hops: u8,
    /// Policy when the handoff channel is full.
    pub overflow: OverflowPolicy,
    /// `listen(2)` backlog for each worker's listener.
    pub backlog: i32,
    /// Set `TCP_NODELAY` on every accepted stream.
    pub nodelay: bool,
    /// Set `SO_INCOMING_CPU` on each listener to its worker's core: the socket
    /// half of recipe step 4. When the NIC's queues are pinned so that a flow's
    /// packets arrive on the core whose worker owns them, the kernel then
    /// prefers that worker's listener for the flow, and accept, I/O and the
    /// interrupt all share one cache.
    ///
    /// **Off by default, on purpose.** The kernel picks the listener whose
    /// `SO_INCOMING_CPU` matches the CPU the SYN arrived on, and only falls back
    /// to the flow hash when none matches. Without the steering from
    /// `scripts/tune-nic.sh` every SYN arrives on the same CPU (or, on loopback,
    /// on the sender's CPU), so turning this on *before* the NIC is tuned
    /// funnels every connection to one worker. Turn it on after the script has
    /// run, and only then.
    pub incoming_cpu: bool,
    /// Pin each worker thread to its core. Turning this off keeps the
    /// per-worker structure but lets the scheduler move threads.
    pub pin: bool,
    /// On shutdown, how long each worker waits for in-flight connections to
    /// finish before its runtime is torn down.
    pub drain_timeout: Duration,
    /// Per-worker `io_uring` setup.
    pub uring: UringConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            workers: Workers::AllCores,
            capacity: 1024,
            prewarm: 0,
            handoff_capacity: 4096,
            max_hops: 4,
            overflow: OverflowPolicy::default(),
            backlog: 4096,
            nodelay: true,
            incoming_cpu: false,
            pin: true,
            drain_timeout: Duration::from_secs(5),
            uring: UringConfig::default(),
        }
    }
}

impl Config {
    /// Check the values that would otherwise fail in a worker thread.
    pub fn validate(&self) -> io::Result<()> {
        fn invalid(msg: &str) -> io::Error {
            io::Error::new(io::ErrorKind::InvalidInput, msg)
        }
        if self.capacity == 0 {
            return Err(invalid("capacity must be at least 1"));
        }
        if self.handoff_capacity == 0 {
            return Err(invalid("handoff_capacity must be at least 1"));
        }
        if self.backlog <= 0 {
            return Err(invalid("backlog must be positive"));
        }
        match &self.workers {
            Workers::Count(0) => return Err(invalid("Workers::Count must be at least 1")),
            Workers::Cores(cores) if cores.is_empty() => {
                return Err(invalid("Workers::Cores must name at least one core"));
            }
            _ => {}
        }
        if self.uring.defer_taskrun && !self.uring.single_issuer {
            return Err(invalid("uring.defer_taskrun requires uring.single_issuer"));
        }
        Ok(())
    }
}
