//! Starting, observing and stopping the workers.

use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::Duration,
};

use crate::{
    config::{Config, OverflowPolicy, UringConfig, Workers},
    cpu::{self, CoreId},
    handoff,
    service::Service,
    stats::{Stats, WorkerStats},
    worker,
};

/// Configures and starts a [`Server`].
#[derive(Debug)]
pub struct Builder<S> {
    service: S,
    config: Config,
}

impl<S: Service> Builder<S> {
    /// Start from [`Config::default`].
    pub fn new(service: S) -> Self {
        Self {
            service,
            config: Config::default(),
        }
    }

    /// Replace the whole configuration.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// See [`Config::addr`].
    pub fn bind(mut self, addr: SocketAddr) -> Self {
        self.config.addr = addr;
        self
    }

    /// See [`Config::workers`].
    pub fn workers(mut self, workers: Workers) -> Self {
        self.config.workers = workers;
        self
    }

    /// See [`Config::capacity`]. Per worker.
    pub fn capacity(mut self, capacity: usize) -> Self {
        self.config.capacity = capacity;
        self
    }

    /// See [`Config::prewarm`].
    pub fn prewarm(mut self, prewarm: usize) -> Self {
        self.config.prewarm = prewarm;
        self
    }

    /// See [`Config::handoff_capacity`].
    pub fn handoff_capacity(mut self, capacity: usize) -> Self {
        self.config.handoff_capacity = capacity;
        self
    }

    /// See [`Config::max_hops`].
    pub fn max_hops(mut self, hops: u8) -> Self {
        self.config.max_hops = hops;
        self
    }

    /// See [`Config::overflow`].
    pub fn overflow(mut self, policy: OverflowPolicy) -> Self {
        self.config.overflow = policy;
        self
    }

    /// See [`Config::backlog`].
    pub fn backlog(mut self, backlog: i32) -> Self {
        self.config.backlog = backlog;
        self
    }

    /// See [`Config::nodelay`].
    pub fn nodelay(mut self, nodelay: bool) -> Self {
        self.config.nodelay = nodelay;
        self
    }

    /// See [`Config::incoming_cpu`].
    pub fn incoming_cpu(mut self, enable: bool) -> Self {
        self.config.incoming_cpu = enable;
        self
    }

    /// See [`Config::pin`].
    pub fn pin(mut self, pin: bool) -> Self {
        self.config.pin = pin;
        self
    }

    /// See [`Config::drain_timeout`].
    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.config.drain_timeout = timeout;
        self
    }

    /// See [`Config::uring`].
    pub fn uring(mut self, uring: UringConfig) -> Self {
        self.config.uring = uring;
        self
    }

    /// Spawn every worker and return once all of them are accepting.
    ///
    /// Worker 0 binds first so that a port of 0 is resolved once; the others
    /// bind the port it got. Any worker failing to bind, build its ring or
    /// prewarm its pool stops the rest and is returned as the error.
    pub fn start(self) -> io::Result<Server> {
        let Builder { service, config } = self;
        config.validate()?;
        let cores = cpu::resolve(&config.workers)?;
        let config = Arc::new(config);
        let (tx, rx) = handoff::channel(config.handoff_capacity);
        let (shutdown_tx, shutdown_rx) = flume::bounded::<()>(1);

        let mut server = Server {
            addr: config.addr,
            config: config.clone(),
            cores: cores.clone(),
            stats: Vec::with_capacity(cores.len()),
            tx: tx.clone(),
            shutdown: Mutex::new(Some(shutdown_tx)),
            threads: Mutex::new(Vec::with_capacity(cores.len())),
        };

        let spawn_one = |server: &mut Server, index: usize, addr: SocketAddr| {
            let stats = Arc::new(WorkerStats::new(index, cores[index].id));
            let (ready_tx, ready_rx) = mpsc::channel();
            let handle = worker::spawn(worker::Args {
                index,
                core: cores[index],
                workers: cores.len(),
                service: service.clone(),
                addr,
                config: config.clone(),
                tx: tx.clone(),
                rx: rx.clone(),
                shutdown: shutdown_rx.clone(),
                stats: stats.clone(),
                ready: ready_tx,
            })?;
            server.stats.push(stats);
            server.threads.lock().unwrap().push(handle);
            Ok::<_, io::Error>(ready_rx)
        };

        let wait_ready =
            |index: usize, ready: mpsc::Receiver<io::Result<SocketAddr>>| match ready.recv() {
                Ok(result) => result,
                Err(_) => Err(io::Error::other(format!(
                    "worker {index} exited before it started accepting"
                ))),
            };

        // Worker 0 alone, so its bound address can seed the others.
        let first = spawn_one(&mut server, 0, config.addr)?;
        server.addr = wait_ready(0, first)?;

        let addr = server.addr;
        let mut pending = Vec::with_capacity(cores.len());
        for index in 1..cores.len() {
            pending.push((index, spawn_one(&mut server, index, addr)?));
        }
        for (index, ready) in pending {
            wait_ready(index, ready)?;
        }
        // `server` dropping on any `?` above signals shutdown and joins whatever
        // did start, so a failed start leaves no thread behind.
        Ok(server)
    }
}

/// A running set of workers. Dropping it stops them and waits for them.
#[derive(Debug)]
pub struct Server {
    addr: SocketAddr,
    config: Arc<Config>,
    cores: Vec<CoreId>,
    stats: Vec<Arc<WorkerStats>>,
    tx: handoff::Sender,
    shutdown: Mutex<Option<flume::Sender<()>>>,
    threads: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl Server {
    /// Begin configuring a server around `service`.
    pub fn builder<S: Service>(service: S) -> Builder<S> {
        Builder::new(service)
    }

    /// The address every worker is listening on. The port is concrete even if
    /// the configuration asked for 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// The configuration the workers run with.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// The cores the workers were assigned, in worker order. Pass these to
    /// `scripts/tune-nic.sh --cpus`.
    pub fn cores(&self) -> &[CoreId] {
        &self.cores
    }

    /// Number of workers.
    pub fn workers(&self) -> usize {
        self.cores.len()
    }

    /// Counters for every worker, their sum, and the handoff queue depth.
    pub fn stats(&self) -> Stats {
        Stats::collect(&self.stats, self.tx.len(), self.config.handoff_capacity)
    }

    /// Tell every worker to stop accepting and claiming, then drain for up to
    /// [`Config::drain_timeout`]. Returns at once; [`join`](Self::join) waits.
    pub fn shutdown(&self) {
        let Some(sender) = self.shutdown.lock().unwrap().take() else {
            return;
        };
        // Dropping the last sender disconnects the channel, which wakes every
        // worker task blocked on it — synchronously, on *this* thread. compio's
        // executor aborts the process if one of its task wakers runs on a thread
        // that is already unwinding (its panic guard cannot tell our panic from
        // its own). So when we are being dropped by a panic, as a test or a
        // `?`-heavy main easily is, the drop is handed to a thread that is not.
        if thread::panicking() {
            let _ = thread::Builder::new()
                .name("compio-pool/shutdown".into())
                .spawn(move || drop(sender))
                .map(|handle| handle.join());
        } else {
            drop(sender);
        }
    }

    /// Shut down and wait for every worker thread to exit. Returns the first
    /// worker panic, if any.
    pub fn join(self) -> thread::Result<()> {
        self.shutdown();
        let threads = std::mem::take(&mut *self.threads.lock().unwrap());
        let mut result = Ok(());
        for handle in threads {
            if let Err(panic) = handle.join()
                && result.is_ok()
            {
                result = Err(panic);
            }
        }
        result
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown();
        let threads = std::mem::take(&mut *self.threads.lock().unwrap());
        for handle in threads {
            let _ = handle.join();
        }
    }
}
