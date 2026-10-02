//! One worker: a pinned thread, its runtime, its listener, its pool, and the
//! two loops that feed the pool (recipe steps 7–17).

use std::{
    io,
    net::SocketAddr,
    rc::Rc,
    sync::{Arc, mpsc},
    thread,
    time::Duration,
};

use compio::{
    driver::ProactorBuilder,
    net::{TcpListener, TcpStream},
    runtime::Runtime,
};
use flume::TrySendError;

use crate::{
    config::{Config, OverflowPolicy, UringConfig},
    cpu::{self, CoreId},
    handoff::{self, Overflow},
    listener,
    pool::{LocalPool, Permit},
    service::{Connection, Route, Service},
    stats::WorkerStats,
    util::{Either, race},
};

/// What a worker knows about itself. Handed to [`Resource::create`](crate::Resource::create).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkerContext {
    /// 0-based index, in `Workers` order.
    pub index: usize,
    /// The core this worker was assigned.
    pub core: CoreId,
    /// Whether the kernel accepted the pin to `core`.
    pub pinned: bool,
    /// Total number of workers.
    pub workers: usize,
    /// This worker's pool capacity.
    pub capacity: usize,
}

pub(crate) struct Args<S> {
    pub index: usize,
    pub core: CoreId,
    pub workers: usize,
    pub service: S,
    pub addr: SocketAddr,
    pub config: Arc<Config>,
    pub tx: handoff::Sender,
    pub rx: handoff::Receiver,
    pub shutdown: flume::Receiver<()>,
    pub stats: Arc<WorkerStats>,
    /// Reports the bound address once accepting, or the error that stopped it.
    pub ready: mpsc::Sender<io::Result<SocketAddr>>,
}

/// Spawn the worker thread (step 7). Everything else happens inside it.
pub(crate) fn spawn<S: Service>(args: Args<S>) -> io::Result<thread::JoinHandle<()>> {
    thread::Builder::new()
        .name(format!("compio-pool/{}", args.index))
        .spawn(move || run(args))
}

struct Worker<S: Service> {
    index: usize,
    pool: LocalPool<S::Resource>,
    service: S,
    tx: handoff::Sender,
    rx: handoff::Receiver,
    stats: Arc<WorkerStats>,
    config: Arc<Config>,
}

fn run<S: Service>(args: Args<S>) {
    let Args {
        index,
        core,
        workers,
        service,
        addr,
        config,
        tx,
        rx,
        shutdown,
        stats,
        ready,
    } = args;

    // Step 8: pin first, so the ring and every allocation after it are made on
    // the core that will use them.
    let pinned = config.pin && cpu::pin_current(core);
    stats.set_pinned(pinned);

    // Step 10 (runtime half): this thread's own io_uring.
    let runtime = match build_runtime(&config.uring) {
        Ok(runtime) => runtime,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };

    runtime.block_on(async move {
        // Steps 9 and 10 (socket half): a raw SO_REUSEPORT socket, bound here,
        // wrapped into this runtime so accepts complete on this ring.
        let incoming_cpu = config.incoming_cpu.then_some(core.id);
        let std_listener = match listener::bind_reuseport(addr, config.backlog, incoming_cpu) {
            Ok(l) => l,
            Err(e) => {
                let _ = ready.send(Err(e));
                return;
            }
        };
        let bound = match std_listener.local_addr() {
            Ok(a) => a,
            Err(e) => {
                let _ = ready.send(Err(e));
                return;
            }
        };
        let listener = match TcpListener::from_std(std_listener) {
            Ok(l) => l,
            Err(e) => {
                let _ = ready.send(Err(e));
                return;
            }
        };

        // Step 11: the thread-local pool.
        let cx = WorkerContext {
            index,
            core,
            pinned,
            workers,
            capacity: config.capacity,
        };
        let pool = LocalPool::<S::Resource>::new(cx, config.capacity);
        if config.prewarm > 0
            && let Err(e) = pool.prewarm(config.prewarm).await
        {
            let _ = ready.send(Err(e));
            return;
        }

        if ready.send(Ok(bound)).is_err() {
            // The server gave up waiting; nobody will ever shut us down.
            return;
        }

        let worker = Rc::new(Worker {
            index,
            pool,
            service,
            tx,
            rx,
            stats,
            config,
        });

        // Steps 12–15 and 16–17 run side by side on this ring.
        let accept =
            compio::runtime::spawn(accept_loop(worker.clone(), listener, shutdown.clone()));
        let claim = compio::runtime::spawn(claim_loop(worker.clone(), shutdown.clone()));

        // `Err(Disconnected)` is the shutdown signal: the server dropped its
        // sender. Nothing is ever sent on this channel.
        let _ = shutdown.recv_async().await;
        let _ = accept.await;
        let _ = claim.await;

        // Let in-flight connections finish, up to a point. Whatever is still
        // running when the runtime drops is cancelled with it.
        let _ = compio::time::timeout(worker.config.drain_timeout, worker.pool.drained()).await;
    });
}

fn build_runtime(uring: &UringConfig) -> io::Result<Runtime> {
    let mut proactor = ProactorBuilder::new();
    if let Some(entries) = uring.entries {
        proactor.capacity(entries);
    }
    if let Some(idle) = uring.sqpoll_idle {
        proactor.sqpoll_idle(idle);
    } else {
        proactor.coop_taskrun(uring.coop_taskrun);
        proactor.taskrun_flag(uring.coop_taskrun);
    }
    proactor.single_issuer(uring.single_issuer);
    if uring.single_issuer {
        proactor.defer_taskrun(uring.defer_taskrun);
    }
    let mut builder = Runtime::builder();
    builder.with_proactor(proactor);
    builder.build()
}

/// Steps 12–15: accept on this ring; serve here if the pool has room, else
/// detach the fd and push it onto the channel.
async fn accept_loop<S: Service>(
    worker: Rc<Worker<S>>,
    listener: TcpListener,
    shutdown: flume::Receiver<()>,
) {
    loop {
        let accepted = match race(listener.accept(), shutdown.recv_async()).await {
            Either::Left(accepted) => accepted,
            Either::Right(_) => return,
        };
        let (stream, peer) = match accepted {
            Ok(conn) => conn,
            Err(e) => {
                worker.stats.accept_errors();
                if is_resource_exhaustion(&e) {
                    // Out of fds or memory: spinning on accept would only burn
                    // the core. Back off and let in-flight connections finish.
                    compio::time::sleep(Duration::from_millis(10)).await;
                }
                continue;
            }
        };
        worker.stats.accepted();
        if worker.config.nodelay {
            let _ = stream.set_nodelay(true);
        }

        // Step 13: the capacity check, synchronous, at the moment of arrival.
        match worker.pool.try_reserve() {
            Some(permit) => {
                worker.stats.served_local();
                let conn = Connection {
                    stream,
                    peer,
                    worker: worker.index,
                    route: Route::Local,
                    oversubscribed: false,
                };
                compio::runtime::spawn(serve(worker.clone(), permit, conn)).detach();
            }
            None => hand_off(&worker, stream, peer).await,
        }
    }
}

/// Steps 14 and 15.
async fn hand_off<S: Service>(worker: &Rc<Worker<S>>, stream: TcpStream, peer: SocketAddr) {
    let Some(fd) = handoff::detach(stream).await else {
        worker.stats.detach_failed();
        return;
    };
    let overflow = Overflow {
        fd,
        peer,
        from: worker.index,
        hops: 0,
    };
    match worker.tx.try_send(overflow) {
        Ok(()) => worker.stats.handed_off(),
        Err(TrySendError::Full(overflow)) => {
            worker.stats.handoff_full();
            channel_full(worker, overflow);
        }
        // Shutting down; the fd closes with `overflow`.
        Err(TrySendError::Disconnected(_)) => {}
    }
}

/// Every worker is full and so is the queue. Apply the policy.
fn channel_full<S: Service>(worker: &Rc<Worker<S>>, overflow: Overflow) {
    match worker.config.overflow {
        OverflowPolicy::Reject => {
            worker.stats.rejected();
            drop(overflow);
        }
        OverflowPolicy::ServeLocally => {
            let Overflow {
                fd,
                peer,
                from,
                hops,
            } = overflow;
            let stream = match handoff::attach(fd) {
                Ok(stream) => stream,
                Err(_) => {
                    worker.stats.attach_failed();
                    return;
                }
            };
            worker.stats.oversubscribed();
            let route = if from == worker.index && hops == 0 {
                Route::Local
            } else {
                Route::Claimed { from, hops }
            };
            let conn = Connection {
                stream,
                peer,
                worker: worker.index,
                route,
                oversubscribed: true,
            };
            let permit = worker.pool.reserve_unbounded();
            compio::runtime::spawn(serve(worker.clone(), permit, conn)).detach();
        }
    }
}

/// Steps 16 and 17: whenever this worker has a free slot, offer to take a
/// connection off the channel and attach it to this ring.
async fn claim_loop<S: Service>(worker: Rc<Worker<S>>, shutdown: flume::Receiver<()>) {
    loop {
        // Do not take what we cannot serve: wait for room first. The slot is
        // not reserved while we wait on the channel, so an idle worker keeps
        // its whole capacity for its own listener.
        if let Either::Right(_) = race(worker.pool.wait_available(), shutdown.recv_async()).await {
            return;
        }
        let mut overflow = match race(worker.rx.recv_async(), shutdown.recv_async()).await {
            Either::Left(Ok(overflow)) => overflow,
            // Channel or shutdown disconnected.
            Either::Left(Err(_)) | Either::Right(_) => return,
        };

        match worker.pool.try_reserve() {
            Some(permit) => {
                let stream = match handoff::attach(overflow.fd) {
                    Ok(stream) => stream,
                    Err(_) => {
                        worker.stats.attach_failed();
                        continue;
                    }
                };
                if worker.config.nodelay {
                    let _ = stream.set_nodelay(true);
                }
                worker.stats.claimed();
                let conn = Connection {
                    stream,
                    peer: overflow.peer,
                    worker: worker.index,
                    route: Route::Claimed {
                        from: overflow.from,
                        hops: overflow.hops,
                    },
                    oversubscribed: false,
                };
                compio::runtime::spawn(serve(worker.clone(), permit, conn)).detach();
            }
            None => {
                // Our own listener took the slot between `wait_available` and
                // here. Put the connection back for someone else — bounded, so a
                // process that is saturated everywhere does not juggle forever.
                if overflow.hops < worker.config.max_hops {
                    overflow.hops += 1;
                    match worker.tx.try_send(overflow) {
                        Ok(()) => worker.stats.bounced(),
                        Err(TrySendError::Full(overflow)) => {
                            worker.stats.handoff_full();
                            channel_full(&worker, overflow);
                        }
                        Err(TrySendError::Disconnected(_)) => return,
                    }
                } else {
                    channel_full(&worker, overflow);
                }
            }
        }
    }
}

/// Turn the permit into a lease and run the handler. The lease, and with it
/// the slot, is released when this returns, however it returns.
async fn serve<S: Service>(worker: Rc<Worker<S>>, permit: Permit<S::Resource>, conn: Connection) {
    worker.stats.connection_started();
    let mut lease = match permit.acquire().await {
        Ok(lease) => lease,
        Err(_) => {
            worker.stats.resource_errors();
            worker.stats.connection_ended();
            return;
        }
    };
    match worker.service.handle(conn, &mut lease).await {
        Ok(()) => worker.stats.completed(),
        Err(_) => worker.stats.handler_errors(),
    }
    worker.stats.connection_ended();
}

fn is_resource_exhaustion(e: &io::Error) -> bool {
    matches!(e.raw_os_error(), Some(EMFILE | ENFILE | ENOBUFS | ENOMEM))
}

// Spelled out rather than adding `libc` as a direct dependency for four
// constants. These are the asm-generic values, shared by x86, ARM, RISC-V,
// LoongArch, PowerPC and s390x. MIPS, SPARC and Alpha number ENOBUFS (and
// some of the others) differently; there the back-off below simply does not
// trigger and the loop retries at once, which is slower, not wrong.
const ENOMEM: i32 = 12;
const ENFILE: i32 = 23;
const EMFILE: i32 = 24;
const ENOBUFS: i32 = 105;
