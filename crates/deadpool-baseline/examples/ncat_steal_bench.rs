//! The tokio + deadpool answer to the question `examples/ncat_steal_bench.rs`
//! asks of compio-pool: given a pool, what does it cost to move a connection
//! between threads, and what does it save?
//!
//! Same server (`ncat` echoing through `/bin/cat`), same load, same two-phase
//! shape, same reported numbers — so the two can be read side by side.
//!
//! # Why the arms are different here
//!
//! compio-pool shards per thread, so the parent example toggles the one thing
//! that lets a connection leave its shard: the exchange. deadpool has no
//! shards. One `Vec` behind one `Mutex`, and a tokio `TcpStream` is `Send`, so
//! migration is not a feature to switch on — it is the only thing the pool
//! does. Toggling it means changing the *topology* instead:
//!
//! | arm | topology | stands in for |
//! |---|---|---|
//! | **split, pool per half** | a runtime and a pool per half | `NoExchange`: phase B cannot see phase A's sockets, so it dials |
//! | **split, shared pool**   | a runtime per half, one pool  | `Reservoir`: phase B pops what phase A returned |
//! | **single runtime**       | one runtime, one pool         | what you would actually write, and the honest throughput baseline |
//!
//! The first two halve the worker threads exactly as the parent example halves
//! the dispatchers. The third gets all `THREADS` of them in one work-stealing
//! scheduler, because that is the shape a tokio service really has.
//!
//! # The catch the arms are built around
//!
//! A tokio `TcpStream` is `Send`, but it is not free of its origin: the fd is
//! registered with the reactor of the runtime that dialled it. Using it from
//! another runtime works only while that first runtime is *alive and driving*.
//! Shut it down and every socket it registered starts failing with
//!
//! ```text
//! A Tokio 1.x context was found, but it is being shutdown.
//! ```
//!
//! — and deadpool will not notice, because `recycle` sees a healthy socket
//! (`peer_addr` is a pure syscall) and hands it straight back out. `caveat()`
//! at the bottom proves it.
//!
//! So the shared-pool arm keeps the warm runtime alive through phase B, just as
//! the parent example keeps the warm dispatcher alive. The reason is not the
//! same: there it is a *choice* about where idle sockets should sit, and
//! `Detach::attach` re-registers the fd with whichever driver claims it. Here
//! it is a *requirement*, and there is no hook to re-register anything.
//!
//! # Running
//!
//! ```text
//! cargo run --release -p deadpool-baseline --example ncat_steal_bench
//! ```
//!
//! Needs `ncat` on `PATH`. Tunables: `THREADS` (4, split in half), `ROUNDS`
//! (2000), `PAYLOAD` (64 bytes), `NCAT_ADDR` to reuse a running server.

#[cfg(not(unix))]
fn main() {
    eprintln!("this example is unix-only; the ncat server it drives execs /bin/cat");
}

#[cfg(unix)]
fn main() -> std::io::Result<()> {
    unix::run()
}

#[cfg(unix)]
mod unix {
    use std::{
        io,
        net::{SocketAddr, TcpListener as StdListener},
        process::{Child, Command, Stdio},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
        time::{Duration, Instant},
    };

    use deadpool::{
        Runtime,
        managed::{Metrics, Pool as DeadPool, RecycleResult},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        runtime::Runtime as Tokio,
    };

    struct Params {
        half: usize,
        rounds: usize,
        payload: usize,
    }

    fn env_usize(key: &str, default: usize) -> usize {
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    }

    // ----------------------------------------------------------- the manager

    struct Ncat {
        addr: SocketAddr,
        dials: Arc<AtomicU64>,
    }

    impl deadpool::managed::Manager for Ncat {
        type Type = TcpStream;
        type Error = io::Error;

        async fn create(&self) -> io::Result<TcpStream> {
            self.dials.fetch_add(1, Relaxed);
            let conn = TcpStream::connect(self.addr).await?;
            conn.set_nodelay(true)?;
            Ok(conn)
        }

        // The same liveness check the compio manager makes, so neither side is
        // paying for a health probe the other skips. Note what it cannot see:
        // a socket whose reactor has gone away still has a peer address.
        async fn recycle(
            &self,
            conn: &mut TcpStream,
            _metrics: &Metrics,
        ) -> RecycleResult<io::Error> {
            conn.peer_addr().map(|_| ())?;
            Ok(())
        }
    }

    type Pool = DeadPool<Ncat>;

    /// `max_size` is process-wide here, unlike compio-pool's per-shard cap, so
    /// it is set wide enough that the semaphore is never what a worker waits on
    /// — the comparison is about where a connection comes from, not queueing.
    /// deadpool has no `min_idle`: every returned connection is simply idle in
    /// the one shared list, which is the closest thing it has to the parent
    /// example's `min_idle(0)`.
    fn build_pool(addr: SocketAddr, dials: Arc<AtomicU64>, params: &Params) -> Pool {
        Pool::builder(Ncat { addr, dials })
            .max_size(params.half * 2)
            .wait_timeout(Some(Duration::from_secs(10)))
            .runtime(Runtime::Tokio1)
            .build()
            .expect("pool config")
    }

    async fn round_trip(conn: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
        conn.write_all(payload).await?;
        let mut echoed = vec![0u8; payload.len()];
        conn.read_exact(&mut echoed).await?;
        assert_eq!(echoed, payload, "echo mismatch");
        Ok(())
    }

    // --------------------------------------------------------------- results

    struct Phase {
        requests: usize,
        wall: Duration,
        /// The first checkout of each worker: a handshake, or a steal.
        cold_start: Duration,
        dials: u64,
    }

    impl Phase {
        fn per_second(&self) -> f64 {
            self.requests as f64 / self.wall.as_secs_f64()
        }
    }

    struct Arm {
        label: &'static str,
        warm: Phase,
        cold: Phase,
    }

    impl Arm {
        fn report(&self) {
            println!("{}", self.label);
            for (name, p) in [("  A warm", &self.warm), ("  B cold", &self.cold)] {
                println!(
                    "{name}  {:>9.0} req/s   cold-start acquire {:>9.1?}   {:>4} dials",
                    p.per_second(),
                    p.cold_start,
                    p.dials,
                );
            }
        }
    }

    // ------------------------------------------------------------- one phase

    /// Runs `half` tasks, each doing `rounds` round trips, on `rt`. Returns the
    /// wall time and the mean first-checkout latency.
    ///
    /// `half` tasks on `half` worker threads is the same *work* as the parent
    /// example's one-task-per-thread dispatch, but not the same guarantee:
    /// tokio work-steals, so nothing pins a task to a thread and nothing keeps
    /// two of them off the same one.
    fn drive(rt: &Tokio, pool: &Pool, params: &Params) -> (Duration, Duration) {
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..params.half {
            let pool = pool.clone();
            let (rounds, payload) = (params.rounds, params.payload);
            handles.push(rt.spawn(async move {
                let payload = vec![b'x'; payload];
                let mut cold_start = Duration::ZERO;
                for round in 0..rounds {
                    let acquired = Instant::now();
                    let mut conn = pool.get().await.expect("acquire");
                    if round == 0 {
                        cold_start = acquired.elapsed();
                    }
                    round_trip(&mut conn, &payload).await.expect("round trip");
                }
                cold_start
            }));
        }

        let cold = rt.block_on(async {
            let mut total = Duration::ZERO;
            for handle in handles {
                total += handle.await.expect("worker panicked");
            }
            total
        });
        (start.elapsed(), cold / params.half as u32)
    }

    fn multi_thread(workers: usize) -> io::Result<Tokio> {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(workers)
            .enable_all()
            .build()
    }

    // ----------------------------------------------------------- the three arms

    /// Phase A on the warm runtime, phase B on the cold one. `pools` decides
    /// whether phase B can see what phase A left behind.
    enum Sharing {
        /// One pool for both halves: phase B pops phase A's sockets.
        Shared,
        /// A pool per half: phase B starts from nothing and dials.
        PerHalf,
    }

    fn run_split(
        label: &'static str,
        sharing: Sharing,
        addr: SocketAddr,
        params: &Params,
    ) -> io::Result<Arm> {
        let dials = Arc::new(AtomicU64::new(0));
        let warm_pool = build_pool(addr, dials.clone(), params);
        let warm_rt = multi_thread(params.half)?;
        let cold_rt = multi_thread(params.half)?;

        let (wall, cold_start) = drive(&warm_rt, &warm_pool, params);
        let warm = Phase {
            requests: params.half * params.rounds,
            wall,
            cold_start,
            dials: dials.load(Relaxed),
        };

        let cold_pool = match sharing {
            Sharing::Shared => warm_pool.clone(),
            Sharing::PerHalf => build_pool(addr, dials.clone(), params),
        };

        // `warm_rt` is deliberately still alive, and must be: its reactor holds
        // the registrations for every socket in the pool. See `caveat()`.
        let before = dials.load(Relaxed);
        let (wall, cold_start) = drive(&cold_rt, &cold_pool, params);
        let cold = Phase {
            requests: params.half * params.rounds,
            wall,
            cold_start,
            dials: dials.load(Relaxed) - before,
        };

        warm_pool.close();
        cold_pool.close();
        Ok(Arm { label, warm, cold })
    }

    /// One work-stealing runtime with the whole thread budget, one pool, two
    /// phases back to back. There is no cold half to speak of — which is the
    /// result, not a flaw in the setup.
    fn run_single(addr: SocketAddr, params: &Params) -> io::Result<Arm> {
        let dials = Arc::new(AtomicU64::new(0));
        let pool = build_pool(addr, dials.clone(), params);
        let rt = multi_thread(params.half * 2)?;

        let (wall, cold_start) = drive(&rt, &pool, params);
        let warm = Phase {
            requests: params.half * params.rounds,
            wall,
            cold_start,
            dials: dials.load(Relaxed),
        };

        let before = dials.load(Relaxed);
        let (wall, cold_start) = drive(&rt, &pool, params);
        let cold = Phase {
            requests: params.half * params.rounds,
            wall,
            cold_start,
            dials: dials.load(Relaxed) - before,
        };

        pool.close();
        Ok(Arm {
            label: "single runtime, shared pool",
            warm,
            cold,
        })
    }

    // ----------------------------------------------------- the shutdown caveat

    /// Shows what the shared-pool arm is avoiding by keeping the warm runtime
    /// alive: shut down the runtime that dialled a socket and the pool keeps
    /// serving it, `recycle` and all, until the first read or write fails.
    fn caveat(addr: SocketAddr, params: &Params) -> io::Result<()> {
        let dials = Arc::new(AtomicU64::new(0));
        let pool = build_pool(addr, dials, params);

        let warm = multi_thread(1)?;
        warm.block_on(async {
            let mut conn = pool.get().await.expect("acquire");
            round_trip(&mut conn, b"warm").await.expect("round trip");
        });
        // The socket is now idle in the pool, registered with `warm`'s reactor.
        warm.shutdown_timeout(Duration::from_secs(1));

        let cold = multi_thread(1)?;
        let outcome = cold.block_on(async {
            // `recycle` passes: the fd is open and has a peer. The pool has no
            // way to know its reactor is gone.
            let mut conn = pool.get().await.expect("acquire");
            round_trip(&mut conn, b"cold").await
        });
        pool.close();

        match outcome {
            Err(e) => println!("  reusing it after shutdown: {e}"),
            Ok(()) => println!("  reusing it after shutdown: worked (tokio no longer minds)"),
        }
        Ok(())
    }

    // --------------------------------------------------------- the ncat server

    struct Server(Option<Child>);

    impl Drop for Server {
        fn drop(&mut self) {
            if let Some(child) = self.0.as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }

    fn spawn_ncat(port: u16, max_conns: usize) -> io::Result<Server> {
        let child = Command::new("ncat")
            .args([
                "--listen",
                "127.0.0.1",
                &port.to_string(),
                "--keep-open",
                "--exec",
                "/bin/cat",
                "--max-conns",
                &max_conns.to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "could not start `ncat` ({e}). Install it (brew install nmap / \
                         apt install ncat), or point the example at a server you are \
                         already running with NCAT_ADDR=host:port"
                    ),
                )
            })?;
        Ok(Server(Some(child)))
    }

    fn free_port() -> io::Result<u16> {
        Ok(StdListener::bind("127.0.0.1:0")?.local_addr()?.port())
    }

    fn wait_ready(addr: SocketAddr) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
                Ok(_) => return Ok(()),
                Err(e) if Instant::now() >= deadline => return Err(e),
                Err(_) => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    }

    // ------------------------------------------------------------------- main

    pub fn run() -> io::Result<()> {
        let threads = env_usize("THREADS", 4).max(2);
        let params = Params {
            half: threads / 2,
            rounds: env_usize("ROUNDS", 2000),
            payload: env_usize("PAYLOAD", 64),
        };

        let (addr, _server) = match std::env::var("NCAT_ADDR") {
            Ok(addr) => (addr.parse().expect("NCAT_ADDR is not host:port"), None),
            Err(_) => {
                let port = free_port()?;
                let server = spawn_ncat(port, threads * 8)?;
                (SocketAddr::from(([127, 0, 0, 1], port)), Some(server))
            }
        };
        wait_ready(addr)?;

        println!(
            "tokio + deadpool: {} threads per half, {} rounds each, against ncat at {addr}\n",
            params.half, params.rounds
        );

        let per_half = run_split("split, pool per half", Sharing::PerHalf, addr, &params)?;
        let shared = run_split("split, shared pool", Sharing::Shared, addr, &params)?;
        let single = run_single(addr, &params)?;

        per_half.report();
        shared.report();
        single.report();

        println!(
            "\nphase B dialled {} times with a pool per half vs {} with one shared pool",
            per_half.cold.dials, shared.cold.dials
        );
        println!(
            "phase B cold start {:.1?} vs {:.1?}",
            per_half.cold.cold_start, shared.cold.cold_start
        );
        println!(
            "phase A throughput {:.0} req/s split vs {:.0} on a single runtime",
            per_half.warm.per_second(),
            single.warm.per_second(),
        );
        println!(
            "  (the arms run one after the other against the same ncat process, so read those \
             pairs as shape, not as a measurement of what sharing costs. And note what the \
             shared arm did *not* have to do: no re-registration, no `attach` — the socket was \
             already the reactor's, which is exactly why the reactor has to outlive it.)"
        );

        println!("\nwhat keeping the warm runtime alive is buying:");
        caveat(addr, &params)?;

        assert_eq!(
            shared.cold.dials, 0,
            "one shared pool should have handed phase B everything phase A returned"
        );
        assert!(
            shared.cold.dials < per_half.cold.dials,
            "a pool per half should have forced phase B to dial its own connections"
        );
        Ok(())
    }
}
