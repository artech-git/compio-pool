//! What the cross-thread exchange is worth, measured against a **real** server:
//! `ncat` running an echo service in a separate process.
//!
//! `examples/ncat_bench.rs` asks what pooling buys over dialling per request.
//! This asks the next question: given a pool, what does letting connections
//! *migrate between threads* buy, and what does it cost? Both are measured
//! here in one process against one server, so the comparison does not rest on
//! run-to-run variance.
//!
//! # Shape of the run
//!
//! Each arm uses two dispatchers, `THREADS / 2` workers each:
//!
//! | phase | who works | what it shows |
//! |---|---|---|
//! | A `warm`  | the first dispatcher | steady state: what the exchange *costs* on the hot path |
//! | B `cold`  | the second, whose shards are empty | load migration: what the exchange *saves* |
//!
//! The warm dispatcher stays alive through phase B, so its shards are exactly
//! the quiet threads holding idle sockets that the exchange exists to drain.
//!
//! The arms differ only in the exchange:
//!
//! * **plain** — [`NoExchange`](compio_pool::NoExchange), the default. Phase A's connections stay in
//!   the warm shards forever, so phase B has to dial its own set: a TCP
//!   handshake plus a `fork`/`exec` of `/bin/cat`, per connection.
//! * **reservoir** — [`Reservoir`](compio_pool::Reservoir), a `VecDeque` behind a fair mutex. A shard whose
//!   free list is over `min_idle` parks the surplus there, and phase B claims
//!   those instead of dialling. [`Detach::attach`](compio_pool::Detach::attach) re-wraps each one in the
//!   claiming thread's driver.
//!
//! # The numbers that matter
//!
//! * **cold-start acquire** — the *first* checkout of each phase-B worker, the
//!   one that either pays a handshake or steals a warm socket. Averaged over a
//!   whole phase this is invisible; on its own it is the entire point.
//! * **dials in phase B** — the reservoir arm should barely move it.
//! * **phase A throughput** — a sanity check, not a measurement. `min_idle`
//!   is 0, so *every* return crosses the shared queue, which is the worst case
//!   for the exchange. But the two arms run one after the other against the
//!   same `ncat` process, and that ordering moves the number by more than the
//!   exchange does. What the exchange costs per operation is
//!   `benches/exchange.rs`.
//!
//! # Running
//!
//! ```text
//! cargo run --release --example ncat_steal_bench
//! ```
//!
//! Needs `ncat` on `PATH`. Tunables: `THREADS` (4, split in half), `ROUNDS`
//! (2000), `PAYLOAD` (64 bytes).

#[cfg(not(unix))]
fn main() {
    eprintln!("this example is unix-only; `Detach` is not sound on IOCP");
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
        num::NonZeroUsize,
        os::fd::{FromRawFd, IntoRawFd, OwnedFd},
        process::{Child, Command, Stdio},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
        time::{Duration, Instant},
    };

    use compio::{
        buf::BufResult,
        dispatcher::Dispatcher,
        driver::ToSharedFd,
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };
    use compio_pool::{Detach, Exchange, Manage, NoExchange, Pool, Reservoir, SlotMeta};

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

    impl Manage for Ncat {
        type Connection = TcpStream;
        type Error = io::Error;

        async fn connect(&self) -> io::Result<TcpStream> {
            self.dials.fetch_add(1, Relaxed);
            let conn = TcpStream::connect(self.addr).await?;
            conn.set_nodelay(true)?;
            Ok(conn)
        }

        async fn recycle(&self, conn: &mut TcpStream, _meta: &SlotMeta) -> io::Result<()> {
            conn.peer_addr().map(|_| ())
        }
    }

    impl Detach for Ncat {
        type Parked = OwnedFd;

        fn detach(conn: TcpStream) -> Option<OwnedFd> {
            // Succeeds only at a strong count of one, which is compio's way of
            // saying no operation is still submitted against this fd — the
            // precondition for moving it between drivers.
            let shared = conn.to_shared_fd();
            drop(conn);
            let socket = shared.try_unwrap().ok()?;
            // SAFETY: `try_unwrap` gave us sole ownership, and `into_raw_fd`
            // gives up the socket's claim on the descriptor.
            Some(unsafe { OwnedFd::from_raw_fd(socket.into_raw_fd()) })
        }

        async fn attach(fd: OwnedFd) -> io::Result<TcpStream> {
            TcpStream::from_std(std::net::TcpStream::from(fd))
        }
    }

    async fn round_trip(conn: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
        let BufResult(written, payload) = conn.write_all(payload.to_vec()).await;
        written?;
        let BufResult(read, echoed) = conn.read_exact(vec![0u8; payload.len()]).await;
        read?;
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

    /// Runs `half` workers, one per thread, each doing `rounds` round trips.
    /// Returns the wall time and the mean first-checkout latency.
    async fn drive<X: Exchange<Ncat>>(
        dispatcher: &Dispatcher,
        pool: &Pool<Ncat, X>,
        params: &Params,
    ) -> (Duration, Duration) {
        let start = Instant::now();
        let mut handles = Vec::new();
        for _ in 0..params.half {
            let pool = pool.clone();
            let (rounds, payload) = (params.rounds, params.payload);
            handles.push(
                dispatcher
                    .dispatch(move || async move {
                        let mut cold_start = Duration::ZERO;
                        for round in 0..rounds {
                            let acquired = Instant::now();
                            let mut conn = pool.acquire().await.expect("acquire");
                            if round == 0 {
                                cold_start = acquired.elapsed();
                            }
                            let op = conn.begin_op();
                            round_trip(&mut conn, &vec![b'x'; payload])
                                .await
                                .expect("round trip");
                            op.complete_op();
                        }
                        cold_start
                    })
                    .expect("dispatch"),
            );
        }

        let mut cold = Duration::ZERO;
        for handle in handles {
            cold += handle.await.expect("worker panicked");
        }
        (start.elapsed(), cold / params.half as u32)
    }

    /// One arm: warm half, then cold half, with the warm threads still alive.
    async fn run_arm<X: Exchange<Ncat>>(
        label: &'static str,
        exchange: X,
        addr: SocketAddr,
        params: &Params,
    ) -> io::Result<Arm> {
        let dials = Arc::new(AtomicU64::new(0));
        let pool = Pool::builder(Ncat {
            addr,
            dials: dials.clone(),
        })
        .max_size(2)
        // 0 means every returned connection is surplus, so it is offered to the
        // exchange immediately. The harshest setting for the exchange, and the
        // one that drains a quiet thread fastest.
        .min_idle(0)
        .acquire_timeout(Duration::from_secs(10))
        .exchange(exchange)
        .build();

        let threads = NonZeroUsize::new(params.half).unwrap();
        let warm_pool = Dispatcher::builder()
            .worker_threads(threads)
            .concurrent(false)
            .build()?;
        let cold_pool = Dispatcher::builder()
            .worker_threads(threads)
            .concurrent(false)
            .build()?;

        let (wall, cold_start) = drive(&warm_pool, &pool, params).await;
        let warm = Phase {
            requests: params.half * params.rounds,
            wall,
            cold_start,
            dials: dials.load(Relaxed),
        };

        // The warm dispatcher is deliberately *not* joined here: its threads go
        // quiet but stay alive, holding whatever the exchange did not take.
        let before = dials.load(Relaxed);
        let (wall, cold_start) = drive(&cold_pool, &pool, params).await;
        let cold = Phase {
            requests: params.half * params.rounds,
            wall,
            cold_start,
            dials: dials.load(Relaxed) - before,
        };

        warm_pool.join().await?;
        cold_pool.join().await?;
        pool.close();
        Ok(Arm { label, warm, cold })
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

    #[compio::main]
    pub async fn run() -> io::Result<()> {
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
            "{} threads per half, {} rounds each, against ncat at {addr}\n",
            params.half, params.rounds
        );

        let plain = run_arm("plain (NoExchange)", NoExchange, addr, &params).await?;
        let steal = run_arm(
            "reservoir",
            Reservoir::<Ncat>::new(params.half * 2),
            addr,
            &params,
        )
        .await?;

        plain.report();
        steal.report();

        println!(
            "\nphase B dialled {} times plain vs {} with the reservoir",
            plain.cold.dials, steal.cold.dials
        );
        println!(
            "phase B cold start {:.1?} plain vs {:.1?} with the reservoir",
            plain.cold.cold_start, steal.cold.cold_start
        );
        println!(
            "phase A throughput {:.0} req/s plain vs {:.0} with the reservoir",
            plain.warm.per_second(),
            steal.warm.per_second(),
        );
        println!(
            "  (the arms run one after the other against the same ncat process, so read that \
             pair as a check that sharing every return did not collapse the hot path, not as \
             a measurement of what it costs. `benches/exchange.rs` measures that in isolation.)"
        );

        assert!(
            steal.cold.dials < plain.cold.dials,
            "the reservoir should have saved phase B most of its handshakes"
        );
        Ok(())
    }
}
