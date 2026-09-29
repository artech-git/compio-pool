//! Implementing [`Exchange`](compio_pool::Exchange) by hand: a cross-thread policy of your own.
//!
//! [`Reservoir`](compio_pool::Reservoir) is one implementation of the
//! [`Exchange`](compio_pool::Exchange) trait, not the only one it admits. The trait is four methods —
//! `park`, `unpark`, `parked`, `clear` — and the pool calls them without
//! knowing what is behind them, so *which* idle socket crosses a thread, and
//! whether it is still worth crossing at all, is yours to decide.
//!
//! This example swaps the reservoir for a `WarmStack`, which differs on both
//! counts:
//!
//! * **Newest first (LIFO).** `Reservoir` is a FIFO queue, which bounds how
//!   long any one socket sits parked. A stack hands back the socket parked most
//!   recently — the warmest, most likely still open at the far end — at the
//!   price of letting the ones underneath go stale.
//! * **A parking deadline**, so staleness is handled rather than rotated away.
//!   Each entry carries the [`SlotMeta`](compio_pool::SlotMeta) it was parked with, so `unpark` can
//!   read `meta.idle_for()` and throw away anything parked longer than
//!   `max_park` instead of handing a caller a socket the peer has dropped.
//!
//! It also keeps counters the pool has no place for, which is the other reason
//! to write your own: [`Metrics`](compio_pool::Metrics) reports `parked` and
//! `unparked`, and nothing about your policy's decisions.
//!
//! # The contract the pool relies on
//!
//! * **`park` must not block and must not await.** It is called from
//!   `Pooled::drop`. Here that means a `Mutex` held for a length check and a
//!   push, never across IO.
//! * **`Parked` is an accounting signal, not just a return value.** `Refused`
//!   hands the connection back whole and the shard keeps it; `Destroyed` says
//!   it is gone. The pool balances `live`/`closed` on that basis, so getting it
//!   wrong leaks a socket or corrupts the counters.
//! * **`Unparked::Lost` means "one was taken and destroyed".** The pool credits
//!   `closed` and dials a replacement. It is the only way to report a
//!   connection that came out of the exchange and did not come back, which is
//!   why the eviction below returns it rather than quietly popping the next
//!   entry.
//!
//! The transport and the [`Detach`](compio_pool::Detach) impl are deliberately the same as
//! `examples/steal.rs`, so the exchange is the only real difference between
//! the two runs — and the run below shows it, by printing the order the
//! sockets came back in.
//!
//! Run with: `cargo run --example custom_exchange`

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
        net::SocketAddr,
        num::NonZeroUsize,
        os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
        time::{Duration, Instant},
    };

    use compio::{
        buf::BufResult,
        dispatcher::Dispatcher,
        driver::ToSharedFd,
        io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    use compio_pool::{Detach, Exchange, Manage, Parked, Pool, SlotMeta, Unparked};

    const CONNS: usize = 3;
    /// Short enough that the last act can simply wait it out.
    const MAX_PARK: Duration = Duration::from_millis(100);

    // ----------------------------------------------------- the exchange

    #[derive(Default)]
    struct Stats {
        accepted: AtomicU64,
        refused: AtomicU64,
        claimed: AtomicU64,
        evicted: AtomicU64,
    }

    struct Entry<P> {
        parked: P,
        /// Carried across the thread boundary untouched and handed back with
        /// the connection, so lifecycle bookkeeping survives the trip — and so
        /// `unpark` can price the entry's age without a clock of its own.
        meta: SlotMeta,
    }

    /// A bounded LIFO exchange with a parking deadline.
    ///
    /// A LIFO `Mutex<Vec<_>>`, where [`Reservoir`](compio_pool::Reservoir) is a
    /// FIFO `VecDeque` behind a `parking_lot::FairMutex`. The shape is the
    /// same — the capacity check, the `detach` and the push share one critical
    /// section, so the push cannot fail — and the differences are the two
    /// policy knobs an exchange actually owns: claim order, and whether the
    /// lock hands off fairly or lets the incumbent barge. A `std` mutex barges,
    /// which is fine here and deliberately not fine for the reservoir; see
    /// `docs/decisions/0010-fair-mutex-reservoir.md`.
    struct WarmStack<M: Detach> {
        stack: Mutex<Vec<Entry<M::Parked>>>,
        capacity: usize,
        max_park: Duration,
        stats: Arc<Stats>,
    }

    impl<M: Detach> Exchange<M> for WarmStack<M> {
        fn park(&self, conn: M::Connection, meta: SlotMeta) -> Parked<M> {
            // Called from `Pooled::drop`: no await, no IO, and the lock is held
            // for a length check and a push.
            let mut stack = self.stack.lock().unwrap();

            if stack.len() == self.capacity {
                // Refusing costs the shard nothing — it gets the connection
                // back whole and keeps it locally. Note the ordering: room is
                // checked *before* `detach`, which consumes the connection and
                // could not be undone here.
                self.stats.refused.fetch_add(1, Relaxed);
                return Parked::Refused(conn, meta);
            }

            let Some(parked) = M::detach(conn) else {
                // An operation was still in flight; `detach` consumed and
                // dropped the connection. `Destroyed` is how the pool learns it
                // has one fewer live connection than it thought.
                return Parked::Destroyed;
            };

            stack.push(Entry { parked, meta });
            self.stats.accepted.fetch_add(1, Relaxed);
            Parked::Accepted
        }

        async fn unpark(&self) -> Unparked<M> {
            // The pop and the lock are one statement, so the guard is gone
            // before the await below. Holding a std `Mutex` across an await
            // would park the whole exchange behind one thread's IO.
            let Some(entry) = self.stack.lock().unwrap().pop() else {
                return Unparked::Empty;
            };

            if entry.meta.idle_for() > self.max_park {
                // Dropping `parked` closes the socket, so the pool has to hear
                // about it: `Lost` credits `closed` and sends the caller off to
                // dial. That is also why this evicts one entry per call rather
                // than draining every stale entry below it — each destroyed
                // connection needs its own `Lost` to stay accounted for.
                self.stats.evicted.fetch_add(1, Relaxed);
                drop(entry.parked);
                return Unparked::Lost;
            }

            // `attach` runs here, on the claiming thread, so the socket is
            // rebuilt in *this* thread's runtime — never at park time.
            match M::attach(entry.parked).await {
                Ok(conn) => {
                    self.stats.claimed.fetch_add(1, Relaxed);
                    Unparked::Claimed(conn, entry.meta)
                }
                Err(_) => Unparked::Lost,
            }
        }

        fn parked(&self) -> u64 {
            self.stack.lock().unwrap().len() as u64
        }

        fn clear(&self) {
            // Called by `Pool::invalidate` and `Pool::close`. Dropping the
            // entries closes the sockets.
            self.stack.lock().unwrap().clear();
        }
    }

    // --------------------------------- the manager, same as `examples/steal.rs`

    struct Echo {
        addr: SocketAddr,
        dials: Arc<AtomicU64>,
    }

    impl Manage for Echo {
        type Connection = TcpStream;
        type Error = io::Error;

        async fn connect(&self) -> io::Result<TcpStream> {
            self.dials.fetch_add(1, Relaxed);
            TcpStream::connect(self.addr).await
        }

        async fn recycle(&self, conn: &mut TcpStream, _meta: &SlotMeta) -> io::Result<()> {
            conn.peer_addr().map(|_| ())
        }
    }

    impl Detach for Echo {
        type Parked = OwnedFd;

        fn detach(conn: TcpStream) -> Option<OwnedFd> {
            let shared = conn.to_shared_fd();
            drop(conn);
            let socket = shared.try_unwrap().ok()?;
            // SAFETY: `try_unwrap` succeeded, so nothing else holds the handle
            // and `into_raw_fd` gives up its claim on the descriptor.
            Some(unsafe { OwnedFd::from_raw_fd(socket.into_raw_fd()) })
        }

        async fn attach(fd: OwnedFd) -> io::Result<TcpStream> {
            TcpStream::from_std(std::net::TcpStream::from(fd))
        }
    }

    // ------------------------------------------------------------- the run

    async fn echo(conn: &mut TcpStream, msg: &str) -> io::Result<()> {
        let BufResult(written, msg) = conn.write_all(msg.as_bytes().to_vec()).await;
        written?;
        let BufResult(read, buf) = conn.read_exact(vec![0u8; msg.len()]).await;
        read?;
        assert_eq!(buf, msg, "echo mismatch");
        Ok(())
    }

    /// Waits for the other worker without blocking this one's driver.
    async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            compio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    async fn serve() -> io::Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        compio::runtime::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                compio::runtime::spawn(async move {
                    loop {
                        let BufResult(read, buf) = stream.read(vec![0u8; 64]).await;
                        let Ok(n @ 1..) = read else { return };
                        let BufResult(written, _) = stream.write_all(buf[..n].to_vec()).await;
                        if written.is_err() {
                            return;
                        }
                    }
                })
                .detach();
            }
        })
        .detach();
        Ok(addr)
    }

    #[compio::main]
    pub async fn run() -> io::Result<()> {
        let addr = serve().await?;
        let stats = Arc::new(Stats::default());
        let dials = Arc::new(AtomicU64::new(0));

        let pool = Pool::builder(Echo {
            addr,
            dials: dials.clone(),
        })
        .max_size(CONNS)
        // `min_idle` defaults to 0, so every returned connection is surplus and
        // is offered to the exchange.
        .exchange(WarmStack::<Echo> {
            stack: Mutex::new(Vec::with_capacity(CONNS)),
            capacity: CONNS,
            max_park: MAX_PARK,
            stats: stats.clone(),
        })
        .build();

        let dispatcher = Dispatcher::builder()
            .worker_threads(NonZeroUsize::new(2).unwrap())
            .concurrent(false)
            .thread_names(|i| format!("worker-{i}"))
            .build()?;

        // Act 1: one worker parks `CONNS` sockets, the other claims them.
        // `concurrent(false)` means a worker cannot take a second task while
        // its first is running, so these two cannot share a thread.
        let park = dispatcher
            .dispatch({
                let pool = pool.clone();
                move || async move {
                    let mut held = Vec::new();
                    for i in 0..CONNS {
                        let mut conn = pool.acquire().await.expect("acquire");
                        let op = conn.begin_op();
                        echo(&mut conn, &format!("hello {i}")).await.expect("echo");
                        op.complete_op();
                        held.push(conn);
                    }
                    let fds: Vec<RawFd> = held.iter().map(|c| c.as_raw_fd()).collect();
                    drop(held);
                    wait_until("the claim", || pool.metrics().unparked as usize == CONNS).await;
                    fds
                }
            })
            .expect("dispatch");

        let claim = dispatcher
            .dispatch({
                let pool = pool.clone();
                move || async move {
                    wait_until("the park", || pool.metrics().parked as usize == CONNS).await;
                    let mut claimed = Vec::new();
                    let mut fds = Vec::new();
                    for i in 0..CONNS {
                        let mut conn = pool.acquire().await.expect("acquire");
                        fds.push(conn.as_raw_fd());
                        let op = conn.begin_op();
                        echo(&mut conn, &format!("again {i}")).await.expect("echo");
                        op.complete_op();
                        claimed.push(conn);
                    }
                    fds
                }
            })
            .expect("dispatch");

        let mut parked_fds = park.await.expect("parker panicked");
        let claimed_fds = claim.await.expect("claimer panicked");

        println!("parked  {parked_fds:?}");
        println!("claimed {claimed_fds:?}");
        parked_fds.reverse();
        assert_eq!(
            claimed_fds, parked_fds,
            "a stack hands back the newest first"
        );
        assert_eq!(dials.load(Relaxed) as usize, CONNS, "a claim is not a dial");

        // Act 2: the claimer's connections went back on the stack when its task
        // ended. Wait past `max_park` and check one out from this thread — the
        // top entry is now stale, so `unpark` evicts it and the pool dials.
        compio::time::sleep(MAX_PARK * 2).await;
        drop(pool.acquire().await.expect("acquire"));

        println!(
            "accepted {}, claimed {}, evicted {}, refused {}",
            stats.accepted.load(Relaxed),
            stats.claimed.load(Relaxed),
            stats.evicted.load(Relaxed),
            stats.refused.load(Relaxed),
        );
        assert_eq!(stats.evicted.load(Relaxed), 1, "one stale entry per unpark");
        assert_eq!(
            dials.load(Relaxed) as usize,
            CONNS + 1,
            "the eviction cost a dial"
        );

        dispatcher.join().await?;
        pool.close();
        assert_eq!(pool.metrics().parked, 0, "close empties the stack");

        println!("\nOK: a LIFO exchange with a parking deadline, across two threads.");
        Ok(())
    }
}
