//! Control experiment: tokio, but thread-per-core.
//!
//! `echo.rs` in this crate is tokio's default model (multi-threaded work-stealing
//! runtime, one shared listener, one shared pool). compio-pool's `echo` differs
//! from it in TWO ways at once: the I/O backend (io_uring vs epoll) and the
//! architecture (thread-per-core, SO_REUSEPORT, no shared state vs work-stealing).
//! This server changes only the second: it keeps tokio's epoll backend but runs
//! one `current_thread` runtime per pinned core, each with its own SO_REUSEPORT
//! listener and its own thread-local buffer, no shared pool, no work stealing.
//!
//! Comparing the three servers separates the variables:
//!   echo (tokio, default)   vs  echo_tpc (tokio, thread-per-core)  = architecture
//!   echo_tpc                vs  compio-pool echo                   = io_uring vs epoll
//!
//! ```text
//! cargo run --release -p deadpool-baseline --example echo_tpc -- [ADDR] [CAPACITY] [WORKERS]
//! ```
//! CAPACITY is accepted for CLI parity with the other echo servers and ignored.

use std::{cell::RefCell, io, net::SocketAddr, rc::Rc};

use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::LocalSet,
};

fn listener(addr: SocketAddr) -> io::Result<std::net::TcpListener> {
    let s = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    s.set_reuse_address(true)?;
    s.set_reuse_port(true)?;
    s.set_nonblocking(true)?;
    s.bind(&addr.into())?;
    s.listen(1024)?;
    Ok(s.into())
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr: SocketAddr = args
        .next()
        .unwrap_or_else(|| "0.0.0.0:7000".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let _capacity = args.next();
    let workers: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1));

    println!("tokio thread-per-core echo on {addr} — {workers} workers (epoll, SO_REUSEPORT, no shared state)");

    let cores = core_ids();
    let handles: Vec<_> = (0..workers)
        .map(|i| {
            let core = cores.get(i % cores.len().max(1)).copied();
            std::thread::spawn(move || -> io::Result<()> {
                if let Some(c) = core {
                    pin(c);
                }
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let local = LocalSet::new();
                local.block_on(&rt, async move {
                    let l = TcpListener::from_std(listener(addr)?)?;
                    // One buffer per worker thread, handed to each connection in
                    // turn: the thread-local analogue of compio-pool's Resource.
                    let pool: Rc<RefCell<Vec<Vec<u8>>>> = Rc::new(RefCell::new(Vec::new()));
                    loop {
                        let (mut stream, _) = l.accept().await?;
                        let _ = stream.set_nodelay(true);
                        let pool = pool.clone();
                        tokio::task::spawn_local(async move {
                            let mut buf = pool
                                .borrow_mut()
                                .pop()
                                .unwrap_or_else(|| vec![0u8; 16 * 1024]);
                            loop {
                                match stream.read(&mut buf).await {
                                    Ok(0) | Err(_) => break,
                                    Ok(n) => {
                                        if stream.write_all(&buf[..n]).await.is_err() {
                                            break;
                                        }
                                    }
                                }
                            }
                            pool.borrow_mut().push(buf);
                        });
                    }
                })
            })
        })
        .collect();
    for h in handles {
        h.join().expect("worker panicked")?;
    }
    Ok(())
}

fn core_ids() -> Vec<usize> {
    // The parent crate already depends on core_affinity; avoid adding a dep here
    // by reading the online CPU list from std.
    (0..std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)).collect()
}

fn pin(cpu: usize) {
    // SAFETY: sched_setaffinity on the calling thread with a zeroed, then
    // populated, cpu_set_t of the correct size.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
    }
}
