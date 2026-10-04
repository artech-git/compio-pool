//! Static-file benchmark: each request names a file, the response is its
//! length and bytes. Disk read → socket write, the classic static-server path.
//!
//! On compio the whole request — open, read_at, socket write — is io_uring
//! submissions from one pinned thread. The tokio baseline must bounce every
//! file operation through its blocking thread pool, which is exactly the
//! difference this example exists to measure.
//!
//! Protocol, one request per line on a persistent connection:
//!   client:  `<filename>\n`
//!   server:  `<size>\n<size raw bytes>`   or   `ERR <why>\n`
//!
//! ```text
//! FILE_DIR=/tmp/bench-files \
//! cargo run --release --example file_server -- [ADDR] [CAPACITY] [WORKERS] [FLAGS]
//! ```
//!
//! Drive it with `examples/load_file.rs`, which also creates the test files.

use std::{io, net::SocketAddr, path::PathBuf, sync::LazyLock, time::Duration};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncReadAt, AsyncWriteExt},
};
use compio_pool::{Connection, Resource, Server, Service, UringConfig, WorkerContext, Workers};

static BASE_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    PathBuf::from(std::env::var("FILE_DIR").unwrap_or_else(|_| "/tmp/bench-files".into()))
});

struct Bufs {
    req: Vec<u8>,
    data: Vec<u8>,
}

impl Resource for Bufs {
    async fn create(_cx: &WorkerContext) -> io::Result<Self> {
        Ok(Bufs {
            req: Vec::with_capacity(4096),
            data: Vec::with_capacity(64 * 1024),
        })
    }

    fn recycle(&mut self) -> bool {
        // A handler error can strand a buffer mid-flight; re-arm so the next
        // connection never sees a zero-capacity vec.
        if self.req.capacity() == 0 {
            self.req = Vec::with_capacity(4096);
        }
        if self.data.capacity() == 0 {
            self.data = Vec::with_capacity(64 * 1024);
        }
        true
    }
}

#[derive(Clone)]
struct FileServer;

impl Service for FileServer {
    type Resource = Bufs;

    async fn handle(&self, mut conn: Connection, bufs: &mut Bufs) -> io::Result<()> {
        loop {
            // One request line: the filename.
            let mut req = std::mem::take(&mut bufs.req);
            req.clear();
            let BufResult(n, req) = conn.stream.read(req).await;
            bufs.req = req;
            match n {
                Ok(0) => return Ok(()),
                Ok(_) => {}
                Err(e) => return Err(e),
            }

            let file_path = {
                let s = match std::str::from_utf8(&bufs.req) {
                    Ok(s) => s.trim_end_matches(['\r', '\n']),
                    Err(_) => {
                        let BufResult(w, _) =
                            conn.stream.write_all(b"ERR utf8\n".to_vec()).await;
                        w?;
                        continue;
                    }
                };
                BASE_DIR.join(s)
            };

            let file = match compio::fs::File::open(&file_path).await {
                Ok(f) => f,
                Err(e) => {
                    let BufResult(w, _) =
                        conn.stream.write_all(format!("ERR {e}\n").into_bytes()).await;
                    w?;
                    continue;
                }
            };
            let size = file.metadata().await?.len();

            let BufResult(w, _) = conn.stream.write_all(format!("{size}\n").into_bytes()).await;
            w?;

            // Stream the file: read_at into the pooled buffer, write to the
            // socket, both through the ring.
            let mut data = std::mem::take(&mut bufs.data);
            let mut offset = 0u64;
            while offset < size {
                data.clear();
                let BufResult(n, d) = file.read_at(data, offset).await;
                data = d;
                let n = match n {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) => {
                        bufs.data = data;
                        return Err(e);
                    }
                };
                offset += n as u64;
                let BufResult(w, d) = conn.stream.write_all(data).await;
                data = d;
                if let Err(e) = w {
                    bufs.data = data;
                    return Err(e);
                }
            }
            bufs.data = data;
        }
    }
}

fn main() -> io::Result<()> {
    let (flags, positional): (Vec<String>, Vec<String>) =
        std::env::args().skip(1).partition(|a| a.starts_with("--"));
    const KNOWN: [&str; 3] = ["--incoming-cpu", "--defer-taskrun", "--no-coop-taskrun"];
    if let Some(unknown) = flags.iter().find(|f| !KNOWN.contains(&f.as_str())) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown flag {unknown}"),
        ));
    }
    let has = |name: &str| flags.iter().any(|f| f == name);
    let incoming_cpu = has("--incoming-cpu");
    let uring = UringConfig {
        defer_taskrun: has("--defer-taskrun"),
        coop_taskrun: !has("--no-coop-taskrun"),
        ..UringConfig::default()
    };
    let mut args = positional.into_iter();
    let addr: SocketAddr = args
        .next()
        .unwrap_or_else(|| "0.0.0.0:7000".into())
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("bad address: {e}")))?;
    let capacity: usize = args
        .next()
        .map(|s| s.parse())
        .transpose()
        .map_err(invalid)?
        .unwrap_or(1024);
    let workers = match args
        .next()
        .map(|s| s.parse::<usize>())
        .transpose()
        .map_err(invalid)?
    {
        Some(n) => Workers::Count(n),
        None => Workers::AllCores,
    };

    let server = Server::builder(FileServer)
        .bind(addr)
        .workers(workers)
        .capacity(capacity)
        .handoff_capacity(4096)
        .incoming_cpu(incoming_cpu)
        .uring(uring.clone())
        .start()?;

    println!(
        "file_server on {} serving {} — {} workers, capacity {} each",
        server.local_addr(),
        BASE_DIR.display(),
        server.workers(),
        capacity
    );
    for (i, core) in server.cores().iter().enumerate() {
        println!(
            "  worker {i:>2} -> cpu {:>3}  smp_affinity {}",
            core.id,
            compio_pool::cpu::affinity_mask(core.id)
        );
    }
    println!();

    loop {
        std::thread::sleep(Duration::from_secs(1));
        let s = server.stats();
        let t = s.totals;
        println!(
            "active {:>5}  queued {:>4}/{:<4}  accepted {:>8}  local {:>8}  handed_off {:>6}  claimed {:>6}  bounced {:>4}  oversub {:>4}  rejected {:>4}  done {:>8}  errs {}",
            t.active,
            s.queued,
            s.handoff_capacity,
            t.accepted,
            t.served_local,
            t.handed_off,
            t.claimed,
            t.bounced,
            t.oversubscribed,
            t.rejected,
            t.completed,
            t.handler_errors + t.resource_errors + t.accept_errors
        );
    }
}

fn invalid(e: std::num::ParseIntError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, e)
}
