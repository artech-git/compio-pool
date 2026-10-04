//! The lowest-level I/O a developer reaches for: a raw **anonymous pipe** — a pair
//! of file descriptors with no socket, no address and no filesystem path.
//!
//! No network here. `pipe::anonymous()` returns a reader fd and a writer fd on one
//! ring; a producer task fills buffers borrowed from the crate's [`Pool`] and
//! writes them into the writer, a consumer task drains the reader and tallies the
//! bytes, and closing the writer is the EOF the reader stops on. It shows two
//! things at once: how plain fd streaming looks on `compio`, and that a [`Pool`]
//! is useful off the network path — any bounded, reused, ring-bound resource fits,
//! and the `!Send` buffers never leave this thread.
//!
//! ```text
//! cargo run --release --example fd_pipe -- [MESSAGES] [SIZE]
//!   MESSAGES   how many records to push   default 100000
//!   SIZE       bytes per record           default 256
//! ```

use std::{io, time::Instant};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncWriteExt},
    runtime::Runtime,
};
use compio_pool::{ManageConnection, Pool};

/// Hands out reusable record buffers sized to one message.
struct Buffers {
    size: usize,
}

impl ManageConnection for Buffers {
    type Connection = Vec<u8>;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<Vec<u8>> {
        Ok(Vec::with_capacity(self.size))
    }

    async fn is_valid(&self, _buf: &mut Vec<u8>) -> io::Result<()> {
        Ok(())
    }

    fn has_broken(&self, _buf: &mut Vec<u8>) -> bool {
        false
    }
}

fn main() -> io::Result<()> {
    let messages: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let size: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(256);

    let pool = Pool::builder().max_size(64).build(Buffers { size });

    // A single runtime: a pipe has no fan-out, and both ends must share the ring.
    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        let local = pool.local();
        let (mut reader, mut writer) = compio::fs::pipe::anonymous().await?;

        // Consumer: drain the reader until the writer is closed (read returns 0).
        let consumer = compio::runtime::spawn(async move {
            let mut total = 0u64;
            let mut buf = Vec::with_capacity(64 * 1024);
            loop {
                buf.clear();
                let BufResult(read, b) = reader.read(buf).await;
                buf = b;
                match read {
                    Ok(0) => break,
                    Ok(n) => total += n as u64,
                    Err(_) => break,
                }
            }
            total
        });

        // Producer: borrow a buffer per record, fill it, write it, hand it back.
        let started = Instant::now();
        for i in 0..messages {
            let mut lease = local.get().await.expect("lease a buffer");
            let mut buf = std::mem::take(&mut *lease);
            buf.clear();
            buf.resize(size, i as u8);
            let BufResult(w, b) = writer.write_all(buf).await;
            *lease = b; // return the buffer to the pool for the next record
            w?;
        }
        // Closing the writer is the reader's EOF; drop it before awaiting the tally.
        drop(writer);

        let total = consumer.await.expect("consumer task panicked");
        let elapsed = started.elapsed();
        println!(
            "piped {messages} records x {size} B = {total} bytes in {:.3}s ({:.0} records/s)",
            elapsed.as_secs_f64(),
            messages as f64 / elapsed.as_secs_f64(),
        );
        Ok(())
    })
}
