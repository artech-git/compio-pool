# deadpool-baseline

The yardstick, not a product. This crate holds no pool of its own: it ports compio-pool's
`echo` example to **tokio + deadpool** so the numbers in
[`docs/performance.md`](../../docs/performance.md) have an incumbent to sit against — under the
same load, driven by the same client, on the same machine.

Nothing here is published (`publish = false`), and it is its own workspace root, so it never
joins the parent crate's build or `Cargo.lock`.

## What is here

| example | the compio-pool example it ports |
|---|---|
| `examples/echo.rs` | [`examples/echo.rs`](../../examples/echo.rs) |

```sh
# the baseline server (tokio runtime + one shared deadpool buffer pool)
cargo run --release -p deadpool-baseline --example echo -- 127.0.0.1:7200 1024 4
# the same client that drives the compio-pool server
cargo run --release --example load -- 127.0.0.1:7200 --conns 64 --seconds 5 --bytes 512
```

CLI and the per-second stats line mirror `echo.rs`, so `examples/load.rs` measures both unchanged.

## What the comparison isolates

Same echo handler, same client, same 16 KiB pooled read buffer. Only the architecture differs:

| | compio-pool `echo` | this baseline |
|---|---|---|
| I/O | io_uring (compio) | epoll (tokio/mio) |
| listeners | one `SO_REUSEPORT` listener per core, kernel-hashed | one shared `TcpListener`, one accept loop |
| scheduling | thread-per-core, pinned, no work stealing | tokio multi-threaded work-stealing |
| the pool | thread-local `Rc`, no sharing | one process-wide `Mutex`-guarded `deadpool` pool |
| a connection | accepted, served and closed on one core | may hop cores between accept and handler |

The pool is sized `CAPACITY * WORKERS` so it matches compio-pool's total slot count and never
blocks on buffers in the no-overflow runs — what is being compared is the I/O and scheduling
architecture, not pool exhaustion.

## Results

See the root chat / [`docs/performance.md`](../../docs/performance.md) comparison section.
Headline, from this repo's 4-vCPU loopback container (medians of 3 runs; treat as relative —
the load generator shares the cores it measures):

* **Small messages scale better per core on compio-pool.** 512 B / 64 conns:
  1→2→4 workers gives compio ~68k→168k→246k req/s (near-linear), tokio+deadpool
  ~72k→148k→138k (plateaus and dips at 4, where the single accept loop and the shared pool
  mutex contend). At 16 conns / 4 workers compio is ~2.5× (273k vs 107k) with roughly half the
  p50.
* **Large messages favor tokio+deadpool here.** 16 KiB / 64 conns / 4 workers:
  ~280k vs ~160k req/s, but with a bimodal latency profile (p50 ~8 µs, p90 ~860 µs) against
  compio's tighter, more uniform distribution (p50 ~340 µs, p90 ~600 µs).
* **Connection churn is a wash.** Reconnect-per-request is bounded by connection setup on
  loopback (~15k conn/s) for both; compio's fd-handoff path (capacity 2) adds nothing
  measurable over capacity 1024.
