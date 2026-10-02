# deadpool-baseline

The yardstick, not a product. This crate holds no pool of its own: it ports compio-pool's
`echo` example to **tokio** so the numbers in
[`docs/performance.md`](../../docs/performance.md) have an incumbent to sit against — under the
same load, driven by the same client, on the same machine.

Nothing here is published (`publish = false`), and it is its own workspace root, so it never
joins the parent crate's build or `Cargo.lock`.

## What is here

| | what it is | the compio-pool counterpart |
|---|---|---|
| `examples/echo.rs` | **tokio default**: one multi-threaded work-stealing runtime, one shared listener, one shared `deadpool` buffer pool | [`examples/echo.rs`](../../examples/echo.rs) |
| `examples/echo_tpc.rs` | **tokio per-core**, the control: epoll like the above, but one pinned `current_thread` runtime per core, `SO_REUSEPORT`, thread-local buffers, nothing shared | the same architecture as compio-pool, on epoll instead of io_uring |
| `probe.py` | runs `examples/load.rs` against all three and records CPU time and context switches per request, with `taskset` control over where server and clients run | |

```sh
cargo build --release --example echo --example load
cargo build --release --manifest-path crates/deadpool-baseline/Cargo.toml --example echo --example echo_tpc

# one server by hand, driven by the same client as compio-pool's
cargo run --release --manifest-path crates/deadpool-baseline/Cargo.toml --example echo -- 127.0.0.1:7200 1024 4
cargo run --release --example load -- 127.0.0.1:7200 --conns 64 --seconds 5 --bytes 512

# all three, with accounting
python3 crates/deadpool-baseline/probe.py --label shared --workers 4 --conns 64 --bytes 16384
python3 crates/deadpool-baseline/probe.py --label split --workers 1 --server-cpus 0 --load-cpus 1,2,3 --bytes 16384
```

The CLI and the per-second stats line of `echo` mirror compio-pool's, so `load` measures every
server unchanged. `echo_tpc` accepts the capacity argument and ignores it.

## What the comparison isolates

Same echo handler, same client, same 16 KiB read buffer per connection:

| | tokio default | tokio per-core | compio-pool `echo` |
|---|---|---|---|
| I/O | epoll (mio) | epoll (mio) | io_uring (compio) |
| listeners | one shared `TcpListener`, one accept loop | one `SO_REUSEPORT` listener per core | one `SO_REUSEPORT` listener per core |
| scheduling | work-stealing, tasks may hop cores | pinned `current_thread` runtimes, no stealing | pinned rings, no stealing |
| buffer pool | one process-wide `Mutex`-guarded `deadpool` pool | thread-local | thread-local `Rc` pool |

Tokio default against compio-pool changes the backend and the architecture together, which is
why the middle column exists: default against per-core changes only the architecture, per-core
against compio-pool changes only the backend.

The default server's pool is sized `CAPACITY * WORKERS` so it matches compio-pool's total slot
count and never blocks on buffers in the no-overflow runs.

## Results

In [`docs/performance.md`](../../docs/performance.md#baseline-tokio), with the conditions they
were taken under. Placement matters as much as the server: on a machine where the load generator
shares the server's cores, results differ from a run with the clients kept apart, and
`probe.py` exists to show which one you are looking at.
