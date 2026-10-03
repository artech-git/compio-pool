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
| `matrix.py` | the comprehensive sweep for a many-core machine: worker scaling, payload, connection count, shared placement, connection churn and the handoff path, NUMA when there is more than one node; writes `results.jsonl` and a Markdown `report.md` | |
| `run-on-vm.sh` | builds everything (installing Rust into `$HOME` if needed) and runs `matrix.py` | |
| `bench.sh` | the same matrix as one self-contained bash script (no Python): clones and builds the branch itself, runs selectable variations, and writes **one log file** (environment, summary, every raw run as TSV) to upload or share | |

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

## Running the full matrix on a bigger machine

**Use the `reuseport-workers` branch.** The repository's default branch, `experimental`, is an
older design without the examples, and `main` is an empty initial commit, so a plain
`git clone` gives a checkout none of this runs on (`run-on-vm.sh` refuses it).

```sh
git clone -b reuseport-workers https://github.com/artech-git/compio-pool
cd compio-pool
crates/deadpool-baseline/run-on-vm.sh --dry-run        # the plan, and how long it will take
nohup crates/deadpool-baseline/run-on-vm.sh > bench.log 2>&1 &   # survives a dropped ssh
tail -f bench.log                                      # results land in bench-results/<host>-<time>/
```

**`bench.sh` is the simplest way to get a log off a machine.** It needs only bash, awk and
coreutils, and it fetches and builds the branch itself, so it can be downloaded on its own:

```sh
curl -fsSLO https://raw.githubusercontent.com/artech-git/compio-pool/reuseport-workers/crates/deadpool-baseline/bench.sh
chmod +x bench.sh
./bench.sh --dry-run                 # the plan and the time estimate
nohup ./bench.sh > bench.out 2>&1 &  # everything; Ctrl-C or a dropped ssh still writes the log
```

It prints the path of one `bench-<host>-<time>.log` at the end. Variations are flags, for example:

```sh
./bench.sh --suites scale512,scale16k --workers "1 2 4 8 16"
./bench.sh --suites payload,conns --servers tokio-per-core,compio-pool,compio-defer,compio-nocoop
./bench.sh --suites onecpu                      # server and clients forced onto one CPU
./bench.sh --suites custom --workers 4 --bytes "512 16384" --conns "64 256" --placement split
./bench.sh --suites custom --workers 4 --conns 256 --servers compio-pool:1,compio-pool:2,compio-pool
./bench.sh --quick                              # a few-minute smoke test of everything
```

`--help` lists them all; `--resume --out DIR` continues an interrupted run. `matrix.py` is the
Python equivalent with a richer Markdown report.

`matrix.py` and `bench.sh` read the machine's topology and give the server dedicated physical cores (SMT
siblings left idle) and the clients every other CPU, so the clients cannot share a core with
the server: the nearest one machine gets to a remote load generator. Every point is flagged
**S** (server-bound), **C** (client-bound: the number mostly measures the load generator) or
**B** (the whole box busy, shared placement), because on any single machine the client costs as
much per request as the server, and a **C** row is a lower bound. Repeats are interleaved
across servers so noisy-neighbour drift lands on all of them. `--quick` is a few-minute smoke
test; `--only scale512,payload` runs part of it; `--report-only DIR` rebuilds the report.

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

In [`docs/performance.md`](../../docs/performance.md#baseline-tokio) for a 4-vCPU container and
[for a 16-vCPU cloud VM](../../docs/performance.md#baseline-tokio-on-a-16-vcpu-vm) (raw logs in
[`docs/results/`](../../docs/results/gcp-epyc-9b45-16vcpu/)), with the conditions they were taken
under. Placement matters as much as the server: on a machine where the load generator
shares the server's cores, results differ from a run with the clients kept apart, and
`probe.py` exists to show which one you are looking at.
