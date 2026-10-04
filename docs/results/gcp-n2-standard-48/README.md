# Results: 48-vCPU Google Cloud VM (compio-pool vs tokio + deadpool)

A full head-to-head of **compio-pool** against **tokio + deadpool** across six I/O workloads
and a 107-point scaling sweep, run on one machine with identical placement, median of three.

* **Machine**: GCP `n2-standard-48` — Intel Xeon (Cascade Lake) @ 2.80 GHz, 48 vCPU =
  2 sockets × 12 physical cores × 2 SMT threads (**24 physical cores**), 2 NUMA nodes
  (local/remote distance 10/20), L2 1 MB/core, L3 33 MB/socket, 188 GiB RAM.
* **OS**: Ubuntu 24.04, kernel `7.0.0-1011-gcp`, `io_uring_disabled=0`, `somaxconn=4096`,
  Spectre/retbleed/SSB mitigations active. rustc 1.99.0. git `reuseport-workers@58ed11f`
  (with the uncommitted workload examples + `iobench.py`).
* **Harnesses**: [`iobench.py`](../../../crates/deadpool-baseline/iobench.py) (six workloads with
  /proc + `perf` resource accounting) and [`matrix.py`](../../../crates/deadpool-baseline/matrix.py)
  (echo scaling / payload / connections / shared / churn / NUMA).
* **Interactive report** (charts, both themes): published as a Claude artifact — see the chat that
  produced this directory.

> **Note.** `iobench.py` had never run before this study: its `ThreadSampler` named an instance
> attribute `_stop`, shadowing `threading.Thread._stop()` and aborting every point with
> `'Event' object is not callable`. Fixed by renaming to `_stop_event`. All `iobench` numbers here
> are from the fixed script.

## Three servers

| id | what it is |
|---|---|
| **compio** | compio-pool: io_uring, one pinned worker per core, SO_REUSEPORT, thread-local `Rc` pool, flume fd-handoff |
| **tokio per-core** | tokio on epoll, one `current_thread` runtime per pinned core, SO_REUSEPORT, per-worker deadpool |
| **tokio default** | tokio multi-thread work-stealing, one shared listener + one shared deadpool — what a plain `tokio::main` gives |

## TL;DR — the verdict

compio-pool is **not uniformly faster**. The organizing principle: it wins decisively wherever a
request touches the **filesystem** (io_uring issues the file op inline on the pinned worker; tokio
bounces every one through a blocking thread pool), ties small-payload sockets while spending far
fewer syscalls, and loses large-payload echo and connection churn.

`iobench`, 4 workers, one NUMA node (server cores 1–4 isolated, clients at 3.5× headroom), median of 3 × 5 s:

| workload | compio req/s | tokio per-core | tokio default | compio ÷ best | verdict |
|---|--:|--:|--:|--:|:--|
| echo-512 (512 B) | 597,968 | 607,719 | 513,990 | 98% | **tie** |
| echo-64k (64 KiB) | 102,869 | 128,214 | 124,599 | 80% | **lose −20%** |
| churn (reconnect) | 141,181 | 236,433 | 143,020 | 60% | **lose −40%** |
| relay-512 (1 hop) | 208,773 | 210,755 | 236,432 | 88% | lose −12% |
| fanout-512 (3 hops) | 74,278 | 60,177 | 69,950 | 106% | **win +6%** |
| file-4k | 198,217 | 88,113 | 47,138 | 225% | **win +125%** |
| file-64k | 118,969 | 65,143 | 39,452 | 183% | **win +83%** |
| wal-sync (fdatasync) | 4,151 | 4,145 | 4,149 | 100% | **tie** (disk-bound) |
| wal-nosync (append) | 440,598 | 239,125 | 141,582 | 184% | **win +84%** |

## Scaling (echo, `matrix.py`)

Throughput as the server is given more pinned cores. **Split** isolates the server on W dedicated
physical cores; **shared** is unpinned (all 48 vCPU contested, up to all 24 physical cores).

**scale512 — 512 B echo, split placement** (req/s):

| W | tokio default | tokio per-core | compio | compio ÷ default |
|--:|--:|--:|--:|--:|
| 1 | 103,887 | 105,782 | 109,325 | 1.05× |
| 2 | 192,918 | 201,023 | 198,066 | 1.03× |
| 4 | 373,144 | 410,720 | 392,283 | 1.05× |
| 8 | 771,297 | 873,304 | 850,942 | 1.10× |
| 13 | 854,021 | 1,419,280 `[SC]` | 1,347,233 `[SC]` | **1.58×** |

**shared — 512 B echo, nothing pinned, all 48 vCPU** (req/s):

| W | tokio default | tokio per-core | compio | compio ÷ default |
|--:|--:|--:|--:|--:|
| 8 | 591,513 | 725,523 | 701,938 | 1.19× |
| 16 | 923,220 | 1,392,464 | 1,311,851 | 1.42× |
| 24 | 1,069,035 | 2,070,034 `[B]` | 2,019,740 `[B]` | **1.89×** |

Reading it:

* **Both thread-per-core designs scale near-linearly.** compio reaches **1.35 M req/s** at W=13 split
  (12.3× on 13 cores, 95% efficiency) and **2.02 M req/s** at W=24 shared, with CPU/req flat
  (~9–10 µs) across the whole range. tokio per-core matches it (it edges ~5% ahead on raw small echo).
* **Work-stealing tokio-default plateaus.** At W=13 split it is **not even CPU-saturated** (no flag) —
  its shared listener and cross-thread coordination cap it below the cores it owns. The gap to
  thread-per-core *widens* with cores: 1.19× → 1.42× → **1.89×** at 24.
* **On a 48-vCPU box, choosing a thread-per-core architecture roughly doubles small-payload echo
  throughput versus the default tokio most reach for.** Backend (io_uring vs epoll) matters far less
  than architecture here: compio and tokio per-core are within ~5% at every point.
* 16 KiB scaling (`scale16k`, `matrix-report.md`) tells the same story: compio 837 K at W=13 split,
  tokio-default 547 K (compio 1.53×).

## Resource utilization

The file wins aren't mysterious — they're a thread-pool story. `iobench` medians:

| metric (file-4k) | compio | tokio per-core | tokio default |
|---|--:|--:|--:|
| req/s | **198,217** | 88,113 | 47,138 |
| high-water threads | **9** | 244 | 101 |
| CPU µs / req | **20.1** | 45.2 | 73.1 |
| syscalls / req (perf) | **1.38** | 20.4 | 23.6 |
| context switches / req | **0.68** | 9.9 | 8.5 |
| peak RSS (MB) | **4.1** | 16.0 | 11.4 |

tokio serves file I/O from a blocking thread pool that balloons to **100–244 threads** and costs a
context-switch handoff (~10 ctx/req) per operation; compio issues the op as an io_uring submission on
the pinned worker (9 threads total, including io-wq). The syscall gap is amplified on this host:
with Spectre/retbleed mitigations active, every kernel entry is taxed, so every elided syscall is
real CPU saved. On pure socket echo the same mechanism shows as **16× fewer syscalls** (0.13 vs
2.07/req) at equal throughput — latent headroom rather than raw speed.

## Payload, connections, NUMA (`matrix.py`, W=4 split)

* **Payload sweep** (compio ÷ tokio-per-core): 64 B 0.96× · 512 B 0.93× · 2 KiB 0.93× · 8 KiB 0.95× ·
  16 KiB 0.96×. compio is a steady ~4–7% behind tokio-per-core up to 16 KiB; the −20% cliff only
  appears at 64 KiB (`echo-64k`). Against tokio-default compio is ahead at every size.
* **Connection sweep** (512 B): compio holds ~390 K flat from 64→1024 conns; tokio-default *degrades*
  at 1024 (315 K, compio 1.24×). Thread-per-core wins at both low (4 conns, 1.20×) and high conn counts.
* **NUMA**: clients on the server's socket → 607 K; clients cross-socket → 391 K. **~36% throughput
  and +55% CPU/req** lost to a wrong placement, equally for all three runtimes — a hardware tax, not
  a runtime trait. This is why the workload matrix was pinned to a single NUMA node.

## Gotchas

1. **Large payloads (buf_ring).** 64 KiB echo loses ~20% and burns 25% more CPU/req. The workload is
   copy-bound, and compio-pool does not yet use io_uring registered/provided buffers (`buf_ring`), so
   each payload takes an extra userspace copy. io_uring's syscall win is irrelevant when copy-bound.
2. **Connection churn.** On lightweight churn compio trails tokio-per-core ~40% with p99 blowing to
   2.5 ms, while staying *below* CPU saturation — coordination-bound, not compute-bound. Forcing the
   fd-handoff path (pool capacity < connections) costs a further 16–23% (matrix churn: cap=1024 20,254
   → cap=2 16,960 → cap=1 15,656 req/s). Root causes: no multishot accept, and a flume handoff channel
   that is mutex-backed, not lock-free. matrix churn is otherwise **client-bound** (`[C]`) — the load
   generator's `connect()` is the wall and all three tie at ~20 K.
3. **Durability barriers.** `wal-sync` ties at ~4,150 req/s for all three — `fdatasync` on GCP network
   storage (~18 ms) is the wall. A synchronous durability barrier erases the async-backend advantage
   on throughput; it survives only as lower CPU/req. Remove it (`wal-nosync`) and compio wins 1.8–3.1×.
4. **The default tokio is the weakest baseline.** Multi-thread work-stealing is the worst scaler
   (~60% of thread-per-core at 24 cores) *and* 2–4× slower on file I/O. If you benchmark against stock
   `tokio::main`, the gap is large and grows with cores.
5. **Measurement.** Closed-loop load generator (one blocking thread per connection, costs ~as much
   CPU/req as the server), so coordinated omission is not captured and high-W split points are
   load-generator-bound (`[C]` = lower bound). Loopback only — no NIC, no TLS.

## Files

| file | what it is |
|---|---|
| `matrix-report.md` | `matrix.py`'s formatted report: all 7 scaling suites, rps + latency + CPU/req, with S/C/B flags |
| `matrix-results.jsonl` | every raw `matrix.py` run (env row + 107 points, each with per-repeat detail) |
| `iobench-results.tsv` | every raw `iobench.py` run (9 workloads × 3 servers × 3 repeats); columns in the header |
| `summary-data.json` | consolidated medians (iobench) + per-suite series (matrix) used for the interactive report |
| `lscpu.txt` | full CPU topology |

Flags — **S**: server ≥85% busy · **C**: client ≥85% busy (lower bound) · **B**: whole machine
≥90% busy · no flag: closed-loop limited (connections ÷ latency).

## Reproduce

```sh
# on a Linux box with io_uring (needs build-essential, python3, rust >= 1.95)
git checkout reuseport-workers           # the workload examples live here (some uncommitted)
cargo build --release --examples
cargo build --release --manifest-path crates/deadpool-baseline/Cargo.toml \
  --example echo --example echo_tpc --example churn --example fanout \
  --example file_server --example relay --example wal

# workloads + resource accounting (size placement to the box; here: node0, server 1-4, clients 5-11,29-35)
python3 crates/deadpool-baseline/iobench.py \
  --workers 4 --server-cpus 1-4 --backend-cpus 0 --client-cpus 5-11,29-35

# scaling sweep (auto-detects topology)
python3 crates/deadpool-baseline/matrix.py
```
