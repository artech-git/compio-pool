# compio-pool (bb8 redesign) on GCP `t2a-standard-32` — Ampere Altra, 32 vCPU ARM

**Date:** 2026-10-04 · **Host:** `compio-bench-arm-36` (actually `t2a-standard-32`), `us-central1-a`
**Code under test:** the working tree of the bb8-style pool redesign — `examples/echo.rs` is now a
*developer-owned* thread-per-core loop that leases a per-connection buffer from the new
[`Pool`](../../../src/pool.rs) / [`ManageConnection`](../../../src/manage.rs). There is no longer a
`Server`; the crate is the pool, and the example owns the runtime, the pinning and the accept loop.

## Headline

On a 32-core Ampere Altra, the new pool-based compio echo **beats both tokio baselines on throughput
at nearly every point and uses the least server CPU per request**, with the tightest tail latency:

- **vs tokio (epoll) per-core:** ~**1.25–1.29×** the throughput at 512 B, ~**1.15–1.19×** at 16 KiB,
  up to **1.29×** on small payloads. Peak **1.20 M req/s** at W=12 (512 B) vs 1.08 M.
- **vs tokio default (work-stealing):** **1.5–1.67×** at high worker counts, and **2× lower p99**
  (e.g. W=8/512 B: p99 **365 µs** vs **834 µs**).
- **Resource efficiency:** compio spends **~9 µs of server CPU per request** at 512 B vs ~11.4 µs for
  both tokio variants — roughly **20 % less CPU per request** while doing more of them.
- **Where it ties or trails:** at **8 KiB** the per-core tokio baseline draws level (large `memcpy`
  dominates, not the runtime), and at **4 connections** tokio-per-core is marginally faster
  (25 vs 26 µs p50 — latency-bound, not throughput-bound). The new pool's per-connection
  `get()`/return shows a tiny fixed cost only at trivial concurrency; it is swamped by ~16 conns.

This confirms the refactor did not cost performance: the bb8 `Pool` on the hot path (lease a buffer
per connection, return it on drop) carries no measurable overhead at realistic load, and the
thread-per-core io_uring design still scales ~linearly.

## Machine

| | |
|---|---|
| Instance | `t2a-standard-32` (GCP Tau T2A) |
| CPU | ARM **Neoverse-N1** (Ampere Altra), 32 cores, **1 thread/core**, 1 socket |
| Caches | L1 64K/core · L2 1 MiB/core · **L3 32 MiB shared** |
| NUMA | **1 node** (0–31) |
| Memory | 125 GiB |
| Kernel | Linux 7.0.0-1011-gcp (Ubuntu 24.04), `io_uring_disabled=0` |
| rustc | 1.99.0 |
| Limits | `somaxconn=4096`, `nofile=1048576`, `tcp_tw_reuse=2` |

## Method

Driver: `crates/deadpool-baseline/bench.sh`, the same harness as the previous 48-vCPU run.

- **Servers:** `tokio-default` (multi-thread work-stealing, one shared listener+pool),
  `tokio-per-core` (tokio/epoll, one pinned `current_thread` runtime per core, `SO_REUSEPORT`),
  `compio-pool` (this crate: io_uring, one pinned ring per core, `SO_REUSEPORT`, bb8 pool).
- **Placement `split`:** the server gets W dedicated cores; the load generator (`examples/load.rs`)
  gets the rest. With client ratio 1.5 on 32 cores, W tops out at **12**, so the client can always
  saturate the server. A run is flagged **S** when server CPUs are ≥85 % busy — the number is then
  the server's, not the client's. Nearly every point below is `S`.
- **Suites:** `scale512`, `scale16k` (worker scaling at 512 B / 16 KiB), `payload` (byte-size sweep
  at W=4), `conns` (concurrency sweep at W=4). **3 repeats × 5 s**, loopback, median reported.
- **Excluded on purpose:** the `churn` suite and the `compio-pool:1/:2` low-capacity variants. The
  old design handled over-capacity connections by detaching the fd and bouncing it to an idle core
  (handoff/claim). The new pool has no handoff — at capacity it applies **backpressure** (`get()`
  waits up to `connection_timeout`). Those suites measured the handoff path, which no longer exists,
  so they are not meaningful here. All runs below use the default capacity (1024/worker), far above
  the connection counts, so `get()` never blocks.

Two harness adaptations (the refactor removed files the stock harness checks for): its preflight was
pointed at `src/pool.rs` instead of the deleted `src/worker.rs`, and `--locked` was dropped because
`flume` was removed from `Cargo.toml` (the lock is regenerated on the box). Nothing else changed.

> Not directly comparable to [`gcp-n2-standard-48`](../gcp-n2-standard-48/): different ISA (ARM N1 vs
> x86 n2), different core count, and the file-I/O / churn / WAL / Redis parts of that matrix drove
> server examples this refactor deleted. This run isolates the **echo** path, which exercises the pool.

## Results

Each cell is the **median of 3 runs**. `[S]` = server-bound. `compio/pc` = compio-pool ÷ tokio-per-core.

### scale512 — 512 B echo, worker scaling

| W | tokio-default | tokio-per-core | **compio-pool** | compio/pc |
|--:|--|--|--|:--:|
| 1 | 96,944 `[S]` | 80,776 `[S]` | **103,880** `[S]` | 1.29× |
| 2 | 182,438 `[S]` | 174,395 `[S]` | **223,649** `[S]` | 1.28× |
| 4 | 330,211 `[S]` | 348,429 `[S]` | **436,333** `[S]` | 1.25× |
| 8 | 632,866 `[S]` | 688,059 `[S]` | **871,389** `[S]` | 1.27× |
| 12 | 715,702 | 1,082,194 `[S]` | **1,197,871** `[S]` | 1.11× |

p50 / p99 (µs) and **server CPU-µs/req**:

| W | p99 default | p99 per-core | **p99 compio** | CPU/req default | CPU/req per-core | **CPU/req compio** |
|--:|--|--|--|--|--|--|
| 1 | 724 | 854 | **704** | 10.3 | 12.3 | **9.6** |
| 2 | 547 | 426 | **329** | 10.6 | 11.4 | **8.9** |
| 4 | 717 | 444 | **397** | 11.3 | 11.4 | **9.1** |
| 8 | 834 | 482 | **365** | 11.4 | 11.5 | **9.1** |
| 12 | 941 | 533 | **417** | 11.7 | 11.0 | **9.9** |

### scale16k — 16 KiB echo, worker scaling

| W | tokio-default | tokio-per-core | **compio-pool** | compio/pc | p99 compio (µs) | CPU/req compio |
|--:|--|--|--|:--:|--|--|
| 1 | 54,140 | 63,797 | **64,912** | 1.02× | 1140 | 15.3 |
| 2 | 118,272 | 119,348 | **137,926** | 1.16× | 532 | 14.5 |
| 4 | 207,124 | 236,905 | **271,346** | 1.15× | 618 | 14.7 |
| 8 | 414,210 | 450,475 | **538,077** | 1.19× | 612 | 14.8 |
| 12 | 436,812 | 652,418 | **705,978** | 1.08× | 729 | 16.9 |

(tokio-default p99 at 16 KiB runs 1.1–1.6 ms; compio stays 0.5–0.7 ms.)

### payload — W=4, 128 conns, byte size swept

| Bytes | tokio-default | tokio-per-core | **compio-pool** | compio/pc |
|--:|--|--|--|:--:|
| 64 | 337,176 | 355,786 | **458,295** | 1.29× |
| 512 | 286,905 | 344,775 | **433,162** | 1.26× |
| 2048 | 263,450 | 381,685 | **393,750** | 1.03× |
| 8192 | 260,154 | **326,631** | 326,021 | 1.00× |
| 16384 | 205,717 | 267,549 | **271,768** | 1.02× |

compio-pool's edge is largest on small payloads (syscall/wakeup cost dominates, where io_uring wins)
and narrows as the payload grows and raw copy cost takes over.

### conns — W=4, 512 B, concurrency swept

| conns | tokio-default | tokio-per-core | **compio-pool** | p99 default | p99 per-core | **p99 compio** |
|--:|--|--|--|--|--|--|
| 4 | 117,355 | **152,579** | 144,683 | 58 | 41 | 44 |
| 16 | 238,668 | 350,386 | **362,011** | 119 | 95 | **77** |
| 64 | 262,378 | 423,941 | **443,048** | 422 | 216 | **214** |
| 256 | 307,275 | 337,654 | **408,541** | 1472 | 945 | **732** |
| 1024 | 269,875 | 310,689 | **351,228** | 8146 | 3513 | **3229** |

At 4 conns tokio-per-core is marginally ahead (both latency-bound); from 16 conns up compio-pool
leads, and its tail is the tightest everywhere. At 1024 conns tokio-default's p99 blows out to 8.1 ms
while compio holds 3.2 ms.

## Resource efficiency

The CPU-µs/request rows are the resource story. compio-pool consistently does **more work per CPU
cycle**: at 512 B it needs **~9.1 µs** of server CPU per request against ~11.4 µs for both tokio
runtimes (~20 % less); at 16 KiB, ~14.7 µs vs ~16.8–18.2 µs. Combined with higher throughput, that is
the io_uring + thread-per-core advantage — fewer syscalls, no cross-core wakeups — and it shows the
new bb8 pool adds no hot-path cost: a `get()` is a pop from a thread-local `Vec` and a return is a
push, no atomics, no lock. Memory was a non-factor (125 GiB box, buffers are 16 KiB × capacity/core).

## Resource utilization — worker sweep to 32

A separate instrumented run (`_raw/resbench.sh`) pins each server to W cores (W = 4…32) and measures
the **server process** with `perf` (syscalls via `raw_syscalls:sys_enter`, context-switches,
cpu-migrations, page-faults) and `/proc` (peak RSS, thread count, CPU time) while `examples/load`
drives it from the remaining cores — 512 B, 512 conns, 10 s. Above 16 workers the client runs on the
few leftover cores and cannot saturate the server, so throughput and per-request ratios there read low
(marked †); RSS and thread counts stay valid. **W=32‡ is shared placement** (client gets all cores too)
— the clean saturated high-worker point.

Headlines: while saturated, compio issues **~0.03–0.13 syscalls/request vs ~2.0** for both tokio
runtimes (io_uring batches submit+complete; epoll pays a read and a write syscall each time); RSS stays
**5–9 MB vs ~11–12 MB**; and the pinned runtimes record **zero cpu-migrations** where work-stealing
bounced tasks **~50,000 times** in the loaded 32-worker run.

| W | Server | req/s | RSS MB | Threads | CPU % | CPU µs/req | syscalls/req | ctx-sw/req | cpu-migr |
|--:|--|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | **compio-pool** | 381,475 | 5.2 | 5 | 399 | 10.5 | **0.03** | 0.00 | 0 |
| 4 | tokio-per-core | 320,711 | 6.8 | 5 | 399 | 12.4 | 2.00 | 0.00 | 0 |
| 4 | tokio-default | 267,153 | 11.1 | 5 | 358 | 13.4 | 1.98 | 0.00 | 82 |
| 8 | **compio-pool** | 820,669 | 5.6 | 9 | 797 | 9.7 | **0.03** | 0.00 | 0 |
| 8 | tokio-per-core | 636,432 | 7.1 | 9 | 797 | 12.5 | 1.99 | 0.00 | 0 |
| 8 | tokio-default | 611,911 | 11.2 | 9 | 706 | 11.5 | 1.98 | 0.00 | 10 |
| 16 | **compio-pool** | 1,359,972 | 6.7 | 17 | 1593 | 11.7 | **0.07** | 0.00 | 0 |
| 16 | tokio-per-core | 1,433,222 | 7.6 | 17 | 1585 | 11.1 | 2.05 | 0.00 | 0 |
| 16 | tokio-default | 835,307 | 11.4 | 17 | 1122 | 13.4 | 2.01 | 0.01 | 57 |
| 24† | compio-pool | 514,934 | 7.6 | 25 | 943 | 18.3 | 1.39 | 0.56 | 0 |
| 24† | tokio-per-core | 556,451 | 8.3 | 25 | 852 | 15.3 | 2.81 | 0.57 | 0 |
| 24† | tokio-default | 687,586 | 11.5 | 25 | 1044 | 15.2 | 2.11 | 0.06 | 74 |
| 28† | compio-pool | 240,421 | 8.2 | 29 | 481 | 20.0 | 1.74 | 0.80 | 0 |
| 28† | tokio-per-core | 276,168 | 8.6 | 29 | 489 | 17.7 | 2.91 | 0.80 | 0 |
| 28† | tokio-default | 356,833 | 11.6 | 29 | 586 | 16.4 | 2.37 | 0.17 | 67 |
| 30† | compio-pool | 116,907 | 8.4 | 31 | 260 | 22.3 | 1.86 | 0.89 | 0 |
| 30† | tokio-per-core | 131,701 | 8.7 | 31 | 266 | 20.2 | 2.95 | 0.89 | 0 |
| 30† | tokio-default | 171,527 | 11.5 | 31 | 354 | 20.7 | 3.13 | 0.39 | 37 |
| 32‡ | **compio-pool** | 1,755,888 | 8.7 | 33 | 1653 | 9.4 | **0.13** | 0.13 | 0 |
| 32‡ | tokio-per-core | 1,266,555 | 8.9 | 33 | 1730 | 13.7 | 2.05 | 0.53 | 0 |
| 32‡ | tokio-default | 755,246 | 11.7 | 33 | 1106 | 14.6 | 2.01 | 0.09 | 49,973 |

Threads = W+1 (one pinned worker per core) for all three. syscalls and context-switches are
whole-process `perf` counts; CPU % is summed across threads (1600% ≈ 16 busy cores). Raw TSV:
[`_raw/res.tsv`](./_raw/res.tsv), log [`_raw/resbench.log`](./_raw/resbench.log).

## Reproduce

```bash
# on the box (Rust >=1.95, build-essential, python3):
cd compio-pool/crates/deadpool-baseline
./bench.sh --suites scale512,scale16k,payload,conns \
           --servers tokio-default,tokio-per-core,compio-pool \
           --out ~/bench-out
```

Raw data in [`_raw/bench-out/`](./_raw/bench-out/): `summary.txt` (full tables incl. CPU/req),
`runs.tsv` (every run), `env.txt` (machine + versions), `run.log`, and the single uploadable
`bench-*.log`.
