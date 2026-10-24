# compio-pool (bb8 redesign) on `ubuntu-mcp` — Dell Precision 3571, i7-12800H, 20-thread x86 hybrid

**Date:** 2026-10-09 · **Host:** `prabhat-Precision-3571` (MCP name `ubuntu-mcp`), connected over SSH
**Code under test:** the same working tree as the [Altra run](../gcp-t2a-standard-32/) — the bb8-style
pool redesign at `experimental@defda0d` + 20 uncommitted files. `examples/echo.rs` is a developer-owned
thread-per-core loop that leases a per-connection buffer from the new
[`Pool`](../../../src/pool.rs) / [`ManageConnection`](../../../src/manage.rs). There is no `Server`;
the crate is the pool and the example owns the runtime, the pinning and the accept loop.

This is the x86 companion to the ARM Altra report, run with the identical harness and method so the
two can be read side by side. **The result is different, and that is the headline.**

## Headline

On this **12th-gen Intel laptop** the three runtimes land **within ~5–10 % of each other on
throughput**, and the big compio wins from the 32-core ARM Altra **do not reproduce**:

- **Throughput order is `tokio-per-core ≥ compio-pool ≥ tokio-default`.** compio-pool sits *between*
  the two tokio variants — a few % ahead of tokio-default, ~**2–5 % behind tokio-per-core** across
  scale512, payload and conns. On Altra compio led per-core by **1.25–1.29×**; here it trails it
  slightly. Peak split throughput: **860 k req/s** (tokio-per-core, 512 B, W=4) vs 835 k (compio).
- **CPU-µs/request is essentially tied** (~4–7 µs server CPU/req, all three within noise). The ~20 %
  CPU-per-request advantage compio showed on the Neoverse-N1 **is gone** on Alder Lake.
- **compio keeps its architectural wins — they just stop translating into a throughput lead:**
  - **Syscalls/request: ~0.03 vs ~2.0** for both tokio variants (io_uring batches submit+complete;
    epoll still pays a read and a write each time). Unchanged — but on x86 those syscalls are cheap.
  - **Memory: RSS 5.6–7.5 MB** vs ~8–9 MB (tokio-per-core) and **~12 MB (tokio-default)** — about
    **half** the footprint of the work-stealing baseline.
  - **Tail latency under high concurrency:** at 1024 conns compio's **p99 is 2219 µs** vs
    tokio-per-core **2649 µs** and tokio-default **4474 µs** — compio's tail is the tightest and
    **2× tighter than tokio-default**.
  - **Zero cpu-migrations** for the two pinned runtimes; tokio-default's work-stealing migrated tasks
    up to **~9,500 times** in the saturated shared run (vs 0).
- **Where compio still edges ahead on throughput:** **16 KiB at low worker counts** (W=1–2, ~1.04–1.06×
  over both tokios) — the one spot where fewer syscalls + no cross-core wakeups win on x86 too.

**Why the contrast with Altra?** Three things. (1) **Syscall cost.** On the Neoverse-N1 a syscall /
context-switch was expensive enough that cutting 2.0 → 0.03 syscalls/req bought ~20 % CPU and real
throughput; on this Alder Lake part (Enhanced IBRS, light effective mitigation cost) a syscall is
cheap, so epoll's two-per-request cost almost nothing and compio's savings don't convert. (2) **Scale
and topology.** Altra gave 32 uniform cores; here the clean `split` has only **5–6 P-cores** for the
server, with **SMT** and a **hybrid P/E** layout, so there is far less room for compio's linear-scaling,
no-steal advantage to compound — and tokio's epoll loop is extremely well-tuned on x86/Linux. (3) **It
is a shared laptop, not a datacenter VM** (`powersave` governor, 4 users logged in, load ~1.1 at start) —
good enough for a clean comparison, not for chasing a few-percent edge. See [Caveats](#caveats).

The refactor conclusion from Altra still holds: the bb8 `Pool` on the hot path (lease a buffer per
connection, return on drop) adds **no measurable overhead** — a `get()` is a pop from a thread-local
`Vec`, a return is a push, no atomics, no lock. compio is not losing to the pool; on x86 it is simply
at parity with a very good epoll runtime.

## Machine

| | |
|---|---|
| Instance | Dell **Precision 3571** mobile workstation (bare metal, `virtualization: none`) |
| CPU | **12th Gen Intel Core i7-12800H** (Alder Lake-H) — **6 P-cores (HT) + 8 E-cores** = 14 cores / **20 threads**, 1 socket |
| Core freq | P-core up to **4.7–4.8 GHz**, E-core up to **3.7 GHz**; governor **`powersave`** (intel_pstate, reaches turbo under load) |
| Caches | L1d 48K + L1i 32K per core · L2 1.25 MiB (8 inst.) · **L3 24 MiB shared** |
| NUMA | **1 node** (0–19) |
| Memory | **31 GiB** (26 GiB available), 2 GiB swap |
| Disk | NVMe `/dev/nvme0n1p4` (308 G, 48 % used): ~1.3 GB/s buffered write, 2.0 GB/s O_DIRECT, 1.5 GB/s cold read, 0.89 ms/fsync |
| Kernel | Linux 6.8.0-138-generic (Ubuntu 22.04.5), **`io_uring_disabled=0`** |
| rustc | **1.99.0** (same as the Altra run) |
| Limits | `somaxconn=4096`, `nofile=524288` (raised for the run), `tcp_tw_reuse=2` |
| Mitigations | Enhanced/Automatic IBRS, SSB via prctl, Clear Register File, IBPB-before-exit — all relatively cheap on this part |

Full `lscpu`, mitigations and cache detail: [`_raw/bench-out/env.txt`](./_raw/bench-out/env.txt).
Hardware snapshot (CPU/disk/fsync): [`_raw/machine-snapshot.txt`](./_raw/machine-snapshot.txt).

## Method

Identical to the Altra run — driver `crates/deadpool-baseline/bench.sh`, same two harness adaptations
(preflight points at `src/pool.rs` not the deleted `src/worker.rs`; `--locked` dropped because `flume`
was removed), same suites, same `split` placement, same 3×5 s medians on loopback.

- **Servers:** `tokio-default` (multi-thread work-stealing, one shared listener + pool),
  `tokio-per-core` (tokio/epoll, one pinned `current_thread` runtime per core, `SO_REUSEPORT`),
  `compio-pool` (this crate: io_uring, one pinned ring per core, `SO_REUSEPORT`, bb8 pool).
- **Placement `split`:** the server gets W dedicated **physical cores** — the harness picks the
  **even-numbered CPUs (one SMT sibling per core), and the 6 P-cores are enumerated first**, so
  **worker scaling W=1→5 runs entirely on P-cores** while the load generator (`examples/load.rs`) takes
  the rest (the spare P-core SMT siblings + all 8 E-cores). This cleanly avoids the P/E heterogeneity
  for the main matrix. With client ratio 1.5 on 14 physical cores, W tops out at **5** (vs 12 on Altra).
- **Flags:** `S` = server CPUs ≥85 % busy, `C` = client ≥85 %, `SC` = both near cap. W=1,2,4 are clean
  server-bound points; **W=5 is `SC`** (the client has too few cores left to stay out of the way, so
  W=5 is a lower bound, not a pure server number).
- **Suites:** `scale512`, `scale16k` (worker scaling at 512 B / 16 KiB), `payload` (byte sweep at W=4),
  `conns` (concurrency sweep at W=4). **3 repeats × 5 s**, median reported. `churn` and the
  `compio-pool:1/:2` capacity variants excluded for the same reason as Altra — the bb8 pool has no
  fd-handoff path; at capacity it applies backpressure.

> Not comparable to the x86 [`gcp-n2-standard-48`](../gcp-n2-standard-48/) matrix either: that was a
> 48-vCPU server covering file-I/O / WAL / Redis paths this refactor deleted. This run isolates **echo**,
> which is what exercises the pool. The right comparison is to [`gcp-t2a-standard-32`](../gcp-t2a-standard-32/)
> (ARM), same code, same harness.

## Results

Each cell is the **median of 3 runs**. `compio/pc` = compio-pool ÷ tokio-per-core; `pc/def` =
tokio-per-core ÷ tokio-default. Full tables incl. CPU/req: [`_raw/bench-out/summary.txt`](./_raw/bench-out/summary.txt).

### scale512 — 512 B echo, worker scaling (P-cores)

| W | tokio-default | **tokio-per-core** | compio-pool | compio/pc | pc/def |
|--:|--|--|--|:--:|:--:|
| 1 | 251,776 `[S]` | **253,776** `[S]` | 242,416 `[S]` | 0.96× | 1.01× |
| 2 | 462,825 `[S]` | **499,914** `[S]` | 479,592 `[S]` | 0.96× | 1.08× |
| 4 | 780,899 `[S]` | **860,116** `[S]` | 834,685 `[S]` | 0.97× | 1.10× |
| 5 | 733,801 `[SC]` | 756,421 `[SC]` | 740,043 `[SC]` | 0.98× | 1.03× |

p50 / p99 (µs):

| W | default | per-core | compio |
|--:|--|--|--|
| 1 | 257 / 274 | 250 / 267 | 259 / 411 |
| 2 | 137 / 204 | 127 / **141** | 128 / 216 |
| 4 | 161 / 286 | 149 / **195** | 146 / 265 |
| 5 | 207 / 452 | 212 / 313 | 206 / 405 |

### scale16k — 16 KiB echo, worker scaling

| W | tokio-default | tokio-per-core | **compio-pool** | compio/pc |
|--:|--|--|--|:--:|
| 1 | 147,929 `[S]` | 147,773 `[S]` | **156,279** `[S]` | **1.06×** |
| 2 | 301,753 `[S]` | 311,598 `[S]` | **325,584** `[S]` | **1.04×** |
| 4 | 492,280 `[S]` | **603,427** `[S]` | 517,872 `[S]` | 0.86× |
| 5 | 460,130 `[SC]` | 495,213 `[SC]` | 470,268 `[SC]` | 0.95× |

compio leads at W=1–2 (large payload + few syscalls favors io_uring); the W=4 point is the noisiest in
the whole run (±6–8 % over repeats) and the one where tokio-per-core jumps ahead.

### payload — W=4, 128 conns, byte size swept

| Bytes | tokio-default | **tokio-per-core** | compio-pool | compio/pc |
|--:|--|--|--|:--:|
| 64 | 706,121 | **776,946** | 740,504 | 0.95× |
| 512 | 700,778 | **764,619** | 736,551 | 0.96× |
| 2048 | 665,507 | **718,610** | 699,401 | 0.97× |
| 8192 | 577,224 | **632,427** | 607,348 | 0.96× |
| 16384 | 485,774 | **528,149** | 517,468 | 0.98× |

Unlike Altra (where compio led 64 B by 1.29×), on x86 tokio-per-core leads at every payload size by a
flat ~4–5 %. The gap does **not** widen on small payloads — the syscall-cost advantage that produced
that effect on ARM is absent here.

### conns — W=4, 512 B, concurrency swept

| conns | tokio-default | **tokio-per-core** | compio-pool | p99 default | p99 per-core | **p99 compio** |
|--:|--|--|--|--|--|--|
| 4 | 275,394 | **325,472** | 311,361 | 23 | **18** | 17 |
| 16 | 636,613 `[S]` | **691,914** `[S]` | 662,385 `[S]` | 44 | 46 | 44 |
| 64 | 674,095 `[S]` | **757,483** `[S]` | 723,029 `[S]` | 189 | **145** | 161 |
| 256 | 719,705 `[S]` | **763,452** `[S]` | 742,862 `[S]` | 719 | **468** | 657 |
| 1024 | 600,330 | **651,774** `[S]` | 641,075 `[S]` | 4474 | 2649 | **2219** |

tokio-per-core leads throughput throughout by ~4–6 %. But at the deepest queue (**1024 conns**)
compio-pool has the **tightest p99 — 2219 µs**, against 2649 µs (per-core) and a 4474 µs blow-out for
work-stealing tokio-default. compio trades a sliver of throughput for the best worst-case latency.

## Resource efficiency

On this machine the CPU-µs/request rows (in [`summary.txt`](./_raw/bench-out/summary.txt)) are
**statistically tied** across all three runtimes — e.g. at 512 B: 5.3 / 5.2 / 5.4 µs server CPU/req
(default / per-core / compio). That is the core difference from Altra, where compio needed ~20 % less
CPU per request. The io_uring syscall savings are real and large (see below) but on Alder Lake a
syscall is cheap enough that removing it frees almost no CPU time. compio's remaining, durable wins are
**memory footprint** and **syscall/migration counts**, not CPU or throughput.

## Resource utilization — instrumented worker sweep

A separate `perf`-instrumented run ([`_raw/resbench.sh`](./_raw/resbench.sh)) pins each server to the
first W **logical** CPUs (W = 4…20; note this is *sequential* pinning that **includes SMT siblings**,
unlike the matrix's one-sibling-per-core `split`), drives it with `examples/load` on the rest — 512 B,
512 conns, 10 s — and measures the **server process** with `perf` (`raw_syscalls:sys_enter`,
context-switches, cpu-migrations, page-faults) + `/proc` (peak RSS, threads, CPU time). `perf` ran
under `sudo` (root bypasses `perf_event_paranoid=4`).

Because this pinning packs SMT siblings onto the server first, the clean saturated points are **W=4**
(2 P-cores) and **W=8** (4 P-cores). **W=12–18 are client-starved** (the server eats P-cores then
E-cores, leaving the client too few cores — marked †) so their throughput reads low. **W=20‡ is shared
placement** (client gets all cores too) — the clean high-worker point.

| W | Server | req/s | RSS MB | Threads | CPU % | CPU µs/req | syscalls/req | ctx-sw/req | cpu-migr |
|--:|--|--:|--:|--:|--:|--:|--:|--:|--:|
| 4 | tokio-per-core | **626,327** | 8.0 | 5 | 398 | 6.4 | 1.99 | 0.00 | 0 |
| 4 | tokio-default | 608,020 | 11.8 | 5 | 396 | 6.5 | 1.98 | 0.00 | 28 |
| 4 | **compio-pool** | 592,407 | **5.6** | 5 | 398 | 6.7 | **0.03** | 0.00 | 0 |
| 8 | **compio-pool** | **875,586** | **5.9** | 9 | 795 | **9.1** | **0.03** | 0.00 | 0 |
| 8 | tokio-per-core | 749,772 | 8.2 | 9 | 796 | 10.6 | 2.00 | 0.00 | 0 |
| 8 | tokio-default | 725,592 | 11.8 | 9 | 783 | 10.8 | 1.99 | 0.00 | 60 |
| 12† | tokio-default | 537,270 | 11.9 | 13 | 599 | 11.2 | 2.90 | 0.35 | 4,980 |
| 12† | compio-pool | 527,238 | 6.5 | 13 | 620 | 11.8 | 1.26 | 0.46 | 0 |
| 12† | tokio-per-core | 525,624 | 8.6 | 13 | 565 | 10.8 | 2.78 | 0.52 | 0 |
| 16† | tokio-default | 312,094 | 11.9 | 17 | 335 | 10.7 | 3.46 | 0.74 | 1,418 |
| 16† | tokio-per-core | 305,894 | 8.7 | 17 | 312 | 10.2 | 2.91 | 0.82 | 0 |
| 16† | compio-pool | 303,851 | 7.0 | 17 | 346 | 11.4 | 1.72 | 0.79 | 0 |
| 18† | tokio-default | 194,767 | 12.0 | 19 | 189 | 9.7 | 4.03 | 1.15 | 650 |
| 18† | compio-pool | 171,129 | 7.2 | 19 | 196 | 11.5 | 1.88 | 0.91 | 0 |
| 18† | tokio-per-core | 170,234 | 8.9 | 19 | 171 | 10.0 | 2.95 | 0.92 | 0 |
| 20‡ | **compio-pool** | **805,629** | **7.5** | 21 | 934 | 11.6 | **0.10** | 0.46 | 0 |
| 20‡ | tokio-per-core | 792,246 | 9.1 | 21 | 947 | 12.0 | 2.03 | 0.48 | 0 |
| 20‡ | tokio-default | 710,949 | 12.1 | 21 | 937 | 13.2 | 1.99 | 0.40 | 9,507 |

Reading it:

- **Syscalls/request:** compio **0.03** vs ~**2.0** for both tokio runtimes at the clean points — a
  ~60× reduction, identical in shape to Altra. io_uring's architectural advantage is intact; it just
  no longer buys throughput on this CPU.
- **Memory:** compio **5.6–7.5 MB**, tokio-per-core 8–9 MB, tokio-default a flat **~12 MB**. compio
  runs in roughly **half** the resident memory of the work-stealing baseline.
- **cpu-migrations:** **0** for both pinned runtimes at every point; tokio-default's work-stealing
  migrated tasks (28 → 60 → 4,980 → up to **9,507** at the saturated shared W=20). Smaller than Altra's
  ~50 k at 32 workers (fewer cores), but the same qualitative behavior — pinning eliminates migration.
- **Where compio leads on throughput here:** the **W=8** clean point (876 k vs 750 k / 726 k, +17 %)
  and the **W=20 shared** point (806 k vs 711 k default). The SMT-dense pinning in this sweep favors
  compio more than the matrix's SMT-sparse `split` does — a reminder that the winner is
  **placement-sensitive** on a hybrid SMT part.

Raw TSV: [`_raw/res.tsv`](./_raw/res.tsv) · log [`_raw/resbench.log`](./_raw/resbench.log).

## Caveats

This is a **shared developer workstation**, not an isolated cloud VM, so treat these as *indicative*
numbers, not datacenter-grade:

- **Governor `powersave`** (left unchanged — it is the machine's real default; intel_pstate still
  reaches turbo under saturation, so full-load points are largely unaffected, but partial-load latency
  can drift).
- **Not fully idle:** 4 users logged in, load average ~1.1 at start. Run-to-run spread was mostly
  ±0–3 % (a few points ±6–8 %, called out above), so the ~5 % runtime gaps are real but not razor-sharp.
- **Hybrid P/E + SMT** make "thread-per-core" placement-dependent; the matrix (`split`, P-cores, one
  sibling/core) and the resbench sweep (sequential, SMT-dense) disagree on the winner at some points.
  Both are reported rather than cherry-picked.
- **Loopback only** — no real NIC, no network stack past `lo`. Server and client share the box.
- **Small clean scale:** only 5–6 dedicated P-cores for the server vs 32 uniform cores on Altra.

## Reproduce

```bash
# from the Mac: copy the working tree to the box (filesystem MCP is read/write only; execution is SSH)
rsync -az --exclude target --exclude node_modules ./ ubuntu-mcp:compio-pool/

# on the box (Rust 1.99, build-essential, python3, perf; io_uring enabled):
cd ~/compio-pool/crates/deadpool-baseline
# two harness adaptations for the bb8 redesign:
sed -i 's#examples/load.rs src/worker.rs#examples/load.rs src/pool.rs#' bench.sh
sed -i 's#cargo build --release --locked#cargo build --release#g' bench.sh
ulimit -n 524288
./bench.sh --suites scale512,scale16k,payload,conns \
           --servers tokio-default,tokio-per-core,compio-pool \
           --out ~/bench-out

# instrumented resource sweep (needs sudo for perf):
bash ~/resbench.sh ~/resbench-out      # see _raw/resbench.sh (WLIST tuned to 20 CPUs)
```

Raw data in [`_raw/`](./_raw/): `bench-out/summary.txt` (full tables incl. CPU/req), `bench-out/runs.tsv`
(every run), `bench-out/env.txt` (machine + versions), `bench-out/bench-*.log` (single uploadable log),
`res.tsv` + `resbench.log` (perf sweep), `machine-snapshot.txt` (hardware snapshot).
