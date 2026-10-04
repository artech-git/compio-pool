# Results: 48-vCPU Google Cloud VM — Redis-backed KV (compio-redis vs tokio + deadpool)

A head-to-head of **compio-pool + compio-redis** against **tokio + deadpool-redis**, both
fronting a real `redis-server` over the *same* line protocol, driven by the same
[`kvload`](../../../examples/kvload.rs) generator. Only two things vary: the runtime
(compio/io_uring vs tokio/epoll) and the pool (compio-pool's thread-local `Resource` vs
deadpool). Median of 5 × 4 s, one machine, identical CPU placement, **0 mismatches / 0 failed
clients across all 405 runs**.

* **Machine**: GCP `n2-standard-48` — Intel Xeon (Cascade Lake-class) @ 2.80 GHz, 48 vCPU =
  2 sockets × 12 physical cores × 2 SMT threads (**24 physical cores**), 2 NUMA nodes
  (node0 = CPU 0-11,24-35; node1 = 12-23,36-47), 189 GiB RAM.
* **OS**: kernel `7.0.0-1011-gcp`, `io_uring_disabled=0`, `somaxconn=4096`, `nofile=1048576`.
  rustc 1.99.0, git `reuseport-workers@58ed11f` (+ the untracked redis crate/examples).
* **Backend**: `redis-server` v8.10.2 (jemalloc 5.3.0), built from source, `--save "" --appendonly
  no --io-threads 1`, each instance pinned to its own core.
* **Harness**: [`redisbench.sh`](../../../crates/deadpool-baseline/redisbench.sh) +
  [`kvload`](../../../examples/kvload.rs). Per-run metrics: req/s, p50/p90/p99/p99.9/max latency,
  proxy user+sys CPU per request and cores-used, redis cores-used, voluntary/involuntary
  context-switches per request, peak RSS (`VmHWM`), and an S/R/C saturation flag.

## Three servers

| id | what it is |
|---|---|
| **compio-pool** | compio-redis dropped into a compio-pool server: io_uring, one pinned worker per core, SO_REUSEPORT, a thread-local pool of outbound Redis connections |
| **tokio per-core** | tokio `current_thread` runtime per pinned core, SO_REUSEPORT, a per-worker deadpool-redis pool — the direct architectural match |
| **tokio default** | tokio multi-thread work-stealing, one shared listener + one shared deadpool pool — what a plain `tokio::main` gives |

Both proxies lease one upstream connection for an inbound connection's whole lifetime (not
per-command), so the pool holds ~one upstream per inbound — the apples-to-apples match.

## Two sweeps (because a single redis is single-threaded)

`redis-server` executes commands on one thread (~1 core). Point every proxy worker at one redis
and the backend, not the proxy, sets the ceiling. So the study runs both wirings:

* **sharded** — one dedicated redis core *per proxy core*; the client fans across them. Nothing
  shared, so throughput scales with cores and the **proxy layer** is what's measured. W = 1/2/4/8
  (each shard = 1 redis core + 1 proxy core → 2W of 24 cores; clients get the rest).
* **single** — all proxy workers share one redis (the realistic backend-bound case). W =
  1/2/4/8/16, redis on 1 core, proxy on W cores, clients on the rest. Measures proxy efficiency
  and latency; throughput plateaus (then collapses) once redis saturates.

## TL;DR — the verdict

When the **proxy layer** is the thing under load (sharded, or low core counts before the single
redis saturates), **compio-pool wins across the board**: ~**+11% throughput**, ~**-10% CPU per
request**, ~**-15% p99 tail**, and ~**-35% memory** — at *every* core count, scaling linearly to
8 cores. When a **single shared redis** is the bottleneck, the backend caps everyone: compio
reaches that cap on fewer cores, but raw throughput converges, and tokio-default's single shared
pool actually weathers over-subscription of the lone backend best.

**Sharded, GET, 16 B values, median of 5** — the proxy-scaling story:

| W | compio req/s | tokio per-core | compio ÷ tpc | p50 c/tpc (µs) | p99 c/tpc (µs) | RSS c/tpc |
|--:|--:|--:|--:|--:|--:|--:|
| 1 | 64,561 | 57,726 | **1.12×** | 986 / 1102 | 1067 / 1268 | 4 / 6 MB |
| 2 | 126,836 | 113,491 | **1.12×** | 503 / 569 | 548 / 644 | 7 / 10 MB |
| 4 | 257,507 | 230,594 | **1.12×** | 496 / 557 | 536 / 634 | 14 / 20 MB |
| 8 | 542,119 | 486,909 | **1.11×** | 497 / 523 | 539 / 636 | 27 / 41 MB |

Linear scaling, W1→W8: compio **8.40×**, tokio per-core **8.43×** (both near-perfect; compio just
starts ~11% higher and stays there). SET and MIX track GET within 1%.

## Scaling the proxy layer (sharded)

compio-pool is faster per core *and* scales just as cleanly. At saturation (W=8, both pinned to
8.1 cores, flag `S`):

| server | req/s | **req/s per proxy-core** | CPU µs/req | RSS |
|---|--:|--:|--:|--:|
| compio-pool | 542,119 | **66,599** | 15.0 | 27 MB |
| tokio per-core | 486,909 | 59,743 | 16.7 | 41 MB |

compio clears **+11.5% more requests per core** on the same silicon, spends ~10% less CPU on each
one, and holds a tighter tail (p99 539 vs 636 µs, −15%). The redis side confirms it's doing more
real work: at W=8 compio drives the 8 redis instances to 3.1–3.3 aggregate cores vs tokio's
2.8–2.9 — more throughput, more backend load, same correctness.

## Single shared redis — three regimes

GET, 16 B, median of 5. `redis_cpus_used` and the flag tell the story (`S` proxy-bound, `R`
redis-bound):

| W | compio | tokio pc | tokio def | cmp÷pc | cmp÷def | redis cores | flags c/pc/def | p50 c/pc/def (µs) |
|--:|--:|--:|--:|--:|--:|--:|:--|--:|
| 1 | 64,861 | 58,436 | 52,053 | 1.11× | **1.25×** | 0.40 | S/S/S | 979 / 1093 / 1246 |
| 2 | 129,032 | 114,977 | 95,056 | 1.12× | **1.36×** | 0.71 | S/S/S | 538 / 548 / 677 |
| 4 | 196,714 | 200,422 | 184,734 | 0.98× | 1.07× | 1.02 | R/SR/SR | 618 / 615 / 688 |
| 8 | 163,906 | 167,948 | 195,114 | 0.98× | 0.84× | 1.05 | R/R/R | 1509 / 1473 / 1232 |
| 16 | 120,598 | 121,224 | 150,742 | 0.99× | 0.80× | 1.14 | R/R/R | 4179 / 4162 / 3268 |

1. **Proxy-bound (W=1-2):** before redis matters, compio wins decisively — **+11-12%** over
   tokio per-core, **+25-36%** over tokio default, at the lowest CPU/req (16.0 vs 17.8 vs 20.0 µs)
   and lowest latency.
2. **Redis ceiling (W=4):** all three converge at ~195-200k because redis is now saturated
   (1.0 core, `R`). tokio per-core nudges ahead by 1.9% — but it burns 3.67 proxy cores to hit
   the ceiling vs compio's **3.32**, and tokio default's 3.90. Same work, less silicon.
3. **Over-subscription collapse (W=8-16):** piling more proxy cores + connections onto *one*
   redis *lowers* throughput and wrecks latency (compio 197k→164k→121k; p50 618µs→1.5ms→4.2ms).
   Here tokio **default degrades most gracefully** (195k/151k): its single shared pool keeps far
   fewer connections pounding the lone redis than N independent per-core pools do. This is the
   realistic cost of over-provisioning a proxy tier in front of a single backend.

> **Caveat on regime 3.** Offered connections scale with W in this harness (`conns = 32·W`), so
> W=8/16 against one redis is deliberately over-driven. The takeaway is directional: past backend
> saturation, more proxy cores hurt, and a shared pool beats per-core pools at shielding one redis.

## Resource utilization

* **Memory (peak RSS).** compio-pool is dramatically lighter everywhere, and the gap widens with
  cores: sharded W=8 **27 MB vs 41 MB** (−34%); single W=16 **12-14 MB vs 31-32 MB** (tokio
  per-core) **/ 28-32 MB** (default) — roughly **½ to ⅓** the footprint. At W=1 it's 4 MB vs 6 MB.
* **CPU per request.** compio is the cheapest in every proxy-bound point (≈16 µs/req vs 17.5-20
  µs). The lead narrows under the redis ceiling because everyone waits on the backend.
* **Context switches.** Low throughout (<0.1/req up to W=4). Under the W=16 single-redis
  over-subscription they climb to ~1.5/req for the per-core servers vs ~0.49/req for default —
  the same shared-pool effect, fewer upstream sockets to juggle.

## Workload variations (single redis, W=8, all redis-bound)

Because these all run against one saturated redis (`R`), raw req/s mostly reflects how little
each proxy leans on the backend; compio and tokio per-core sit within ~2% of each other, default
posts higher raw numbers for the regime-3 reason above. The *shape* is what's informative:

| variation | compio | tokio pc | tokio def | note |
|---|--:|--:|--:|:--|
| PING (no keyspace) | 179,867 | 182,580 | 211,169 | cheapest op — pure proxy overhead |
| GET 256 B | 157,554 | 160,665 | 181,075 | −4% vs 16 B |
| GET 4096 B | 122,482 | 121,623 | 140,880 | −25% vs 16 B (payload cost) |
| GET pipeline 8 | 154,080 | 167,442 | 197,594 | batches cut CPU/req (compio 21.4→18.6 µs) |

Pipelining 8 commands per batch drops compio's CPU/req from 21.4 to 18.6 µs (syscall
amortization), though at the single-redis ceiling it can't turn that into more throughput; p50
is per-8-batch (~13 ms) by construction.

## Gotchas

* **Correctness under load.** 0 mismatches / 0 failed clients over all 405 runs. compio-redis's
  cancellation-safety invariant (exactly one reply per request, or the connection is dropped on
  `recycle`) held at 540k req/s.
* **Cold pools.** The first timed run after a proxy starts pays to lazily dial its upstream pool
  (~35% slow). The harness does one untimed warmup per point so all 5 recorded repeats are
  steady-state; spread is <1% at almost every point.
* **`redis-server` isn't exactly 1 core.** It burns ~1.0 core on command execution plus jemalloc
  / background threads, so `redis_cpus_used` reads 1.0-1.6 at the ceiling. The `R` flag's 0.85×1
  threshold fires a touch early; read the raw `redis_cpus_used` column, not just the flag.
* **No docker on the box** — redis was built from source (the stack modules fail to build; the
  core `redis-server` is all that's needed and builds clean).
* **Sharded tops out at W=8** here: each shard needs a dedicated redis core *and* proxy core
  (2W of 24), leaving the rest for clients. That's the honest scaling ceiling of this box, not of
  the architecture.

## Files

* [`data.json`](data.json) — the 81 median-of-5 points, every metric (drives the interactive report).
* [`_raw/runs.tsv`](_raw/runs.tsv) — all 405 raw runs, one line each (the `#`-header names the columns).
* [`_raw/summary.txt`](_raw/summary.txt) — the per-point median tables as the harness printed them.
* [`_raw/redisbench-compio-bench-48-20261003T202907Z.log`](_raw/redisbench-compio-bench-48-20261003T202907Z.log) — the single uploadable log (env + summary + raw + progress).
* [`_raw/env.txt`](_raw/env.txt) — full environment + `lscpu`.

## Reproduce

```sh
# on a Linux box with io_uring (needs build-essential, curl, rust >= 1.95)
# 1. build redis from source
cd ~ && curl -fsSLO https://download.redis.io/redis-stable.tar.gz
tar xzf redis-stable.tar.gz && make -C redis-stable -j"$(nproc)" BUILD_TLS=no   # core binary is enough

# 2. build the three proxies + the load generator
cd compio-pool
cargo build --release --example kvload
cargo build --release --manifest-path crates/compio-redis/Cargo.toml --example server
cargo build --release --manifest-path crates/deadpool-baseline/Cargo.toml --example redis_proxy

# 3. run both sweeps (auto-detects topology, writes one uploadable log)
crates/deadpool-baseline/redisbench.sh --seconds 4 --repeats 5
#   --sweep single|sharded|both   --servers ...   --quick   (see top of the script)
```
