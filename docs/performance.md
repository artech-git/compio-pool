# Performance

What was measured, how, and what the numbers say. Everything here comes from the two shipped
examples: [`echo`](../examples/echo.rs) is the server, [`load`](../examples/load.rs) the
client. Both are release builds, and the commands next to each table reproduce it; nothing
else is needed. The [tokio baseline](#baseline-tokio) adds two tokio servers and a small
accounting harness from [`crates/deadpool-baseline`](../crates/deadpool-baseline/).

## The machine

A `limactl` VM on an Apple Silicon host: Ubuntu 25.04, kernel 6.14, 12 vCPUs, 20 GiB, Apple
Virtualization framework, `virtio_net` with a single combined queue and receive hashing fixed
off. Traffic is loopback. **The load generator's 64 client threads run on the same 12 vCPUs as
the 12 workers**, so every number below includes contention with the thing measuring it. Treat
them as relative; absolute throughput on dedicated hardware will differ.

No NIC tuning was applied in the VM (it cannot be: see
[operations.md](operations.md#what-the-script-does-on-hardware-it-cannot-tune)), so
`incoming_cpu` was off throughout.

## Scaling with workers

64 persistent connections, 512-byte echo, 5 seconds. Nothing overflows: capacity 1024 per
worker, so every connection is accepted and served on one core and the handoff path is idle.

```sh
cargo run --release --example echo -- 127.0.0.1:7100 1024 W
cargo run --release --example load -- 127.0.0.1:7100 --conns 64 --seconds 5 --bytes 512
```

| workers | req/s | p50 µs | p90 µs | p99 µs | p99.9 µs |
|---|---|---|---|---|---|
| 1 | 157,360 | 387.9 | 484.9 | 520.9 | 735.5 |
| 2 | 178,194 | 348.0 | 451.7 | 620.7 | 2,895.2 |
| 4 | 458,624 | 106.2 | 223.0 | 734.6 | 3,019.2 |
| 8 | 1,406,218 | 34.1 | 76.8 | 221.7 | 586.5 |
| 12 | 1,210,525 | 29.6 | 90.9 | 461.0 | 1,651.7 |

Throughput scales close to linearly to 8 workers. At 12 it drops: 12 workers plus 64 client
threads on 12 vCPUs is oversubscribed, and the workers are pinned while the clients are not.
On a host where the clients live elsewhere, expect the curve to keep going.

## The cost of a handoff

To put every connection through the overflow path, the server runs with capacity 1 or 2 per
worker and the client opens a fresh connection for every request (`--reconnect 1`). Each request
then includes connect, the kernel's hash to a listener, `accept`, and — for every connection but
the one each core is already serving — detach, channel, attach on another core. 64 clients, 5
seconds.

```sh
cargo run --release --example load -- 127.0.0.1:7100 --conns 64 --seconds 5 --bytes 512 --reconnect 1
```

| server | conn/s | p50 µs | p90 µs | p99 µs | served local | handed off | claimed | bounced | rejected |
|---|---|---|---|---|---|---|---|---|---|
| 12 workers, capacity 1024 | 15,566 | 270.6 | 9,886 | 61,426 | 77,892 | 0 | 0 | 0 | 0 |
| 12 workers, capacity 2 | 15,857 | 419.8 | 17,372 | 45,810 | 8,117 | 71,232 | 71,232 | 6 | 0 |
| 12 workers, capacity 1 | 16,744 | 698.0 | 17,666 | 28,084 | 10,544 | 73,241 | 73,241 | 4 | 0 |
| 4 workers, capacity 1 | 16,068 | 1,386.9 | 21,605 | 26,816 | 9,560 | 70,843 | 70,843 | 2 | 0 |

Three things to read off this:

* **Connection rate is unchanged.** ~16k connections/s whether 0% or 90% of them go through the
  channel. The ceiling here is connection setup on loopback in a VM with 64 contending threads
  (the p90 and p99 columns are `connect()` latency, present in every row), not the handoff.
* **The median moves by the queue wait, not the mechanism.** 271 µs with room everywhere, 420 µs
  with 24 slots process-wide, 698 µs with 12. With 64 clients and 12 slots, a connection
  arriving at a full core *has* to wait for one of 12 handlers to finish; that wait is the
  difference. The detach + `try_send` + `recv_async` + `from_std` sequence itself is a few
  reference-count moves and one task wake.
* **Bounces are rare.** 4–6 in ~72,000 handoffs. The race in
  [decision 0004](decisions/0004-claim-without-holding-a-slot.md) is real and small.

Everything handed off was claimed; nothing was rejected or served over capacity, because the
channel (4096) never filled.

## Saturation with long-lived connections

64 persistent connections against 12 workers of capacity 2 (24 slots). The 40 surplus
connections are accepted, parked in the channel, and wait for a slot that only frees when a
served connection *ends* — which, with persistent clients, is the end of the run.

| req/s | p50 µs | p99 µs | max | served local | handed off | claimed | bounced |
|---|---|---|---|---|---|---|---|
| 1,796,032 | 9.6 | 55.4 | 4.99 s | 17 | 47 | 47 | 8 |

Higher throughput and far lower latency than the unconstrained 12-worker run, because only 24
connections are active instead of 64 and the cores stop thrashing. The 4.99 s maximum is a
parked connection's first request: it waited the whole run. That is what "at capacity" means in
this design — admission is per core, and a parked connection is a deliberate queue, not a
dropped one. If that is not acceptable, `capacity` is too small, or `OverflowPolicy::Reject`
turns the wait into a fast failure.

## Baseline: tokio

The numbers above say how the design scales against itself. This says how it compares to tokio.
[`crates/deadpool-baseline`](../crates/deadpool-baseline/) holds two tokio servers with the same
echo handler and the same 16 KiB pooled buffer as [`echo`](../examples/echo.rs), and the same
[`load`](../examples/load.rs) client drives all three:

| server | I/O | scheduling | buffer pool |
|---|---|---|---|
| **tokio default** (`echo`, in the baseline crate) | epoll | one multi-threaded work-stealing runtime, one shared listener | one shared `deadpool` pool |
| **tokio per-core** (`echo_tpc`) | epoll | one pinned `current_thread` runtime per core, `SO_REUSEPORT`, nothing shared | thread-local |
| **compio-pool** (`echo`) | io_uring | one pinned ring per core, `SO_REUSEPORT`, nothing shared | thread-local |

The middle row is a control. Tokio default against compio-pool changes two things at once, the
backend and the architecture. Default against per-core changes only the architecture (both are
epoll); per-core against compio-pool changes only the backend (both are thread-per-core).

These rows were taken in a **4-vCPU container** (Intel Xeon at 2.1 GHz, 48 KiB L1d and 2 MiB L2
per core, kernel 6.18, loopback), not the 12-vCPU VM above. Every cell is the median of three runs
of 4-5 s, with zero echo mismatches. Run-to-run spread is large: compio-pool at 4 workers measured
anywhere from about 207k to 268k req/s across seven runs, so treat differences under ~15% as ties.

```sh
cargo build --release --example echo --example load
cargo build --release --manifest-path crates/deadpool-baseline/Cargo.toml --example echo --example echo_tpc
python3 crates/deadpool-baseline/probe.py --label shared --workers 4 --conns 64 --bytes 16384
python3 crates/deadpool-baseline/probe.py --label split --workers 1 --server-cpus 0 --load-cpus 1,2,3 --bytes 16384
```

`probe.py` is `load` plus accounting: it also records the server's and the client's CPU time per
request and the server's context switches. It matters because **where the kernel puts the client
threads changes the answer**, as the next three subsections show.

### Clients sharing the server's cores

Plain `cargo run` for both programs, so the client threads and the workers all share the same
4 vCPUs, as in the tables above. Cells are req/s with p50 latency in µs.

| config | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| 512 B, 64 conns, 1 worker | 65,581 (936) | 68,961 (887) | 66,590 (897) |
| 512 B, 64 conns, 2 workers | 146,278 (410) | 177,229 (338) | 169,088 (351) |
| 512 B, 64 conns, 4 workers | 140,231 (406) | 191,440 (295) | 207,209 (261) |
| 512 B, 16 conns, 4 workers | 103,312 (130) | 313,831 (37) | 261,871 (50) |
| 16 KiB, 64 conns, 4 workers | 279,052 (7.8) | 263,157 (8.6) | 163,774 (343) |

* **The architecture is worth 1.2× to 3.0×.** Default to per-core, same epoll backend: 1.05×, 1.21×,
  1.37× and 3.04× on the four 512 B rows, with non-overlapping run ranges from two workers up. That
  is the thread-per-core effect, and tokio can have it without io_uring.
* **The backend is roughly a tie at 512 B.** Per-core to compio-pool is 0.97×, 0.95×, 1.08× and
  0.83×: no consistent direction, run ranges overlapping in three of the four rows, and the
  fourth (4 workers, where compio-pool is ahead) within what it measured in other sessions.
* **Why default flattens.** Not the accept loop or the pool mutex: both run once per *connection*,
  64 times in a run, and are nowhere near the request path of a persistent connection. What runs
  per request is the scheduler, and work stealing moves tasks and wakes parked workers across
  cores, which costs most when 64 client threads and the workers contend for 4 CPUs. The control
  removes that and keeps epoll, and the throughput comes back.
  [Nanakos's 24-core echo benchmark](https://www.include.gr/writing/rust-thread-per-core-async.html)
  finds the same split: tokio about 285k msg/s with or without `SO_REUSEPORT`, monoio on epoll
  1.65M, monoio on io_uring 1.63M.
* **16 KiB is the one row where compio-pool loses**, to both tokio servers. The next two
  subsections are about that row.

### Clients kept off the server's cores

The shared-core table cannot tell the server's cost from the kernel's placement of the clients,
so this pins them apart with `taskset`: the server on its own CPUs, the clients on the rest. It is
the closest this machine gets to a client on another host. Cells are req/s, then server CPU
µs per request (user + system).

| config | tokio default | tokio per-core | compio-pool |
|---|---|---|---|
| 512 B, 64 conns, 1 worker (cpu 0) | 64,282 · 15.3 | 67,627 · 14.5 | 64,588 · 15.1 |
| 16 KiB, 64 conns, 1 worker | 54,665 · 18.0 | 54,030 · 18.1 | 52,067 · 18.6 |
| 16 KiB, 256 conns, 1 worker | 51,012 · 19.4 | 49,440 · 19.5 | 48,190 · 19.9 |
| 512 B, 64 conns, 2 workers (cpus 0-1) | 178,135 · 10.4 | 187,249 · 9.5 | 188,386 · 10.0 |
| 16 KiB, 64 conns, 2 workers | 128,334 · 14.3 | 140,606 · 12.2 | 145,073 · 12.7 |

With one worker the server is the bottleneck (0.96-0.99 of a CPU), the three are within 6% of each
other on CPU per request, and compio-pool is 4-6% behind the better tokio server on throughput (8%
in a five-run repeat of the 256-connection case). With two, compio-pool is level with per-core
tokio (+1% at 512 B, +3% at 16 KiB) and 6-13% ahead of tokio default. **The 16 KiB deficit is
essentially absent**, from 16 to 256 connections, and compio-pool's p99 and p99.9 are in line with
the tokio servers across those five repeats.

Two limits. The architecture effect is much smaller here (default to per-core is 5-10% at two
workers, against 1.2-3.0× above): part of what the shared-core table credits to thread-per-core
is that pinned workers cope better with an oversubscribed box. And two workers cannot show the
scaling difference that published thread-per-core measurements find on 16-24 cores. This machine
has no CPUs left for the clients beyond that.

### What happens at 16 KiB when the clients share the core

On the shared-core 16 KiB row the box is 98-99% busy, so throughput is simply
`cpus / (server µs + client µs per request)`, and the accounting says where the difference is
(a separate run of the same configuration, hence slightly different req/s):

| 16 KiB, 64 conns, 4 workers | req/s | server µs/req (user + sys) | client µs/req | server preemptions/req |
|---|---|---|---|---|
| tokio default | 282,636 | 0.74 + 5.61 | 7.3 | 0.83 |
| tokio per-core | 270,076 | 0.67 + 5.84 | 8.1 | 0.75 |
| compio-pool | 161,865 | 1.05 + 9.72 | 13.5 | 0.13 |

compio-pool spends 1.7× the CPU per request, almost all of it kernel time, **on the client as
well as the server**, although the client code is identical. User-space time is 0.7-1.1 µs in all
three, so it is not compio's runtime. It is not syscalls either: compio-pool made 0.13
`io_uring_enter` calls per request against about 2 syscalls for per-core tokio (`strace -c`, one
worker; indicative only, since tracing changes the batching), and is still not cheaper at 16 KiB.
The tokio servers' 8 µs p50 fits the same picture: a client and the server alternating on one CPU
without waiting in a run queue.

Forcing everything onto one CPU (one worker, clients pinned to the same core) isolates it. Cells
are server µs per request, tokio per-core / compio-pool:

| connections, 16 KiB | 2 | 4 | 8 | 16 | 32 |
|---|---|---|---|---|---|
| server µs/req | 3.86 / 4.42 | 3.78 / 4.10 | 3.67 / 3.84 | 3.93 / 4.51 | 3.91 / **7.00** |

| payload, 16 conns | 512 B | 2 KiB | 4 KiB | 8 KiB | 16 KiB |
|---|---|---|---|---|---|
| server µs/req | 2.90 / 2.79 | 2.95 / 2.72 | 3.23 / 3.06 | 3.34 / 3.37 | 3.8-4.1 / 4.5-5.2 |

Tokio's cost stays flat. At 16 connections compio-pool is a little cheaper than epoll up to
4 KiB (4-8%), level at 8 KiB, and dearer at 16 KiB, where it climbs steeply as connections are
added, to 0.59× the throughput at 32. The scheduling differs as well: the epoll servers are
preempted 0.5-0.8 times per request (each woken client takes the CPU straight back), compio-pool
0.03-0.13 times from four connections up, because it works through every completion that is ready
before it yields.

**What is established:** the deficit appears when the clients share the server's CPU (kept apart,
a few percent remains at most); it grows with connections at 16 KiB; and it is not compio's
user-space code and not syscall count. **What is only consistent with the data:** the epoll
servers settle into a one-request-at-a-time ping-pong that keeps each request's buffers in L1/L2,
while compio-pool works through every connection's data in waves, which at 16 KiB with many
connections touches more memory between reuses than the 2 MiB L2 holds. That predicts the climb
with connections. It does not explain why 8 KiB × 64 and 4 KiB × 128, which have the same
connections × payload (512 KiB) as 16 KiB × 32, reach 0.90× and 0.95× of per-core tokio's
throughput instead of 0.59×, so payload size matters on its own and the threshold is open.
Hardware counters would be the way to settle it, and `perf` is not available in this container.
The io_uring setup flags are not the lever, going by a one-off run (16 KiB, 16 connections, one
CPU) made before the shipped `echo` had flags for them: `defer_taskrun` recovered under half of
the gap to per-core tokio, and turning `coop_taskrun` or `single_issuer` off made no measurable
difference. `echo --defer-taskrun` and `--no-coop-taskrun` now reproduce the first two
(`single_issuer` has no flag), and `bench.sh --suites onecpu --servers
tokio-per-core,compio-pool,compio-defer,compio-nocoop` runs them in that regime.

For a server with remote clients the relevant regime is the previous subsection. The thing to
avoid is reading the shared-core 16 KiB row, or any benchmark with a co-resident load generator,
large payloads and many connections, as a property of the server.

### Connection churn

With `--reconnect 1` (a fresh connection per request, 64 conns, 512 B, 4 workers) compio-pool and
tokio default are both bounded by connection setup on loopback at about 15k conn/s: 15,019
(compio-pool, capacity 1024) and 15,089 (tokio default). compio-pool's fd handoff path costs
nothing measurable on top: capacity 2 pushed about 65k of 74k connections through it and still
did 14,875. The per-core tokio control was not run here.

### One caveat on connection counts

`SO_REUSEPORT` hashes connections to listeners, so with only a handful of connections the kernel
may stack several onto one listener. compio-pool with 4 connections on 4 workers swung from 63k to
496k req/s across three repeats; 16 and 64 connections are steady. Any `SO_REUSEPORT` design has
this property; tokio default, with its one shared listener, does not (66k-73k over the same
three repeats).

## What is not measured

* Real NIC steering (steps 1–4). The VM has one queue. The recipe's cache-locality gain —
  interrupt, accept and I/O on one core — is unmeasured here and is the point of running the
  script on real hardware.
* More than two pinned workers with the clients on other CPUs. The container has four vCPUs, so
  the multi-core scaling difference between thread-per-core and work stealing that published
  measurements find on 16–24 cores is not reproduced here.
* Hardware counters. The cache explanation for the 16 KiB shared-core result is a hypothesis
  because `perf` is not available where this was measured.
* `sqpoll`, and the `defer_taskrun` and `coop_taskrun` flags beyond the one-off run in the
  baseline section. The tables use defaults.
* Anything but echo. The handler is the cheapest possible one so that the server's own cost
  shows.
