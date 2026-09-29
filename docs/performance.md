# Performance

What is measured, how, and what it says. Every figure below is from the author's machine —
reproduce with `cargo bench`. Treat them as shape, not as numbers to quote.

## What exists

| target | question it answers |
|---|---|
| `benches/acquire.rs` | What does the acquire fast path cost, and what is it made of? |
| `benches/exchange.rs` | Lock-free queue or mutex for the exchange? |
| `examples/ncat_bench.rs` | What does pooling buy over dialling per request, against a real server? |
| `examples/ncat_steal_bench.rs` | Given a pool, what does cross-thread migration buy and cost? |
| `crates/deadpool-baseline/examples/ncat_steal_bench.rs` | What does the same workload look like on tokio + deadpool? |
| `crates/fs-bench` | On *files* rather than sockets, what does the thread handoff under `tokio::fs` cost? |

The two `ncat` examples are end-to-end against an external process, not microbenchmarks: `ncat`
forks `/bin/cat` per connection, so every round trip crosses the kernel twice and every dial costs
a real handshake plus a `fork`/`exec`.

## The acquire fast path

`cargo bench --bench acquire`

```text
~65 ns/op    acquire_timeout = None
~127 ns/op   acquire_timeout armed
```

The difference is the timer, not the pool. The benchmark also decomposes the cost — shard lookup,
`Instant::now`, the guard allocation — and reports per-thread scaling at 1/2/4/8 threads.

What is actually happening in those 65 ns: a thread-local map lookup, a `Vec::pop` out of a
`RefCell`, a `recycle` call (a no-op in this benchmark, by design — the manager does as little as
possible so what is measured is the pool), and one `Rc` allocation for the guard. No lock, no
atomic RMW, no cache line shared with another core.

**Read the multi-thread numbers with care.** The 4-thread figure swings between 362 and 837 ns/op
across runs of a *single* binary. It is there to show that per-thread cost does not degrade as
cores are added, not to resolve small differences — which is precisely why it could not be used to
evaluate the exchange change below.

## The exchange: four designs, one binary

`cargo bench --bench exchange`

Neither pre-existing benchmark could see this change at all: `benches/acquire` and
`examples/ncat_bench` both drive the pool with `NoExchange`, whose park path is a plain move. So
`benches/exchange.rs` compiles **every design the exchange has had** into one binary and alternates
them, so the comparison does not ride on run-to-run scheduling variance:

* **Mutex** — what ships today: a FIFO `VecDeque` behind a `parking_lot::Mutex`, which barges.
* **FairMutex** — the same structure behind `parking_lot::FairMutex`, which hands the lock to the
  longest waiter on every unlock.
* **ArrayQueue** — the lock-free bounded ring of
  [decision 0006](decisions/0006-lock-free-reservoir.md), with the atomic admission counter a
  fallible push needs.
* **Mutex<Vec>** — the original: a LIFO stack behind a `std::sync::Mutex`.

Best of two alternating passes each, so these are the floor rather than a distribution.

```text
park + unpark round trip, ns/op (worst thread)
 threads       Mutex   FairMutex  ArrayQueue  Mutex<Vec>
       1        26.8        25.3        22.1        26.8
       2       120.7       145.8       179.6       130.6
       4       361.8     19434.5       422.9       417.7
       8      1116.8     39488.5      1814.5       945.2
```

Two things to read here. The first is that the shipping `Mutex` is the fastest of the sane arms at
eight threads — 1.6× the ring. The second is `FairMutex`, and it is not a typo.

```text
latency distribution at 8 threads, ns
                   p50      p90      p99      p99.9        max
       Mutex       250     5792    37375      68917     164667
   FairMutex     39625    44625    49416      59500     143750
  ArrayQueue      1541     3167     5750       8917     132833
  Mutex<Vec>        42     3000    29417      57541     305667
```

**The mutex trades tail for throughput: 6.5× worse p99 and 7.7× worse p99.9 than the ring.** A p50
of 250 ns against a mean of 1117 ns is a bimodal, unfair distribution — the thread that just
unlocked reacquires while the others queue. (It is the same barging pattern the acquire waiter
queue shows; see [decision 0007](decisions/0007-thread-local-waiters.md).) That trade is deliberate
and it is the subject of [decision 0010](decisions/0010-mutex-reservoir.md).

**`FairMutex` is not the fix, and that is the most useful number on this page.** Its distribution
is *tight* — p50 39.6µs to p99.9 59.5µs — and uniformly catastrophic. Fair handoff wakes the next
waiter through the OS on every unlock, so a ~25 ns critical section pays a ~20µs thread
park/unpark. Fairness is not what costs; waking a thread is. The arm stays compiled in so nobody
reaches for the obvious lock twice.

Once there is real work between exchange operations, which is the actual workload, the ring keeps
its lead at the top end:

```text
full acquire path, min_idle=0, ns/op
 threads       Mutex   FairMutex  ArrayQueue  Mutex<Vec>
       1        80.0        79.5        81.4        82.9
       2       294.4       304.4       304.8       356.5
       4       666.1     18983.3       552.6       841.1
       8      4095.8     40727.4      1784.2      2751.3
```

So the exchange is not lock-free because it is faster — it is not. It takes a lock because one
critical section around the capacity check, the `detach` and the push makes the push infallible,
which deletes the atomic admission counter that a fallible push needs. Recorded here, and in
[decision 0010](decisions/0010-mutex-reservoir.md), so the next person to look at the p99 knows it
was bought on purpose.

Measured on macOS (Darwin 25.3). `FairMutex`'s absolute numbers would be smaller on Linux, where
futex wakeups are cheaper; an OS wakeup per unlock is orders of magnitude above a 25 ns critical
section on any platform, so the shape holds.

## End to end, against a real server

Both need `ncat` (`nmap-ncat`) on `PATH`.

```sh
cargo run --release --example ncat_bench
cargo run --release --example ncat_steal_bench
```

**`ncat_bench`** runs two arms over a `Dispatcher` — one shard per worker thread, because
`max_size` is per shard — pooled against unpooled. The unpooled arm is the point: it dials per
request, so it hits `TIME_WAIT` accumulation and ephemeral-port exhaustion, the failure mode a pool
exists to prevent and the one no microbenchmark will ever show you. It runs far fewer requests for
exactly that reason.

**`ncat_steal_bench`** runs two arms against one server in one process, exchange on and off. Each
arm has two dispatchers: a warm half that populates the shards, then a cold half whose shards are
empty while the warm threads stay alive and idle. It reports cold-start acquire latency,
connections actually dialled in the cold phase, and what the exchange costs the warm phase.

## Against tokio + deadpool

```sh
cargo run --release -p deadpool-baseline --example ncat_steal_bench
```

`crates/deadpool-baseline` is a yardstick, not a product: it ports `ncat_steal_bench` to tokio and
deadpool so the shape above can be checked against the incumbent on the same machine, against the
same server, instead of against numbers quoted from someone else's run.

deadpool has no shards — one `Vec` behind one `Mutex`, and a tokio `TcpStream` is `Send` — so there
is no exchange to toggle. The arms change the topology instead: a pool per half (nothing to share),
one pool across two runtimes (everything shared), and one runtime with the whole thread budget,
which is the shape a tokio service actually has and the honest throughput baseline.

Cross-thread migration is therefore free there, and the cold-start numbers show it. What it is
anchored to is the finding worth recording: a tokio `TcpStream` is `Send`, but its fd stays
registered with the reactor of the runtime that dialled it. A shared pool across runtimes works
only while the dialling runtime is alive and driving; once it shuts down, every socket it registered
fails with `A Tokio 1.x context was found, but it is being shutdown.` — and deadpool keeps handing
them out, because `recycle` sees an open socket with a peer address. The example reproduces that
failure rather than describing it.

Both examples keep their warm half alive through the cold phase for reasons that only look alike. In
this crate it is a choice about where idle sockets should sit, and `Detach::attach` re-registers the
fd with whichever driver claims it. There it is a requirement, and there is no hook to re-register
anything.

## On files, against tokio + deadpool

```sh
cargo build --profile maxopt -p fs-bench --bins
crates/fs-bench/run.sh results.jsonl
crates/fs-bench/summarize.py results.jsonl --json summary.json
```

`crates/fs-bench` asks the socket question again with a file on the other end, where the two
designs differ more sharply than they do over TCP. `tokio::fs` is not asynchronous — every call
is `spawn_blocking` around the blocking syscall — while `compio::fs` submits one SQE on the
calling thread, so the comparison is mostly a measurement of the handoff.

Three arms, because two would be a strawman: plain `tokio::fs` (what the documented API gives
you, two dispatches for a random read), tokio + deadpool (pooled descriptor, one `pread`, one
dispatch), and compio + compio-pool (no dispatch). Seven cases from `stat` to
`write_4k_fsync`, swept over threads and over queue depth, on ext4 and on tmpfs.

`fs_floor` measures the two constants the rest divides by — the bare syscall, and an empty
`spawn_blocking` round trip. On the author's machine those were **~500 ns** and **~30.8 µs**,
and that ratio predicts nearly every other number in the suite.

What the run found, on one VM and one kernel:

* The tokio arms' throughput does not respond to worker threads at all. Both sit near 66–79k
  ops/s from one thread to twelve on every read and metadata case; compio goes from 1.3M to
  9.4M on the same read. The blocking pool, not the worker count, is the ceiling.
* Voluntary context switches per operation are 0.00, 3.00 and 5.96 — the tokio figure is
  exactly twice deadpool's, because its random read is a seek and then a read. That counter is
  the explanation of the latency gap, in the units of the thing causing it.
* On `write_4k_fsync` over ext4 the three converge and the order inverts (14k / 15k / 19k at
  12 threads): once every operation waits for the journal, 30 µs of handoff is noise. The
  tmpfs control is what proves the convergence is the device — with it removed, the same case
  separates by 69×.
* compio loses two cases. `write_1m_buffered` below 8 threads, because a pinned ring does its
  own copying on one core while tokio's blocking pool spreads over all twelve; and
  `open_close`, which is the one read-side case that gets *worse* as threads are added, as
  twelve rings contend on one directory's dentry locks.

The caveats are recorded with the figures rather than under them: reads are page-cache warm on
purpose, the handoff cost is inflated by nested virtualisation, and `threads` does not mean the
same thing to a pinned ring as it does to a runtime with a 4096-thread blocking pool. Only the
12-thread row gives all three arms the same hardware.

## Methodology notes

* Benchmarks warm up before timing, so first-touch shard creation is not counted.
* The microbenchmark manager does nothing — `connect` returns a `u64`, `recycle` returns `Ok`. A
  real manager's `connect` and `recycle` will dominate any of these numbers.
* Multi-threaded figures report the **worst** thread, not the mean across threads. A pool that is
  fast on average and terrible on one thread is a pool with a latency problem.
* Percentiles matter more than means everywhere in this crate. The two places where the design
  takes a deliberate loss — the exchange, and waiter fairness — are both invisible in a mean and
  obvious in a p99.9.
