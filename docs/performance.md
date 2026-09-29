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

## The exchange: queue vs. mutex

`cargo bench --bench exchange`

Neither pre-existing benchmark could see this change at all: `benches/acquire` and
`examples/ncat_bench` both drive the pool with `NoExchange`, whose park path is a plain move. So
`benches/exchange.rs` compiles **both** designs — the `ArrayQueue` reservoir and the `Mutex<Vec>`
it replaced — into one binary and alternates them, so the comparison does not ride on run-to-run
scheduling variance.

The result is not the clean win the phrase "lock-free" suggests.

```text
park + unpark, mean ns/op (worst thread)
  threads    ArrayQueue    Mutex<Vec>
        1          28.7          30.1
        2         183.1         157.1
        4         556.1         403.3
        8        1821.8         857.4
```

Hammered with nothing between operations, the mutex is up to 2.1× faster on the mean. The latency
distribution says why:

```text
8 threads          p50     p99    p99.9       max
  ArrayQueue      1500    5875    10834    331209
  Mutex<Vec>        42   29291    62958    477917
```

A p50 of 42 ns against a mean of 857 ns is a bimodal, unfair distribution: a thread that already
holds the lock reacquires it while others queue behind. (It is the same barging pattern the
acquire waiter queue shows — see [decision 0007](decisions/0007-thread-local-waiters.md).) The
queue trades median for fairness: **5.0× better p99, 5.8× better p99.9.**

Once there is real work between exchange operations, which is the actual workload, the queue also
wins outright:

```text
full acquire path, min_idle=0
  threads    ArrayQueue    Mutex<Vec>
        1          73.8          77.5
        2         386.8         389.5
        4         543.5         782.5
        8        1907.8        2695.3
```

The justification for the lock-free design is tail latency and behaviour as threads are added, not
raw throughput. Recorded here, and in
[decision 0006](decisions/0006-lock-free-reservoir.md), so the next person to look at the mean
does not "optimise" it back to a mutex.

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
