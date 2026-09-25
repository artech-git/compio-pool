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

**`ncat_bench`** runs 45 connections in flight over 5 compio threads — 5 shards of 9, because
`max_size` is per shard — in three phases: steady state, oversubscribed, and no pool at all. The
third phase is the point: the unpooled path hits `TIME_WAIT` accumulation and ephemeral-port
exhaustion, which is the failure mode a pool exists to prevent and which no microbenchmark will
ever show you.

**`ncat_steal_bench`** runs two arms against one server in one process, exchange on and off, and
reports cold-start acquire latency, connections actually dialled, and what the exchange costs on
the hot path.

## Methodology notes

* Benchmarks warm up before timing, so first-touch shard creation is not counted.
* The microbenchmark manager does nothing — `connect` returns a `u64`, `recycle` returns `Ok`. A
  real manager's `connect` and `recycle` will dominate any of these numbers.
* Multi-threaded figures report the **worst** thread, not the mean across threads. A pool that is
  fast on average and terrible on one thread is a pool with a latency problem.
* Percentiles matter more than means everywhere in this crate. The two places where the design
  takes a deliberate loss — the exchange, and waiter fairness — are both invisible in a mean and
  obvious in a p99.9.
