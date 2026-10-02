# Performance

What was measured, how, and what the numbers say. Everything here comes from the two shipped
examples: [`echo`](../examples/echo.rs) is the server, [`load`](../examples/load.rs) the
client. Both are release builds, and the commands next to each table reproduce it; nothing
else is needed.

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

## What is not measured

* Real NIC steering (steps 1–4). The VM has one queue. The recipe's cache-locality gain —
  interrupt, accept and I/O on one core — is unmeasured here and is the point of running the
  script on real hardware.
* `defer_taskrun` and `sqpoll`. Defaults only.
* Anything but echo. The handler is the cheapest possible one so that the server's own cost
  shows.
