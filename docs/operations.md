# Operating the pool

Sizing it, tuning it, reading its metrics, and the failure modes worth knowing about before they
happen. For the reasoning behind any of this, see [decisions/](decisions/).

## Sizing: the arithmetic you have to do

**`max_size` and `min_idle` are per compio thread, not per process.**

```text
worst-case connections to your backend = max_size × compio threads
steady-state idle connections          = min_idle × compio threads
```

Four threads at `max_size = 8` is 32 connections. Sixteen threads at `min_idle = 2` is 32 idle
sockets against a backend that may have been sized for 8. There is no global cap and there is not
going to be one on the fast path — see [decision 0002](decisions/0002-per-shard-sizing.md).

Work it backwards instead:

1. Take the backend's real ceiling (Postgres `max_connections` minus the superuser reserve and
   whatever else connects; an API's documented concurrency limit).
2. Divide by your compio thread count, leaving headroom for a deploy where old and new processes
   overlap.
3. That is `max_size`. If it lands below 1, you have more threads than the backend can serve and
   this crate is not the right shape for the problem.

## Configuration reference

| Setting | Default | Notes |
|---|---|---|
| `max_size` | 16 | **per thread**; asserts non-zero |
| `min_idle` | 0 | idle kept thread-locally; also the local/shared split point. Clamped to `max_size` |
| `acquire_timeout` | 30s | `None` waits forever |
| `max_lifetime` | 30min | hard age cap |
| `idle_timeout` | 10min | reaped while idle |
| `max_uses` | `None` | retire after N checkouts |
| `reap_interval` | 30s | how often each shard's reaper runs; asserts non-zero |

### `min_idle` with an exchange installed

`min_idle` is the split point between keeping a connection local and offering it to other threads.
Leaving it at `0` means *every* return crosses the shared queue — correct, but it puts the exchange
on the hot path for no benefit. Set it to your steady-state per-thread concurrency so the fast path
stays lock-free and only the surplus is shared.

### `acquire_timeout` is not free

The acquire fast path roughly doubles with a timeout armed — ~65 ns/op to ~127 ns/op. The
difference is the timer, not the pool ([performance.md](performance.md)). It is still the right
default: without it, waiter unfairness ([decision 0007](decisions/0007-thread-local-waiters.md))
presents as a hang rather than an error.

### `reap_interval` is your timeout resolution

A connection with `idle_timeout = 10s` and `reap_interval = 30s` may sit idle for up to 40 seconds
before the reaper closes it. It will never be *handed out* stale — expiry is rechecked on the way
out of the free list — so this only affects how long dead sockets linger.

## Lifecycle operations

| call | effect |
|---|---|
| `warm()` | Opens connections on the **current thread** up to `min_idle`. Call once per compio thread at startup so the first real request does not pay a handshake |
| `invalidate()` | Bumps the generation: every connection created before the call is retired. Live checkouts keep working and are closed when returned. Use after a credential rotation or a failover |
| `close()` | `acquire` now fails with `Error::Closed`. This thread's idle connections close immediately; other threads' close when they next touch the pool, on their reaper's next tick, or at thread exit |
| `metrics()` | Process-wide snapshot |
| `local_size()` / `local_idle()` | This thread's shard only |

`invalidate()` is cheap and global: it is one `Relaxed` increment, not a walk of any data
structure, and it takes effect on every thread at once without touching another thread's shard.

## Reading the metrics

| field | kind | what a change means |
|---|---|---|
| `live` | gauge | Connections owned by a shard, idle or checked out |
| `idle` | gauge | Sitting in a shard free list |
| `parked` | gauge | Sitting in the cross-thread reservoir |
| `created` / `closed` | counter | Churn. Rising together at a steady `live` means connections are being retired and redialled |
| `acquires` | counter | Successful checkouts |
| `waits` | counter | Checkouts that had to park because the shard was at `max_size` |
| `timeouts` | counter | Gave up at `acquire_timeout` |
| `poisoned` | counter | Discarded because a checkout was cancelled mid-operation |
| `recycle_failures` | counter | Discarded because `recycle` rejected them |
| `unparked` | counter | Claimed from the reservoir by a thread that did not open them |

Gauges are summed across threads without a lock and can momentarily disagree with each other;
counters are monotonic ([decision 0008](decisions/0008-relaxed-counters.md)). Alert on ratios and
trends, not on instantaneous gauge arithmetic.

Useful ratios:

* `waits / acquires` climbing → `max_size` is too small for this thread's concurrency.
* `timeouts` non-zero → either genuinely oversubscribed, or waiter barging is starving someone
  ([decision 0007](decisions/0007-thread-local-waiters.md)).
* `poisoned / acquires` non-trivial → something is cancelling mid-operation more than you think.
  Each one is a destroyed connection *and* a redial.
* `recycle_failures` rising → the backend is dropping idle connections faster than `idle_timeout`
  reaps them. Lower `idle_timeout` rather than making `recycle` cheaper.
* `created` ≫ `acquires / max_uses` → churn. Check `max_lifetime` and `idle_timeout` against your
  traffic shape.
* `unparked` near zero with an exchange installed → `min_idle` is too high, or load is not skewed
  and you are paying for the exchange without using it.

## Failure modes

**Over-dialling the backend.** The `× threads` arithmetic above. Shows up as connection refusals
from the backend, not as anything the pool reports.

**Protocol corruption on a later request.** A connection returned after a cancelled operation. The
symptom is a response that belongs to someone else's request, arriving on an unrelated call. Fix by
auditing `begin_op`/`complete_op` coverage around every `select!`, timeout and early return; watch
`poisoned` to confirm the guard is actually firing
([decision 0004](decisions/0004-cancellation-destroys-the-connection.md)).

**A latency cliff at checkout.** `recycle` runs on the request's latency path. If it is a round
trip (which over a Unix socket it has to be — `peer_addr()` keeps succeeding long after the peer
process exits), that round trip is in your p99.
`examples/unix_socket.rs` prices this per phase.

**Idle connections on the wrong thread.** Skewed load with no exchange installed: one shard at
`max_size` with waiters while another sits on idle sockets. `waits` rising while `idle` stays high
is the signature. The fix is a `Reservoir`, which needs `Detach`
([decision 0005](decisions/0005-detach-is-opt-in.md)).

**An unbounded tail under sustained oversubscription.** Waiter barging. `acquire_timeout` converts
it from a hang into a timeout, which is a mask rather than a fix.

## Platform support

| driver | `Detach` sound? | notes |
|---|---|---|
| io_uring (Linux) | yes | `attach` is a no-op; the fd table is process-wide |
| poll (Unix fallback) | yes | same |
| IOCP (Windows) | **no** | a handle binds to one completion port for life |

Everything except cross-thread migration works identically on all three. CI covers Linux, macOS
and Windows on stable, plus beta on Linux.

## Requirements

Rust edition 2024, Rust 1.88+, and `compio` 0.18. The library depends only on `compio` (`runtime`,
`time`) and `crossbeam-queue`.
