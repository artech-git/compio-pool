# Design decisions

One record per load-bearing choice. Each states the constraint that forced it, what was decided,
what was rejected, and what the decision costs — because every one of these has a cost, and the
records exist so the next person does not pay it twice by accident.

| # | decision | costs you |
|---|---|---|
| [0001](0001-per-thread-shards.md) | Connections live in per-thread shards, not in the pool handle | A global view of the pool is approximate |
| [0002](0002-per-shard-sizing.md) | `max_size` and `min_idle` are per shard, not per process | You do the `× threads` arithmetic yourself |
| [0003](0003-recycle-on-acquire.md) | `recycle` runs at checkout, not at return | Validation is on the request's latency path |
| [0004](0004-cancellation-destroys-the-connection.md) | A checkout cancelled mid-operation is destroyed | `begin_op`/`complete_op` discipline is on the caller |
| [0005](0005-detach-is-opt-in.md) | Cross-thread migration is opt-in via `Detach` | No connection sharing on Windows/IOCP |
| [0006](0006-lock-free-reservoir.md) | The exchange is a lock-free `ArrayQueue`, not a `Mutex<Vec>` | Worse mean throughput under pure contention |
| [0007](0007-thread-local-waiters.md) | Waiters queue on their own thread only | Waiters are not FIFO-fair; late arrivals can barge |
| [0008](0008-relaxed-counters.md) | Metrics are `Relaxed` atomics, not a consistent snapshot | Gauges can disagree with each other momentarily |
| [0009](0009-per-shard-reaper.md) | One reaper task per shard, spawned lazily | A timer task per thread; none at all outside a runtime |

Related reading: [../architecture.md](../architecture.md) for what the code actually does, and
[../performance.md](../performance.md) for the measurements 0006 rests on.
