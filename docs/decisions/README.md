# Design decisions

One record per load-bearing choice. Each states the constraint that forced it, what was decided,
what was rejected, and what the decision costs — every one of these has a cost, and the records
exist so the next person does not pay it twice by accident.

These records describe the current design: a bb8-style pool you clone onto your own thread-per-core
loop. The accept-side *server framework* that preceded it — a `Server` that owned the loop, a
`Resource` trait, and a cross-thread fd-handoff channel — was retired when the pool became a
component rather than a framework. Records that only described that framework went with it: the
fd-handoff record is gone, and 0002–0004 and 0006 were rewritten for the pool. An even earlier
`Manage`-trait pool with a cross-thread `Reservoir` was retired before that. Nothing below inherits
from either.

| # | decision | costs you |
|---|---|---|
| [0001](0001-listener-per-worker.md) | One `SO_REUSEPORT` listener per pinned worker; the kernel distributes connections by flow hash | No admission control before accept; distribution is only as even as the hash |
| [0002](0002-blueprint-and-local-pool.md) | A `Send` blueprint (`Pool`) cloned onto each thread stamps out a `!Send` per-thread `LocalPool` | Every size is per-thread; no work-stealing or connection sharing between threads |
| [0003](0003-reserve-before-create.md) | The slot is reserved synchronously (an RAII `Held`) before `connect` is awaited | The slot count includes reservations in flight, not just live connections |
| [0004](0004-claim-without-holding-a-slot.md) | `get` waits for room without holding a slot, then wakes all and re-checks | A benign retry when another task wins the slot first |
| [0005](0005-incoming-cpu-opt-in.md) | `SO_INCOMING_CPU` is off until the NIC is tuned | You flip one switch (`bind_reuseport`'s `incoming_cpu`) after running the script |
| [0006](0006-shutdown-and-the-unwinding-thread.md) | Shutdown is a shared flag, not a cross-thread wake | `close` is not instantaneous; a parked `get` waits for a local wake or its timeout |
| [0007](0007-linux-only.md) | Linux is the only supported target | No macOS or Windows runs; CI is Ubuntu only |
| [0009](0009-core-affinity-harness.md) | Core affinity is an opt-in, feature-gated harness (`cpu::CoreSet` + `workers::PerCore`) | A sysfs topology reader to maintain; graceful shutdown stays yours; `0008` is reserved for the reuseport plan |
| [0010](0010-metrics-zero-cost-observer.md) | Metrics is an off-by-default, zero-cost observer feature | Counters read zero without the feature; a registry lock on the cold scrape path |

Related reading: [../architecture.md](../architecture.md) for what the code does, and
[../operations.md](../operations.md) for running it.
