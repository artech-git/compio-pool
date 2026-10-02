# Design decisions

One record per load-bearing choice. Each states the constraint that forced it, what was
decided, what was rejected, and what the decision costs — every one of these has a cost, and the
records exist so the next person does not pay it twice by accident.

The previous design (a `Manage`-trait connection pool with per-thread shards and a cross-thread
`Reservoir`) and its decision records were retired wholesale when the crate was rebuilt around
the accept-side recipe. Nothing below inherits from them.

| # | decision | costs you |
|---|---|---|
| [0001](0001-listener-per-worker.md) | One `SO_REUSEPORT` listener per pinned worker; the kernel distributes connections | No admission control before accept; distribution is only as even as the hash |
| [0002](0002-reserve-before-create.md) | The pool hands out a slot synchronously and produces the resource later | Two-step checkout; a `Permit` in the API |
| [0003](0003-fd-handoff-over-flume.md) | Overflow travels as a raw fd on one bounded `flume` channel | A detach + attach per overflow connection; the channel is a process-wide bound |
| [0004](0004-claim-without-holding-a-slot.md) | The claim loop waits for room but does not hold a slot while it waits; losers bounce | A race, bounded by `max_hops`; `bounced` is a counter you watch |
| [0005](0005-incoming-cpu-opt-in.md) | `SO_INCOMING_CPU` is off until the NIC is tuned | You flip one switch after running the script |
| [0006](0006-shutdown-and-the-unwinding-thread.md) | Shutdown is a channel disconnect, detoured through a helper thread when the caller is panicking | One short-lived thread on a panic path |
| [0007](0007-linux-only.md) | Linux is the only supported target | No macOS or Windows runs; CI is Ubuntu only |

Related reading: [../architecture.md](../architecture.md) for what the code does, and
[../performance.md](../performance.md) for the measurements 0003 and 0004 rest on.
