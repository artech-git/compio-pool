# compio-pool documentation

The [README](../README.md) is the front door: what the crate is, why `bb8` and `deadpool`
cannot do this job, and how to get a connection out of it. These pages are the rest — how it
is built, why it is built that way, and what that costs you in production.

## The pages

| page | what it answers |
|---|---|
| [architecture.md](architecture.md) | What the types are, where state lives, and what happens on every step of `acquire` and release |
| [decisions/](decisions/) | Why each load-bearing choice was made, and what it cost |
| [operations.md](operations.md) | Sizing, tuning, reading the metrics, and the failure modes to watch for |
| [performance.md](performance.md) | What is measured, how, and the numbers — including the one that does not favour the current design |
| [testing.md](testing.md) | The invariant oracle, the seeded suites, the libFuzzer targets, and CI |

## Reading order

**Adopting the crate.** README → [operations.md](operations.md) → the two decisions that will
bite you: [per-shard sizing](decisions/0002-per-shard-sizing.md) and
[cancellation](decisions/0004-cancellation-destroys-the-connection.md).

**Changing the crate.** [architecture.md](architecture.md) → [decisions/](decisions/) in order →
[testing.md](testing.md). The conservation invariant in
[architecture.md](architecture.md#the-invariants) is the thing every change has to keep true, and
[testing.md](testing.md) is how you find out whether you did.

**Deciding whether the design is right.** [decisions/](decisions/) is written for this: each
record states the alternative that was rejected and what was given up by rejecting it.
[performance.md](performance.md) records a measurement that argues *against* the current
exchange implementation on mean throughput, kept deliberately.
