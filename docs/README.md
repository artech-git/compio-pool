# compio-pool documentation

The [README](../README.md) is the front door: what the crate is, the recipe it implements, and
how to put a handler behind it. These pages are the rest — how it is built, why it is built
that way, how to run it, and what it measures.

| page | what it answers |
|---|---|
| [architecture.md](architecture.md) | What each worker owns, the three loops, where every piece of state lives, and the accounting invariants |
| [decisions/](decisions/) | Why each load-bearing choice was made, what was rejected, and what it costs |
| [operations.md](operations.md) | Host tuning, sizing, the `io_uring` flags, reading the counters, failure modes |
| [performance.md](performance.md) | What was measured, how, and the numbers |
| [testing.md](testing.md) | What the tests prove, how to run them on the Linux VM, and CI |

## Reading order

**Deploying it.** README → [operations.md](operations.md) → [decision 0005](decisions/0005-incoming-cpu-opt-in.md)
before you touch `incoming_cpu`.

**Changing it.** [architecture.md](architecture.md) → [decisions/](decisions/) in order →
[testing.md](testing.md). The invariants in [architecture.md](architecture.md#invariants) are
what every change has to keep true.
