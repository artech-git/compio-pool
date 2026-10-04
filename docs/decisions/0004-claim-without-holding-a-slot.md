# 0004 — The claim loop waits for room, then takes; losers bounce

## Constraint

Recipe step 16 asks every worker to poll the channel continuously. Taken literally, a worker
with no free slot would pull a connection it cannot serve and have to push it straight back —
and a saturated process would juggle fds between cores forever.

## Decision

The claim loop waits on `pool.wait_available()` *before* `rx.recv_async()`, and the wait does
not reserve the slot. After a successful receive it calls `try_reserve`; if the slot is gone
(the worker's own listener used it between the wait and the receive), the fd goes back onto the
channel with `hops + 1`, up to `max_hops`, after which it is treated as if the channel were full
(served over capacity by default, or rejected).

## Rejected

* **Reserve a slot, then wait on the channel.** Simple and race-free, but the reservation sits
  idle while the worker waits for something that may never come. With `capacity = 1` the
  listener could never serve anything locally. The first draft did this, and the handoff tests
  caught it immediately.
* **Receive first, then check.** The literal reading. Ping-pongs under saturation.
* **A single task that selects over accept and receive.** Would avoid the race by construction
  but needs a `select!` and complicates both loops; the race is cheap and bounded instead.

## Costs

* `bounced` exists. A rising rate means a worker's listener and claim loop are competing for
  the same last slot, which means the worker is at capacity — a sizing signal, not a bug.
* A connection can be pushed back `max_hops` times before it is finally placed. Each hop is a
  `try_send` of an `OwnedFd`, no syscall.
* Under the 64-client reconnect load against 12 workers of capacity 1, 73,241 handoffs produced
  4 bounces.
