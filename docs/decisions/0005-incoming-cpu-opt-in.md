# 0005 — `SO_INCOMING_CPU` is off until the NIC is tuned

## Constraint

Recipe step 4 aligns the kernel's receive hashing with the socket polling threads. The socket
half of that is `SO_INCOMING_CPU`: tell the kernel which CPU a listener's worker runs on, so
that when a flow's packets arrive on that CPU the kernel picks that listener. With queues pinned
one per core (steps 1–3), accept, I/O and the interrupt then share a cache.

## What the kernel actually does

`reuseport_select_sock_by_hash` first picks by hash. If *any* socket in the group has
`sk_incoming_cpu` set, it then walks the group looking for one whose `incoming_cpu` equals the
CPU the SYN is being processed on, and returns it if found; only when none matches does the hash
pick stand. So the option is not a tie-breaker, it is an override.

Without steering, every SYN is processed on one CPU — whichever core the NIC's single interrupt
lands on, or, on loopback, the sender's core. Whichever worker's core that is gets *every*
connection.

## Measured

A throwaway probe in the test VM (not in the repository): 4 workers on CPUs 0–3, 64 sequential
loopback connections from one client thread, 20 trials per row. On loopback the SYN is
processed on the client thread's CPU, so where that thread runs plays the part of "which queue
the NIC delivered the packet to".

| workers | `incoming_cpu` | client thread | one worker took all 64 | largest share |
|---|---|---|---|---|
| pinned | on | pinned to CPU 1 | 20 / 20 trials — always worker 1 | 64 / 64 |
| pinned | off | pinned to CPU 1 | 0 / 20 | 25 / 64 |
| pinned | on | pinned to CPU 7 (no worker there) | 0 / 20 | 22 / 64 |
| unpinned | on | unpinned | 6 / 20 | 64 / 64 |
| unpinned | off | unpinned | 0 / 20 | 24 / 64 |

The first row is the mechanism, and it is exactly what the recipe wants once the NIC is tuned:
a flow processed on CPU 1 goes to the worker on CPU 1. The third row is the fallback: no
listener matches CPU 7, so the hash decides. The fourth row is the hazard: an untuned host
where the softirq CPU happens to coincide with a worker's core sends that worker everything,
intermittently. The integration test that asserts connections spread over several listeners
failed in roughly that proportion while this option still defaulted to on.

## Decision

`Config::incoming_cpu` defaults to `false`. The listener code sets it when asked; the
documentation and the reference server tell you to turn it on after `tune-nic.sh` has run, and
the tests leave it off with a comment saying why.

## Rejected

* **On by default, as the recipe implies.** Correct on a tuned host and silently catastrophic
  on an untuned one, including every developer laptop and CI runner.
* **Auto-detect.** Whether the NIC is tuned is not reliably knowable from inside the process.

## Costs

One switch to remember after running the script. `Server::stats()` shows the collapse
immediately if you forget: one worker's `accepted` climbs and the rest sit at zero.
