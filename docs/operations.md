# Operating guide

Deploying, sizing, tuning and watching a `compio-pool` server. For how the pieces fit, see
[architecture.md](architecture.md); for why, [decisions/](decisions/).

## Deploying

The order matters, because [`incoming_cpu`](#so_incoming_cpu) must not be on before the NIC is
steered.

1. **Decide the worker cores.** `Workers::AllCores` uses the process's affinity mask, so
   `taskset -c 2-11 ./server` keeps cores 0–1 for the kernel and the NIC. `Workers::Cores(vec)`
   is explicit. The reference server prints the final list at startup:

   ```text
   echo on 0.0.0.0:7000 — 12 workers, capacity 1024 each
     worker  0 -> cpu   0  smp_affinity 00000001
     worker  1 -> cpu   1  smp_affinity 00000002
     ...
   tune the NIC for this layout with:
     sudo scripts/tune-nic.sh --cpus 0,1,2,3,4,5,6,7,8,9,10,11
   ```

2. **Check the NIC.** `scripts/tune-nic.sh --check --cpus <list>` is read-only. It prints the
   queue maximums, whether receive hashing and ntuple filters are available, the RX IRQs it
   found and their current masks, the irqbalance state and the RFS tables, then exits 0 if the
   full recipe can be applied and 2 if not, saying which step cannot.

3. **Tune.** `sudo scripts/tune-nic.sh --cpus <list>` applies recipe steps 1–4:

   | step | command | effect |
   |---|---|---|
   | 1 | `ethtool -L IFACE combined N` | one RX/TX queue pair per worker |
   | 2 | `systemctl stop/disable/mask irqbalance` | the pins below stay put |
   | 3 | `echo MASK > /proc/irq/IRQ/smp_affinity` per RX queue | queue *i*'s interrupts land on worker *i*'s core |
   | 4 | `ethtool -K IFACE ntuple on`; `net.core.rps_sock_flow_entries=32768`; `rps_flow_cnt` per queue | accelerated RFS: the NIC steers a flow to the queue of the core that last processed it |

   `--dry-run` prints the commands instead. `--software-rps` additionally spreads each RX queue
   over the worker cores with `rps_cpus` when there are fewer queues than workers. Masks are
   written in `smp_affinity`'s format (comma-separated 32-bit words), the same format
   `compio_pool::cpu::affinity_mask` produces.

4. **Turn on `incoming_cpu`** in the server's configuration and restart it.

5. **Verify.** `ss -lnt | grep :PORT` shows one listener per worker. `watch -n1 'grep IFACE
   /proc/interrupts'` shows each RX queue's counter climbing on exactly one CPU column.
   `Server::stats()` shows `accepted` spread across workers.

### What the script does on hardware it cannot tune

It never fails a step it can skip. On a single-queue virtual NIC (lima/vz virtio, many cloud
instances) it leaves the queue count alone, pins nothing, enables software RFS only, and says
so. The server runs fine there; it just does not get the per-core packet steering the hardware
steps exist for. The test VM is such a machine — see [performance.md](performance.md#the-machine).

## Sizing

**`capacity` is per worker.** It is the number of connections one core serves concurrently.
Process-wide capacity is `capacity × workers`, plus `handoff_capacity` waiting, plus whatever
`OverflowPolicy::ServeLocally` admits. Choose it from what one core can hold in cache and keep
responsive, not from a global target.

**`handoff_capacity`** is how deep the overflow queue can get. A connection in it is accepted,
owned by nobody, and waiting for any core to free a slot. Size it to the burst you want to absorb
without rejecting; watch `Stats::queued` and `handoff_full`.

**`prewarm`** creates resources before the first connection. Set it to roughly the steady-state
concurrency per worker if `Resource::create` is slow (a backend handshake); leave it at 0 for
buffers.

**`backlog`** per listener. With N listeners the kernel holds up to N × backlog pending
connections; the default 4096 is high on purpose because a burst at accept time is the normal
case for this design.

## `SO_INCOMING_CPU`

Off by default. The kernel picks the listener whose `SO_INCOMING_CPU` matches the CPU the SYN
arrived on and only falls back to the flow hash when no listener matches. With steering in
place that is exactly right: the core that took the interrupt accepts the connection and does
its I/O. Without steering every SYN arrives on one CPU and that CPU's worker gets everything.
Turn it on *after* step 4 and only then. [Decision 0005](decisions/0005-incoming-cpu-opt-in.md)
has the measurement.

## `io_uring` flags

`UringConfig` maps onto `io_uring_setup(2)`:

| field | default | flag | kernel | note |
|---|---|---|---|---|
| `entries` | compio's default | SQ size | — | raise for very high per-core concurrency |
| `coop_taskrun` | on | `COOP_TASKRUN` + `TASKRUN_FLAG` | 5.19 | completions are processed when the worker enters the kernel, not by interrupting it |
| `single_issuer` | on | `SINGLE_ISSUER` | 6.0 | true by construction: one pinned thread per ring |
| `defer_taskrun` | off | `DEFER_TASKRUN` | 6.1 | needs `single_issuer`; defers task work to the wait call. Worth measuring on your kernel |
| `sqpoll_idle` | off | `SQPOLL` | — | a kernel polling thread per ring. Disables `coop_taskrun`. Not thread-per-core's trade-off |

A flag the kernel does not support fails `io_uring_setup` with `EINVAL`, which surfaces as a
`Server::start` error naming the worker. Clear the flag.

## Reading the counters

`Server::stats()` returns every worker's counters, their sum, and the channel depth. Per
worker, `served_local + handed_off == accepted` always holds. Over a quiet server,
`claimed + oversubscribed + rejected == handed_off`.

| counter | normal | worth a look |
|---|---|---|
| `accepted` | spread across workers | one worker far ahead: `incoming_cpu` on an untuned host, or a client that reuses one source port |
| `handed_off` | some, under bursts | climbing steadily: `capacity` is too small for the steady state |
| `queued` | near 0 | sustained > 0: every core is full; parked connections wait |
| `handoff_full` / `rejected` / `oversubscribed` | 0 | the process is saturated and the queue is full; grow `capacity`, `handoff_capacity`, or the machine |
| `bounced` | rare | rising: listeners and claim loops are fighting over the last slot — a core-at-capacity signal |
| `accept_errors` | 0 | `EMFILE`/`ENFILE`: raise `ulimit -n`; the loop backs off 10 ms per hit |
| `resource_errors` | 0 | `Resource::create` failing; the connection is closed |
| `detach_failed` / `attach_failed` | 0 | should not happen; a stream had an operation in flight at handoff, or `from_std` failed |
| `active` | ≤ `capacity × workers` | above it: oversubscription is admitting connections |

## Shutdown

`Server::shutdown()` stops accepting and claiming on every worker and gives in-flight
connections `drain_timeout` to finish; `join()` waits for the threads and returns the first
worker panic. Dropping the `Server` does both. Listeners close when the runtimes drop, so the
port is free again as soon as `join` returns.

## Failure modes

* **A worker cannot bind** (`EADDRINUSE` from something that is not in the reuseport group,
  or no permission): `start` fails with that error after stopping the workers it did start.
* **A worker panics** (a bug in a handler that escapes `catch_unwind`, say): its thread exits,
  its listener closes, and the kernel stops hashing to it; the rest keep running. `join`
  reports the panic.
* **Out of file descriptors:** `accept_errors` climbs, the loop backs off, nothing is lost that
  was already accepted.
* **The channel is full and so is every core:** by default connections are served over
  capacity on the core that accepted them (`oversubscribed`); with `OverflowPolicy::Reject`
  they are closed (`rejected`). Either way the condition is visible in the counters before it is
  visible to users.
