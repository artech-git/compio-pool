# Operating guide

Deploying, sizing, tuning and watching a thread-per-core server built on `compio-pool`. For how
the pieces fit, see [architecture.md](architecture.md); for why, [decisions/](decisions/).

The crate is the pool plus the accept-side helpers (`cpu`, `listener`, `workers`); the accept loop
and the runtime are yours. The guidance below is written against the reference server,
[`examples/echo.rs`](../examples/echo.rs) — one pinned thread per core, each binding its own
`SO_REUSEPORT` listener and leasing from a per-thread `LocalPool`.

## Deploying

The order matters, because [`incoming_cpu`](#so_incoming_cpu) must not be on before the NIC is
steered.

1. **Decide the worker cores.** Passing `cpu::cores()` to `PerCore::new` runs one worker per
   permitted logical CPU, so `taskset -c 2-11 ./server` keeps cores 0–1 for the kernel and the NIC.
   For a topology-aware subset, pass a [`cpu::CoreSet`](#worker-placement-the-affinity-feature)
   instead (the `affinity` feature). The reference server prints its worker count at startup:

   ```text
   echo on 0.0.0.0:7000: 12 workers, capacity 1024/worker
   ```

   The cores you ran on are the list to hand the NIC script next; `compio_pool::cpu::affinity_mask(cpu)`
   formats one as an `smp_affinity` mask if you pin IRQs by hand.

2. **Check the NIC.** `scripts/tune-nic.sh --check --cpus <list>` is read-only. It prints the queue
   maximums, whether receive hashing and ntuple filters are available, the RX IRQs it found and
   their current masks, the irqbalance state and the RFS tables, then exits 0 if the full recipe can
   be applied and 2 if not, saying which step cannot.

3. **Tune.** `sudo scripts/tune-nic.sh --cpus <list>` applies recipe steps 1–4:

   | step | command | effect |
   |---|---|---|
   | 1 | `ethtool -L IFACE combined N` | one RX/TX queue pair per worker |
   | 2 | `systemctl stop/disable/mask irqbalance` | the pins below stay put |
   | 3 | `echo MASK > /proc/irq/IRQ/smp_affinity` per RX queue | queue *i*'s interrupts land on worker *i*'s core |
   | 4 | `ethtool -K IFACE ntuple on`; `net.core.rps_sock_flow_entries=32768`; `rps_flow_cnt` per queue | accelerated RFS: the NIC steers a flow to the queue of the core that last processed it |

   `--dry-run` prints the commands instead. `--software-rps` additionally spreads each RX queue over
   the worker cores with `rps_cpus` when there are fewer queues than workers. Masks are written in
   `smp_affinity`'s format (comma-separated 32-bit words), the same format
   `compio_pool::cpu::affinity_mask` produces.

4. **Turn on `incoming_cpu`.** Pass `Some(core)` as `bind_reuseport`'s third argument and restart.
   In the reference server that value is `WorkerCtx::core`; it binds `None` until the NIC is tuned,
   per [Decision 0005](decisions/0005-incoming-cpu-opt-in.md).

5. **Verify.** `ss -lnt | grep :PORT` shows one listener per worker. `watch -n1 'grep IFACE
   /proc/interrupts'` shows each RX queue's counter climbing on exactly one CPU column. With the
   `metrics` feature on, `Pool::state` shows `connections` spread evenly across the process.

### What the script does on hardware it cannot tune

It never fails a step it can skip. On a single-queue virtual NIC (virtio-net under virtualisation,
many cloud instances) it leaves the queue count alone, pins nothing, enables software RFS only, and
says so. The server runs fine there; it just does not get the per-core packet steering the hardware
steps exist for. The test VM is such a machine — see [performance.md](performance.md#the-machine).

## Sizing

Every size is **per worker thread**, because each thread has its own `LocalPool`.

**`max_size`** caps the connections one thread opens (idle + checked out). The process-wide ceiling
is `max_size × workers`. Choose it from what one core can hold and keep responsive, not a global
target; a `get` beyond it waits up to `connection_timeout`. In the reference server the CLI
`capacity` argument is exactly this.

**`min_idle`** is the floor that [`warm`](#maintenance) / `maintain` top back up to. Set it to
roughly the steady-state concurrency per worker when `connect` is slow (a backend handshake), so a
burst does not pay the handshake inline; leave it 0 for cheap resources like buffers.

**`connection_timeout`** bounds how long `get` parks for a slot before `RunError::TimedOut`
(default 30 s). **`idle_timeout`** / **`max_lifetime`** retire connections by age; both are checked
lazily, so run `maintain` on a timer to retire them promptly. **`test_on_check_out`** (default on)
runs `is_valid` on a reused connection before handing it out.

**`backlog`** is `bind_reuseport`'s second argument, per listener. With N listeners the kernel holds
up to N × backlog pending connections; keep it high (the reference server uses 1024) because a burst
at accept time is the normal case for this design.

## Worker placement (the `affinity` feature)

With the optional `affinity` feature on, `cpu::CoreSet` chooses which cores to run on and
`workers::PerCore` spawns one pinned worker per core. `CoreSet::permitted()` starts from the process
affinity mask — so `taskset -c 2-11 ./server` already keeps cores 0–1 for the kernel and the NIC —
and `.physical_only()`, `.on_numa_node(n)`, `.on_package(n)`, `.spread()` / `.pack()` and `.take(n)`
narrow or order it from the sysfs topology. Each worker is handed its `CoreId` as `WorkerCtx::core`,
which is the value to pass as `SO_INCOMING_CPU` (below) once the NIC is tuned. See
[Decision 0009](decisions/0009-core-affinity-harness.md).

## `SO_INCOMING_CPU`

Off by default. The kernel picks the listener whose `SO_INCOMING_CPU` matches the CPU the SYN
arrived on and only falls back to the flow hash when no listener matches. With steering in place
that is exactly right: the core that took the interrupt accepts the connection and does its I/O.
Without steering every SYN arrives on one CPU and that CPU's worker gets everything. Turn it on
*after* step 4 and only then. [Decision 0005](decisions/0005-incoming-cpu-opt-in.md) has the
measurement.

## `io_uring` flags

You own the runtime, so you own its ring. With `PerCore::block_on_each`, hand each worker's runtime
a `compio` `ProactorBuilder` via [`PerCore::proactor`](../src/workers.rs); with `spawn_each` you
build the runtime yourself. `proactor` is a thin passthrough — `compio` owns the flag semantics:

| flag | default | kernel | note |
|---|---|---|---|
| SQ `entries` | compio's default | — | raise for very high per-core concurrency |
| `coop_taskrun` | on | 5.19 | completions are processed when the worker enters the kernel, not by interrupting it |
| `single_issuer` | on | 6.0 | true by construction: one pinned thread per ring |
| `defer_taskrun` | off | 6.1 | needs `single_issuer`; defers task work to the wait call. Worth measuring on your kernel |
| `sqpoll` | off | — | a kernel polling thread per ring; disables `coop_taskrun`. Not thread-per-core's trade-off |

A flag the kernel does not support fails ring setup, which panics on the worker thread and surfaces
as that worker's [`Workers::join`](../src/workers.rs) `Err`. Clear the flag.

## Reading the counters (the `metrics` feature)

The pool's counters are behind the off-by-default `metrics` feature; without it the calls below
compile and return zeroes, so calling code builds either way. Each `LocalPool` keeps its own
lock-free counters and registers them with the `Pool`. Read one thread's, or sum every thread's:

```rust
let here = local.state();       // this thread, lock-free
let all  = pool.state();        // summed across every thread (takes the registry lock)
let s    = pool.statistics();   // cumulative counters, summed
```

`State` is a live gauge; `Statistics` is cumulative, so two snapshots subtracted give the activity
between them — the shape an exporter wants. Call `Pool::state` / `statistics` off the hot path (a
scrape, a health check) since they lock the registry; the `LocalPool` versions are lock-free.

| counter | what it is | worth a look |
|---|---|---|
| `connections` / `idle_connections` | open now, and how many are idle | `idle` pinned at 0 with `connections` at `max_size`: the thread is saturated and callers are waiting |
| `pending_waiters` | tasks parked in `get` | sustained > 0: `max_size` is too small for this core's concurrency, or `connect` is slow |
| `get_direct` / `get_waited` | checkouts served at once vs after parking | `get_waited` rising as a share of the total: raise `max_size` or `min_idle` |
| `get_timed_out` | `get` calls that hit `connection_timeout` | > 0: the pool cannot keep up, or the backend is down |
| `connections_created` | opened by the manager | climbing without a matching idle population: churn, so `connect`'s cost is paid repeatedly — raise `min_idle` |
| `connections_closed_broken` | dropped by `has_broken` / a failed `is_valid` | a rising rate points at an unhealthy backend |
| `connections_closed_idle_timeout` / `_max_lifetime` | retired by age | expected with `maintain` on a timer — the reaper working |
| `total_wait_time` / `average_wait_time()` | time callers spent parked | a growing average means contention for slots |

## Maintenance

Expiry is lazy, so drive it per worker — thread-per-core has no shared reaper thread:

```rust
compio::runtime::spawn(async move {
    loop {
        compio::time::sleep(Duration::from_secs(30)).await;
        let _ = local.maintain().await; // reap expired idle, then warm back to min_idle
    }
})
.detach();
```

`clear` drops a thread's idle connections unconditionally — useful after a backend failover.

## Shutdown

`Pool::close` makes every thread's `get` fail with `RunError::Closed`, stops recycling (a returned
connection is dropped, not parked), and turns `warm` / `maintain` into no-ops. `is_closed` reports
the flag. The flag is shared, but the wakers that unpark a parked `get` live on each worker's own
thread, so for a prompt drain you stop each worker's accept loop and call `LocalPool::clear` on its
thread. Join the worker threads with [`Workers::join`](../src/workers.rs), which returns each
worker's result and surfaces a panic as an `Err`; the listeners close when the runtimes drop,
freeing the port.

## Failure modes

* **A worker cannot bind** (`EADDRINUSE` from something outside the reuseport group, or no
  permission): `bind_reuseport` returns that error; in the reference server the worker's future
  returns it and `Workers::join` reports it.
* **A worker panics** (a handler bug, say): its thread exits, its listener closes, and the kernel
  stops hashing to it; the rest keep running. `Workers::join` returns that worker's `Err`.
* **Out of file descriptors:** `accept` fails with `EMFILE` / `ENFILE`; handle it in your accept
  loop (the raw-path examples back off 10 ms and retry). Connections already leased are unaffected.
* **The backend is down:** `connect` returns `Err`, so `get` returns `RunError::User(e)` and `warm`
  stops early with it. Nothing leaks — the reserved slot is released.
* **A core saturates:** `get` parks up to `connection_timeout`, then returns `RunError::TimedOut`;
  with `metrics` on, `pending_waiters` and `get_timed_out` show it before users do.
