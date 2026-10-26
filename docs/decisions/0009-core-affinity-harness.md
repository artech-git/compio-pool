# 0009 — Core affinity is an opt-in, feature-gated harness

## Constraint

Every thread-per-core server in `examples/` repeats the same loop by hand: enumerate `cpu::cores()`,
spawn one named thread per core, `cpu::pin_current` it, build that thread's `compio` runtime, bind a
`SO_REUSEPORT` listener, run an accept loop, join. Only the manager, the startup line and the
per-connection handler differ. The pivot to the bb8-style pool (`4ae6cf3`) deliberately deleted the
old `Server` / `Workers::AllCores` abstraction that used to own this loop, on the principle that the
pool is a component you pull into your own loop, not a framework that runs it for you. But the
boilerplate stayed — copied into every example — and the `cpu` module stops at `cores()` /
`pin_current()`, with no way to say "physical cores only" or "spread across sockets".

## Decision

Add an **optional** `affinity` Cargo feature, off by default, in two layers:

* **Selection.** `cpu::CoreSet` reads the sysfs topology (`/sys/devices/system/cpu`,
  `/sys/devices/system/node`), intersected with the affinity mask from `cores()`, and offers
  `physical_only`, `on_numa_node`, `on_package`, `filter`, `spread` / `pack` and `take`. Pure
  `std::fs`, no new dependency, no `unsafe`.
* **Harness.** `workers::PerCore` owns exactly the repeated skeleton — the empty-cores guard, thread
  naming, `pin_current` *before* the ring, the runtime build, handle collection and join — and
  nothing else. `spawn_each` pins and runs your closure; `block_on_each` also builds the `compio`
  runtime and drives your future. Both return `Workers`, whose `JoinHandle`s you own.

The harness **does not** bind the listener, coordinate the bind, or hand off fds. Each worker binds
its own `SO_REUSEPORT` listener in the body and runs its own accept loop, so [0001](0001-listener-per-worker.md)'s
uncoordinated per-worker model is preserved; the harness only removes the thread-spawn boilerplate
around it. The raw `cpu::cores()` / `pin_current()` / `affinity_mask()` path is unchanged and ungated.

Off by default keeps the zero-cost ethos of the `metrics` feature: the default build pulls in no new
crate, and `#![forbid(unsafe_code)]` stays intact — the pointed contrast with the planned
`cpu-steering` feature, whose cBPF `setsockopt` is `unsafe` and must relax that guarantee.

## Rejected

* **A mandatory runtime that owns the accept loop** — the thing the pivot removed. It would put the
  bind, an overflow policy and shutdown back inside the crate and take the loop away from the caller.
  The harness stops at spawn + pin + runtime and hands the loop back.
* **Folding the `SO_REUSEPORT` bind into the harness.** That would make the harness know the listener
  kind (TCP vs UDP vs a pooled backend) and nudge toward coordinated binding. Left to the body
  (0001).
* **Pinning through `compio`.** `compio` 0.19 *does* ship `RuntimeBuilder::thread_affinity`, but using
  it *and* `cpu::pin_current` would pin each worker twice. The harness pins once, via `core_affinity`,
  to stay consistent with the `cpu` module and to pin before the ring is created.
* **An `hwloc` dependency** for topology — a heavy C-library build dependency that pulls in FFI and
  would force relaxing `#![forbid(unsafe_code)]`. Pure sysfs gives physical / SMT / NUMA without
  either.

## Costs

One more feature flag, and a sysfs reader to maintain — the riskiest part, since sysfs topology is
host-dependent: offline CPUs drop their `topology/` directory, containers show host-wide topology
behind a restricted affinity mask, and VMs flatten it. `CoreSet` mitigates this by intersecting with
the affinity mask, skipping offline CPUs, grouping physical cores by `(package, core)` rather than
`core_id` alone, preferring `core_cpus_list`, and degrading to a single-node / no-SMT view on any
missing file; the `spread` / `pack` heuristics still have no view of L3 / CCX islands. `Workers`
holds only `JoinHandle`s with no shutdown channel, so graceful shutdown is the caller's job — and a
shutdown signal that wakes a `compio` task must follow [0006](0006-shutdown-and-the-unwinding-thread.md)'s
panicking-thread detour.

This record takes number **0009**, not the next-free `0008`: the
[reuseport feature plan](../reuseport-feature-plan.md) reserves `0008` on paper for
`0008-cbpf-cpu-steering.md`.

## Related

[0001](0001-listener-per-worker.md) (uncoordinated per-worker bind), [0005](0005-incoming-cpu-opt-in.md)
(`SO_INCOMING_CPU` opt-in, which a worker passes as `Some(ctx.core.id)` once the NIC is tuned),
[0006](0006-shutdown-and-the-unwinding-thread.md) (the unwinding-thread hazard `Workers` avoids by
construction), [0007](0007-linux-only.md) (why the sysfs reader is Linux-only).
