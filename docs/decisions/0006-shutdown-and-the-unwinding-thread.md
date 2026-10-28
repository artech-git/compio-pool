# 0006 — Shutdown is a shared flag, not a cross-thread wake

## Constraint

`Pool::close` is called from one thread but has to stop every thread's `LocalPool`, and a `get`
already parked on another thread must eventually come unblocked. The tempting implementation —
reach across and wake every parked `get` the instant `close` is called — is a trap on `compio`.

## Decision

`close` sets a single shared `AtomicBool` in `Shared`. Nothing is sent, and no waker is fired
across a thread boundary. Each thread observes the flag on its own: `get` checks it on entry and
after every wake and returns `RunError::Closed`; a returned `PooledConnection` is dropped instead of
recycled; `warm` / `maintain` become no-ops.

A `get` already parked waiting for a slot therefore unblocks on its next **local** wake — another
connection on that thread returning, or [`clear`](../operations.md#shutdown) — or when its
`connection_timeout` elapses, not the microsecond `close` runs elsewhere. That is the whole point:
the crate never runs another thread's task waker.

## The abort it avoids

Firing a parked task's waker from the closing thread would run it **synchronously, on that thread**.
Those wakers belong to `compio` tasks, and `compio-executor` guards its task code with a
`panic_guard!` that calls `std::process::abort()` if `std::thread::panicking()` is true when the
guard drops — it cannot tell a panic inside the executor from a panic already in progress on a
thread that merely happened to call a waker.

So a cross-thread wake would mean: a test asserts, fails, unwinds, drops its `Pool`, the drop wakes
a parked `get` on another core's behalf — and the whole process dies with `SIGABRT` instead of one
test failing, the real message lost. The previous design hit exactly this with a `flume`-disconnect
shutdown; the flag avoids it by construction, because a plain `AtomicBool::store` wakes nothing.

If you *do* build a graceful shutdown on top of the harness that wakes a `compio` task from another
thread, it must detour through a helper thread when `std::thread::panicking()` — the hazard
[`workers::Workers`](../../src/workers.rs) is careful to never trip, see
[0009](0009-core-affinity-harness.md).

## Rejected

* **A shared channel or waker the closing thread fires**, to make `close` instantaneous. Reintroduces
  the abort hazard and the unwinding-thread detour, to buy a promptness the local-wake-or-timeout
  path already delivers well enough.
* **An eager per-thread broadcast driven from `close`.** Same hazard, same thread-boundary waker.

## Costs

* `close` is not instantaneous: a parked `get` waits for its next local wake or its
  `connection_timeout`. For a prompt drain you stop each worker's accept loop and call `clear` on
  its thread, which wakes that thread's own waiters locally.
* The flag is checked on every `get` and every connection return — a relaxed atomic load, off to the
  side of the `Rc`/`Cell` hot path.
