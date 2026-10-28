# 0003 — Reserve the slot before the connection exists

## Constraint

`get` must never let a thread open more than `max_size` connections, but opening one means awaiting
`ManageConnection::connect`, which may legitimately be slow (a backend handshake). If the count
were bumped only *after* `connect` returned, two concurrent `get`s on the same thread would both
see room, both await `connect`, and both overshoot the cap.

## Decision

The slot is claimed synchronously, before anything is awaited. `try_reserve` compares `slots` (a
`Cell<u32>`) against `max_size` and, if there is room, increments it and returns a `Held` — an RAII
guard standing for one reserved-but-not-yet-filled slot. Only then is `connect` awaited and the
connection moved into the `Held`.

`Held`'s `Drop` releases the slot unless the slot was committed with `into_pooled` (handed to a
`PooledConnection`) or `into_idle` (parked on the free list). That is what makes every early return
and cancellation inside `get` / `warm` leak-free: a `connect` that errors, a future dropped
mid-await, a failed `is_valid` — each drops its `Held` and the slot goes straight back. So `slots`
counts idle + checked-out + **reserved**.

The whole structure is `Rc<Inner>` with `Cell` / `RefCell` inside and is never `Send`
([0002](0002-blueprint-and-local-pool.md)), so none of this needs a lock or an atomic.

## Rejected

* **Increment after `connect` returns.** Two `get`s race past `max_size` while both handshakes are
  in flight.
* **A counting semaphore over unmanaged connections.** Loses the idle list and recycling, which is
  the point of calling it a pool rather than a limiter.
* **Atomics on the count "just in case".** Nothing outside the owning thread can reach `Inner`; an
  atomic read-modify-write on the lease path is a cost with no buyer.

## Costs

* `slots` includes reservations, so a slot can be "in use" with no connection behind it for the
  duration of a `connect`. That is the correct accounting — the connection is being established —
  but it means the invariant is `idle + checked_out + reserved == slots`, not
  `idle + live == slots`.
* One more RAII type (`Held`) on the internal path, earning its keep by making the many early exits
  in `get` / `warm` correct by construction rather than by hand.
