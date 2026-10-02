# 0002 — Reserve the slot synchronously, create the resource later

## Constraint

Recipe step 13 is a *check* at the moment a connection arrives: is there room on this core? The
accept loop must answer without awaiting anything, or accepts queue up behind resource creation
— and a `Resource::create` may legitimately be slow (a backend handshake).

## Decision

`LocalPool::try_reserve` is a `Cell` compare that returns a `Permit` or `None`. The accept loop
spawns a task with the permit and goes straight back to `accept`. The task turns the permit into
a `Lease` with `Permit::acquire`, which pops an idle resource or awaits `Resource::create`.
Dropping a permit or a lease releases the slot; a lease also recycles its resource onto the idle
list if `recycle()` agrees and there is room.

The pool is `Rc<Inner>` with `Cell` and `RefCell` inside — recipe step 11's "standard reference
counting structures". It is never sent anywhere, so there is nothing to lock.

## Rejected

* **Acquire the resource inline in the accept loop.** One slow `create` stalls every accept on
  the core.
* **A counting semaphore plus unmanaged resources.** Loses the idle list and recycling, which
  is the point of calling it a pool.
* **Atomics in the pool "just in case".** Nothing else can reach it; an atomic RMW on the hot
  path is a cost with no buyer.

## Costs

* Two-step checkout in the API: `Permit` then `Lease`.
* `taken` counts permits as well as leases, so a slot can be "in use" with no resource behind it
  for the duration of `create`. That is the correct accounting — the connection is in service —
  but it means `idle + taken <= capacity` is the invariant, not `idle + leases <= capacity`.
