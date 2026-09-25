# 0003 — `recycle` runs at checkout, not at return

## Context

A pool wants to hand out connections that work. Two moments are available for checking: when a
connection comes back, or when it goes out.

Returning a connection happens in `Pooled::drop`. `Drop` cannot await. There is no way to run an
async liveness probe there, and spawning a detached task to do it would mean the connection is
neither in the pool nor out of it for an unbounded time, with no owner to attribute a failure to.

## Decision

`Manage::recycle(&self, conn, meta)` is an **acquire-side** hook. It runs in `acquire_inner` on
every connection taken off the free list, before the `Pooled` guard is built:

```rust
let reserved = Reserved::holding(&shard, &self.counters);
let recycled = self.manager.recycle(&mut conn, &meta).await;
reserved.disarm();
```

Returning `Err` discards the connection (`recycle_failures++`), and the loop transparently tries
the next idle one, or falls through to dialling a fresh connection. The caller never sees it.

`SlotMeta` is passed in so a manager can make its own policy decisions — for example, pinging only
a connection that has been idle for more than a second, and trusting one returned a millisecond
ago.

`Manage::disconnect` is the mirror image and is **synchronous** for the same reason: it runs from
`Drop` paths and from thread teardown, where awaiting is not possible. It defaults to a no-op;
override it only for protocols with an explicit goodbye (Redis `QUIT`, Postgres `Terminate`).

## Consequences

**Validation is on the latency path.** A `PING` in `recycle` is paid by the request, not by a
background task. `examples/unix_socket.rs` exists partly to price this: over a Unix socket the
cheap check (`peer_addr()`) keeps succeeding long after the peer process has exited, so a real
round trip is the only honest liveness check, and the example reports what it costs per phase.

**It doubles as protocol-state reset.** Roll back an open transaction, drain a pipeline, reset
session variables. Doing this at checkout rather than at return means it also covers connections
whose last user returned them in a state nobody checked.

**A cancelled `recycle` is accounted for.** At that point the connection is out of the free list
but not yet inside a guard, so a cancellation would otherwise lose both the connection and the
budget. `Reserved::holding` is exactly that case: on drop it counts `live--`, `closed++` and
refunds the shard. See [0004](0004-cancellation-destroys-the-connection.md).

**Rejected: validate on return.** It cannot await, so it would be limited to synchronous checks —
which, as the Unix socket example shows, are frequently the useless ones.
