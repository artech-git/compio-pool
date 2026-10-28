# 0001 — One `SO_REUSEPORT` listener per worker

## Constraint

A `compio::net::TcpStream` belongs to the ring that created it and is `!Send`. Whoever accepts a
connection owns it and serves it from that thread's `LocalPool`
([0002](0002-blueprint-and-local-pool.md)). With one shared listener, a single thread would accept
every connection and have to serve the whole process from its one pool and ring — the other cores'
pools would sit idle.

## Decision

Every worker creates its own socket with `socket2`, sets `SO_REUSEADDR` and `SO_REUSEPORT` before
`bind`, binds the same address, listens, and wraps the result in its own runtime — all of which is
what [`listener::bind_reuseport`](../../src/listener.rs) does. The kernel hashes each incoming
connection's 4-tuple onto one socket in the group, so `accept` completes on the ring of the worker
that will serve it, and that worker leases from its own `LocalPool`.

The bind is uncoordinated: each worker binds independently in its own accept loop, and the harness
([0009](0009-core-affinity-harness.md)) deliberately does not take that over.

## Rejected

* **One listener, dispatch after accept.** Funnels every connection onto one thread's ring and
  pool, and would force a cross-thread fd handoff the design is built to avoid — the point is that
  each core serves what its own ring accepted.
* **One listener per worker on different ports.** Clients would have to know the layout.
* **A BPF `SO_ATTACH_REUSEPORT_CBPF` program for exact steering.** Would let the kernel pick by CPU
  rather than hash directly. Deferred: `SO_INCOMING_CPU` ([0005](0005-incoming-cpu-opt-in.md))
  achieves the same alignment once the NIC is tuned, without shipping bytecode.

## Costs

* The kernel decides who gets a connection, and it decides by hash. Six connections against two
  listeners can all land on one; distribution is only as even as the flow hash, and the accepting
  worker serves them all from its own pool.
* No admission control before accept. A connection is always accepted, then served by the worker
  whose listener the kernel chose.
* `SO_REUSEPORT` groups require the same effective UID on every socket. All workers are threads of
  one process, so this is automatic, but a second process cannot join the group unnoticed.
