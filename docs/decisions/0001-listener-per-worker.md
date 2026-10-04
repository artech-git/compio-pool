# 0001 — One `SO_REUSEPORT` listener per worker

## Constraint

A `compio::net::TcpStream` belongs to the ring that created it and is `!Send`. Whoever accepts
a connection owns it. With one shared listener, the accepting thread would own every connection
and have to hand *all* of them to the other cores — the overflow path would be the only path.

## Decision

Every worker creates its own socket with `socket2`, sets `SO_REUSEADDR` and `SO_REUSEPORT`
before `bind`, binds the same address, listens, and wraps the result in its own runtime
(recipe steps 9 and 10). The kernel hashes each incoming connection's 4-tuple onto one socket in
the group, so `accept` completes on the ring of the worker that will serve it. Worker 0 binds
first so a configured port of 0 is resolved once; the others bind the port it was given.

## Rejected

* **One listener, dispatch after accept.** Serialises every connection through one core and
  one ring, and makes the fd handoff mandatory rather than exceptional.
* **One listener per worker on different ports.** Clients would have to know the layout.
* **A BPF `SO_ATTACH_REUSEPORT_CBPF` program for exact steering.** Would let the kernel pick by
  CPU rather than hash directly. Deferred: `SO_INCOMING_CPU` (step 4, [0005](0005-incoming-cpu-opt-in.md))
  achieves the same alignment once the NIC is tuned, without shipping bytecode.

## Costs

* The kernel decides who gets a connection, and it decides by hash. Six connections against two
  listeners can all land on one; the tests allow for it and the handoff path exists for it.
* No admission control before accept. A connection is always accepted, then placed.
* `SO_REUSEPORT` groups require the same effective UID on every socket. All workers are threads
  of one process, so this is automatic, but a second process cannot join the group unnoticed.
