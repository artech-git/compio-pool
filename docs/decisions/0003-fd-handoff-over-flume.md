# 0003 — Overflow travels as a raw fd on one bounded `flume` channel

## Constraint

A stream cannot cross threads; its file descriptor can. Recipe steps 6, 14, 15 and 17 are
explicit about the mechanism: strip the socket to an owned fd, push it onto a single bounded
multi-producer multi-consumer channel, and have the receiving thread rebuild the stream on its
own ring.

## Decision

* **What crosses:** `Overflow { fd: OwnedFd, peer, from, hops }`. `detach` takes the compio
  stream's `SharedFd`, drops the stream's own reference, and `try_unwrap`s the socket; for a
  freshly accepted socket that is synchronous and cannot fail. `attach` is
  `TcpStream::from_std` on the claiming worker, which binds the fd to that ring.
* **How:** one `flume::bounded(handoff_capacity)` shared by every worker. Every worker holds a
  `Sender` and a `Receiver`. `try_send` on the push side, `recv_async` on the claim side.
* **Bounded:** the channel is the process-wide count of accepted-but-unserved connections. When
  it is full as well, the policy in `OverflowPolicy` applies.

## Rejected

* **Move the `TcpStream` itself.** Not `Send`; and moving the compio handle would leave a ring
  with an attachment it no longer owns.
* **A per-worker inbox (N channels) and a chooser.** Needs load information to choose well, and
  that information is exactly the cross-core traffic the design avoids. One shared queue lets
  the idle worker self-select.
* **`std::sync::mpsc`.** Single consumer.
* **`crossbeam` or a hand-rolled `ArrayQueue`.** No async receive; the claim loop would have to
  poll or park a thread. `flume` gives a lock-free MPMC queue with an async `recv` that wakes the
  waiting compio task, and nothing else is needed from it (`default-features = false`,
  `features = ["async"]`).
* **Unbounded.** Hides saturation until memory runs out. A bound makes the "everything is full"
  state explicit and gives `OverflowPolicy` something to decide.

## Costs

* A detach and an attach per overflow connection: two reference-count moves and one
  `Runtime::attach`, which is a no-op on io_uring. The measured median cost including the queue
  wait at capacity 1 was about 0.4 ms over the roomy configuration, most of it waiting.
* The accepting worker pays for the `accept` and the detach of a connection it never serves.
* `handoff_capacity` is one more number to size. Its depth is `Stats::queued`.
