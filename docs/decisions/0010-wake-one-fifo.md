# 0010 — A returned connection wakes one waiter, oldest first

## Constraint

A thread at `max_size` parks every further `get` on a waiter list. The first bb8-style pool woke
the **whole** list on every connection return or freed slot: all `n` parked tasks were polled, one
won, and the other `n - 1` registered again — each with a linear scan of the list to find its old
entry. Under overload that is the common case, and the cost is super-linear in the depth of the
queue: draining 500, 1000 and 2000 parked waiters through a single slot measured 56 ms, 284 ms and
1.42 s on one pinned core, roughly 5× per doubling. A flood of clients beyond capacity therefore
turned each connection return into milliseconds of CPU on the very core that should be serving,
and the README's description ("one parked waiter is woken") did not match the code.

The wake-everyone design was also what `LocalPool::clear` relied on to unblock waiters, and the
metrics-gating change (`760401a`) left that wake, and `clear`'s slot release, inside the
metrics-only block — so in the default build a drain after a backend failover wedged the thread's
pool for good. The accounting on that path had to stop depending on which features are on.

## Decision

Waiters are a **FIFO**, and capacity is rationed **one wake per unit**:

* A returned connection or a freed slot wakes the **oldest** waiter only. `reap` and `clear`
  wake as many as they freed. A closed pool wakes everyone, since there is nothing left to ration,
  and the first waiter to see the flag passes it down the queue.
* A wake is a unit of capacity owned by the waiter it went to. If that future is dropped before it
  acts — its `connection_timeout` won the race — its `Drop` hands the wake to the next in line, so
  freed capacity is never stranded behind a cancelled task.
* A woken waiter that finds nothing to take (a direct `get` call arrived first and took it) rejoins
  at the back under a fresh id.
* Ids are issued in increasing order and entries are only pushed at the back, so the queue is
  sorted by id: a waiter is found by binary search, and a `woken_upto` watermark tells a waiter it
  has already been taken off the queue without searching at all.
* `clear`, `reap`, every `Held` and `PooledConnection` drop release slots through the one
  `release_slots` path and notify through the one `notify` path, both unconditional; the metrics
  gauges only mirror them.

Draining the same 500 / 1000 / 2000 waiters through one slot now costs one poll per waiter.

## Rejected

* **Keep wake-all and make re-registration O(1).** Fixes the scan but not the herd: every return
  still polls every parked task, and the cost stays linear in queue depth per return.
* **Wake one, without the hand-on in `Drop`.** The simplest FIFO, and it loses capacity: a
  waiter woken in the same instant its timeout fires takes the wake with it, and the returned
  connection sits idle until the next return. The test
  `a_woken_waiter_that_gives_up_passes_the_wake_on` exists for this case.
* **A slab plus an index queue for O(1) everything.** More code for a cold path; binary search over
  a sorted `VecDeque` is O(log n) to find and a memmove to remove, which is already far below the
  cost of the poll it saves.

## Costs

* Fairness is strict FIFO. A long-waiting task is served before a newly arrived one even when the
  new one would be cheaper to serve; there is no priority.
* Two counters (`next_id`, `woken_upto`) and the sorted-by-id invariant to keep in mind when
  touching `Waiters`: entries must only ever be pushed at the back.
* A spurious poll of a parked `get` (a combinator polling it for another reason) still does one
  binary search to refresh its waker.
