# 0004 — Wait for room without holding a slot; wake all and re-check

## Constraint

When a thread is at `max_size`, `get` has to wait for a slot. Two things make that subtle on a
single thread running many tasks:

* The waiter must **not** reserve a slot while it waits. A reservation held across the wait would
  tie up capacity that other tasks on the thread could be using, and with `max_size = 1` a parked
  `get` would deadlock the thread against itself.
* By the time a waiter wakes, what it waited for may be gone — another task on the same thread can
  take the freed slot or idle connection synchronously between the wake and the waiter's next poll.
  Acting on the stale observation would overshoot the cap.

## Decision

`get_inner` loops around `wait_ready`. `wait_ready` parks until the pool is *ready* — the idle list
is non-empty, or `slots < max_size`, or the pool is closed — and reserves **nothing**. Only after
it wakes does `get` do the synchronous check-and-take (`pop_fresh_idle`, then `try_reserve`). If
another task won the slot in between, both return nothing and the loop simply waits again.

Every slot release and every connection returned to the idle list calls `wake_all`. Waking more
than one waiter is deliberate and safe: each re-runs the synchronous check, at most one takes the
freed slot, and the rest re-park. Each waiter owns an id and withdraws its waker if its future is
dropped before it resolves, so nothing stale is ever woken.

## Rejected

* **Reserve a slot, then wait.** Race-free, but the reservation sits idle while the waiter waits
  for work that may never come; at `max_size = 1` the thread could never serve anything.
* **Wake exactly one waiter per release.** A `wait_ready` waiter woken for a slot it does not
  consume could strand another waiter while a slot is actually free. `wake_all` + re-check removes
  that class of bug for the cost of a few spurious wakes.
* **A cross-thread parking primitive (condvar, semaphore).** There is nothing to synchronise across
  threads — the pool is single-threaded ([0002](0002-blueprint-and-local-pool.md)); plain task
  wakers on the owning thread suffice.

## Costs

* A benign retry: a woken `get` that lost the race goes round the loop once more. No syscall, no
  reservation is churned — it re-parks on the same waiter list.
* `wake_all` is O(waiters) on each release, bounded by the thread's own in-flight `get` count. On a
  thread that is merely busy, not saturated, the waiter list is empty and the wake is free.
