# 0004 — Wait for room without holding a slot, then re-check

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

The re-check is what makes a wake safe: a woken waiter never assumes the capacity it was woken for
is still there — it looks again, and if another task took it first, re-parks. Each waiter owns an id
and withdraws its waker if its future is dropped before it resolves, so nothing stale is ever woken.
*How many* waiters a release wakes is a separate question, decided in
[0010](0010-wake-one-fifo.md): one, oldest first. This record only fixes that a parked waiter holds
no slot and always re-checks after waking.

## Rejected

* **Reserve a slot, then wait.** Race-free, but the reservation sits idle while the waiter waits
  for work that may never come; at `max_size = 1` the thread could never serve anything.
* **A cross-thread parking primitive (condvar, semaphore).** There is nothing to synchronise across
  threads — the pool is single-threaded ([0002](0002-blueprint-and-local-pool.md)); plain task
  wakers on the owning thread suffice.

## Costs

* A benign retry: a woken `get` that lost the race goes round the loop once more. No syscall, no
  reservation is churned — it re-parks on the same waiter list. With the wake-one policy of
  [0010](0010-wake-one-fifo.md) this is rare; it takes a same-thread task slipping in between the
  wake and the waiter's next poll.
