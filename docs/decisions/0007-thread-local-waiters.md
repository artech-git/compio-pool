# 0007 — Waiters queue on their own thread only

## Context

When a shard is at `max_size`, `acquire` has to wait. The question is what it waits *for*.

A conventional pool has one global wait queue: a returning connection anywhere wakes the
longest-waiting caller anywhere. That needs a cross-thread notification on every return — a
condvar, a channel, or a waker registry — which is a shared mutable structure on the return path,
touched by every core.

## Decision

Each shard keeps its own `VecDeque<Rc<Waiter>>`. `Shard::wait()` parks the caller there, and
`wake_one()` — called from `push_idle` and from `release` — pops the front and wakes it.

This cannot deadlock, and the reason is worth stating precisely: a thread at `max_size` has
`max_size` outstanding checkouts **of its own**, and every one of them returns through
`Pooled::drop` on this same thread. Waiting locally always makes progress, so no cross-thread
wakeup is needed at all.

Cancellation is handled in `WaitForSlot::drop`:

```rust
if waiter.notified.get() {
    // Handed a wakeup, but being cancelled. Passing it on is mandatory.
    self.shard.wake_one();
} else {
    // Remove ourselves from the queue.
}
```

Swallowing a notification would leave the connection that triggered it idle while the next waiter
sleeps until its own timeout — a self-inflicted latency spike with no other symptom.

## Consequences

**Waiters are not FIFO-fair.** This is the known wart. `acquire_inner` checks the free list
*before* parking, so a caller arriving after a waiter can take the connection the waiter was just
woken for. Under sustained oversubscription this produces an unbounded tail rather than fair
queueing: a waiter can be repeatedly overtaken.

`acquire_timeout` bounds it — the symptom becomes a timeout rather than a hang — which is a mask,
not a fix. It is tracked as a follow-up. The fix is a handoff: wake a waiter by giving it the
connection directly rather than by telling it to go and look.

Note the symmetry with [0006](0006-lock-free-reservoir.md): the mutex exchange showed the same
barging pattern, with a p50 of 42 ns and a p99.9 of 63 µs. Barging is cheap on average and awful
at the tail, in both places.

**A returning connection never crosses threads to serve a waiter.** `Pool::release` refuses to
park a connection in the exchange while `shard.has_waiters()`, so the local waiter is served first.
The reaper makes the same check before dialling a refill: a waiter will be served by a returning
connection sooner than by a fresh handshake.
