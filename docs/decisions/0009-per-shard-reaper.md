# 0009 — One reaper task per shard, spawned lazily

## Context

`idle_timeout`, `max_lifetime` and `min_idle` cannot be enforced by the acquire path alone.
Checkout-time expiry checks cover connections someone asks for; they say nothing about a
connection nobody asks for, which is exactly the one that goes stale, and nothing about refilling
a shard that has drained.

A background task is needed. Since connections are `!Send` and live in thread-locals
([0001](0001-per-thread-shards.md)), it cannot be one global task — it has to run on the thread
that owns the connections it touches.

## Decision

One reaper per shard, spawned on the shard's **first touch**, and only when the configuration
actually needs it:

```rust
fn needs_reaper(&self) -> bool {
    self.min_idle > 0 || self.idle_timeout.is_some() || self.max_lifetime.is_some()
}
```

Three details carry weight:

**It holds a `Weak<Shard>`.** The task upgrades each tick and returns when the upgrade fails, so it
stops on its own when the thread's shard goes away. A strong `Rc` would keep the shard — and its
connections — alive past the point anything wanted them.

**It is spawned outside the `SHARDS.with` closure.** The thread-local is no longer borrowed by the
time the task first runs, so the reaper is free to reach for its own shard without a `RefCell`
double-borrow panic.

**No runtime is not an error.** If `Runtime::try_with_current` fails there is nothing to spawn
onto, so spawning is skipped silently. The pool still works; expiry is then enforced at checkout
instead. This is what lets the pool be constructed and exercised outside a compio runtime, which
the unit tests rely on.

Each tick: sleep `reap_interval` → if the pool is closed, drain everything and stop → drain expired
slots → refill up to `effective_min_idle()`, but never while a waiter is parked.

## Consequences

**A timer task per thread per pool.** At the default `reap_interval` of 30s this is negligible, but
it is not nothing — a process with many short-lived compio threads pays a spawn per thread.
Configurations with no lifetime limits and `min_idle = 0` pay nothing, because no task is spawned.

**`reap_interval` is the resolution of your timeouts.** A connection with `idle_timeout = 10s` and
`reap_interval = 30s` can sit idle for up to 40 seconds before the reaper notices — though it will
be caught at checkout the moment someone asks for it, so a stale connection is never *handed out*.
`Config::reap_interval` asserts non-zero.

**Refill defers to waiters.** `while shard.idle_len() < target && !shard.has_waiters()` — a parked
waiter will be served by a returning connection sooner than by a fresh handshake, and dialling into
contention would only push the shard toward `max_size` ([0007](0007-thread-local-waiters.md)).

**A failed refill dial is silent.** It breaks out of the refill loop and waits for the next tick;
there is no caller to return an error to. The dial is still counted, so a backend that is refusing
connections shows up as `created`/`closed` churn rather than as a log line.
