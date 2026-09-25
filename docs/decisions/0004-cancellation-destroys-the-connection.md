# 0004 — A checkout cancelled mid-operation is destroyed

## Context

This is the sharpest difference between a readiness-based pool and this one, and it is a genuine
burden the crate pushes onto the caller.

With readiness-based IO (tokio), dropping a future with a read pending cancels nothing that
matters: the syscall has not happened, and the connection is still at a known protocol position.
Returning it to the pool is safe.

With completion-based IO it is not. The operation has been submitted; the kernel owns the buffer
and the peer's response is still coming. `compio` keeps the *buffer* sound — that is its job — but
the *protocol* is now out of step. The next reader on that connection sees the tail of someone
else's response. The failure surfaces on a later, unrelated request, which is a miserable thing to
debug.

Cancellation is not exotic. Any `select!`, any timeout, any early `?` return between submit and
completion produces it — including the pool's own `acquire_timeout`.

## Decision

Make it a type-level obligation rather than a comment in a design document.

```rust
let op = conn.begin_op();
let n = conn.read(&mut buf).await?;   // if this await is cancelled…
op.complete_op();                      // …this never runs
```

`OpGuard` holds an `Rc<Cell<bool>>` shared with its `Pooled`. Its `Drop` sets the flag unless
`complete_op` disarmed it first, and `Pooled::drop` destroys a poisoned connection instead of
returning it (`poisoned++`). The guard is `#[must_use]`, and it borrows nothing from the `Pooled`,
so the connection stays fully usable while the guard is alive. `Pooled::poison()` is the manual
form, for when you learn the connection is bad some other way: a protocol error, a partial write,
a response you could not parse.

**The same rule applies inside the pool.** `acquire` is itself cancellable at three awaits —
`recycle`, `Exchange::unpark`, `Manage::connect` — and each has claimed shard budget before
awaiting. `Reserved` is the RAII guard for that: on drop it refunds the budget, and in the
`holding` variant also accounts for the connection the cancelled future is about to drop. Without
it, a shard would slowly leak capacity until it sat at `max_size` holding nothing.

## Consequences

**Discipline is on the caller.** Forget `begin_op` around an operation you cancel and you will
return a desynchronized connection. If you genuinely never cancel, you can skip it — everyone
believes that about their code right up until they add a timeout.

**Poisoned connections are never parked.** A connection reaches the cross-thread exchange only
after its guard dropped cleanly, which is half of what makes `Detach` sound
([0005](0005-detach-is-opt-in.md)).

**It is observable.** `Metrics::poisoned` counts them, so a rising poison rate is a signal that
something upstream is cancelling more than you thought.

**Rejected: drain the connection on the pool's behalf.** The pool does not know the protocol — it
cannot tell how many bytes are still coming, and a generic "read until quiet" heuristic would
guess wrong on exactly the protocols where it matters.

**Rejected: make `begin_op` implicit by wrapping every deref.** `Pooled` derefs to the raw
connection precisely so any compio API works through it. Intercepting that would mean wrapping
every IO method on every connection type the crate has never heard of.
