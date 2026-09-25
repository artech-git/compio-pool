# 0002 — `max_size` and `min_idle` are per shard

## Context

Given per-thread shards ([0001](0001-per-thread-shards.md)), a process-wide connection cap needs a
counter every thread increments and decrements on every checkout and every return. In practice
that is a semaphore or an atomic fetch-update on one cache line, touched by every core, on the
hottest path in the crate.

That cost is precisely the contention a thread-per-core runtime exists to avoid. Paying it on the
fast path would make the pool a worse version of `bb8` rather than a different thing.

## Decision

`Config::max_size` and `Config::min_idle` apply **per shard**. A pool with `max_size = 8` on four
compio threads can hold up to 32 connections. There is no global cap and no global counter on the
acquire path; `Shard::try_reserve` is a `Cell<usize>` compare-and-increment with no atomics at all.

`min_idle` is clamped to `max_size` (`Config::effective_min_idle`), which is the only interaction
between the two.

## Consequences

**Your real ceiling is `max_size × threads`.** This is the single most common way to get the crate
wrong, so it is stated in the crate docs, the `Config` docs, the README and here. Against a backend
with a hard `max_connections` — Postgres, a rate-limited API — the arithmetic is yours to do, and a
pool sized for one thread will over-dial on sixteen.

**Warm connections multiply too.** Sixteen threads at `min_idle = 2` is 32 idle connections against
a backend that may have been sized for 8.

**Skewed load wastes connections.** A quiet thread holds idle sockets a busy thread could use.
That is what the optional exchange exists to fix ([0005](0005-detach-is-opt-in.md)), and without it
you are on pure sharding.

**Rejected: a global semaphore with a per-thread cache.** A batched reservation — take ten from the
global counter, hand them out locally — would restore a global cap at an amortised cost. It also
reintroduces a shared mutable counter, a refill/drain policy, and an unfair distribution under
skew, in exchange for a guarantee most compio deployments can get by multiplying. Not worth it at
this stage; revisit if a real deployment needs a hard cap it cannot express as `per-thread ×
threads`.
