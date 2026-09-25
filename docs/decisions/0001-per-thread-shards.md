# 0001 — Connections live in per-thread shards

## Context

`compio` is completion-based and thread-per-core. IO handles are bound to the driver that created
them and buffers are owned by the kernel while an operation is in flight, so
`compio::net::TcpStream` is `!Send`.

Every general-purpose Rust pool — `bb8`, `deadpool`, `r2d2` — is built on the opposite assumption:
connections are `Send`, the pool keeps one `Arc<Mutex<Vec<Conn>>>`, and any worker may take any
connection. `bb8::ManageConnection` spells this out as a `Connection: Send` bound, which rules a
compio stream out at the type level. This is not a missing feature in those crates; it is a
different runtime model.

A pool for compio has to hold `!Send` values behind a handle that is `Send + Sync`, because the
handle is cloned onto every runtime thread.

## Decision

Split the two apart. `Pool<M, X>` is an `Arc<Inner>` holding the manager, config, exchange,
counters and lifecycle flags — **and no connections at all**. The connections live in a
`Shard<M>` kept in a thread-local map keyed by pool id:

```rust
thread_local! {
    static SHARDS: RefCell<HashMap<u64, Box<dyn Any>>> = RefCell::new(HashMap::new());
}
```

A value in a thread-local is, by construction, only reachable from its own thread. That is what
makes the arrangement sound: `Pool` can be `Send + Sync` while `M::Connection` has no bounds
whatsoever, and `Manage`'s futures need no `Send` bound either.

`Box<dyn Any>` is there because one thread's map serves every pool in the process, of any `M`. The
downcast is infallible in practice — the id is allocated from a process-wide counter, so a pool id
uniquely determines the shard type.

## Consequences

**What it buys.** The acquire fast path is a `Vec::pop` on a `RefCell`: no lock, no atomic RMW, no
cache line shared with another core. `Shard` itself is built from `Cell`/`RefCell` rather than
atomics for the same reason. Per-thread cost stays flat as cores are added, which is the entire
reason thread-per-core runtimes exist. Cache and NUMA locality follow for free: a connection, its
buffers and its driver stay on one core.

**Shutdown is correct by construction.** At thread exit the map drops, `Shard::drop` runs, and the
connections are closed *on the thread whose driver owns them* — the only thread allowed to close
them. Every `Pooled` guard holds an `Rc<Shard>`, so by the time the shard drops nothing can still
be checked out.

**What it costs.** There is no single place to look at the pool, so metrics are summed across
threads ([0008](0008-relaxed-counters.md)) and sizing is per shard
([0002](0002-per-shard-sizing.md)). A shard is never removed from `SHARDS` while the thread lives,
which is also why the fuzz harness reuses a fixed set of pools rather than building one per input.
