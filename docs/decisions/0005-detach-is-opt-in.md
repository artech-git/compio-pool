# 0005 — Cross-thread migration is opt-in, behind `Detach`

## Context

Pure sharding ([0001](0001-per-thread-shards.md)) wastes connections when load is skewed: a quiet
thread holds idle sockets a busy thread would otherwise pay a handshake for. The obvious fix is to
let idle connections migrate.

Whether that is *sound* is not a property of the pool. It is a property of the driver:

| driver | `Driver::attach` | may a connection change threads? |
|---|---|---|
| io_uring (Linux) | no-op | yes — the fd table is process-wide |
| poll (Unix fallback) | no-op | yes |
| IOCP (Windows) | `CreateIoCompletionPort` | **no** — compio documents that a handle can and only can attach once, to one driver |

Under IOCP a socket bound to thread A's completion port keeps delivering completions to thread A's
port forever. A "stolen" socket would look fine and then deliver its completions to the wrong
thread.

It is also not a property of the fd alone. Everything carried across the boundary must be `Send`:
buffers, TLS session state, prepared-statement caches. A connection whose state includes an `Rc`
cannot migrate regardless of platform.

## Decision

Two traits, not one.

`Manage` is the base and places **no** bounds on `Connection`. `Detach: Manage` is a separate,
opt-in trait adding `type Parked: Send`, `detach` and `attach`. `Reservoir` is bounded on
`Detach`, so a pool that cannot legally migrate cannot be built with one — the constraint is
enforced by the type system rather than by documentation.

`detach` is **fallible**, returning `Option<Self::Parked>`:

```rust
fn detach(conn: Self::Connection) -> Option<Self::Parked>;
```

Moving a handle between drivers is sound only when nothing is still submitted against it. Rather
than have the pool assume that, the implementation gets to *check* it. With compio the check is
`SharedFd::try_unwrap`, which succeeds exactly at a strong count of one. The connection is consumed
either way: `None` means it has been dropped, and the pool accounts for it as closed
(`Parked::Destroyed`) and dials a replacement.

The pool upholds the other half of the invariant: a connection reaches `detach` only after its
`Pooled` guard has dropped cleanly, and a checkout cancelled mid-operation is poisoned and
destroyed rather than parked ([0004](0004-cancellation-destroys-the-connection.md)).

`attach` is async because it runs on the *claiming* thread and re-wraps the socket in that
thread's runtime (`TcpStream::from_std`, or a `from_raw_fd` constructor). Returning `Err` drops
the parked connection and the caller falls back to dialling (`Unparked::Lost`).

## Consequences

**Windows gets pure sharding.** On IOCP-backed handles, do not implement `Detach`. This is a real
limitation, not a temporary one.

**The default stays free.** `NoExchange` is a ZST whose `park` is a move and whose `unpark` is a
constant — the acquire fast path is unchanged whether or not the feature exists.

**Three-way results instead of `Option`.** `Parked::{Accepted, Refused, Destroyed}` and
`Unparked::{Claimed, Empty, Lost}` exist so that a connection lost to a failed detach or attach
stays visible in the counters. An earlier `Option`-shaped API made those disappear silently, which
broke the conservation invariant the fuzz suite checks.

**There is a worked example.** `examples/steal.rs` implements `Detach` for a real
`compio::net::TcpStream` — `try_unwrap` to check for in-flight ops, `into_raw_fd` into an
`OwnedFd` to cross the thread, `TcpStream::from_std` on the far side — and runs echo round trips
over the stolen sockets to prove the re-wrap produces a working stream rather than just
bookkeeping.
