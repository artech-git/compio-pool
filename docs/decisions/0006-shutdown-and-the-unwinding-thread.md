# 0006 — Shutdown is a channel disconnect, detoured when the caller is panicking

## Constraint

Workers block in `accept` and in `recv_async`. Shutdown has to wake both, on every worker, from
a thread that is not any of them.

## Decision

Each worker holds a clone of a `flume::Receiver<()>`; the `Server` holds the only `Sender`.
Nothing is ever sent. `Server::shutdown` drops the sender, every `recv_async` resolves with
`Disconnected`, both loops exit (they `race` their I/O against that receive), the worker drains
for `drain_timeout`, and the runtime drops.

## The abort

Dropping the last sender wakes the waiting receivers' wakers synchronously, on the dropping
thread. Those wakers belong to compio tasks, and `compio-executor` guards its task code with a
`panic_guard!` that calls `std::process::abort()` if `std::thread::panicking()` is true when the
guard drops — it cannot tell a panic inside the executor from a panic already in progress on the
thread that happened to call a waker.

So: a test asserts, fails, unwinds, drops its `Server`, which drops the sender, which runs a
task waker on the unwinding test thread — and the whole process dies with `SIGABRT` instead of
one test failing. Every assertion failure in the first run of the suite surfaced this way, with
the real failure message lost because the harness never got to print it.

`Server::shutdown` therefore checks `thread::panicking()`. On the normal path it drops the sender
directly. When the caller is unwinding, it spawns a short-lived thread to drop it and joins that
thread. The test `dropping_a_server_while_unwinding_does_not_abort` is `#[should_panic]`; without
the detour the test binary aborts rather than passing.

## Rejected

* **An `AtomicBool` the loops check.** They are blocked in the kernel; nobody checks anything.
* **Waking through the driver's waker instead of a task waker.** The loops await futures that
  register task wakers; there is no way to reach them without one.
* **Leaving it.** A library whose handle aborts the process when dropped during a panic is not
  a library anyone can use with `?` in `main`.

## Costs

One extra thread spawn on the panic path. Nothing on the normal path.
