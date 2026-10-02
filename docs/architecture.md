# Architecture

How the server is put together, where every piece of state lives, and what runs on each
event. For *why* any of it is shaped this way, see [decisions/](decisions/).

## The constraint everything follows from

`compio` is completion-based and thread-per-core. Each thread runs its own `io_uring`, I/O
handles are bound to the ring that created them, and buffers are handed to the kernel by
ownership for the duration of an operation. `compio::net::TcpStream` is `!Send`.

So the unit of ownership is the core. Everything a connection touches — the listener that
accepted it, the ring its I/O goes through, the pool it borrows from, the task that serves it —
belongs to one worker thread. The only things that cross threads are the raw file descriptor of
a connection nobody local can serve, and counters.

## One worker

```text
 thread "compio-pool/<i>", pinned to core c_i
 ┌───────────────────────────────────────────────────────────────────────┐
 │ compio Runtime (io_uring; coop_taskrun, single_issuer)                │
 │                                                                       │
 │  TcpListener         socket2: SO_REUSEADDR, SO_REUSEPORT, [SO_INCOMING_CPU=c_i], bind, listen
 │  LocalPool<R>        Rc<Inner>: idle: RefCell<Vec<R>>, taken: Cell, waiters
 │  Rc<Worker<S>>       the service clone, Sender/Receiver of the channel, Arc<WorkerStats>
 │                                                                       │
 │  task accept_loop    accept → try_reserve → spawn serve | hand_off     │
 │  task claim_loop     wait_available → recv → try_reserve → attach → spawn serve
 │  tasks serve × n     permit.acquire → service.handle(conn, &mut lease)  │
 └───────────────────────────────────────────────────────────────────────┘
```

Startup order inside the thread matters and is fixed: pin (step 8), build the ring (10), bind
the listener (9), wrap it in the runtime (10), build the pool (11), prewarm, report ready, then
spawn the two loops. Worker 0 starts alone so that a configured port of 0 is resolved once; the
others bind the port it got.

## State, and which thread can see it

| state | type | visible to |
|---|---|---|
| connections, resources, pool, runtime, ring | `Rc`, `Cell`, `RefCell` | one worker |
| the service | `S: Clone + Send` | one clone per worker |
| handoff channel | `flume::bounded<Overflow>` | every worker; lock-free MPMC |
| per-worker counters | `WorkerStats`, relaxed atomics, 128-byte aligned | written by one worker, read by anyone |
| configuration | `Arc<Config>` | everyone, read-only |
| shutdown signal | `flume::Receiver<()>` per worker; the `Server` holds the only `Sender` | everyone |

`WorkerStats` is aligned to its own cache line so two workers never write the same line. The
totals in `Server::stats()` are a sum of per-worker snapshots, taken counter by counter, not a
consistent cut.

## The three loops

### accept (steps 12–15)

```text
loop
  (stream, peer) = race(listener.accept(), shutdown)      shutdown → return
  stats.accepted++ ; set_nodelay
  match pool.try_reserve()                                 Cell compare, no await
    Some(permit) → stats.served_local++ ; spawn serve(permit, Connection{route: Local})
    None         → fd = detach(stream).await               SharedFd::try_unwrap, immediate for a fresh socket
                   match tx.try_send(Overflow{fd, peer, from: i, hops: 0})
                     Ok           → stats.handed_off++
                     Full(ov)     → stats.handoff_full++ ; channel_full(ov)
                     Disconnected → drop (shutting down)
```

`accept` errors that mean "out of descriptors" (`EMFILE`, `ENFILE`, `ENOBUFS`, `ENOMEM`) back
off 10 ms before the next accept; anything else is counted and the loop continues.

### claim (steps 16–17)

```text
loop
  race(pool.wait_available(), shutdown)                    shutdown → return
  ov = race(rx.recv_async(), shutdown)                     shutdown / disconnected → return
  match pool.try_reserve()
    Some(permit) → stream = attach(ov.fd)                  TcpStream::from_std on THIS ring
                   stats.claimed++ ; spawn serve(permit, Connection{route: Claimed{from, hops}})
    None         → the listener took the slot meanwhile
                   if ov.hops < max_hops → ov.hops++ ; tx.try_send(ov) → stats.bounced++ (Full → channel_full)
                   else                  → channel_full(ov)
```

The wait comes before the receive on purpose: a worker only takes what it can serve, and it
does not hold a slot while it waits, so an idle worker's full capacity stays available to its
own listener. The price is the race in the `None` arm, which is bounded by `max_hops`.

### channel_full

Only reached when every worker is at capacity *and* the queue is full.

* `OverflowPolicy::ServeLocally` (default): `attach` on this ring, `reserve_unbounded`, serve.
  The pool is over capacity by one until the connection ends; `stats.oversubscribed++`.
* `OverflowPolicy::Reject`: drop the `Overflow`, which closes the socket; `stats.rejected++`.

### serve

```text
stats.active++
lease = permit.acquire().await          pop idle, else R::create(&cx).await; Err → stats.resource_errors++, slot released
service.handle(conn, &mut lease).await  Ok → completed++ ; Err → handler_errors++
stats.active--                          lease drops: recycle() ? idle.push : drop ; slot released
```

## The pool

```text
capacity        fixed
idle            Vec<R>, LIFO
taken           permits + leases outstanding
invariant       idle.len() + taken <= capacity   (except through reserve_unbounded)
```

* `try_reserve` → `Permit` if `taken < capacity`; `taken += 1`. Synchronous.
* `Permit::acquire` → `Lease`: pops an idle resource or creates one. Dropping an unconverted
  permit refunds the slot.
* `Lease` drop: `taken -= 1`; the resource goes back on the idle list if `recycle()` says so
  and there is room, else it is dropped. Over-capacity leases therefore never grow the idle list.
* Waiting (`reserve`, `wait_available`, `drained`) registers exactly one waker per waiting
  future, removed when the future resolves *or is dropped*, so nothing stale is ever woken.
  Every release wakes all capacity waiters: a `wait_available` waiter does not consume the slot
  it was woken for, so waking one could strand a `reserve` waiter while a slot is free.

## Moving an fd between rings

```text
detach(stream):  shared = stream.to_shared_fd()      refcount 2
                 drop(stream)                        refcount 1
                 shared.try_unwrap()                 Ok(socket2::Socket) → OwnedFd
                 (Err → shared.take().await: an op is still in flight; wait for it)
attach(fd):      TcpStream::from_std(std::net::TcpStream::from(fd))
                 → Socket::from_socket2 → Attacher::new → Runtime::with_current(|r| r.attach(fd))
```

Nothing is submitted against a freshly accepted socket, so `detach` is synchronous in practice.
`attach` must run inside the destination worker's runtime; that is what binds the fd to its
ring. Between the two the fd is just an `OwnedFd` inside an `Overflow`, and dropping an
`Overflow` closes it.

## Shutdown

1. `Server::shutdown` drops the only `Sender<()>`. Every worker's `recv_async` on its clone of
   the receiver resolves with `Disconnected`.
2. Both loops exit (their `race` against shutdown resolves); the worker awaits them.
3. The worker waits for `pool.drained()` up to `drain_timeout`: in-flight handlers finish.
4. `block_on` returns and the runtime drops. Whatever is still pending is cancelled with it; the
   listener and any remaining sockets close.
5. `Server::join` (or `Drop`) joins the threads.

If the `Server` is dropped *while the dropping thread is unwinding*, the sender is dropped from a
helper thread instead. compio's executor aborts the process when one of its task wakers runs on
a thread that is already panicking, and the disconnect wakes worker tasks synchronously. See
[decision 0006](decisions/0006-shutdown-and-the-unwinding-thread.md).

## Invariants

These hold in every test and are what a change must keep true:

* `idle + taken <= capacity` on every pool, except by `reserve_unbounded`.
* `served_local + handed_off == accepted` per worker, at all times.
* Over a quiescent server: `claimed + oversubscribed + rejected == handed_off` summed over
  workers, `queued == 0`, `active == 0`.
* `bounced` only ever increments from the claim loop's `None` arm, and a connection bounces at
  most `max_hops` times.
* A stream is only ever polled by the ring it is currently attached to. `detach` happens on the
  accepting worker, `attach` on the claiming worker, and nothing touches the fd in between.
