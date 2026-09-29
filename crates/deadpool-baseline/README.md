# deadpool-baseline

The yardstick, not a product. This crate holds no pool of its own: it ports compio-pool's
end-to-end examples to **tokio + deadpool** so the claims in [`docs/performance.md`](../../docs/performance.md)
can be checked against the incumbent under the same load, against the same server, on the same
machine — rather than against numbers quoted from someone else's run.

Nothing here is published (`publish = false`).

## What is here

| example | the compio-pool example it ports |
|---|---|
| `examples/ncat_steal_bench.rs` | [`examples/ncat_steal_bench.rs`](../../examples/ncat_steal_bench.rs) |

```sh
cargo run --release -p deadpool-baseline --example ncat_steal_bench
```

Needs `ncat` on `PATH` (`brew install nmap`, `apt install ncat`). Same tunables as the parent:
`THREADS`, `ROUNDS`, `PAYLOAD`, `NCAT_ADDR`.

## Reading `ncat_steal_bench`

The parent example toggles the **exchange**, because compio-pool shards per thread and the exchange
is the one thing that lets a connection leave its shard. deadpool has no shards — one `Vec` behind
one `Mutex`, and a tokio `TcpStream` is `Send` — so migration is not a feature to switch on. The
arms change the topology instead:

| arm | topology | stands in for |
|---|---|---|
| split, pool per half | a runtime and a pool per half | `NoExchange`: phase B cannot see phase A's sockets |
| split, shared pool | a runtime per half, one pool | `Reservoir`: phase B pops what phase A returned |
| single runtime | one runtime, one pool | what you would actually write, and the honest throughput baseline |

The interesting result is not that the shared pool wins the cold start — it does, by two orders of
magnitude, and for free. It is **what that freedom is anchored to**: a tokio `TcpStream` is `Send`,
but its fd is registered with the reactor of the runtime that dialled it. Hand it to another
runtime and it keeps working only while the first is alive and driving; shut that one down and every
socket it registered starts failing with `A Tokio 1.x context was found, but it is being shutdown.`
— and deadpool hands them out anyway, because `recycle` sees a healthy socket. The example's
`caveat()` proves it.

So both examples keep the warm half alive through the cold phase, for reasons that are not the
same. In compio-pool that is a choice about where idle sockets should sit, and `Detach::attach`
re-registers the fd with whichever driver claims it. Here it is a requirement, and there is no hook
to re-register anything.
