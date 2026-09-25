# Testing

The pool's correctness claims are conservation of connections, respect for `max_size`, and
cancellation safety. None of those are provable by example-based tests alone, so the suite is
built around an **invariant oracle** that randomized drivers and coverage-guided fuzzers share.

```sh
cargo test                  # unit, integration, property and limit suites
cargo test --test fuzz      # the randomized suites on their own
cargo test --doc            # the crate-level example
```

## The oracle

`tests/common/mod.rs` holds the test doubles and the driver. `run_ops` executes a decoded workload
and re-checks, after **every step**:

**Conservation — no connection is ever lost.**

```text
created == closed + live + parked + taken + cleared
```

`taken` is `Pooled::take`, which hands ownership out of the pool by design. `cleared` is
`Exchange::clear` — reached from both `Pool::close` and `Pool::invalidate` — which drops parked
connections *without* counting them closed, so `created` ends up ahead. The driver tracks that term
explicitly rather than pretending it does not happen, and standalone tests pin the behaviour
directly.

**Capacity.** `shard.size() <= max_size`, always, including across a cancellation.

**Exclusivity.** No connection id is handed out twice at once.

**Retirement is final.** A connection retired by generation, lifetime, idle time or use count never
reappears.

**Counter monotonicity.** `created`, `closed`, `acquires` and friends never decrease.

The test connections are `!Send` — `LocalConn` holds an `Rc<()>` — which is exactly the property
that rules out `bb8` and `deadpool`, so the suite would stop compiling if a stray `Send` bound
crept back into the crate. The managers can be told to fail or hang `connect` and `recycle` on
command, which is how cancellation at each awaited step is reached deterministically.

`src/test_support.rs` is the same idea for the crate's own unit tests, where a test needs to reach
`pub(crate)` internals — `Shard`, `Slot`, `Counters` — without going through the public API.

## The suites

| suite | what it covers |
|---|---|
| `tests/acquire.rs` | checkout, reuse, waiting, timeouts, warming |
| `tests/lifecycle.rs` | retirement policies, `invalidate`, `close`, metrics |
| `tests/guard.rs` | `Pooled`, `OpGuard`, poisoning, `take` |
| `tests/exchange.rs` | the reservoir, single-thread and cross-thread |
| `tests/reaper.rs` | the background reaper |
| `tests/cancellation.rs` | cancellation at each awaited step of `acquire` |
| `tests/builder.rs` | the constructors and every config setter |
| `tests/fuzz.rs` | randomized workloads against the invariants above |
| `tests/limits.rs` | degenerate configurations — zero, one, `usize::MAX` |
| `tests/pool.rs` | end-to-end behaviour of the public API |

Each `src/*.rs` also carries unit tests for its own logic — `Slot::is_expired` against every
retirement reason, `Config` setters and their assertions, `Error` display and source.

## The randomized suites

`tests/fuzz.rs` is seeded rather than genuinely random: each test loops over a range of seeds, and
every assertion names the seed and step that produced it, so a failure replays exactly. They run in
CI at a small size and double as a soak run:

```sh
COMPIO_POOL_FUZZ_SEEDS=2000 COMPIO_POOL_FUZZ_STEPS=5000 cargo test --release --test fuzz
```

One detail is worth knowing when a randomized test fails: the workload uses a short
`acquire_timeout` (2 ms) so that a seed which pins the shard at `max_size` makes progress instead
of stalling the run. A dial never yields in these tests, so nothing times out except real
contention.

## Coverage-guided fuzzing

`fuzz/` holds two libFuzzer targets sharing the oracle above. They need nightly, and `fuzz/` is its
own workspace so the parent crate keeps building on stable.

```sh
cargo +nightly fuzz run pool_ops
cargo +nightly fuzz run reservoir_ops
```

**`pool_ops`** decodes a byte string into a workload and runs it through the same `run_ops` driver
as the seeded suite. It keeps a small fixed set of pools (one per `max_size` from 1 to 4 — small
shards are where contention, waiting and the cap being hit actually live) and reuses them across
inputs, because a shard is never removed from the `SHARDS` thread-local and a fresh pool per input
would grow the harness without bound over a long campaign. The consequence is that a reproducer is
the whole corpus prefix, not a single file.

**`reservoir_ops`** drives `Exchange` directly against a model of what should be resident, with
`detach` and `attach` failing under the input's control. The property it proves is that admission
never leaks: whatever sequence of parks, claims, failed detaches, failed attaches and clears
happens, it must still be possible to fill the reservoir to exactly `capacity`. That is the
invariant the CAS-before-detach admission scheme exists to hold — see
[decision 0006](decisions/0006-lock-free-reservoir.md).

## CI

Two workflows. [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) runs on every push and
pull request:

| job | what it guards |
|---|---|
| `fmt` | `cargo fmt --all --check` |
| `clippy` | `--all-targets`, warnings denied |
| `test` | Linux, macOS and Windows on stable, plus beta on Linux; builds and tests `--all-targets` (examples and benches included) and runs doc tests |
| `msrv` | `cargo build --lib` on 1.88, the floor `rust-version` claims |
| `docs` | `cargo doc --no-deps`; `#![warn(missing_docs)]` means a new public item without docs fails here |
| `miri` | `cargo miri test --lib` under strict provenance — the unit suite is runtime-free, so it runs unmodified |
| `fuzz` | Builds both libFuzzer targets and runs a short smoke campaign — enough to catch a target that stopped compiling or an invariant that now trips immediately, not a real campaign |
| `minimal-features` | `cargo build --lib`, which ignores dev-dependencies and so catches accidental reliance on compio features only enabled for tests |
| `links` | Relative links between README and `docs/` resolve (`lychee --offline`) |

`-D warnings` is set per job rather than workflow-wide. The beta and nightly legs compile against
lints that do not exist on stable yet, and an upstream deprecation landing in nightly should not
turn every open PR red for a reason no PR caused — so those legs report failures, not warnings.

The Windows leg matters beyond portability: it is what keeps the IOCP constraint in
[decision 0005](decisions/0005-detach-is-opt-in.md) honest, since the crate must build and pass
there without `Detach` being implementable. It also checks the `#[cfg(unix)]` gates on
`examples/unix_socket.rs` and `examples/steal.rs` are actually right rather than merely present.

`miri` is the one that earns its keep on the lock-free paths. The randomized suites only ever
observe the interleaving they happen to get; Miri checks the orderings themselves, which is what
the CAS-before-detach admission of [decision 0006](decisions/0006-lock-free-reservoir.md) and the
relaxed counters of [decision 0008](decisions/0008-relaxed-counters.md) rest on.

### The nightly soak

[`.github/workflows/soak.yml`](../.github/workflows/soak.yml) runs the same harnesses at a size
that actually finds things, on a nightly schedule and on demand
(`workflow_dispatch`, with the seeds, steps and per-target seconds as inputs):

| job | what it does |
|---|---|
| `seeded` | `cargo test --release --test fuzz` with `COMPIO_POOL_FUZZ_SEEDS=2000` and `COMPIO_POOL_FUZZ_STEPS=5000` |
| `libfuzzer` | 15 minutes per target by default, then `cargo fuzz cmin` |
| `miri-many-seeds` | the unit suite under `-Zmiri-many-seeds=0..64`, a different schedule and weak-memory seed each run |

The libFuzzer corpus lives in the Actions cache rather than in the tree, so each campaign starts
from what the last one found interesting. A crash uploads its reproducer as a workflow artifact;
for `pool_ops` remember that the reproducer is the whole corpus prefix, not a single file, because
the target reuses its pools across inputs.
