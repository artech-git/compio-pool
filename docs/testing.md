# Testing

What the tests prove, how to run them, and what CI does.

## Where tests run

Linux only (`io_uring`); when the host is not Linux, a Linux VM works fine. The repository may be
mounted read-only, so the loop is: edit on the host, mirror into the build machine, build there.

```sh
rsync -a --delete --exclude target --exclude .git --exclude Cargo.lock \
      /path/to/compio-pool/ ~/compio-pool/ &&
cd ~/compio-pool &&
cargo fmt --all --check &&
cargo clippy --all-targets -- -D warnings &&
cargo test --all-targets &&
cargo test --doc
```

`Cargo.lock` is generated on the build machine against current crates and copied back, so the
lockfile that is committed is the one that was built.

## The suites

| file | proves |
|---|---|
| [`tests/pool.rs`](../tests/pool.rs) | `LocalPool` behaviour, each test inside its own single-threaded compio runtime with a `u32`-id manager steered by thread-locals: an idle connection is reused; `max_size` is enforced and a `get` beyond it returns `RunError::TimedOut`, after which a freed slot lets the next succeed; `has_broken` and a failed `is_valid` drop the connection and free its slot; a failing `connect` surfaces `RunError::User` and frees the reserved slot; `warm` opens `min_idle` up front; `reap` / `maintain` retire expired idle then refill; `clear` drains idle; `close` rejects `get` and drops returns. The counter assertions — `State` / `Statistics`, and a parked waiter counted then served — are `#[cfg(feature = "metrics")]`, so the default build checks behaviour and the `--features metrics` build also checks the numbers. A compile-time assertion pins `Pool: Send + Sync + Clone` |
| [`tests/affinity.rs`](../tests/affinity.rs) | *(the `affinity` feature)* `CoreSet` selection on synthetic topologies — `physical_only` drops SMT siblings and keeps distinct `(package, core)` across packages, `spread` round-robins packages while `pack` groups them, filters and `take` narrow the set; `permitted()` is a subset of the affinity mask; an empty core set is rejected; and `PerCore::spawn_each` / `block_on_each` pin, run and join one worker per core |
Both `tests/pool.rs` and `tests/affinity.rs` are self-contained; the harness tests build a compio
runtime on each pinned worker, and the suite keeps CPU pinning best-effort since it may run under a
restricted affinity mask.

### Things the tests had to learn

* **Behaviour and counters are tested apart.** The pool behaves identically with or without the
  `metrics` feature, so the behavioural assertions always run and only the counter assertions are
  `#[cfg(feature = "metrics")]`. A test that read `State` unconditionally would fail the default
  build, where every counter is zero.
* **Each test is one thread.** `LocalPool` is `!Send`, so a test builds one compio runtime and runs
  the whole scenario on it — including the parked-waiter case, where a second task blocks in `get`
  and is woken when the first connection is returned, all on that one thread. A compile-time check
  keeps `Pool` `Send + Sync + Clone`.
* **Waiting is driven by the timeout.** The max-size test sets a short `connection_timeout` so the
  over-cap `get` parks and then returns `RunError::TimedOut` without wedging the test; dropping the
  holder frees the slot and the next `get` succeeds immediately.

## Pinning is best-effort

The `affinity` harness tests assert that every worker *ran* and was joined, not that the kernel
accepted each pin: a restricted cpuset (CI runners, containers) can refuse a core, and `PerCore`
then runs that worker unpinned rather than failing. The `CoreSet` selection tests build synthetic
topologies in memory, so they do not depend on the runner's real CPU layout and stay deterministic.

## The script

`scripts/tune-nic.sh` is checked three ways: `bash -n`, `shellcheck`, and a `--check` run,
which is read-only and exits 0 or 2 depending on the runner's NIC. `--dry-run` prints every
command it would run.

## CI

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml), Ubuntu only:

| job | what |
|---|---|
| fmt | `cargo fmt --all --check` |
| clippy | `cargo clippy --all-targets -- -D warnings`, and again `--features affinity` |
| test (stable, beta) | build all targets, run all tests and doctests, both default and `--features affinity`; `-D warnings` on stable |
| msrv | `cargo build --lib --locked` on Rust 1.95, the bisected minimum |
| docs | `cargo doc --no-deps --all-features` (nightly) with `-D warnings --cfg docsrs` |
| shell | `bash -n`, `shellcheck`, `tune-nic.sh --self-test` and `--check` |
| links | `lychee --offline` over every Markdown file |
