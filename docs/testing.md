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
| [`tests/pool.rs`](../tests/pool.rs) | The bb8-style pool on one thread, each test inside its own compio runtime: an idle connection is reused, not reopened; at `max_size` a `get` times out with `RunError::TimedOut` and succeeds once the slot frees; a connection that `has_broken` is dropped on return, and an idle one that fails `is_valid` is replaced on check-out; a failed `connect` surfaces as `RunError::User` and releases its slot; `warm` opens `min_idle` up front; statistics count direct gets, waits, timeouts and creations, and the `Pool` roll-up equals the one thread's; a parked getter is counted, then served the freed connection; `reap` retires connections past `max_lifetime` and `maintain` refills to `min_idle`; `clear` drains the idle list; `close` rejects `get`, makes `warm` a no-op and drops connections as they come back. A compile-time check keeps `Pool` `Send + Sync + Clone` |

The manager is a stand-in whose connection is just its own `u32` id, so a test can tell a reused
connection from a fresh one. Thread-locals steer it — fail the next `connect`, fail `is_valid`
once, report connections broken — and since every `#[test]` runs on its own thread, that state
is isolated without a lock.

## Flakiness

Every wait in the suite is one-sided: a `get` that must time out (50 ms, nothing can free the
slot), a sleep that must outlast `max_lifetime` (25 ms against 10 ms), and a sleep that only has
to let a spawned getter park on the single-threaded runtime (20 ms). A slow runner makes them
slower, not wrong.

## The script

`scripts/tune-nic.sh` is checked three ways: `bash -n`, `shellcheck`, and a `--check` run,
which is read-only and exits 0 or 2 depending on the runner's NIC. `--dry-run` prints every
command it would run.

## CI

[`.github/workflows/ci.yml`](../.github/workflows/ci.yml), Ubuntu only:

| job | what |
|---|---|
| fmt | `cargo fmt --all --check` |
| clippy | `cargo clippy --all-targets --locked -- -D warnings` |
| test (stable, beta) | build all targets, run all tests, doctests; `-D warnings` on stable |
| msrv | `cargo build --lib --locked` on Rust 1.95, the bisected minimum |
| docs | `cargo doc --no-deps` with `-D warnings --cfg docsrs` |
| shell | `bash -n`, `shellcheck`, `tune-nic.sh --check` |
| links | `lychee --offline` over every Markdown file |
