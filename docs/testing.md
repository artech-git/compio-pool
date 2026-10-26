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
| [`tests/pool.rs`](../tests/pool.rs) | `LocalPool` accounting in isolation, inside one compio runtime: permits count against capacity and refund on drop; leases create lazily and reuse idle resources; `recycle() == false` and `discard` drop; a failed `create` releases the slot; `reserve` and `wait_available` wake on release and `wait_available` does not take the slot; `drained` resolves at zero; `reserve_unbounded` goes over capacity without growing the idle list; `prewarm` is capped |
| [`tests/affinity.rs`](../tests/affinity.rs) | *(the `affinity` feature)* `CoreSet` selection on synthetic topologies — `physical_only` drops SMT siblings and keeps distinct `(package, core)` across packages, `spread` round-robins packages while `pack` groups them, filters and `take` narrow the set; `permitted()` is a subset of the affinity mask; an empty core set is rejected; and `PerCore::spawn_each` / `block_on_each` pin, run and join one worker per core |
Both `tests/pool.rs` and `tests/affinity.rs` are self-contained; the harness tests build a compio
runtime on each pinned worker, and the suite keeps CPU pinning best-effort since it may run under a
restricted affinity mask.

### Things the tests had to learn

* **The kernel decides who accepts.** Six connections against two listeners can land 6/0 or
  3/3. Assertions are on totals (`active == 2`, `queued == 4`, `served_local ∈ {1, 2}`), and
  every client speaks from its own thread, because the parked ones are only served once the
  active ones close, and which is which is not knowable in advance.
* **Counters tick after the client has its bytes.** `completed` increments when the handler
  returns, which is after the client's `read` succeeded. Tests poll with `eventually` rather
  than reading a counter straight after a roundtrip.
* **A panic with a live `Server` used to abort the process.** compio's executor aborts if a
  task waker runs on an unwinding thread; `Server::shutdown` now detours in that case. The
  regression test is `#[should_panic]` — if the detour breaks, the whole `shutdown` binary dies
  with `SIGABRT` rather than that one test passing. [Decision 0006](decisions/0006-shutdown-and-the-unwinding-thread.md).

## Flakiness

The full suite was run repeatedly in the VM after the fixes above; the count and result of the
last run are in the commit that introduced this page. The one distribution test
(`every_worker_has_its_own_listener_on_the_same_port`) asserts only that *at least two* of four
listeners accepted something over 64 connections. With `incoming_cpu` off, 60 trials of that
shape in the VM never put more than 25 of 64 on one listener.

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
