# Plan: a `reuseport` module — builder, UDP, and CBPF CPU steering

Status: **plan only** — not yet implemented. Base branch is `experimental` (@ `2acfb41`), *not*
`feat/metrics`. Three decisions (§3) are yours before any code.

## 1. Context

`SO_REUSEPORT` is not new to the crate — the baseline already ships:

* [`listener::bind_reuseport`](../src/listener.rs) — a `socket2` TCP socket with `SO_REUSEADDR`
  + `SO_REUSEPORT` set before `bind`, an optional `SO_INCOMING_CPU` hint, `listen`, returned as a
  `std::net::TcpListener`. Re-exported at [lib.rs:117](../src/lib.rs#L117).
* The **UDP** counterpart is copy-pasted into [`examples/udp_echo.rs:53`](../examples/udp_echo.rs#L53)
  (`bind_reuseport_udp`), with a comment saying the crate's helper is TCP-only.
* [ADR 0001](decisions/0001-listener-per-worker.md#L22) **deferred** a BPF
  `SO_ATTACH_REUSEPORT_CBPF` program for exact steering, in favour of `SO_INCOMING_CPU`
  ([ADR 0005](decisions/0005-incoming-cpu-opt-in.md)), "without shipping bytecode".

So "the `so_reuseport` feature" is really three additions on top of a working base: **(a)** fold
UDP into the crate, **(b)** a builder (IPv6/dual-stack, backlog, opts) so the free function stops
growing positional args, and **(c)** the deferred CBPF steering. The user picked all three.

### Base-branch facts that shape the plan

* **Branch from `experimental`, not `feat/metrics`.** Reuseport is orthogonal to the metrics
  gating. `experimental` has **no `[features]` section yet** — this feature would add the first
  one (see §3-B). It merges cleanly whatever happens to `feat/metrics`.
* `listener.rs`, `cpu.rs`, `lib.rs`, `README.md` are byte-identical on `experimental` and the
  current checkout; only `Cargo.toml` differs. All line numbers below are valid on `experimental`.
* **Doc-debt not to inherit.** `2acfb41` claims to drop "the last of the old thread-per-core
  Server material", but [operations.md](operations.md) still documents `Server::stats()`,
  `Workers::AllCores`, `OverflowPolicy`, `handoff_capacity`, and [testing.md](testing.md) still
  lists `tests/server.rs` / `handoff.rs` / `config.rs` / `shutdown.rs` — **none of which exist**
  (`tests/` is only `pool.rs`). This feature touches only the parts of those docs that are true
  for the current examples-own-the-loop model; it does not try to fix the pre-existing rot.

### The crate-wide constraint CBPF collides with

[lib.rs:101](../src/lib.rs#L101) is `#![forbid(unsafe_code)]`. `socket2` 0.6 exposes **nothing**
for `SO_ATTACH_REUSEPORT_CBPF`, so attaching a classic-BPF program means a raw
`libc::setsockopt(fd, SOL_SOCKET, SO_ATTACH_REUSEPORT_CBPF, &prog, len)` — which is `unsafe`.
`forbid` cannot be locally `#[allow]`-ed. This is the feature's central design tension (§3-B),
and it is the reason (b)+(a) are unconditional while (c) is feature-gated.

## 2. Scope

Most of it is one new file, `src/reuseport.rs` (a rename of `listener.rs`;
it now covers datagrams too, so `listener` is the wrong name). Around it:

| area | change |
|---|---|
| `src/reuseport.rs` | `ReusePort` builder; `bind_tcp`/`bind_udp`; keep `bind_reuseport` + add `bind_reuseport_udp` as thin wrappers; `#[cfg]`-gated `cbpf` submodule |
| `src/lib.rs` | `pub mod reuseport;` + re-exports; fix the two `[listener::…]` doc links ([79](../src/lib.rs#L79)/[92](../src/lib.rs#L92)); the `forbid`→`deny` policy (§3-B) |
| `Cargo.toml` | add `libc` (already in the lock at 0.2.189, transitive) as a direct dep *behind the feature*; add the `[features]` section |
| examples | `udp_echo.rs` uses the crate helper (delete its local copy); optional new `echo_steered.rs` for the coordinated-bind CPU-steering shape |
| `README.md` | update the reuseport bullet ([337](../README.md#L337)) and `SO_INCOMING_CPU` line ([416](../README.md#L416)); add a short "exact CPU steering" note + the feature flag |
| `docs/decisions/0008-cbpf-cpu-steering.md` | **new ADR** (next number is 0008), relating to 0001 + 0005 |
| `docs/operations.md`, `docs/testing.md` | add the steering + test story (current-model parts only) |
| `.github/workflows/ci.yml` | a job that builds/clippies/tests `--features cpu-steering` |

Nothing in `src/pool.rs`, `manage.rs`, `builder.rs`, `cpu.rs` needs to change.

## 3. OPEN DECISIONS (yours)

### 3-A — Is CBPF worth it over the `SO_INCOMING_CPU` we already ship? *(the big one)*

ADR 0001's deferral reasoning still holds: for **one worker per CPU**, `SO_INCOMING_CPU` already
"walks the group and returns the socket whose `incoming_cpu` == the running CPU"
([ADR 0005](decisions/0005-incoming-cpu-opt-in.md#L11)). A CBPF program that returns the running
CPU index lands on the *same* socket. Both depend on the SYN being processed on the right CPU
(i.e. the NIC being tuned); neither conjures locality the hardware isn't already giving. CBPF's
**marginal** wins are narrow:

* arbitrary policy the hint can't express (hash *within* a CPU's socket subset; weighting);
* one `setsockopt` on the group instead of one-per-socket + the "any socket set ⇒ override"
  group semantics of the hint.

Its **costs** are not narrow: `unsafe`, a `libc` dep, kernel-version-dependent index/modulo
semantics (§6), and — the structural one — it needs an **ordered, coordinated bind** that the
crate's deliberately *uncoordinated* per-worker model (ADR 0001) does not provide (§3-C).

> **Recommendation:** land (a)+(b) first (Phases 1–2, pure `socket2`, no `unsafe`, immediate
> value), then do CBPF as a **feature-gated, clearly-experimental** Phase 3 whose steering is
> *validated in the Lima VM before merge* the way ADR 0005's hint was. That way the full module
> ships, but the risky third of it can't destabilise the safe two-thirds, and you can still decide
> after Phase 2 that the hint is enough and stop.

### 3-B — The `unsafe` policy and the feature name

The default build must stay provably `unsafe`-free (it is the crate's stated guarantee). Proposal:

```rust
#![cfg_attr(not(feature = "cpu-steering"), forbid(unsafe_code))]
#![cfg_attr(feature = "cpu-steering", deny(unsafe_code))]   // deny, so the one audited block…
```
…and `#[allow(unsafe_code)]` on the `cbpf` submodule only (one `setsockopt` call). Off by default
⇒ zero `unsafe`, zero `libc`, zero cost — same ethos as the `metrics` feature.

*Name:* the gated thing is CPU steering, not reuseport (reuseport is unconditional), so
**`cpu-steering`** reads better than `reuseport`/`cbpf`. Decision: confirm the name.

### 3-C — How CBPF gets socket-index == CPU-id (the coordination cost)

A classic program steering by CPU is ~2–3 instructions returning `SKF_AD_CPU` (the running CPU),
used as an index into the group's socket array **in bind order**. So `socks[i]` must be the worker
pinned to CPU `i` — which requires workers to **bind in CPU order**, and the program to be attached
to the group once. The current examples spawn all workers and each binds whenever it is scheduled:
no ordering. Two ways to supply it:

* **(i) Main-thread ordered bind + fd handoff** *(recommended):* the main thread builds the N
  sockets in CPU order, attaches the CBPF program to the first, hands socket `i` to worker `i`,
  which wraps it with `TcpListener::from_std` on its own ring. Clean, deterministic; changes the
  example/README shape for the steered case (a new `echo_steered.rs` rather than editing `echo.rs`).
* **(ii) A bind barrier:** workers bind in index order behind a tiny `mpsc`/`Barrier` relay. Keeps
  the per-worker shape but adds cross-thread ordering to the hot startup path.

Plus the **fewer-workers-than-CPUs** case (`taskset`): a flow on an unmanned CPU must not index out
of range, so the program needs `A %= num_socks` baked in at attach time — which means the worker
count must be known when the program is built (fine for (i), awkward for a purely per-worker model).

## 4. Phases

### Phase 1 — `ReusePort` builder + UDP *(safe, unconditional, no `unsafe`)*
- New `src/reuseport.rs`. `ReusePort { backlog, incoming_cpu, only_v6, reuse_address }` with
  `new()`, chainable setters, `bind_tcp(addr) -> TcpListener`, `bind_udp(addr) -> UdpSocket`.
- `bind_tcp` is today's `bind_reuseport` body (STREAM + `listen`); `bind_udp` is the example's
  body (DGRAM, no `listen`). `set_incoming_cpu` stays the `#[cfg(target_os = "linux")]` no-op-
  elsewhere helper it already is ([listener.rs:38](../src/listener.rs#L38)).
- `backlog` default 1024 (matches every call site today; the old Server's 4096 was a different knob).

### Phase 2 — dual-stack, back-compat, de-dupe
- `only_v6(bool)` → `socket2::Socket::set_only_v6` (dual-stack on a `::` bind); `Domain::for_address`
  already picks v4/v6. No-op/skip on v4 addresses.
- Keep `pub fn bind_reuseport(addr, backlog, incoming_cpu)` as a wrapper over the builder so the
  four TCP examples ([echo:98](../examples/echo.rs#L98), [wal:141](../examples/wal.rs#L141),
  [file_server:106](../examples/file_server.rs#L106), [tcp_pool:135](../examples/tcp_pool.rs#L135))
  and the re-export are untouched. Add `pub fn bind_reuseport_udp(addr)`.
- `examples/udp_echo.rs`: delete the local helper ([53](../examples/udp_echo.rs#L53)), call the
  crate's. Rename `listener` → `reuseport`; fix the `listener::` doc links in `lib.rs` and the
  README bullet. (`compio_pool::{bind_reuseport, cpu}` imports are re-exports, so they don't move.)
  This path change is an accepted pre-1.0 break (README already carries the "API may change" note).

### Phase 3 — CBPF CPU steering *(feature `cpu-steering`, the only `unsafe`)*
- `cbpf` submodule: build the classic program (`SKF_AD_CPU` → `%= num_socks` → return), fill a
  `libc::sock_fprog`, one `libc::setsockopt(…, SO_ATTACH_REUSEPORT_CBPF, …)`. Define the handful of
  `BPF_*`/`SKF_AD_*` constants locally if `libc` lacks them (§6).
- A coordinated entry point per §3-C(i): `ReusePort::bind_tcp_group(addr, cpus) -> Vec<TcpListener>`
  that binds in order, attaches the program to the first, returns the listeners for the caller to
  hand to workers. (A UDP sibling if wanted.)
- `steer_by_cpu()` on the builder is only compiled with the feature.

### Phase 4 — ADR + docs
- `docs/decisions/0008-cbpf-cpu-steering.md`: Constraint (ordered group, §3-C) / Decision (feature-
  gated, coordinated bind) / Rejected (keep hint-only; eBPF instead of cBPF) / Costs (`unsafe`,
  `libc`, kernel-version semantics, coordination). Link from [decisions/README.md](decisions/README.md)
  and from 0001's "deferred" bullet.
- README: reuseport bullet + the one-paragraph steering note + the feature flag. `operations.md`:
  a "CBPF exact steering" subsection next to `SO_INCOMING_CPU`, current-model wording only.
  `testing.md`: how the steering test is run (VM, `#[ignore]`, per ADR 0005's probe method).

### Phase 5 — CI + verify
- `tests/reuseport.rs` (new): unconditional — bind a small group on `127.0.0.1:0`, assert the hash
  path spreads connections (CI already allows binding reuseport on loopback, per
  [ci.yml:58](../.github/workflows/ci.yml#L58)); a `#[cfg_attr(not Linux, ignore)]` UDP roundtrip.
  CBPF steering assertion is `#[ignore]` (kernel/pinning-dependent) and run by hand in the VM.
- `ci.yml`: add `--features cpu-steering` to build/clippy/test (the guardrail for the `unsafe`
  path); keep the default jobs as the feature-off path. MSRV job stays `--lib` (libc is gated out).
- Green both ways, per the project's VM loop:
  ```
  cargo clippy --all-targets --locked -- -D warnings            # off (default)
  cargo test --all-targets --locked && cargo test --doc --locked
  cargo clippy --all-targets --features cpu-steering --locked -- -D warnings
  cargo test  --all-targets --features cpu-steering --locked
  cargo doc --no-deps --all-features --locked
  ```

## 5. Site checklist

| site | where | phase | action |
|---|---|---|---|
| `bind_reuseport` body | [listener.rs:20-34](../src/listener.rs#L20) | 1 | becomes `ReusePort::bind_tcp`; fn kept as wrapper |
| `set_incoming_cpu` | [listener.rs:38-44](../src/listener.rs#L38) | 1 | unchanged, moves into new module |
| UDP helper | [udp_echo.rs:53](../examples/udp_echo.rs#L53) | 2 | delete; call `bind_reuseport_udp` |
| module decl + re-export | [lib.rs:111](../src/lib.rs#L111)/[117](../src/lib.rs#L117) | 2 | `listener`→`reuseport`; add `ReusePort`, `bind_reuseport_udp` |
| `forbid(unsafe_code)` | [lib.rs:101](../src/lib.rs#L101) | 3 | → cfg'd `forbid`/`deny` per §3-B |
| doc links `[listener::…]` | [lib.rs:79](../src/lib.rs#L79)/[92](../src/lib.rs#L92), [README:337](../README.md#L337) | 2 | retarget to `reuseport` |
| `libc` dep + `[features]` | [Cargo.toml](../Cargo.toml) | 1/3 | first `[features]` on `experimental`; `libc` optional |
| TCP example call sites | echo/wal/file_server/tcp_pool | 2 | none (wrapper preserves signature) |
| new ADR + index | [decisions/](decisions/) | 4 | 0008 + table row + 0001 backlink |
| CI feature job | [ci.yml](../.github/workflows/ci.yml) | 5 | add `--features cpu-steering` |

## 6. Risks

* **Kernel-version-dependent CBPF semantics.** Exactly how `run_bpf_filter` maps the program's
  return to a socket index (in-range vs. `reciprocal_scale`/modulo, fallback-to-hash) has shifted
  across kernels. Do **not** assert it from memory — probe it empirically in the VM as ADR 0005 did
  for the hint, and pin the ADR's claims to that probe.
* **It changes ADR 0001's uncoordinated model.** Ordered bind (§3-C) is a real new constraint on
  startup; keep it confined to the steered entry point so the default path stays uncoordinated.
* **`unsafe` audit / `forbid` relaxation.** Only the `setsockopt` is `unsafe`; the program bytes are
  a fixed `const` array. Review that the `sock_fprog` pointer/len outlive the call.
* **`libc` BPF constants.** `SO_ATTACH_REUSEPORT_CBPF` and `sock_fprog`/`sock_filter` are in `libc`
  on linux-gnu; some `BPF_*`/`SKF_AD_*` opcodes may not be — define them locally with a comment if so.
* **Doc rot is not this feature's job.** `operations.md`/`testing.md` still describe the removed
  `Server`; this plan adds only current-model content and does not attempt the wider cleanup.
```
