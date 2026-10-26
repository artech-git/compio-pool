# Plan: an optional `affinity` feature — topology-aware core selection + a pinned-worker harness

Status: **implemented** on branch `feat/core-affinity`, cut from `experimental` (@ `760401a`, after
the metrics feature merged in #15). The six decisions in §3 were resolved as recommended, with two
refinements (below). Verified green on macOS — type-check, clippy `-D warnings`, rustdoc and
doctests, both feature-off and feature-on — and on Linux/`io_uring` in the Lima VM, where the real
sysfs reader, the harness tests and the unchanged pool suite all pass.

> **Refinements during implementation.** (1) `CoreSet::permitted()` is **infallible** — it returns
> `CoreSet`, not `io::Result<CoreSet>`: the sysfs read is best-effort and degrades gracefully, so it
> never errors. (2) The three rewritten examples feed `PerCore::new(cpu::cores())`, which preserves
> their exact benchmark worker count (one per logical CPU); `CoreSet`'s topology selection is
> demonstrated in the `workers` rustdoc, the README and `tests/affinity.rs` instead of changing what
> the benchmarks measure. The optional `PerCore::proactor(…)` passthrough (§3-E) shipped in v1.

## 1. Context

Core affinity is **not new** to the crate — it is just hand-rolled. The baseline ships three thin
helpers in [`cpu`](../src/cpu.rs) and nothing that spawns a thread:

* [`cpu::cores()`](../src/cpu.rs#L11) — the process affinity mask (honours `taskset` / cgroup
  cpusets), sorted and de-duplicated.
* [`cpu::pin_current(core)`](../src/cpu.rs#L22) — pin the calling thread, best-effort.
* [`cpu::affinity_mask(cpu)`](../src/cpu.rs#L36) — the `smp_affinity` hex-word format for IRQ tuning.

`core_affinity 0.8.3` is already a **hard dependency** ([Cargo.toml:48](../Cargo.toml#L48)), used by
those helpers. So this feature adds nothing to the dependency graph (§3-C).

The gap is that **every server example repeats the same loop by hand**. [`echo.rs`](../examples/echo.rs#L64-L108),
[`file_server.rs`](../examples/file_server.rs#L75-L120) and [`tcp_pool.rs`](../examples/tcp_pool.rs#L102-L145)
each: guard on empty cores, `cpu::cores()`, `thread::Builder::new().name(..).spawn(move || worker(core, …))`,
`cpu::pin_current(core)` as the first line of the worker, build a `compio` runtime, bind a
`SO_REUSEPORT` listener, run an accept loop, and join the handles in `main`. Only four things vary:
the `Pool::builder()…build(Manager)` line, the startup `println!`, the per-connection `serve`
closure, and `file_server`'s per-accept [`set_nodelay`](../examples/file_server.rs#L116). The pivot
to the bb8 pool (`4ae6cf3`) deliberately removed the heavyweight `Server`/`Workers::AllCores`
abstraction that used to own this loop; the crate ethos is now *"the pool is the product; spawning
threads, pinning them and running the accept loop are yours."*

So the feature is two layers that respect that ethos while deleting the duplication, **both opt-in
behind one off-by-default flag** — the same zero-cost ethos as `metrics`:

* **(a) selection** — a topology-aware `CoreSet` added to the `cpu` module: physical cores vs SMT
  siblings, NUMA nodes, spread-vs-pack ordering. The raw `cores()`/`pin_current()`/`affinity_mask()`
  path stays exactly as it is.
* **(b) a harness** — a thin `PerCore` builder that owns *only* the repeated skeleton (enumerate →
  name → spawn → pin → build runtime → collect/join), hands each worker a `WorkerCtx`, and returns
  owned `JoinHandle`s. It never binds the listener and never coordinates — the accept loop stays in
  the body (ADR [0001](decisions/0001-listener-per-worker.md)).

### Two facts the adversarial check corrected (do not get these wrong in code)

* **`compio` 0.19.2 *does* ship thread pinning.** `RuntimeBuilder::thread_affinity(cpus: HashSet<usize>)`
  (compio-runtime 0.12.6) binds the runtime thread at `build()` via a cross-platform `affinity.rs`
  (`sched_setaffinity` on Linux). The harness still pins with `core_affinity` — to stay consistent
  with the `cpu` module and the examples, and to pin *before* the ring is created — but it must
  **not also** set `thread_affinity`, or every worker is pinned twice. (Also: `single_issuer` /
  `defer_taskrun`, which the README snippet at [238](../README.md#L238) shows, are
  **`ProactorBuilder`** methods reached via `RuntimeBuilder::with_proactor(…)`, not `RuntimeBuilder`
  options — relevant to §3-E.)
* **`core_affinity 0.8.3` has no topology.** Its entire public API is `get_core_ids() -> Option<Vec<CoreId>>`,
  `set_for_current(CoreId) -> bool`, and `CoreId { id: usize }` — no physical/SMT/NUMA data. Layer
  (a) must read topology itself (§3-B/§3-C).

### Base-branch facts that shape the plan

* **Branch from `experimental`, not `feat/metrics`.** Affinity is orthogonal to the metrics gating.
* **`experimental` now carries `[features] metrics = []`** (merged in #15 at `760401a`), so
  `affinity` is the **second** feature and follows the metrics precedent exactly. (This plan was
  first drafted against `2acfb41`, before #15 merged, when it noted — like the
  [reuseport plan](reuseport-feature-plan.md#L26) — that it would add the *first* `[features]`
  section. The CI feature-matrix wiring #15 planned had not landed, so the `affinity` jobs added
  here are the first to exercise a feature on *and* off.)
* **Line numbers are `experimental`'s.** `cpu.rs`, `listener.rs`, `builder.rs` and the three examples
  are byte-identical to the working checkout, so those anchors are valid. `lib.rs` is **not**: on
  `experimental` `#![forbid(unsafe_code)]` is [line 101](../src/lib.rs#L101) and `feature(doc_cfg)`
  is line 100 (the metrics branch drifted them to 104/103).
* **The metrics precedent is the template.** Gate with item-level `#[cfg(feature = "affinity")]`,
  `#[cfg_attr(docsrs, doc(cfg(feature = "affinity")))]` on public items (doc_cfg is already wired),
  a one-line empty feature, and a CI matrix that greens **both** configurations.
* **Doc-debt not to inherit.** [operations.md](operations.md#L11) still documents `Workers::AllCores`
  / `Server::stats()`, and [testing.md](testing.md#L28-L31) still lists `tests/server.rs`,
  `handoff.rs`, `shutdown.rs`, `config.rs` — **none exist** (`tests/` is only `pool.rs`). This
  feature adds only current-model content next to those; it does not attempt the wider cleanup.
* **ADR number collides with the reuseport plan.** The highest ADR on disk is `0007`, so the next
  free number is `0008` — but [reuseport-feature-plan.md:57](reuseport-feature-plan.md#L57) already
  reserves `0008` on paper for `0008-cbpf-cpu-steering.md`. This plan takes **`0009`** to avoid the
  clash (§3-F).

## 2. Scope

Layer (a) extends one existing file; layer (b) is one new file. Everything is behind the flag.

| area | change |
|---|---|
| `src/cpu.rs` | add `CoreInfo`, `CoreSet` + a `#[cfg(target_os="linux")]` sysfs reader (with a non-Linux sibling), all `#[cfg(feature="affinity")]`; raw path untouched & ungated |
| `src/workers.rs` | **new**, whole module gated: `PerCore` builder, `WorkerCtx`, `Workers<T>`, `spawn_each` + `block_on_each` |
| `src/lib.rs` | `#[cfg(feature="affinity")] pub mod workers;` + gated re-exports; one "core affinity" bullet in the production doc block ([31](../src/lib.rs#L31)); helper prose ([90-92](../src/lib.rs#L90)). **`forbid` at [101](../src/lib.rs#L101) stays verbatim** |
| `Cargo.toml` | add the first `[features]` section with `affinity = []` (no deps); `[[example]]` tables with `required-features` for the three rewritten examples |
| examples | rewrite `echo` / `file_server` / `tcp_pool` onto the harness; leave `udp_echo` / `unix_echo` / `wal` hand-rolled so the raw path stays demonstrated by default |
| `README.md` | update the "One ring per core" snippet ([193-226](../README.md#L193)) and the `cpu` helper bullet ([335](../README.md#L335)) |
| `docs/decisions/0009-core-affinity-harness.md` | **new ADR** (0009, not 0008 — §3-F), relating to 0001/0005/0006/0007 |
| `docs/operations.md`, `docs/testing.md` | add the placement + test story (current-model only) |
| `.github/workflows/ci.yml` | build/clippy/test `--features affinity` alongside the default off path |
| `tests/affinity.rs` | **new**, whole file gated |

Nothing in `src/pool.rs`, `manage.rs`, `builder.rs`, `listener.rs` needs to change.

## 2.5 The API

```rust
// --- src/cpu.rs : LAYER (a), all #[cfg(feature = "affinity")] ----------------
// raw path stays ungated: cores() @L11, pin_current() @L22, affinity_mask() @L36

/// Topology of one logical CPU from `/sys/devices/system/cpu`. `id` feeds
/// `CoreId` / `SO_INCOMING_CPU`; the rest drive the filters and ordering.
#[non_exhaustive]
pub struct CoreInfo { pub id: usize, pub core: usize, pub package: usize, pub numa_node: usize }
impl CoreInfo { pub fn core_id(&self) -> CoreId; }

/// A chainable selection over the cores THIS PROCESS MAY RUN ON — glommio's
/// `CpuSet` shape, named after the affinity mask, not the machine.
pub struct CoreSet { /* Vec<CoreInfo> in selection order */ }
impl CoreSet {
    /// Seeded by intersecting `cpu::cores()` (the cpuset-aware mask) with sysfs,
    /// so it can never name a core the process may not use. Non-Linux: every
    /// permitted core is its own physical core on package 0, node 0 (ADR 0007).
    pub fn permitted() -> std::io::Result<CoreSet>;
    pub fn from_infos(infos: impl IntoIterator<Item = CoreInfo>) -> CoreSet; // testable w/o /sys

    pub fn physical_only(self) -> CoreSet;          // drop SMT siblings, keep lowest id
    pub fn on_numa_node(self, node: usize) -> CoreSet;
    pub fn on_package(self, package: usize) -> CoreSet;
    pub fn filter(self, pred: impl FnMut(&CoreInfo) -> bool) -> CoreSet;

    pub fn spread(self) -> CoreSet;                 // maximally separated (glommio MaxSpread)
    pub fn pack(self) -> CoreSet;                   // fill a core/package first (MaxPack)
    pub fn take(self, n: usize) -> CoreSet;

    pub fn len(&self) -> usize;  pub fn is_empty(&self) -> bool;
    pub fn infos(&self) -> &[CoreInfo];  pub fn core_ids(&self) -> Vec<CoreId>;
}
impl IntoIterator for CoreSet { type Item = CoreId; } // drops into PerCore::new and `for`

// --- src/workers.rs : LAYER (b), whole module #[cfg(feature = "affinity")] ---

#[non_exhaustive]
pub struct WorkerCtx { pub index: usize, pub core: CoreId, pub pinned: bool }

/// Owns ONLY the repeated skeleton: empty guard, thread naming, pin-before-ring,
/// runtime build, handle collection, join, spawn-error `?`. Never binds, never
/// coordinates, never hands off fds (ADR 0001).
pub struct PerCore { /* cores, name, proactor: Option<compio::driver::ProactorBuilder> */ }
impl PerCore {
    pub fn new(cores: impl IntoIterator<Item = CoreId>) -> PerCore; // cpu::cores() or a CoreSet
    pub fn name(self, prefix: impl Into<String>) -> PerCore;        // worker i = "{prefix}/{i}"
    pub fn proactor(self, p: compio::driver::ProactorBuilder) -> PerCore; // thin passthrough (§3-E)

    /// Primitive: pin, then run `body`; you build your own runtime (UDP/Unix/non-compio).
    pub fn spawn_each<F, T>(self, body: F) -> std::io::Result<Workers<T>>
    where F: FnOnce(WorkerCtx) -> T + Clone + Send + 'static, T: Send + 'static;

    /// Headline: pin, build this thread's compio runtime, `block_on` the body's future.
    /// The future runs only on its thread, so it need NOT be `Send`.
    pub fn block_on_each<F, Fut, T>(self, body: F) -> std::io::Result<Workers<T>>
    where F: FnOnce(WorkerCtx) -> Fut + Clone + Send + 'static,
          Fut: std::future::Future<Output = T>, T: Send + 'static;
}

/// Caller owns these (ADR 0006): just `JoinHandle`s, no shutdown channel, so
/// neither join nor Drop can wake a compio task on an unwinding thread.
#[must_use]
pub struct Workers<T = ()> { /* Vec<std::thread::JoinHandle<T>> */ }
impl<T> Workers<T> {
    pub fn handles(&self) -> &[std::thread::JoinHandle<T>];
    pub fn into_handles(self) -> Vec<std::thread::JoinHandle<T>>;
    pub fn join(self) -> Vec<std::thread::Result<T>>;
}
```

A server example then collapses to (the bind stays **inside** the body — ADR 0001):

```rust
let pool = Pool::builder().max_size(cap).build(Buffers);
PerCore::new(CoreSet::permitted()?.physical_only().spread())   // or PerCore::new(cpu::cores())
    .name("echo")
    .block_on_each(move |_ctx| { let pool = pool.clone(); async move {
        let local = pool.local();
        let l = TcpListener::from_std(
            bind_reuseport(addr, 1024, None /* Some(_ctx.core.id) once the NIC is tuned */)?)?;
        loop {
            let Ok((s, _)) = l.accept().await else { continue };
            compio::runtime::spawn(serve(local.clone(), s)).detach();
        }
    }})?
    .join();
```

## 3. OPEN DECISIONS (yours)

### 3-A — The flag name *(the lead one)*

One flag gates both layers, so the name must cover both. `affinity` is the umbrella (placement +
pinning) but lightly overloads the existing ungated [`cpu::affinity_mask`](../src/cpu.rs#L36).
`topology` names layer (a) precisely but undersells the harness, which works fine on the raw
`cpu::cores()` with no topology at all. Either must stay distinct from the reuseport plan's
`cpu-steering`.

> **Recommendation:** `affinity`. Decide first — it threads through `Cargo.toml`, every `#[cfg]`,
> CI, the ADR and the docs.

### 3-B — Topology richness: full physical/SMT/NUMA/package, or physical-core dedup only?

The sysfs reader is the riskiest, highest-maintenance code here (the adversarial verdict on it was
**PARTIAL**). Real edge cases: offline/hot-plugged CPUs (their `topology/` dir vanishes), non-NUMA
hosts (no `nodeN`), big.LITTLE heterogeneous cores, and chiplet CPUs where same-package cores do
**not** share L3 (sysfs exposes no L3/CCX island, so `spread`/`pack` can only key on
package→NUMA→core). A wrong read mis-pins silently rather than failing loudly.

> **Recommendation:** ship `physical_only` + `spread` + `take` + `on_numa_node` / `on_package`, but
> treat the NUMA/pack paths as the first thing to cut if parsing proves flaky, and **validate the
> reader in the Lima VM before merge** the way [ADR 0005](decisions/0005-incoming-cpu-opt-in.md)
> validated the hint. The five corrections in §4 Phase 1 are **mandatory**, not optional.

### 3-C — Optional dependency, or pure sysfs?

Pure `std::fs` parsing of `/sys` adds zero crates and keeps `#![forbid(unsafe_code)]` intact
(verification **confirmed** it is root-free, FFI-free, unsafe-free). An `hwloc`/`hwlocality` dep
would give richer, battle-tested topology (including cache islands) but is a heavy C-library build
dependency that pulls FFI — and would force exactly the `forbid`→`deny` relaxation the
[reuseport plan §3-B](reuseport-feature-plan.md#L88) needs for its cBPF `setsockopt`.

> **Recommendation:** pure sysfs. Zero deps, zero `unsafe`, same story as `metrics` — and the clean
> contrast with the reuseport feature, which cannot stay `unsafe`-free. Reconsider `hwloc` only if
> the hand-rolled reader proves unmaintainable on exotic hardware, and if so gate it so the default
> build stays provably safe.

### 3-D — Harness ownership / shutdown model (ADR 0006)

Options: **(a)** return `Workers<T>` holding plain owned `JoinHandle`s, no shutdown channel; **(b)**
a scope guard that joins on drop; **(c)** a join/shutdown handle with a cancellation signal.
[ADR 0006](decisions/0006-shutdown-and-the-unwinding-thread.md) is decisive: dropping anything that
synchronously wakes a `compio` task on an unwinding thread trips compio-executor's `panic_guard!` →
`std::process::abort()`. Options (b)/(c) re-introduce exactly that hazard and would have to copy the
panicking-thread detour.

> **Recommendation:** **(a)** — owned `JoinHandle`s, `#[must_use]`, `join()` returning per-worker
> `thread::Result<T>`. It avoids the abort *by construction* and matches the examples (accept loops
> loop forever and just join). Graceful shutdown stays the caller's job; the ADR 0009 doc should
> cross-link 0006 so a caller who later adds a `flume` shutdown signal knows to apply the detour.

### 3-E — Ring flags: bare runtime + escape hatch, or a bespoke config?

Because `compio` owns real ring knobs (`ProactorBuilder::{single_issuer, defer_taskrun, coop_taskrun,
sqpoll_cpu}` via `RuntimeBuilder::with_proactor`), the harness *could* surface them. But a bespoke
`ProactorConfig` with validation (e.g. reject `defer_taskrun` without `single_issuer`) duplicates
compio's API and has to track its kernel-version semantics.

> **Recommendation:** `block_on_each` builds the **bare default** runtime (exactly like today's
> examples); `spawn_each` is the escape hatch for a fully custom runtime; and an optional
> `PerCore::proactor(compio::driver::ProactorBuilder)` is a **thin passthrough** cloned per worker
> into `with_proactor`. Do not reinvent compio's config. Decide whether `.proactor()` ships in v1 or
> is deferred.

### 3-F — ADR number, and sequencing with the reuseport plan

Both features are plan-only off `experimental`; both add a first-ever `[features]` section. They are
orthogonal: this one stays on ADR 0001's **uncoordinated** side (the harness never binds; the body
binds per-worker), while reuseport's cpu-steering deliberately breaks that with an ordered
`bind_tcp_group`. The next free ADR on disk is `0008`, but the reuseport plan already wrote `0008`
into its text.

> **Recommendation:** land this independently; take ADR **`0009`** (leave `0008` to reuseport) and
> the name `affinity` (not `cpu-steering`). Whichever feature lands first owns any `listener` →
> `reuseport` module-rename doc fixes; the body's `bind_reuseport` call is a re-export, so it moves
> with that rename either way.

## 4. Phases

### Phase 1 — Topology selection in `cpu` *(safe, gated, no new deps)*
- Add `CoreInfo` and `CoreSet` to [`src/cpu.rs`](../src/cpu.rs) after [line 47](../src/cpu.rs#L47),
  all `#[cfg(feature="affinity")]` + `#[cfg_attr(docsrs, doc(cfg(feature="affinity")))]`. The raw
  path is left untouched and ungated.
- Write the reader as `#[cfg(target_os="linux")]` with a `#[cfg(not(target_os="linux"))]` sibling of
  the **same signature** (each permitted core a singleton) — the pattern
  [`listener::set_incoming_cpu`](../src/listener.rs#L38) already uses, so it `cargo check`s on macOS
  (ADR 0007).
- **The five mandatory corrections** (from the adversarial PARTIAL verdict): (1) `permitted()` seeds
  from [`cpu::cores()`](../src/cpu.rs#L11) and **intersects** it with sysfs (the mask is the only
  cpuset-aware source — note it reflects the cpuset but *not* a CFS `cpu.max` quota); (2) **skip
  offline CPUs** (their `topology/` dir is absent — honour `cpu/online`); (3) group physical cores by
  `core_cpus_list` membership / the `(physical_package_id, core_id)` tuple, **never `core_id` alone**
  (it is per-package); (4) treat every file as optionally-absent/empty with a graceful
  single-node/no-SMT fallback; (5) prefer `core_cpus_list`, fall back to the deprecated
  `thread_siblings_list`.
- `from_infos()` makes the dedup/spread/pack logic unit-testable without `/sys`.
- Gated re-export `pub use cpu::{CoreInfo, CoreSet}` after [lib.rs:119](../src/lib.rs#L119).

### Phase 2 — The harness module `src/workers.rs` *(gated)*
- New file, whole module `#[cfg(feature="affinity")]`: `PerCore` (`new`, `name`, optional
  `proactor`), `WorkerCtx`, `Workers<T>`, `spawn_each` (primitive), `block_on_each` (headline).
- The harness owns only the byte-identical skeleton: empty-cores guard returning
  `io::Error(Unsupported, "no cores to run on")` (matches [echo.rs:65-67](../examples/echo.rs#L65)),
  `thread::Builder::new().name("{prefix}/{i}")`, `cpu::pin_current` as the first line on each thread,
  handle collection, join, spawn-error `?`.
- **Pin via `core_affinity` only** — never also `compio`'s `thread_affinity` (double-pin).
- **Honour ADR 0006 by construction:** `Workers` holds only `JoinHandle`s and no shutdown channel, so
  neither `join` nor `Drop` wakes a compio task; mark it `#[must_use]`.
- `#[cfg(feature="affinity")] pub mod workers;` after [lib.rs:113](../src/lib.rs#L113); re-export
  `pub use workers::{PerCore, Workers, WorkerCtx}`. Add a "core affinity" bullet to the production doc
  block ([lib.rs:31](../src/lib.rs#L31)) mirroring the metrics bullet; refresh the helper prose at
  [lib.rs:90-92](../src/lib.rs#L90).

### Phase 3 — Cargo feature + wiring
- Add the **first** `[features]` section to [`Cargo.toml`](../Cargo.toml) after
  [line 48](../Cargo.toml#L48): `affinity = []`, with a one-line comment like the metrics one.
- Add explicit `[[example]]` tables with `required-features = ["affinity"]` for the three examples
  rewritten in Phase 4 (cargo auto-discovers examples today, so the tables are new). This keeps the
  default `cargo build --all-targets` green by **skipping** them — no duplicate fallback `main`.
- Confirm `#![forbid(unsafe_code)]` ([lib.rs:101](../src/lib.rs#L101)) is **not** modified (the
  contrast with reuseport §3-B) and that `feature(doc_cfg)` (lib.rs:100) + `all-features` docs.rs
  config ([Cargo.toml:21](../Cargo.toml#L21)) already carry the badges.

### Phase 4 — Rewrite echo / file_server / tcp_pool onto the harness
- Collapse each `main` + `worker` onto `PerCore::new(…).name(..).block_on_each(move |_ctx| { let pool
  = pool.clone(); async move { /* bind_reuseport + from_std + accept loop */ } })?.join()`. The empty
  guard, spawn loop ([echo.rs:73-80](../examples/echo.rs#L73)), pin ([echo.rs:90](../examples/echo.rs#L90)),
  runtime build ([echo.rs:92](../examples/echo.rs#L92)) and join all move into the harness.
- **Stays example-specific:** the `Pool::builder().max_size(cap).build(Manager)` line, the startup
  `println!`, the `serve` closure, and `file_server`'s per-accept
  [`set_nodelay(true)`](../examples/file_server.rs#L116) (inside the loop).
- Keep `bind_reuseport(addr, 1024, None)` **inside** the closure (ADR 0001 uncoordinated bind;
  `incoming_cpu` stays `None` per ADR 0005). These three now compile only under `--features affinity`
  via their `[[example]]` tables.

### Phase 5 — ADR + docs *(current-model content only)*
- `docs/decisions/0009-core-affinity-harness.md`: Constraint (the loop every example repeats) /
  Decision (opt-in, gated harness that spawns+pins+builds but leaves the uncoordinated bind to the
  body) / Rejected (a mandatory runtime; folding the bind in; `compio` `thread_affinity`
  double-pinning; an `hwloc` dep) / Costs (one feature, a sysfs reader to maintain). **Take 0009, not
  0008** (§3-F) and say why. Add the row to [decisions/README.md](decisions/README.md) and cross-link
  0001 / 0005 / 0006 / 0007.
- README: the "One ring per core" snippet ([193-226](../README.md#L193)) shows `PerCore`; extend the
  `cpu` helper bullet ([335](../README.md#L335)); adjust the overview ([15](../README.md#L15)) to
  name the opt-in harness.
- `operations.md`: a short worker-placement note next to the `SO_INCOMING_CPU` section
  ([75](operations.md#L75)) — **do not** inherit the removed `Workers::AllCores` / `Server::stats()`
  rot ([11](operations.md#L11)). `testing.md`: a `tests/affinity.rs` row — **do not** inherit the
  nonexistent `tests/{server,handoff,config,shutdown}.rs` rows ([28-31](testing.md#L28)).

### Phase 6 — Tests + CI + verify both configurations
- `tests/affinity.rs`, whole file `#[cfg(feature="affinity")]`: `CoreSet::permitted()` is non-empty
  and a subset of `cpu::cores()` ids; `physical_only().len() <= len()`; `from_infos(..)` dedup +
  `spread`/`pack` ordering on **synthetic** topologies (property-style, since they are heuristics);
  `PerCore::new(empty)` → `Unsupported`; `PerCore::new([one core]).block_on_each(|_| async {..})`
  spawns/runs/joins on `127.0.0.1:0`. Keep pinning best-effort and `incoming_cpu` off with the ADR
  0005 comment, matching [tests/pool.rs](../tests/pool.rs) conventions.
- `.github/workflows/ci.yml`: add the `--features affinity` path to clippy
  ([37](../.github/workflows/ci.yml#L37)) and to the test job's build/test/doctest block
  ([56-63](../.github/workflows/ci.yml#L56)); keep the default jobs as the off path; the Docs job
  already builds `all-features`.
- Green **both** ways, per the project's Lima-VM loop:
  ```
  cargo clippy --all-targets --locked -- -D warnings            # off (default)
  cargo test --all-targets --locked && cargo test --doc --locked
  cargo clippy --all-targets --features affinity --locked -- -D warnings
  cargo test  --all-targets --features affinity --locked
  cargo doc --no-deps --all-features --locked
  ```

## 5. Site checklist

| site | where | phase | action |
|---|---|---|---|
| `CoreInfo` / `CoreSet` + sysfs reader | [cpu.rs:47](../src/cpu.rs#L47) | 1 | add gated selection layer; the five mandatory corrections |
| raw `cores`/`pin_current`/`affinity_mask` | [cpu.rs:11](../src/cpu.rs#L11)/[22](../src/cpu.rs#L22)/[36](../src/cpu.rs#L36) | — | **unchanged, ungated** |
| `PerCore` / `WorkerCtx` / `Workers` | `src/workers.rs` (new) | 2 | whole-module gated harness; pin via core_affinity only; owned handles |
| module decl + re-exports | [lib.rs:113](../src/lib.rs#L113)/[119](../src/lib.rs#L119) | 1-2 | gated `pub mod workers` + `CoreInfo`/`CoreSet`/`PerCore`/… |
| production + helper prose | [lib.rs:31](../src/lib.rs#L31)/[90-92](../src/lib.rs#L90) | 2 | add the core-affinity bullet; refresh helper prose |
| `#![forbid(unsafe_code)]` | [lib.rs:101](../src/lib.rs#L101) | 3 | **do not touch** (contrast reuseport §3-B) |
| first `[features]` + `[[example]]` | [Cargo.toml:48](../Cargo.toml#L48) | 3/4 | `affinity = []` (no deps); `required-features` on the three examples |
| echo / file_server / tcp_pool | [echo.rs:64](../examples/echo.rs#L64), [file_server.rs:75](../examples/file_server.rs#L75), [tcp_pool.rs:102](../examples/tcp_pool.rs#L102) | 4 | collapse main+worker onto `PerCore`; bind stays in the body |
| udp_echo / unix_echo / wal | examples | 4 | **leave hand-rolled** — keep the raw path demonstrated by default |
| new ADR + index | [decisions/](decisions/) | 5 | `0009` + table row + 0001/0005/0006/0007 backlinks |
| README snippet + bullet | [README.md:193](../README.md#L193)/[335](../README.md#L335) | 5 | show `PerCore`; extend the `cpu` bullet |
| operations / testing | [operations.md:75](operations.md#L75), [testing.md:27](testing.md#L27) | 5 | add current-model content; do not inherit the `Server` rot |
| CI feature path | [ci.yml:37](../.github/workflows/ci.yml#L37)/[56-63](../.github/workflows/ci.yml#L56) | 6 | `--features affinity` on clippy/build/test/doc |
| `tests/affinity.rs` | `tests/` (new) | 6 | gated unit + one-core round-trip |

## 6. Risks

* **Sysfs reader correctness (adversarial PARTIAL).** Offline/hot-plugged CPUs lose their `topology/`
  dir; non-NUMA hosts have no `nodeN`; containers show host-wide topology but a restricted affinity
  mask; VMs flatten topology (package 0, no SMT). Mitigate with the five corrections in Phase 1, unit
  tests via `from_infos()`, and a Lima-VM validation before merge. A wrong read mis-pins silently —
  this is the feature's central hazard.
* **`spread`/`pack` are heuristics.** No single right answer across sockets/NUMA/SMT, and sysfs
  exposes no L3/CCX island, so ordering can be suboptimal on chiplet CPUs. Document the limitation;
  cover ordering with property tests on synthetic topologies, not just examples.
* **Default-build breakage.** An example that calls the gated harness fails `cargo build
  --all-targets` with the feature **off** unless it is `required-features`-gated. This is the exact
  class of bug the metrics CI was added to catch; the Phase 6 off+on matrix is the guardrail.
* **Do not claim compio can't pin.** It ships `RuntimeBuilder::thread_affinity` and
  `ProactorBuilder::sqpoll_cpu`. The harness pins via `core_affinity` by choice; it must **not** also
  set `thread_affinity`, or workers are double-pinned.
* **ADR 0006 abort returns with any shutdown channel.** v1 keeps `Workers` to owned `JoinHandle`s
  with no wake-on-drop. A future graceful-shutdown addition must copy the panicking-thread detour
  (check `thread::panicking()`, drop the waker-triggering object from a spawned-and-joined helper).
* **ADR number collision.** `0008` is the next free number on disk but is reserved on paper by the
  reuseport plan — take `0009` and state why.
* **Doc rot is not this feature's job.** `operations.md` / `testing.md` still describe the removed
  `Server`; add only current-model content and do not attempt the wider cleanup (same stance as the
  reuseport plan).
