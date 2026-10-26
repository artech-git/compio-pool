# Plan: make `metrics` a clean, zero-cost feature

Status: **implemented.** The pool-side code (Phases 1–3) landed in PR #15 on `experimental`;
this branch adds the Phase 4 collateral — CI that exercises both configurations, the README /
rustdoc feature notes, and gating for the two examples. The open design decision (§3) was
resolved as **(C)**; see there.

> The line numbers below point at the pre-merge WIP and are kept as the plan was written. The
> merged code reorganized the file (the slot counter became `Inner::slots: Cell<u32>`, metric
> writes moved behind paired `note_*` helpers), but the shape matches the phases.

## 1. Context

The `[features] metrics = []` section is **new in the current uncommitted diff** — before
this, the metrics code was unconditional. The refactor to gate it is **half-finished**:
`src/pool.rs` has ~30 `.metrics` touch sites but only ~27 `#[cfg]` gates, so the crate
**does not compile with the feature off**:

```
$ cargo check --no-default-features
error[E0609]: no field `metrics` on type `Arc<Shared<M>>`   # src/pool.rs:136 (Pool::state)
...  # 18 distinct failing sites in total
```

Note `Pool::state` currently uses `if cfg!(feature = "metrics") { … }`. That is **not**
conditional compilation — `cfg!` expands to a `bool`; both arms are still type-checked, so
the dead arm references the `#[cfg]`-gated `metrics` field that doesn't exist when off. The
attribute `#[cfg(...)]` is the real thing (strips tokens before type-check).

### The real blocker: `Metrics` holds load-bearing state, not just reporting

`Inner` has no slot counter of its own (fields: `shared`, `idle`, `metrics`, `waiters`).
The "live gauges" inside `Metrics` are doing double duty:

| gauge | role | if it compiles out… |
|-------|------|---------------------|
| `connections` | **the only slot counter**; `try_reserve` ([pool.rs:689](../src/pool.rs#L689)) checks it vs `max_size` | `connections()` returns `0` → pool **ignores `max_size`** (correctness bug, currently masked by the compile error) |
| `idle` | read by `ready_now` ([pool.rs:724](../src/pool.rs#L724)) | logic loses readiness signal (but it's redundant with the idle `Vec`) |
| `waiters` | cross-thread mirror of `Waiters.entries` len | pure reporting; redundant with `entries` |

So "make metrics a feature" is **not** cfg-sprinkling. Step one is separating pool
bookkeeping (always needed) from reporting (optional). `idle`/`waiters` gauges are
redundant mirrors of structures that already exist (`idle: RefCell<Vec<Conn>>`,
`Waiters.entries`); only `connections` is genuinely load-bearing with no backing structure.

### Invariant the feature depends on

> No core-logic path may read from `metrics`. Metrics is a pure observer.
> `max_size` enforcement, readiness, and waiter wakeups must work identically on and off.

## 2. Scope

One file does almost all of it: `src/pool.rs`. Plus `Cargo.toml`, `.github/workflows/ci.yml`,
`tests/pool.rs`, and two examples. No other `src/` file references metrics (lib.rs only
names `State`/`Statistics` in docs + the re-export at [lib.rs:119](../src/lib.rs#L119)).

## 3. DECISION (resolved: **C**) — public introspection API when metrics is OFF

**Resolved as (C): the snapshot API is always present and reports zeroes without the feature.**
`State` / `Statistics` and all four `state()` / `statistics()` methods compile in both
configurations — their bodies are `#[cfg]`-split, returning the live view with the feature on and
`Default::default()` off — so downstream code that calls them builds either way. This is the most
API-stable option; PR #15 shipped it and unified the previously-inconsistent gating onto it. The
"lying zeroes" caveat is handled by documenting the zero behavior on each method, in the module
docs, and in the README. Because no *public* item is feature-gated under (C), no `doc(cfg)`
annotations were needed.

For the record, the original options and the state at the time were:

Today this is inconsistent: `LocalPool::state/statistics` are already `#[cfg]`-gated off
([pool.rs:444](../src/pool.rs#L444)/[451](../src/pool.rs#L451)), but `Pool::state` uses the
broken `cfg!` and `Pool::statistics` is ungated. Pick one policy for all four + the
`State`/`Statistics` types:

- **(A) Gate off entirely** *(recommended)* — `state()`/`statistics()`/`State`/`Statistics`
  exist only with `--features metrics`. Consistent with the already-gated `LocalPool`
  methods and how `tests/pool.rs` is written; no misleading zero values. Downstream must
  enable the feature to introspect.
- **(B) Keep live `State` always on** — `state()` always works (cheap, derivable from the
  slot counter + idle/waiter lens; useful for health checks without metrics); only
  `statistics()` (cumulative counters) needs the feature. Costs a bit more wiring: the slot
  counter must be cross-thread-readable (atomic in a small always-present block).
- **(C) Always present, zero when off** — both methods always exist, return zeros/empty
  without the feature. Most API-stable, but can hand back lying zeroes and contradicts the
  existing `LocalPool` gating.

The phases below were written assuming (A); the shipped code took **(C)**, which changes only
Phase 3 (the snapshot methods stay present with a `#[cfg]`-split body) and the test/example
gating (the examples print live counts only with the feature on, falling back otherwise).

## 4. Phases

### Phase 1 — Separate load-bearing state from reporting *(prerequisite)*
- Add `slots: Cell<u32>` to `Inner` (per-thread, `!Send`, so no atomic).
- Reroute logic off `metrics`:
  - `Inner::connections()` ([pool.rs:360-368](../src/pool.rs#L360-L368)) → `self.slots.get()`;
    delete the paired `#[cfg(not)]` fallback (no longer needed).
  - `try_reserve` ([689-691](../src/pool.rs#L689-L691)) → ceiling check + `+1` on `slots`.
  - `release_slot` ([377](../src/pool.rs#L377)) and `reap` ([519-520](../src/pool.rs#L519-L520)) → `-n` on `slots`.
  - `ready_now` ([724-725](../src/pool.rs#L724-L725)) → idle via `!self.idle.borrow().is_empty()`, connections via `slots.get()`.
- After this, **no core logic references `metrics`**.

### Phase 2 — Reduce `Metrics` to pure reporting, behind cfg-free helpers
- `Metrics` (gated, [253-316](../src/pool.rs#L253-L316)) keeps cumulative counters + the
  live-gauge atomics, but the atomics are now **only** a cross-thread mirror for
  `Pool::state` — written alongside `slots`/the lists, never read by logic.
- Centralize every write behind **paired helper methods** (`#[cfg(feature="metrics")]` real
  body + `#[cfg(not)]` empty body), matching the existing `connections()` idiom, so call
  sites are **cfg-free** and a future metric can't "miss a site" (the bug's root cause).
- Sites to route through helpers: `connections_created` ([482](../src/pool.rs#L482)/[662](../src/pool.rs#L662)),
  `connections_closed_broken` ([639](../src/pool.rs#L639)), `get_timed_out` ([590](../src/pool.rs#L590)),
  `record_acquire` ([677-683](../src/pool.rs#L677-L683)), `note_expired`
  ([511](../src/pool.rs#L511)/[710](../src/pool.rs#L710)/~828 drain path), the gauge mirrors
  (`inc/dec_conn`, `inc/dec_idle`, `waiters.store` in `Waiters::register`/`unregister`),
  and the `Inner::drop` zeroing ([404-406](../src/pool.rs#L404-L406)).

### Phase 3 — Gate the public snapshot API consistently *(assumes decision A)*
- Replace the `cfg!` in `Pool::state` ([132](../src/pool.rs#L132)) with a plain
  `#[cfg(feature="metrics")]` method (body unchanged). Same for `Pool::statistics` ([155](../src/pool.rs#L155)).
- Add `#[cfg_attr(docsrs, doc(cfg(feature = "metrics")))]` to every gated public item;
  `feature(doc_cfg)` is already wired at [lib.rs:100](../src/lib.rs#L100) and docs.rs builds `all-features`.
- `Debug for LocalPool` ([737-746](../src/pool.rs#L737-L746)) must **not** call the gated
  `state()` — rewrite cfg-free using `slots`/idle-len (gauges only when on) so `Debug` works in both builds.

### Phase 4 — Collateral (fallout of gating)
- **`tests/pool.rs`**: ~20 assertions call `state()`/`statistics()` expecting nonzero values.
  Move the metrics-asserting tests under `#[cfg(feature = "metrics")]`; leave behavioral
  tests ungated so they run in both builds.
- **`examples/warmup.rs`** (whole point is printing `State`) and the `local.state()` print in
  **`examples/fanout_requests.rs:142`**: gate behind the feature, with a
  `#[cfg(not(feature="metrics"))] fn main()` fallback so `cargo build --all-targets` stays
  green by default.
- **`.github/workflows/ci.yml`**: nothing currently exercises on/off. Add it (the guardrail
  that would have caught this): keep the default jobs as the **off** path, and add
  `cargo build/clippy/test --features metrics` + `cargo test --doc` both ways.
- Keep `metrics` **off by default** (zero-cost default = the crate's ethos). Document
  enabling in the `Cargo.toml` feature comment and README.

### Phase 5 — Verify both configurations
Both must be green:
```
# off (default)
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --doc --locked
# on
cargo clippy --all-targets --features metrics --locked -- -D warnings
cargo test --all-targets --features metrics --locked
cargo test --doc --features metrics --locked
cargo doc --no-deps --all-features --locked
```

## 5. Site checklist (`src/pool.rs`)

| Site | Line(s) | Phase | Action |
|------|---------|-------|--------|
| `Shared.metrics` field + registry | 58-59 | 2 | stays gated (metrics-only) |
| `Inner.metrics` field | 352-354 | 2 | stays gated |
| add `Inner.slots: Cell<u32>` | (new) | 1 | always present |
| `Inner::connections()` paired | 360-368 | 1 | → `slots.get()`, un-pair |
| `release_slot` | 375-379 | 1 | `slots -1` (+ gated mirror) |
| `push_idle`/`pop_idle` | 381-394 | 2 | idle derives from Vec; gated mirror only |
| `Inner::drop` zeroing | 397-408 | 2 | gated mirror reset |
| `LocalPool::new` construction | 429-438 | 1+2 | build with `slots`; gated metrics path |
| `LocalPool::state/statistics` | 444-455 | 3 | already gated; add `doc(cfg)` |
| `try_reserve` | 688-700 | 1 | `slots` ceiling + bump |
| `record_acquire` | 674-685 | 2 | paired helper |
| `pop_fresh_idle` note_expired | 710 | 2 | paired helper |
| `ready_now` | 722-726 | 1 | derive from `slots`/idle Vec |
| `Debug for LocalPool` | 737-746 | 3 | cfg-free, drop `self.state()` |
| `get` created/broken/timeout | 590,639,662 | 2 | paired helpers |
| `reap` dec_idle/dec_conn | 519-520 | 1 | `slots -reaped` |
| drain/`clear` path | ~820-828 | 2 | paired helper |
| `Waiters` + register/unregister | 867-906 | 2 | gated `waiters.store` mirror |
| `Pool::state` (`cfg!`) | 132 | 3 | → `#[cfg]` method |
| `Pool::statistics` (ungated) | 155 | 3 | → `#[cfg]` method |
| `State`/`Statistics` types + re-export | 206-; lib.rs:119 | 3 | gate per decision A |

## 6. Risks
- **Silent `max_size` regression** if Phase 1 is skipped and the slot counter is left tied to
  metrics. Phase 1 must land first; the CI off-build + a `max_size` behavioral test (ungated)
  guard it.
- **Doc-tests** run with default features (off); any `# use`/example calling `state()` must be
  gated or the fence marked `ignore`.
- **MSRV**: `metrics = []` adds no deps, no MSRV impact. `doc(cfg)` stays behind `cfg(docsrs)`.
