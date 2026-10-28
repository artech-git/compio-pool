# Changelog

All notable changes to this crate are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the crate aims to
follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

The entries below carry breaking API changes, so the next published version is
intended to be 0.2.0.

### Added

- `LocalPool::try_get`: a checkout that never waits, returning `Ok(None)` the
  moment the thread is at `max_size` with nothing to reuse — for shedding load
  instead of queueing.
- `PooledConnection::take`: removes a connection from the pool for good, freeing
  its slot and handing the connection to the caller (for a Redis `SUBSCRIBE`, a
  protocol upgrade, and the like).
- `Builder::retry_connection`: an opt-in flag, off by default, that makes `get`
  retry a failed dial (after a short pause) until it succeeds or
  `connection_timeout` elapses, rather than surfacing the first error.
- `LocalPool::is_closed` and `Pool::manager`.
- `AsRef` and `AsMut` for `PooledConnection`, and `Clone` + `Debug` for
  `Builder`.
- `Statistics::connections_closed_manual`, counting connections dropped by
  `clear` or returned to a closed pool.
- `BuildError`, returned by `Builder::build`.

### Changed

- **Breaking:** `Builder::build` now returns `Result<Pool, BuildError>` instead
  of panicking when `max_size` is 0.
- **Breaking:** `State`, `Statistics` and `RunError` are now `#[non_exhaustive]`,
  so later additions do not break downstream code.
- `warm` and `maintain` now refill the idle floor by counting idle connections,
  not total slots, so `min_idle` is restored after a burst of checkouts.
- `warm` now bounds each dial by `connection_timeout`, so a dead backend cannot
  wedge the maintenance loop.
- `LocalPool::state` now reports live open/idle/waiting counts without the
  `metrics` feature.
- `get` skips arming a timeout timer when it can hand back a ready idle
  connection with no validation.

### Fixed

- `clear` on a closed pool now wakes parked `get` callers even when there was
  nothing idle to free, so a drain after `close` is prompt instead of leaving
  them to time out.
- `reap` and `clear` now drop connections after releasing the idle-list borrow,
  so a connection whose `Drop` reaches back into the pool no longer panics.

## [0.1.0]

Initial crates.io release: the bb8-style per-thread `Pool` / `LocalPool`, the
`cpu` and `listener` helpers for the thread-per-core loop, and the optional
`metrics` and `affinity` features.
