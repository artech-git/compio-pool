//! The yardstick compio-pool is measured against.
//!
//! This crate holds no pool of its own. It exists so that the claims in
//! `docs/performance.md` can be checked against the incumbent — `tokio` with
//! `deadpool` — under the same load, against the same server, on the same
//! machine, instead of against numbers quoted from someone else's run.
//!
//! The examples are deliberate ports, not rewrites:
//!
//! | here | in the parent crate |
//! |---|---|
//! | `examples/ncat_steal_bench.rs` | [`examples/ncat_steal_bench.rs`] — the cross-thread question |
//!
//! Nothing here is published; see each example's own module docs for what it
//! measures and how to read it.
//!
//! [`examples/ncat_steal_bench.rs`]: https://github.com/artech-git/compio-pool/blob/experimental/examples/ncat_steal_bench.rs
