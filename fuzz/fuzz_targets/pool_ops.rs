#![no_main]
//! Coverage-guided fuzzing of the pool's operation sequences.
//!
//! libFuzzer supplies the byte string, [`Op::decode_all`] turns it into a
//! workload, and `common::run_ops` re-checks every invariant after every step —
//! conservation, the `max_size` cap, counter monotonicity, no connection handed
//! out twice, no retired connection handed out again. A violation is a panic,
//! which libFuzzer minimises and saves as an artifact.
//!
//! The oracle is shared with `tests/fuzz.rs` on purpose: the seeded suite there
//! runs in CI, and this explores the same properties far deeper.
//!
//! # Why state is reused between inputs
//!
//! A thread's shard is never removed from the `SHARDS` thread-local, so a fresh
//! pool per input would grow the harness without bound over a long campaign. A
//! small fixed set of pools keeps memory flat instead, at the cost of carrying
//! pool state across inputs — which is why a reproducer is the whole corpus
//! prefix, not a single file.

use std::{cell::RefCell, time::Duration};

use libfuzzer_sys::fuzz_target;

#[path = "../../tests/common/mod.rs"]
mod common;

use common::{LocalManager, Op, Tally, cfg, run_ops};
use compio_pool::Pool;

/// One pool per `max_size` from 1 to 4. Small shards are where the interesting
/// cases live: contention, waiting, and the cap being hit.
const POOLS: usize = 4;

struct Harness {
    runtime: compio::runtime::Runtime,
    pools: Vec<(Pool<LocalManager>, Tally)>,
}

impl Harness {
    fn new() -> Self {
        let pools = (1..=POOLS)
            .map(|max_size| {
                let pool = Pool::new(
                    LocalManager::new(),
                    cfg()
                        .max_size(max_size)
                        // Short, so a saturated shard yields instead of stalling
                        // the worker. `min_idle` stays 0 so no reaper is spawned
                        // and the harness has no background timers.
                        .acquire_timeout(Duration::from_millis(2)),
                );
                (pool, Tally::default())
            })
            .collect();
        Self {
            runtime: compio::runtime::Runtime::new().expect("a fuzz worker needs a runtime"),
            pools,
        }
    }
}

thread_local! {
    static HARNESS: RefCell<Harness> = RefCell::new(Harness::new());
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, rest)) = data.split_first() else {
        return;
    };
    // `Op::decode` never emits `Close`, which is what makes reusing the pools
    // across inputs viable: a single close would retire one for the rest of the
    // campaign. The deterministic suite covers closing.
    let ops = Op::decode_all(rest);
    if ops.is_empty() {
        return;
    }

    HARNESS.with(|harness| {
        let mut harness = harness.borrow_mut();
        let Harness { runtime, pools } = &mut *harness;
        let which = selector as usize % POOLS;
        let max_size = which + 1;
        let (pool, tally) = &mut pools[which];
        runtime.block_on(run_ops(pool, max_size, &ops, "fuzz", tally));
    });
});
