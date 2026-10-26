//! Tests for the optional `affinity` feature. The whole file compiles to nothing
//! without it, so the default build is unaffected.
//!
//! The topology logic (dedup, ordering, filters) is exercised on *synthetic*
//! `CoreInfo` rows via `CoreSet::from_infos`, so it is deterministic and does not
//! depend on the test host's `/sys`. The harness tests use the real affinity mask
//! but keep pinning best-effort (the suite may run under a restricted mask, so
//! `WorkerCtx::pinned` is not asserted), matching `tests/pool.rs`' conventions.
#![cfg(feature = "affinity")]

use compio_pool::{
    PerCore,
    cpu::{self, CoreInfo, CoreSet},
};

fn info(id: usize, core: usize, package: usize, numa_node: usize) -> CoreInfo {
    CoreInfo {
        id,
        core,
        package,
        numa_node,
    }
}

fn ids(set: CoreSet) -> Vec<usize> {
    set.core_ids().into_iter().map(|c| c.id).collect()
}

#[test]
fn permitted_is_a_subset_of_the_affinity_mask() {
    let mask: std::collections::HashSet<usize> = cpu::cores().into_iter().map(|c| c.id).collect();
    assert!(
        !mask.is_empty(),
        "the test host must expose at least one core"
    );

    let set = CoreSet::permitted();
    assert_eq!(
        set.len(),
        mask.len(),
        "permitted() must match the mask size"
    );
    for core in set.infos() {
        assert!(
            mask.contains(&core.id),
            "permitted() named cpu {} outside the affinity mask",
            core.id
        );
    }
}

#[test]
fn physical_only_drops_smt_siblings() {
    // Two physical cores on one package, each with two SMT threads.
    let set = CoreSet::from_infos([
        info(0, 0, 0, 0),
        info(1, 0, 0, 0), // sibling of cpu0
        info(2, 1, 0, 0),
        info(3, 1, 0, 0), // sibling of cpu2
    ]);
    assert_eq!(set.len(), 4);
    assert_eq!(ids(set.physical_only()), vec![0, 2]);
}

#[test]
fn physical_only_keeps_distinct_cores_across_packages() {
    // The same core_id on a different package is a DIFFERENT physical core, so
    // grouping on core_id alone would be a bug.
    let set = CoreSet::from_infos([info(0, 0, 0, 0), info(1, 0, 1, 1)]);
    assert_eq!(set.physical_only().len(), 2);
}

#[test]
fn spread_round_robins_packages_while_pack_groups_them() {
    let infos = [
        info(0, 0, 0, 0),
        info(1, 1, 0, 0),
        info(2, 0, 1, 1),
        info(3, 1, 1, 1),
    ];
    // spread alternates packages: pkg0/core0, pkg1/core0, pkg0/core1, pkg1/core1.
    assert_eq!(ids(CoreSet::from_infos(infos).spread()), vec![0, 2, 1, 3]);
    // pack keeps each package contiguous.
    assert_eq!(ids(CoreSet::from_infos(infos).pack()), vec![0, 1, 2, 3]);
}

#[test]
fn filters_and_take_narrow_the_set() {
    let infos = [
        info(0, 0, 0, 0),
        info(1, 1, 0, 0),
        info(2, 0, 1, 1),
        info(3, 1, 1, 1),
    ];
    assert_eq!(CoreSet::from_infos(infos).on_package(1).len(), 2);
    assert_eq!(CoreSet::from_infos(infos).on_numa_node(0).len(), 2);
    assert_eq!(
        CoreSet::from_infos(infos).filter(|c| c.id % 2 == 0).len(),
        2
    );
    assert_eq!(CoreSet::from_infos(infos).take(2).len(), 2);
    assert_eq!(CoreSet::from_infos(infos).take(99).len(), 4, "take clamps");
}

#[test]
fn an_empty_core_set_is_rejected() {
    let err = PerCore::new(Vec::<cpu::CoreId>::new())
        .spawn_each(|_| ())
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
}

#[test]
fn spawn_each_pins_and_runs_every_worker() {
    let cores: Vec<_> = cpu::cores().into_iter().take(2).collect();
    let n = cores.len();
    assert!(n >= 1, "the test host must expose at least one core");

    let workers = PerCore::new(cores).spawn_each(|ctx| ctx.index).unwrap();
    assert_eq!(workers.len(), n);

    let mut got: Vec<usize> = workers.join().into_iter().map(Result::unwrap).collect();
    got.sort_unstable();
    assert_eq!(got, (0..n).collect::<Vec<_>>());
}

#[test]
fn block_on_each_builds_a_runtime_and_runs_the_future() {
    // One worker: a pinned thread builds its compio runtime and drives a trivial
    // future to completion. Exercises the headline path without network flakiness.
    let one: Vec<_> = cpu::cores().into_iter().take(1).collect();
    let workers = PerCore::new(one)
        .block_on_each(|ctx| async move { ctx.core.id })
        .unwrap();

    let results = workers.join();
    assert_eq!(results.len(), 1);
    results[0]
        .as_ref()
        .expect("the worker built its runtime and returned");
}
