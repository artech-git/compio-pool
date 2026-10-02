//! Configuration validation happens before any thread is spawned.

mod common;

use std::io::ErrorKind;

use common::{Echo, builder};
use compio_pool::{Config, UringConfig, Workers};

#[test]
fn zero_capacity_is_rejected() {
    let err = builder(Echo::default(), 1, 0)
        .start()
        .expect_err("capacity 0");
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert!(err.to_string().contains("capacity"));
}

#[test]
fn zero_workers_is_rejected() {
    let err = builder(Echo::default(), 0, 1)
        .start()
        .expect_err("0 workers");
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
}

#[test]
fn empty_core_list_is_rejected() {
    let err = builder(Echo::default(), 1, 1)
        .workers(Workers::Cores(vec![]))
        .start()
        .expect_err("no cores");
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
}

#[test]
fn core_outside_the_affinity_mask_is_rejected() {
    let err = builder(Echo::default(), 1, 1)
        .workers(Workers::Cores(vec![usize::MAX / 2]))
        .start()
        .expect_err("absurd core id");
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
    assert!(err.to_string().contains("affinity"));
}

#[test]
fn zero_handoff_capacity_is_rejected() {
    let err = builder(Echo::default(), 1, 1)
        .handoff_capacity(0)
        .start()
        .expect_err("handoff 0");
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
}

#[test]
fn defer_taskrun_needs_single_issuer() {
    let uring = UringConfig {
        single_issuer: false,
        defer_taskrun: true,
        ..UringConfig::default()
    };
    let err = builder(Echo::default(), 1, 1)
        .uring(uring)
        .start()
        .expect_err("invalid uring combo");
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
}

#[test]
fn validate_matches_start() {
    let mut config = Config::default();
    assert!(config.validate().is_ok());
    config.backlog = 0;
    assert_eq!(
        config.validate().unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
}

#[test]
fn more_workers_than_cores_share_cores() {
    let cores = compio_pool::cpu::cores().len();
    let server = builder(Echo::default(), cores + 1, 1)
        .start()
        .expect("start");
    assert_eq!(server.workers(), cores + 1);
    let first = server.cores()[0];
    let wrapped = server.cores()[cores];
    assert_eq!(
        first.id, wrapped.id,
        "worker N wraps round to the first core"
    );
}

#[test]
fn affinity_mask_matches_smp_affinity_format() {
    assert_eq!(compio_pool::cpu::affinity_mask(0), "00000001");
    assert_eq!(compio_pool::cpu::affinity_mask(31), "80000000");
    assert_eq!(compio_pool::cpu::affinity_mask(32), "00000001,00000000");
    assert_eq!(
        compio_pool::cpu::affinity_mask(64),
        "00000001,00000000,00000000"
    );
}
