//! Start, serve, observe, stop.

mod common;

use std::time::Duration;

use common::{Echo, builder, echo_once, eventually};
use compio_pool::Workers;

#[test]
fn binds_one_port_and_echoes() {
    let service = Echo::default();
    let server = builder(service.clone(), 2, 8).start().expect("start");
    let addr = server.local_addr();
    assert_ne!(addr.port(), 0, "port 0 must be resolved to a real port");
    assert_eq!(server.workers(), 2);
    assert_eq!(server.cores().len(), 2);

    for i in 0..10u8 {
        let payload = vec![i; 64 + i as usize];
        assert_eq!(echo_once(addr, &payload), payload);
    }

    assert!(eventually(Duration::from_secs(5), || {
        server.stats().totals.completed == 10
    }));
    let stats = server.stats();
    assert_eq!(stats.totals.accepted, 10);
    assert_eq!(
        stats.totals.served_local, 10,
        "with room everywhere nothing is handed off"
    );
    assert_eq!(stats.totals.handed_off, 0);
    assert_eq!(stats.totals.active, 0);
    assert_eq!(stats.queued, 0);
    assert_eq!(stats.workers.len(), 2);
    assert_eq!(
        stats
            .workers
            .iter()
            .map(|w| w.counters.accepted)
            .sum::<u64>(),
        10
    );

    server.join().expect("no worker panicked");
}

#[test]
fn every_worker_has_its_own_listener_on_the_same_port() {
    // SO_REUSEPORT: N sockets, one address. The kernel splits connections across
    // them by hash, so over enough connections more than one worker accepts.
    let server = builder(Echo::default(), 4, 8).start().expect("start");
    let addr = server.local_addr();
    for _ in 0..64 {
        echo_once(addr, b"x");
    }
    assert!(eventually(Duration::from_secs(5), || {
        server.stats().totals.completed == 64
    }));
    let busy = server
        .stats()
        .workers
        .iter()
        .filter(|w| w.counters.accepted > 0)
        .count();
    assert!(
        busy >= 2,
        "expected the kernel to spread 64 connections over 4 listeners, got {busy}"
    );
}

#[test]
fn explicit_core_list_is_honoured_in_order() {
    let cores = compio_pool::cpu::cores();
    let pick: Vec<usize> = cores.iter().take(2).map(|c| c.id).collect();
    let server = builder(Echo::default(), 1, 4)
        .workers(Workers::Cores(pick.clone()))
        .start()
        .expect("start");
    let assigned: Vec<usize> = server.cores().iter().map(|c| c.id).collect();
    assert_eq!(assigned, pick);
    let snapshot: Vec<usize> = server.stats().workers.iter().map(|w| w.core).collect();
    assert_eq!(snapshot, pick);
}

#[test]
fn pinning_is_reported() {
    let server = builder(Echo::default(), 1, 4)
        .pin(true)
        .start()
        .expect("start");
    let stats = server.stats();
    assert_eq!(stats.workers.len(), 1);
    // `sched_setaffinity` to a core already in the mask always succeeds on Linux.
    assert!(
        stats.workers[0].pinned,
        "worker should report a successful pin"
    );
}

#[test]
fn prewarm_creates_resources_before_the_first_connection() {
    // Capacity 4, prewarm 10: capped at capacity, and it must not fail.
    let server = builder(Echo::default(), 1, 4)
        .prewarm(10)
        .start()
        .expect("start");
    assert_eq!(echo_once(server.local_addr(), b"warm"), b"warm");
}

#[test]
fn bind_failure_is_returned_not_panicked() {
    // Hold the port with a plain listener (no SO_REUSEPORT), so the worker's
    // reuseport bind is refused.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let err = builder(Echo::default(), 2, 4)
        .bind(taken.local_addr().unwrap())
        .start()
        .expect_err("binding a taken port must fail");
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
}
