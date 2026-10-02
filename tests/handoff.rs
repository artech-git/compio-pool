//! The overflow path: pool full → fd detached → channel → claimed on another
//! ring → still a working stream.
//!
//! Which clients the kernel hashes onto which listener, and therefore which of
//! them are served first, is not under the test's control. So every client
//! speaks from its own thread, and the assertions are about totals.

mod common;

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    sync::atomic::Ordering::Relaxed,
    thread,
    time::Duration,
};

use common::{Echo, builder, connect, eventually, roundtrip};
use compio_pool::OverflowPolicy;

/// Opens `n` connections and holds them all open before any of them speaks,
/// so a server with less total capacity than `n` has to park the surplus.
fn burst(addr: SocketAddr, n: usize) -> Vec<TcpStream> {
    (0..n).map(|_| connect(addr)).collect()
}

/// Every client does one echo roundtrip concurrently and closes. The active
/// ones answer first and free their slots; the parked ones follow.
fn roundtrip_all(clients: Vec<TcpStream>) {
    let handles: Vec<_> = clients
        .into_iter()
        .enumerate()
        .map(|(i, mut client)| {
            thread::spawn(move || {
                let payload = vec![i as u8 + 1; 128];
                assert_eq!(roundtrip(&mut client, &payload), payload, "client {i} echo");
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn surplus_connections_are_handed_off_and_served() {
    // 2 workers × capacity 1 = 2 served at once; 6 clients, all open at once.
    let service = Echo::default();
    let server = builder(service.clone(), 2, 1)
        .handoff_capacity(64)
        .start()
        .expect("start");
    let addr = server.local_addr();

    let clients = burst(addr, 6);
    // The stable state before anyone speaks: one connection in service per
    // worker, the other four parked in the channel. How many of the six were
    // served where they were accepted depends on the kernel's hash — if all
    // six land on one listener, that worker serves one and hands off five, and
    // the other worker's first connection is a claim rather than an accept.
    assert!(eventually(Duration::from_secs(5), || {
        let s = server.stats();
        s.totals.accepted == 6 && s.totals.active == 2 && s.queued == 4
    }));
    let parked = server.stats();
    assert!(
        parked.totals.served_local == 1 || parked.totals.served_local == 2,
        "{parked:?}"
    );
    assert_eq!(parked.totals.handed_off, 6 - parked.totals.served_local);

    roundtrip_all(clients);

    assert!(eventually(Duration::from_secs(5), || {
        server.stats().totals.completed == 6
    }));
    let done = server.stats();
    assert_eq!(
        done.totals.handed_off, parked.totals.handed_off,
        "no handoffs after the burst"
    );
    assert_eq!(
        done.totals.claimed, done.totals.handed_off,
        "every parked connection was claimed"
    );
    assert!(done.totals.claimed >= 4);
    assert_eq!(done.totals.rejected, 0);
    assert_eq!(done.totals.oversubscribed, 0);
    assert_eq!(done.totals.detach_failed, 0);
    assert_eq!(done.totals.attach_failed, 0);
    assert_eq!(done.queued, 0);
    assert_eq!(done.totals.active, 0);
    // The handler saw the routes too: a claimed stream is attached to a new ring
    // and is fully usable there.
    assert_eq!(service.claimed.load(Relaxed) as u64, done.totals.claimed);
    assert_eq!(service.local.load(Relaxed) as u64, done.totals.served_local);
    assert_eq!(service.oversubscribed.load(Relaxed), 0);
}

#[test]
fn full_channel_serves_locally_by_default() {
    // 1 worker, capacity 1, channel of 1: the third simultaneous connection
    // finds both full and is served over capacity.
    let service = Echo::default();
    let server = builder(service.clone(), 1, 1)
        .handoff_capacity(1)
        .start()
        .expect("start");
    let addr = server.local_addr();

    let clients = burst(addr, 3);
    assert!(eventually(Duration::from_secs(5), || {
        server.stats().totals.oversubscribed == 1
    }));
    let s = server.stats();
    assert_eq!(s.totals.accepted, 3);
    assert_eq!(s.totals.handoff_full, 1);
    assert_eq!(
        s.totals.active, 2,
        "the local one plus the oversubscribed one"
    );
    assert_eq!(s.queued, 1);

    roundtrip_all(clients);

    assert!(eventually(Duration::from_secs(5), || {
        server.stats().totals.completed == 3
    }));
    assert_eq!(service.oversubscribed.load(Relaxed), 1);
    assert_eq!(server.stats().totals.rejected, 0);
}

#[test]
fn full_channel_rejects_when_asked() {
    let server = builder(Echo::default(), 1, 1)
        .handoff_capacity(1)
        .overflow(OverflowPolicy::Reject)
        .start()
        .expect("start");
    let addr = server.local_addr();

    let clients = burst(addr, 3);
    assert!(eventually(Duration::from_secs(5), || {
        server.stats().totals.rejected == 1
    }));

    // Exactly one of the three was closed by the server. A write into the
    // rejected socket may still land in the kernel buffer; its read sees EOF or
    // a reset. The queued one is only served once the active one has closed.
    let handles: Vec<_> = clients
        .into_iter()
        .map(|mut client| {
            thread::spawn(move || -> std::io::Result<()> {
                client.write_all(b"ping")?;
                let mut buf = [0u8; 4];
                client.read_exact(&mut buf)?;
                assert_eq!(&buf, b"ping");
                Ok(())
            })
        })
        .collect();
    let closed = handles
        .into_iter()
        .map(|h| h.join().unwrap().is_err())
        .filter(|&failed| failed)
        .count();
    assert_eq!(
        closed, 1,
        "one connection was rejected, the other two served"
    );

    assert!(eventually(Duration::from_secs(5), || {
        server.stats().totals.completed == 2
    }));
}

#[test]
fn handoff_survives_many_rounds() {
    // Churn: far more clients than capacity, several waves, every byte must
    // come back. Exercises bounce (slot lost between wait and claim) too.
    let service = Echo::default();
    let server = builder(service.clone(), 3, 2)
        .handoff_capacity(256)
        .max_hops(8)
        .start()
        .expect("start");
    let addr = server.local_addr();

    let waves = 4;
    let per_wave = 24;
    for wave in 0..waves {
        let handles: Vec<_> = (0..per_wave)
            .map(|i| {
                thread::spawn(move || {
                    let mut c = connect(addr);
                    let payload = vec![(wave * per_wave + i) as u8; 256];
                    assert_eq!(roundtrip(&mut c, &payload), payload);
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
    }

    let total = (waves * per_wave) as u64;
    assert!(eventually(Duration::from_secs(10), || {
        server.stats().totals.completed == total
    }));
    let s = server.stats();
    assert_eq!(s.totals.accepted, total);
    assert_eq!(s.totals.served_local + s.totals.handed_off, total);
    assert_eq!(s.totals.rejected, 0);
    assert_eq!(s.totals.detach_failed + s.totals.attach_failed, 0);
    assert_eq!(s.totals.active, 0);
    assert_eq!(s.queued, 0);
    // Not every wave necessarily overflows, but 24 simultaneous connections
    // against 6 slots must.
    assert!(s.totals.handed_off > 0, "expected overflow: {s:?}");
    assert_eq!(
        s.totals.claimed + s.totals.oversubscribed,
        s.totals.handed_off,
        "everything handed off was either claimed or served over capacity: {s:?}"
    );
}
