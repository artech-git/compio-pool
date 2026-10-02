//! Stopping: new connections are refused, in-flight ones get the drain window,
//! and `join` returns within it.

mod common;

use std::{
    io::{Read, Write},
    time::{Duration, Instant},
};

use common::{Echo, builder, connect, eventually};

#[test]
fn join_returns_promptly_with_an_idle_connection_open() {
    let server = builder(Echo::default(), 2, 4)
        .drain_timeout(Duration::from_millis(300))
        .start()
        .expect("start");
    let addr = server.local_addr();

    // A connected client that never sends: its handler sits in `read`.
    let mut idle = connect(addr);
    assert!(eventually(Duration::from_secs(5), || server
        .stats()
        .totals
        .active
        == 1));

    let t = Instant::now();
    server.join().expect("no panic");
    let took = t.elapsed();
    assert!(
        took < Duration::from_secs(3),
        "join must give up on the idle connection after drain_timeout, took {took:?}"
    );

    // The worker's runtime is gone, and with it the socket.
    let mut buf = [0u8; 1];
    let _ = idle.write_all(b"x");
    let eof = matches!(idle.read(&mut buf), Ok(0) | Err(_));
    assert!(eof, "connection should be closed by the server");

    // And the port is free again: nothing is listening.
    let refused = std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(500));
    assert!(refused.is_err(), "no listener should remain on {addr}");
}

#[test]
fn drop_stops_the_workers_too() {
    let addr = {
        let server = builder(Echo::default(), 1, 1).start().expect("start");
        server.local_addr()
    };
    let refused = std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(500));
    assert!(
        refused.is_err(),
        "dropping the server must close its listeners"
    );
}

#[test]
fn shutdown_lets_an_active_exchange_finish() {
    let server = builder(Echo::default(), 1, 2)
        .drain_timeout(Duration::from_secs(2))
        .start()
        .expect("start");
    let addr = server.local_addr();
    let mut client = connect(addr);
    assert!(eventually(Duration::from_secs(5), || server
        .stats()
        .totals
        .active
        == 1));

    server.shutdown();
    // The accept loop is gone but the handler is still in `read`; a request
    // during the drain window is still answered.
    client.write_all(b"last words").unwrap();
    let mut back = [0u8; 10];
    client.read_exact(&mut back).expect("served during drain");
    assert_eq!(&back, b"last words");
    drop(client);
    server.join().expect("no panic");
}

/// Regression test for an abort, not a panic: compio's executor aborts the
/// process when one of its task wakers runs on a thread that is already
/// unwinding, and dropping the shutdown sender wakes worker tasks from the
/// dropping thread. `Server::shutdown` must not do that directly while
/// panicking. If it does, this whole test binary dies with SIGABRT instead of
/// this one test failing as expected.
#[test]
#[should_panic(expected = "deliberate")]
fn dropping_a_server_while_unwinding_does_not_abort() {
    let server = builder(Echo::default(), 2, 1).start().expect("start");
    let _held = connect(server.local_addr());
    assert!(eventually(Duration::from_secs(5), || server
        .stats()
        .totals
        .active
        == 1));
    panic!("deliberate");
}
