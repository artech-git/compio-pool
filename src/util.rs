//! A two-way race, so a loop blocked in `accept` or `recv` can also notice
//! shutdown without pulling in a `select!` macro.

use std::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};

pub(crate) enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Resolve with whichever of `a` and `b` finishes first; the other is dropped,
/// which for a compio operation means it is cancelled.
pub(crate) async fn race<A: Future, B: Future>(a: A, b: B) -> Either<A::Output, B::Output> {
    let mut a = pin!(a);
    let mut b = pin!(b);
    poll_fn(|cx| {
        if let Poll::Ready(x) = a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(x));
        }
        if let Poll::Ready(y) = b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(y));
        }
        Poll::Pending
    })
    .await
}
