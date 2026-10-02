//! Counters. Each worker writes its own set on its own cache line with relaxed
//! atomics, which is as cheap as a plain store on the writer and never
//! contends. Readers sum the sets; the totals are a snapshot taken counter by
//! counter, not a consistent cut.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

/// Per-worker counters, aligned to a cache line of their own so that two
/// workers never write the same line.
#[repr(align(128))]
#[derive(Debug)]
pub(crate) struct WorkerStats {
    index: usize,
    core: usize,
    pinned: AtomicBool,
    accepted: AtomicU64,
    served_local: AtomicU64,
    handed_off: AtomicU64,
    handoff_full: AtomicU64,
    claimed: AtomicU64,
    bounced: AtomicU64,
    oversubscribed: AtomicU64,
    rejected: AtomicU64,
    completed: AtomicU64,
    handler_errors: AtomicU64,
    resource_errors: AtomicU64,
    accept_errors: AtomicU64,
    detach_failed: AtomicU64,
    attach_failed: AtomicU64,
    active: AtomicU64,
}

macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        impl WorkerStats {
            $(
                #[inline]
                pub(crate) fn $name(&self) {
                    self.$name.fetch_add(1, Relaxed);
                }
            )*
        }
    };
}

counters!(
    accepted,
    served_local,
    handed_off,
    handoff_full,
    claimed,
    bounced,
    oversubscribed,
    rejected,
    completed,
    handler_errors,
    resource_errors,
    accept_errors,
    detach_failed,
    attach_failed,
);

impl WorkerStats {
    pub(crate) fn new(index: usize, core: usize) -> Self {
        Self {
            index,
            core,
            pinned: AtomicBool::new(false),
            accepted: AtomicU64::new(0),
            served_local: AtomicU64::new(0),
            handed_off: AtomicU64::new(0),
            handoff_full: AtomicU64::new(0),
            claimed: AtomicU64::new(0),
            bounced: AtomicU64::new(0),
            oversubscribed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            handler_errors: AtomicU64::new(0),
            resource_errors: AtomicU64::new(0),
            accept_errors: AtomicU64::new(0),
            detach_failed: AtomicU64::new(0),
            attach_failed: AtomicU64::new(0),
            active: AtomicU64::new(0),
        }
    }

    pub(crate) fn set_pinned(&self, pinned: bool) {
        self.pinned.store(pinned, Relaxed);
    }

    #[inline]
    pub(crate) fn connection_started(&self) {
        self.active.fetch_add(1, Relaxed);
    }

    #[inline]
    pub(crate) fn connection_ended(&self) {
        self.active.fetch_sub(1, Relaxed);
    }

    pub(crate) fn snapshot(&self) -> WorkerSnapshot {
        WorkerSnapshot {
            index: self.index,
            core: self.core,
            pinned: self.pinned.load(Relaxed),
            counters: Counters {
                accepted: self.accepted.load(Relaxed),
                served_local: self.served_local.load(Relaxed),
                handed_off: self.handed_off.load(Relaxed),
                handoff_full: self.handoff_full.load(Relaxed),
                claimed: self.claimed.load(Relaxed),
                bounced: self.bounced.load(Relaxed),
                oversubscribed: self.oversubscribed.load(Relaxed),
                rejected: self.rejected.load(Relaxed),
                completed: self.completed.load(Relaxed),
                handler_errors: self.handler_errors.load(Relaxed),
                resource_errors: self.resource_errors.load(Relaxed),
                accept_errors: self.accept_errors.load(Relaxed),
                detach_failed: self.detach_failed.load(Relaxed),
                attach_failed: self.attach_failed.load(Relaxed),
                active: self.active.load(Relaxed),
            },
        }
    }
}

/// Event counts for one worker, or summed over all of them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Counters {
    /// Connections this worker's own listener accepted.
    pub accepted: u64,
    /// Of those, served on the same core because the pool had room.
    pub served_local: u64,
    /// Of those, detached and pushed onto the handoff channel.
    pub handed_off: u64,
    /// Times the channel was full at handoff; what happened next is in
    /// `oversubscribed` or `rejected`.
    pub handoff_full: u64,
    /// Connections taken off the channel and attached to this worker's ring.
    pub claimed: u64,
    /// Connections taken off the channel and pushed straight back because the
    /// slot that was free a moment ago went to a local accept.
    pub bounced: u64,
    /// Connections served over `capacity` (policy `ServeLocally`, or hop limit).
    pub oversubscribed: u64,
    /// Connections closed without service (policy `Reject`).
    pub rejected: u64,
    /// Handlers that returned `Ok`.
    pub completed: u64,
    /// Handlers that returned `Err`.
    pub handler_errors: u64,
    /// `Resource::create` failures. The connection is closed.
    pub resource_errors: u64,
    /// `accept` calls that returned an error.
    pub accept_errors: u64,
    /// Handoffs abandoned because the fd could not be taken out of the stream.
    pub detach_failed: u64,
    /// Claims abandoned because the fd could not be wrapped on this ring.
    pub attach_failed: u64,
    /// Connections being served right now. A gauge, not a count.
    pub active: u64,
}

impl Counters {
    fn add(&mut self, other: &Counters) {
        self.accepted += other.accepted;
        self.served_local += other.served_local;
        self.handed_off += other.handed_off;
        self.handoff_full += other.handoff_full;
        self.claimed += other.claimed;
        self.bounced += other.bounced;
        self.oversubscribed += other.oversubscribed;
        self.rejected += other.rejected;
        self.completed += other.completed;
        self.handler_errors += other.handler_errors;
        self.resource_errors += other.resource_errors;
        self.accept_errors += other.accept_errors;
        self.detach_failed += other.detach_failed;
        self.attach_failed += other.attach_failed;
        self.active += other.active;
    }
}

/// One worker's counters plus where it runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerSnapshot {
    /// Worker index, 0-based, in `Workers` order.
    pub index: usize,
    /// CPU id the worker was assigned.
    pub core: usize,
    /// Whether the kernel accepted the pin.
    pub pinned: bool,
    /// Its counters.
    pub counters: Counters,
}

/// A snapshot of every worker, their sum, and the state of the handoff channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stats {
    /// One entry per worker, in index order.
    pub workers: Vec<WorkerSnapshot>,
    /// Sum over `workers`.
    pub totals: Counters,
    /// Connections waiting in the handoff channel right now.
    pub queued: usize,
    /// The channel's capacity.
    pub handoff_capacity: usize,
}

impl Stats {
    pub(crate) fn collect(
        workers: &[std::sync::Arc<WorkerStats>],
        queued: usize,
        handoff_capacity: usize,
    ) -> Self {
        let workers: Vec<_> = workers.iter().map(|w| w.snapshot()).collect();
        let mut totals = Counters::default();
        for w in &workers {
            totals.add(&w.counters);
        }
        Self {
            workers,
            totals,
            queued,
            handoff_capacity,
        }
    }
}
