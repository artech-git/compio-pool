#![no_main]
//! Coverage-guided fuzzing of the cross-thread reservoir's own bookkeeping.
//!
//! Drives [`Exchange`] directly, with `detach` and `attach` failing under the
//! input's control, against a model of what should be resident. The property
//! that matters is that admission never leaks: whatever sequence of parks,
//! claims, failed detaches, failed attaches and clears happens, it must still be
//! possible to fill the reservoir to exactly `capacity`.
//!
//! libFuzzer runs one input at a time per process, so the `CAN_DETACH` /
//! `CAN_ATTACH` globals need no lock here.

use std::{cell::RefCell, sync::atomic::Ordering::SeqCst};

use libfuzzer_sys::fuzz_target;

#[path = "../../tests/common/mod.rs"]
mod common;

use common::{MovableConn, MovableManager};
use compio_pool::{Exchange, Parked, Reservoir, SlotMeta, Unparked};

struct Harness {
    runtime: compio::runtime::Runtime,
    /// `SlotMeta` has no public constructor, so one real instance is cloned for
    /// every park.
    proto: SlotMeta,
    next_id: u64,
}

impl Harness {
    fn new() -> Self {
        let runtime = compio::runtime::Runtime::new().expect("a fuzz worker needs a runtime");
        let proto = runtime.block_on(common::sample_meta());
        Self {
            runtime,
            proto,
            next_id: 0,
        }
    }

    fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}

thread_local! {
    static HARNESS: RefCell<Harness> = RefCell::new(Harness::new());
}

fuzz_target!(|data: &[u8]| {
    let Some((&size, steps)) = data.split_first() else {
        return;
    };
    // A fresh reservoir per input: unlike a pool, it owns no thread-local state,
    // so nothing accumulates.
    let capacity = size as usize % 8 + 1;

    HARNESS.with(|harness| {
        let mut harness = harness.borrow_mut();
        let reservoir: Reservoir<MovableManager> = Reservoir::new(capacity);
        // Ids parked and accepted, minus those claimed or lost. FIFO, so this is
        // ordered oldest first.
        let mut resident: Vec<u64> = Vec::new();

        for (step, &byte) in steps.iter().enumerate() {
            common::CAN_DETACH.store(byte & 0b0100_0000 == 0, SeqCst);
            common::CAN_ATTACH.store(byte & 0b1000_0000 == 0, SeqCst);

            match byte % 10 {
                0..=4 => {
                    let id = harness.id();
                    let meta = harness.proto.clone();
                    match reservoir.park(MovableConn::new(id), meta) {
                        Parked::Accepted => resident.push(id),
                        Parked::Refused(conn, _) => {
                            assert_eq!(conn.id, id, "step {step}: refusal returned another conn");
                            assert_eq!(
                                reservoir.len(),
                                capacity,
                                "step {step}: refused while not full"
                            );
                        }
                        // `detach` declined, and consumed the connection doing so.
                        Parked::Destroyed => {}
                    }
                }
                5..=8 => {
                    let claimed = harness
                        .runtime
                        .block_on(Exchange::<MovableManager>::unpark(&reservoir));
                    match claimed {
                        Unparked::Claimed(conn, _) => {
                            let at = resident
                                .iter()
                                .position(|&r| r == conn.id)
                                .unwrap_or_else(|| panic!("step {step}: claimed unparked id"));
                            resident.remove(at);
                        }
                        Unparked::Lost => {
                            assert!(!resident.is_empty(), "step {step}: lost nothing");
                            resident.remove(0);
                        }
                        Unparked::Empty => {
                            assert!(resident.is_empty(), "step {step}: empty but not empty")
                        }
                    }
                }
                _ => {
                    Exchange::<MovableManager>::clear(&reservoir);
                    resident.clear();
                }
            }

            assert!(
                reservoir.len() <= capacity,
                "step {step}: {} over capacity {capacity}",
                reservoir.len()
            );
            assert_eq!(
                reservoir.len(),
                resident.len(),
                "step {step}: reservoir and model disagree"
            );
            // Deliberately compared against `len`: the two must agree.
            #[allow(clippy::len_zero)]
            {
                assert_eq!(reservoir.is_empty(), reservoir.len() == 0);
            }
            assert_eq!(
                Exchange::<MovableManager>::parked(&reservoir) as usize,
                reservoir.len()
            );
        }

        // Every admission slot must have come back.
        common::CAN_DETACH.store(true, SeqCst);
        common::CAN_ATTACH.store(true, SeqCst);
        Exchange::<MovableManager>::clear(&reservoir);
        for i in 0..capacity {
            let id = harness.id();
            let meta = harness.proto.clone();
            assert!(
                matches!(reservoir.park(MovableConn::new(id), meta), Parked::Accepted),
                "admission leaked: only {i} of {capacity} slots usable after churn"
            );
        }
    });
});
