//! A guided tour of the pool's **lifecycle knobs** — [`min_idle`], [`warm`],
//! [`state`] — on a single compio runtime, with no sockets and no threads at all.
//!
//! There is no server here and no [`std::thread::spawn`]: just one [`Runtime`]
//! driving one [`LocalPool`], printing a [`State`] snapshot after each step so you
//! can watch the open/idle counts move. The pooled "connection" is an abstract
//! resource with an id — the pool is just as useful for any bounded, reused thing
//! as it is for sockets — and a counter records how many were ever opened, which
//! is what [`warm`] and reuse are there to keep down.
//!
//! What it demonstrates, in order:
//!
//!  * [`warm`] opens [`min_idle`] connections up front and parks them idle;
//!  * [`get`] on a warm pool reuses an idle connection — no new open;
//!  * dropping a lease returns the connection to the idle list;
//!  * a burst reuses the idle ones first, then opens more only up to
//!    [`max_size`], which is the ceiling.
//!
//! ```text
//! cargo run --release --example warmup
//! ```
//!
//! [`min_idle`]: compio_pool::Builder::min_idle
//! [`max_size`]: compio_pool::Builder::max_size
//! [`warm`]: compio_pool::LocalPool::warm
//! [`get`]: compio_pool::LocalPool::get
//! [`state`]: compio_pool::LocalPool::state
//! [`State`]: compio_pool::State

use std::{
    io,
    sync::atomic::{AtomicU64, Ordering},
};

use compio::runtime::Runtime;
use compio_pool::{ManageConnection, Pool};

/// How many resources the manager has ever opened — the number `warm` and reuse
/// exist to hold down.
static OPENS: AtomicU64 = AtomicU64::new(0);

/// One pooled resource. A real manager would open a socket or a client here; this
/// one just stamps an id so `connect` does something observable.
struct Resource {
    #[allow(dead_code)]
    id: u64,
}

/// Opens abstract resources and counts every open.
struct Manager;

impl ManageConnection for Manager {
    type Connection = Resource;
    type Error = io::Error;

    async fn connect(&self) -> io::Result<Resource> {
        let id = OPENS.fetch_add(1, Ordering::Relaxed);
        Ok(Resource { id })
    }

    async fn is_valid(&self, _conn: &mut Resource) -> io::Result<()> {
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Resource) -> bool {
        false
    }
}

fn opened() -> u64 {
    OPENS.load(Ordering::Relaxed)
}

fn main() -> io::Result<()> {
    // Keep at most 8 per thread; warm 4 of them ahead of demand.
    let pool = Pool::builder().max_size(8).min_idle(4).build(Manager);

    // One runtime, one thread — the pool's natural unit.
    let runtime = Runtime::builder().build().expect("build compio runtime");
    runtime.block_on(async move {
        let local = pool.local();
        println!("fresh pool:     {:?}  opened={}", local.state(), opened());

        // Warm opens min_idle connections up front and parks them on the idle list.
        let made = local.warm().await.expect("warm");
        println!(
            "after warm:     {:?}  opened={}  (warm made {made})",
            local.state(),
            opened()
        );

        // Checking three out pulls them off the warm idle list — no new opens.
        let a = local.get().await.expect("get a");
        let b = local.get().await.expect("get b");
        let c = local.get().await.expect("get c");
        println!("3 checked out:  {:?}  opened={}", local.state(), opened());

        // Dropping the leases returns the resources to the idle list.
        drop((a, b, c));
        println!("3 returned:     {:?}  opened={}", local.state(), opened());

        // A burst reuses the 4 idle resources first, then opens more — but only up
        // to max_size=8, which caps the pool. A 9th get here would wait for a free
        // slot rather than open a ninth.
        let mut held = Vec::new();
        for _ in 0..8 {
            held.push(local.get().await.expect("burst"));
        }
        println!("8 checked out:  {:?}  opened={}", local.state(), opened());

        drop(held);
        println!("8 returned:     {:?}  opened={}", local.state(), opened());
    });

    Ok(())
}
