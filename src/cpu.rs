//! Core enumeration and thread pinning (recipe steps 7 and 8).

use std::io;

pub use core_affinity::CoreId;

use crate::config::Workers;

/// The cores this process may run on, sorted by id, with duplicates removed.
///
/// This is the affinity mask, not the machine: under `taskset` or a cgroup
/// cpuset only the permitted cores are returned, and that is the right set to
/// put workers on.
pub fn cores() -> Vec<CoreId> {
    let mut cores = core_affinity::get_core_ids().unwrap_or_default();
    cores.sort_by_key(|c| c.id);
    cores.dedup_by_key(|c| c.id);
    cores
}

/// Pin the calling thread to `core`. Returns whether the kernel accepted it.
///
/// Called inside each worker's thread closure before its runtime is built, so
/// the ring and everything allocated after it belong to that core.
pub fn pin_current(core: CoreId) -> bool {
    core_affinity::set_for_current(core)
}

/// The `/proc/irq/<n>/smp_affinity` mask that names exactly `cpu`:
/// comma-separated 32-bit hex words, most significant first.
///
/// `scripts/tune-nic.sh` writes the same format, so a worker's log line and the
/// IRQ it should be fed by can be compared by eye.
///
/// ```
/// assert_eq!(compio_pool::cpu::affinity_mask(3), "00000008");
/// assert_eq!(compio_pool::cpu::affinity_mask(35), "00000008,00000000");
/// ```
pub fn affinity_mask(cpu: usize) -> String {
    let words = cpu / 32 + 1;
    let mut out = String::with_capacity(words * 9);
    for w in (0..words).rev() {
        let bits: u32 = if w == cpu / 32 { 1 << (cpu % 32) } else { 0 };
        if !out.is_empty() {
            out.push(',');
        }
        out.push_str(&format!("{bits:08x}"));
    }
    out
}

/// Turn a [`Workers`] choice into the concrete core list, in worker order.
pub(crate) fn resolve(workers: &Workers) -> io::Result<Vec<CoreId>> {
    let all = cores();
    if all.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "could not enumerate the CPU cores this process may run on",
        ));
    }
    match workers {
        Workers::AllCores => Ok(all),
        Workers::Count(0) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Workers::Count must be at least 1",
        )),
        Workers::Count(n) => Ok((0..*n).map(|i| all[i % all.len()]).collect()),
        Workers::Cores(ids) if ids.is_empty() => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Workers::Cores must name at least one core",
        )),
        Workers::Cores(ids) => ids
            .iter()
            .map(|&id| {
                if all.iter().any(|c| c.id == id) {
                    Ok(CoreId { id })
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("cpu {id} is not in this process's affinity mask"),
                    ))
                }
            })
            .collect(),
    }
}
