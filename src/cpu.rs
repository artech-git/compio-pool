//! Core enumeration and thread pinning — the helpers you need to stand up your
//! own thread-per-core runtime around a [`Pool`](crate::Pool).

pub use core_affinity::CoreId;

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
/// Call it inside each thread's closure before you build that thread's runtime,
/// so the ring and everything allocated after it belong to that core.
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
