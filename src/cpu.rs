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

// ---------------------------------------------------------------------------
// Topology-aware core selection — the optional `affinity` feature, layer (a).
//
// Everything below is gated behind `#[cfg(feature = "affinity")]`; the raw path
// above (`cores`, `pin_current`, `affinity_mask`) is unconditional and unchanged.
// It is pure `std::fs` over sysfs, so the default build stays provably
// `unsafe`-free and pulls in no new crate.
// ---------------------------------------------------------------------------

/// The topology of one logical CPU, read from `/sys/devices/system/cpu`.
///
/// `id` is the value that feeds [`CoreId`] and `SO_INCOMING_CPU`; the other
/// three drive [`CoreSet`]'s filters and ordering. Two logical CPUs are hardware
/// threads of the *same physical core* when they share a `(package, core)` pair
/// — which is how [`CoreSet::physical_only`] dedups them (never `core` alone,
/// since `core` ids are only unique within a package).
#[cfg(feature = "affinity")]
#[cfg_attr(docsrs, doc(cfg(feature = "affinity")))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CoreInfo {
    /// The logical CPU id — the `cpuN` in sysfs, and the value `CoreId`/
    /// `SO_INCOMING_CPU` use. Unique across the machine.
    pub id: usize,
    /// The physical core id *within its package* (sysfs `topology/core_id`).
    /// Hardware threads of one core share it. Not unique across packages.
    pub core: usize,
    /// The physical package / socket id (sysfs `topology/physical_package_id`).
    pub package: usize,
    /// The NUMA node this CPU belongs to, or `0` when the host exposes none.
    pub numa_node: usize,
}

#[cfg(feature = "affinity")]
impl CoreInfo {
    /// The [`CoreId`] for this CPU — what [`pin_current`] and [`CoreSet`]'s
    /// iterator hand out.
    pub fn core_id(&self) -> CoreId {
        CoreId { id: self.id }
    }
}

/// A chainable selection over the cores **this process may run on**.
///
/// It is seeded from [`cores()`] — the affinity mask, so it honours `taskset`
/// and cgroup cpusets and can never name a core the process is not allowed to
/// use — then annotated with the sysfs topology. Build one with
/// [`permitted`](Self::permitted), narrow it with [`physical_only`](Self::physical_only)
/// / [`on_numa_node`](Self::on_numa_node) / [`on_package`](Self::on_package) /
/// [`filter`](Self::filter), order it with [`spread`](Self::spread) /
/// [`pack`](Self::pack), cut it with [`take`](Self::take), then hand it to
/// [`PerCore::new`](crate::workers::PerCore::new) or iterate it directly.
///
/// The selection keeps its order, and the methods consume and return `self`, so
/// they chain. It reflects the cpuset mask but **not** a CFS `cpu.max` quota — a
/// 1.5-CPU cgroup still shows every masked CPU.
#[cfg(feature = "affinity")]
#[cfg_attr(docsrs, doc(cfg(feature = "affinity")))]
#[derive(Clone, Debug)]
pub struct CoreSet {
    infos: Vec<CoreInfo>,
}

#[cfg(feature = "affinity")]
impl CoreSet {
    /// The permitted cores, each annotated with its sysfs topology, in id order.
    ///
    /// This is [`cores()`] intersected with `/sys/devices/system/cpu`: offline
    /// CPUs and ones outside the affinity mask never appear. It is best-effort —
    /// on a host that exposes no topology (a stripped container, a flattened VM,
    /// a non-Linux build) every core comes back as its own physical core on
    /// package `0`, node `0`, which makes [`physical_only`](Self::physical_only)
    /// a no-op and leaves the raw set intact rather than failing.
    pub fn permitted() -> CoreSet {
        let ids: Vec<usize> = cores().into_iter().map(|c| c.id).collect();
        CoreSet {
            infos: topology_for(&ids),
        }
    }

    /// Build a set from explicit rows, bypassing sysfs.
    ///
    /// For tests that want to exercise the dedup and ordering on a synthetic
    /// topology, and for callers that enumerate cores their own way.
    pub fn from_infos(infos: impl IntoIterator<Item = CoreInfo>) -> CoreSet {
        CoreSet {
            infos: infos.into_iter().collect(),
        }
    }

    /// One logical CPU per physical core — drop the SMT siblings, keeping the
    /// lowest-id thread of each `(package, core)`.
    pub fn physical_only(mut self) -> CoreSet {
        let mut seen = HashSet::new();
        self.infos.retain(|c| seen.insert((c.package, c.core)));
        self
    }

    /// Keep only the cores on NUMA node `node`.
    pub fn on_numa_node(mut self, node: usize) -> CoreSet {
        self.infos.retain(|c| c.numa_node == node);
        self
    }

    /// Keep only the cores on package / socket `package`.
    pub fn on_package(mut self, package: usize) -> CoreSet {
        self.infos.retain(|c| c.package == package);
        self
    }

    /// Keep only the cores for which `pred` returns `true` — the escape hatch for
    /// a policy the built-in filters do not cover.
    pub fn filter(mut self, mut pred: impl FnMut(&CoreInfo) -> bool) -> CoreSet {
        self.infos.retain(|c| pred(c));
        self
    }

    /// Order the set so cores taken in sequence are maximally **separated**:
    /// round-robin across packages, then NUMA nodes, then physical cores, with
    /// SMT siblings last. Combined with [`take`](Self::take) this picks `n` cores
    /// spread across the machine. The general-purpose default.
    ///
    /// `spread`/`pack` are heuristics over what sysfs exposes (package → NUMA →
    /// core); sysfs has no L3/CCX view, so on chiplet CPUs same-package cores that
    /// do not share a last-level cache are still treated as near.
    pub fn spread(mut self) -> CoreSet {
        let mut groups: BTreeMap<usize, Vec<CoreInfo>> = BTreeMap::new();
        for c in self.infos.drain(..) {
            groups.entry(c.package).or_default().push(c);
        }
        let mut columns: Vec<std::vec::IntoIter<CoreInfo>> = groups
            .into_values()
            .map(|mut g| {
                g.sort_by_key(|c| (c.numa_node, c.core, c.id));
                g.into_iter()
            })
            .collect();
        let mut out = Vec::new();
        let mut draining = true;
        while draining {
            draining = false;
            for column in &mut columns {
                if let Some(c) = column.next() {
                    out.push(c);
                    draining = true;
                }
            }
        }
        self.infos = out;
        self
    }

    /// Order the set so cores are **packed**: fill a physical core's SMT siblings,
    /// then a package, then a NUMA node, before spilling to the next.
    pub fn pack(mut self) -> CoreSet {
        self.infos
            .sort_by_key(|c| (c.package, c.numa_node, c.core, c.id));
        self
    }

    /// Keep at most the first `n` cores in the current order.
    pub fn take(mut self, n: usize) -> CoreSet {
        self.infos.truncate(n);
        self
    }

    /// How many cores the selection holds.
    pub fn len(&self) -> usize {
        self.infos.len()
    }

    /// Whether the selection is empty.
    pub fn is_empty(&self) -> bool {
        self.infos.is_empty()
    }

    /// The selected cores, with their full topology, in selection order.
    pub fn infos(&self) -> &[CoreInfo] {
        &self.infos
    }

    /// The selected cores as [`CoreId`]s, in selection order.
    pub fn core_ids(&self) -> Vec<CoreId> {
        self.infos.iter().map(CoreInfo::core_id).collect()
    }
}

/// Iterating a `CoreSet` yields its [`CoreId`]s in selection order, so it drops
/// straight into `for core in set { … }` and into
/// [`PerCore::new`](crate::workers::PerCore::new).
#[cfg(feature = "affinity")]
impl IntoIterator for CoreSet {
    type Item = CoreId;
    type IntoIter = std::vec::IntoIter<CoreId>;

    fn into_iter(self) -> Self::IntoIter {
        self.core_ids().into_iter()
    }
}

/// Read the sysfs topology for each id in `ids` (already the permitted, online
/// set from [`cores()`], so the intersection and offline-skip are done).
///
/// Every file is optional: a missing `core_id` falls back to the cpu id (so the
/// core is treated as its own physical core — no false SMT grouping), a missing
/// `physical_package_id` to `0`, and an absent NUMA link to node `0`.
#[cfg(all(feature = "affinity", target_os = "linux"))]
fn topology_for(ids: &[usize]) -> Vec<CoreInfo> {
    ids.iter()
        .map(|&id| {
            let topo = format!("/sys/devices/system/cpu/cpu{id}/topology");
            CoreInfo {
                id,
                core: read_sysfs_usize(&format!("{topo}/core_id")).unwrap_or(id),
                package: read_sysfs_usize(&format!("{topo}/physical_package_id")).unwrap_or(0),
                numa_node: numa_node_of(id),
            }
        })
        .collect()
}

/// Non-Linux stand-in (ADR 0007: the crate only type-checks off Linux). Each
/// permitted core is its own physical core on package `0`, node `0`.
#[cfg(all(feature = "affinity", not(target_os = "linux")))]
fn topology_for(ids: &[usize]) -> Vec<CoreInfo> {
    ids.iter()
        .map(|&id| CoreInfo {
            id,
            core: id,
            package: 0,
            numa_node: 0,
        })
        .collect()
}

/// Read a single non-negative integer out of a sysfs attribute, or `None` if the
/// file is absent, unreadable, or not a number.
#[cfg(all(feature = "affinity", target_os = "linux"))]
fn read_sysfs_usize(path: &str) -> Option<usize> {
    std::fs::read_to_string(path).ok()?.trim().parse().ok()
}

/// The NUMA node a CPU belongs to, from the `nodeN` symlink sysfs places in the
/// cpu's directory. `0` when the kernel exposes no NUMA topology.
#[cfg(all(feature = "affinity", target_os = "linux"))]
fn numa_node_of(id: usize) -> usize {
    let dir = format!("/sys/devices/system/cpu/cpu{id}");
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        if let Some(node) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.strip_prefix("node"))
            .and_then(|n| n.parse::<usize>().ok())
        {
            return node;
        }
    }
    0
}

#[cfg(feature = "affinity")]
use std::collections::{BTreeMap, HashSet};
