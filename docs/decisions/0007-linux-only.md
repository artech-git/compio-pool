# 0007 — Linux is the only supported target

## Constraint

Everything the crate is for is Linux-specific: `io_uring`, `SO_REUSEPORT` *load balancing*
(other BSDs have the option with different semantics; macOS delivers to one socket), `SO_INCOMING_CPU`,
and the IRQ-affinity recipe in `scripts/tune-nic.sh`.

## Decision

The crate compiles on any Unix so that rust-analyzer and `cargo check` work on a macOS laptop,
and `compile_error!`s on non-Unix. Nothing is built, tested or benchmarked anywhere but Linux;
CI is Ubuntu only; docs.rs builds the two Linux targets. Every `cargo` command in this
repository's history ran on Linux.

## Rejected

* **`compile_error!` on anything but Linux.** Would make the crate red in every editor on
  macOS for no safety gain: the APIs used exist on other Unixes, they just behave differently.
* **Supporting macOS or Windows.** compio's polling and IOCP backends would run the Rust, but
  the kernel half of the design does not exist there, so "support" would mean a different
  architecture with the same name.

## Costs

No portability. If you want this shape on another OS, you want a different crate.
