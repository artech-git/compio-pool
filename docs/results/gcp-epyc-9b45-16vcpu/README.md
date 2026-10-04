# Results: 16-vCPU Google Cloud VM

Raw logs from [`crates/deadpool-baseline/bench.sh`](../../../crates/deadpool-baseline/bench.sh)
on one machine: AMD EPYC 9B45, 16 vCPUs (8 physical cores × 2 threads), one NUMA node, 62 GiB,
kernel 6.12, loopback, `io_uring_disabled=0`, git `reuseport-workers@1ffe0be`. Each file holds the
environment, a summary table and every raw run as TSV (the columns are in its header line).
They are analysed in [`docs/performance.md`](../../performance.md#baseline-tokio-on-a-16-vcpu-vm).
The home directory in one progress-log line was redacted; nothing else was changed.

| file | command | what it is |
|---|---|---|
| `bench-20261002T192557Z.log` | `./bench.sh` | the full default matrix, 312 runs: scaling, payload, connections, shared, one-CPU, churn |
| `bench-20261003T025706Z.log` | `--suites scale512,scale16k --workers "1 2 4 8 16"` | the scaling suites again seven hours later (8 and 16 workers cannot be placed on 8 cores and were skipped) |
| `bench-20261003T030310Z.log` | `--suites payload,conns --servers tokio-per-core,compio-pool,compio-defer,compio-nocoop` | the io_uring ring modes |
| `bench-20261003T031625Z.log` | `--suites onecpu` | the one-CPU regime again |
| `bench-20261003T032519Z.log` | `--suites custom --workers 4 --bytes "512 16384" --conns "64 256" --placement split` | four workers, two payloads, two connection counts |
| `bench-20261003T032929Z.log` | `--suites custom --workers 4 --conns 256 --servers compio-pool:1,compio-pool:2,compio-pool` | admission control: capacity 1, 2 and 1024 with 256 persistent connections |
