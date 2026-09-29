#!/usr/bin/env bash
# Runs the whole filesystem comparison and leaves one JSONL file behind.
#
# Two passes, with the arms in opposite orders, because a run that does compio
# first and deadpool last charges deadpool for whatever the host was doing by
# then. Pooling the reps of both passes cancels it. `benches/exchange.rs`
# alternates its two designs inside one binary for the same reason; separate
# processes cannot do that, so the order is reversed instead.
#
# Usage: crates/fs-bench/run.sh [out.jsonl]
set -euo pipefail

OUT="${1:-$HOME/fsb-results.jsonl}"
BIN="${BIN:-$HOME/build/target/maxopt}"

# The depth sweep puts 3072 operations in flight, and the deadpool arm holds a
# descriptor for each. The default 1024 would make this a benchmark of `ulimit`.
ulimit -n 1048576

export FSB_THREADS="${FSB_THREADS:-1,2,4,8,12}"
export FSB_DEPTH="${FSB_DEPTH:-32}"
export FSB_SWEEP_CASES="${FSB_SWEEP_CASES:-read_4k_rand,acquire_read4k}"
export FSB_SWEEP_DEPTHS="${FSB_SWEEP_DEPTHS:-1,4,16,64,256}"
export FSB_SWEEP_THREADS="${FSB_SWEEP_THREADS:-12}"
# Three per pass, two passes: six reps per point, balanced for order.
export FSB_REPS="${FSB_REPS:-3}"
export BENCH_JSON="$OUT"

: > "$OUT"

run_surface() {
  local surface="$1" dir="$2" rf="$3" rmib="$4" wf="$5" wmib="$6" cases="$7"
  export FSB_SURFACE="$surface" FSB_DIR="$dir"
  export FSB_READ_FILES="$rf" FSB_READ_FILE_MIB="$rmib"
  export FSB_WRITE_FILES="$wf" FSB_WRITE_REGION_MIB="$wmib"
  export FSB_CASES="$cases"

  echo "=== $surface: $dir ==="
  "$BIN/fs_setup"
  # Warm the read set deliberately, so every arm meets the same cache state
  # instead of the first arm paying for the others' page faults.
  cat "$dir"/r/*.dat > /dev/null
  "$BIN/fs_floor"

  for pass in 1 2; do
    local arms="fs_compio fs_tokio fs_deadpool"
    [ "$pass" = 2 ] && arms="fs_deadpool fs_tokio fs_compio"
    for a in $arms; do
      echo "--- $surface pass $pass: $a ---"
      "$BIN/$a"
    done
  done
}

ALL=stat,open_close,acquire_read4k,read_4k_rand,read_64k_seq,write_4k_fsync,write_1m_buffered

# ext4 on the VM's own virtual disk: the surface the full matrix runs on.
run_surface ext4 /var/tmp/fsbench 64 32 16 128 "$ALL"

# tmpfs, reduced. Not a second data point about ext4 — it is the control. On
# tmpfs there is no device and no writeback, so whatever is left is the syscall
# and the machinery around it, which is the quantity this whole comparison is
# about.
FSB_THREADS=1,12 FSB_SWEEP_CASES=read_4k_rand FSB_SWEEP_DEPTHS=1,32 \
  run_surface tmpfs /tmp/fsbench 8 32 4 64 \
  stat,open_close,acquire_read4k,read_4k_rand,write_4k_fsync

echo "=== done: $(wc -l < "$OUT") records in $OUT ==="
