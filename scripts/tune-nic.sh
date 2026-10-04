#!/usr/bin/env bash
# tune-nic.sh — the host side of the compio-pool deployment recipe.
#
# Steps 1-4 of the recipe are kernel and NIC configuration, not Rust. This
# script applies them to a real multi-queue NIC so that the kernel's receive
# hashing lands each flow on the core whose worker owns the matching
# SO_REUSEPORT listener:
#
#   1. Size the NIC's RX/TX queue pairs to the worker count         ethtool -L
#   2. Stop, disable and mask irqbalance                            systemctl
#   3. Pin each RX queue's IRQ to the core of the worker it feeds   /proc/irq/<n>/smp_affinity
#   4. Turn on accelerated RFS and size the flow tables             ethtool -K ntuple on,
#                                                                   net.core.rps_sock_flow_entries,
#                                                                   queues/rx-<i>/rps_flow_cnt
#
# The CPU list must be the same list, in the same order, that the server was
# started with (`Workers::Cores`), so queue i, IRQ i and worker i agree.
#
# Usage:
#   sudo scripts/tune-nic.sh [--iface IFACE] [--cpus LIST] [--queues N]
#                            [--flow-entries N] [--software-rps] [--dry-run]
#        scripts/tune-nic.sh --check [--iface IFACE] [--cpus LIST]
#
#   --iface IFACE     interface to tune (default: the default-route interface)
#   --cpus LIST       comma-separated CPU ids, worker order (default: all online CPUs)
#   --queues N        queue pairs to configure (default: number of CPUs in --cpus)
#   --flow-entries N  net.core.rps_sock_flow_entries (default: 32768)
#   --software-rps    when the NIC has fewer RX queues than workers, also spread
#                     each RX queue over the worker CPUs with rps_cpus (software RPS)
#   --dry-run         print every command instead of running it
#   --check           read-only: report capability and current state, change
#                     nothing. Exits 0 if the hardware can take the recipe, 2 if not.
#   --self-test       check the mask and CPU-list helpers against known values
#
# Virtualised NICs (virtio-net under lima/vz, many cloud instances) often expose a
# single combined queue with receive hashing fixed off. `--check` says so, and the
# recipe cannot be applied there; the server still works, it just cannot get the
# per-core packet steering the hardware steps are for.

set -euo pipefail

IFACE=""
CPU_LIST=""
QUEUES=""
FLOW_ENTRIES=32768
SOFTWARE_RPS=0
DRY_RUN=0
CHECK=0
SELF_TEST=0

usage() { sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --iface) IFACE="$2"; shift 2 ;;
    --cpus) CPU_LIST="$2"; shift 2 ;;
    --queues) QUEUES="$2"; shift 2 ;;
    --flow-entries) FLOW_ENTRIES="$2"; shift 2 ;;
    --software-rps) SOFTWARE_RPS=1; shift ;;
    --dry-run) DRY_RUN=1; shift ;;
    --check) CHECK=1; shift ;;
    --self-test) SELF_TEST=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 64 ;;
  esac
done

log() { printf '%s\n' "$*" >&2; }

# run CMD... — echo the command, then execute it unless --dry-run.
run() {
  log "+ $*"
  if [ "$DRY_RUN" -eq 0 ]; then "$@"; fi
}

# write VALUE FILE — like `echo VALUE > FILE`, through run().
write() {
  log "+ echo $1 > $2"
  if [ "$DRY_RUN" -eq 0 ]; then echo "$1" > "$2"; fi
}

# expand_cpu_list "0-3,8,10-11" -> "0 1 2 3 8 10 11"
expand_cpu_list() {
  local out=() part lo hi
  IFS=',' read -ra parts <<< "$1"
  for part in "${parts[@]}"; do
    if [[ "$part" == *-* ]]; then
      lo="${part%-*}"; hi="${part#*-}"
      for ((i = lo; i <= hi; i++)); do out+=("$i"); done
    else
      out+=("$part")
    fi
  done
  echo "${out[@]}"
}

# cpu_mask CPU — the smp_affinity hex mask for one CPU: comma-separated
# 32-bit words, most significant first, e.g. cpu 35 -> "00000008,00000000".
cpu_mask() {
  local target=$1
  local words=$(( target / 32 + 1 ))
  local w bits mask=""
  for ((w = words - 1; w >= 0; w--)); do
    if (( w == target / 32 )); then bits=$(( 1 << (target % 32) )); else bits=0; fi
    mask+=$(printf '%08x' "$bits")
    if (( w > 0 )); then mask+=","; fi
  done
  echo "$mask"
}

# ethtool_max KEY — the "Pre-set maximums" value for KEY (Combined/RX/TX); "n/a" or 0 when absent.
ethtool_max() {
  ethtool -l "$IFACE" 2>/dev/null | awk -v key="$1:" '
    /Pre-set maximums/ { inmax = 1; next }
    /Current hardware settings/ { inmax = 0 }
    inmax && $1 == key { print $2; exit }'
}

# rx_irqs — IRQ numbers of the interface's receive queues, in queue order.
# Names differ per driver (eth0-rx-0, eth0-TxRx-3, virtio0-input.0,
# mlx5_comp2@pci:0000:…); the trailing number is the queue index for all of them.
# The MSI-X table lives on the PCI function, which for a virtio NIC is the
# *parent* of the device the interface points at, so both are tried.
rx_irqs() {
  local devpath msi dev irq name idx
  devpath=$(readlink -f "/sys/class/net/$IFACE/device" 2>/dev/null || true)
  msi=""
  for cand in "$devpath/msi_irqs" "$devpath/../msi_irqs"; do
    if [ -n "$devpath" ] && [ -d "$cand" ]; then msi="$cand"; break; fi
  done
  if [ -n "$msi" ]; then
    for irq in "$msi"/*; do
      irq=$(basename "$irq")
      name=$(awk -v irq="$irq:" '$1 == irq { print $NF }' /proc/interrupts)
      case "$name" in
        *[Rr]x*|*input*|*comp*|*-"$IFACE"-*) ;;
        *) continue ;;
      esac
      idx=$(printf '%s' "$name" | grep -oE '[0-9]+$' || echo 0)
      printf '%s %s\n' "$idx" "$irq"
    done | sort -n | awk '{ print $2 }'
  else
    # No MSI table to read: match interrupt names on the interface or its
    # device (virtio0, a PCI address) and keep the receive-side ones.
    dev=$(basename "$devpath" 2>/dev/null || echo "$IFACE")
    awk -v a="$IFACE" -v b="$dev" '
      index($0, a) || index($0, b) {
        n = $NF
        if (n ~ /[Rr]x|input|comp/) { gsub(/:/, "", $1); print $1 }
      }' /proc/interrupts
  fi
}

if [ "$SELF_TEST" -eq 1 ]; then
  fail=0
  for pair in 0:00000001 3:00000008 31:80000000 32:00000001,00000000 35:00000008,00000000 \
              64:00000001,00000000,00000000; do
    got=$(cpu_mask "${pair%%:*}")
    if [ "$got" != "${pair#*:}" ]; then echo "cpu_mask ${pair%%:*}: got $got, want ${pair#*:}"; fail=1; fi
  done
  got=$(expand_cpu_list "0-3,8,10-11")
  [ "$got" = "0 1 2 3 8 10 11" ] || { echo "expand_cpu_list: got '$got'"; fail=1; }
  [ "$fail" -eq 0 ] && echo "self-test passed"
  exit "$fail"
fi

if [ -z "$IFACE" ]; then
  IFACE=$(ip -o -4 route show to default 2>/dev/null | awk '{ print $5; exit }')
  [ -n "$IFACE" ] || { log "could not determine the default interface; pass --iface"; exit 64; }
fi
[ -d "/sys/class/net/$IFACE" ] || { log "no such interface: $IFACE"; exit 64; }

if [ -z "$CPU_LIST" ]; then
  CPU_LIST=$(cat /sys/devices/system/cpu/online)
fi
read -ra CPUS <<< "$(expand_cpu_list "$CPU_LIST")"
[ "${#CPUS[@]}" -gt 0 ] || { log "empty --cpus"; exit 64; }
QUEUES=${QUEUES:-${#CPUS[@]}}

command -v ethtool >/dev/null || { log "ethtool is required"; exit 69; }

# ---------------------------------------------------------------- --check ----
MAX_COMBINED=$(ethtool_max Combined)
MAX_RX=$(ethtool_max RX)
HAVE_MULTIQ=0
for m in "$MAX_COMBINED" "$MAX_RX"; do
  if [[ "$m" =~ ^[0-9]+$ ]] && [ "$m" -ge "$QUEUES" ]; then HAVE_MULTIQ=1; fi
done
NTUPLE=$(ethtool -k "$IFACE" 2>/dev/null | awk '/ntuple-filters/ { print $2, $3 }')
RXHASH=$(ethtool -k "$IFACE" 2>/dev/null | awk '/receive-hashing/ { print $2, $3 }')
DRIVER=$(ethtool -i "$IFACE" 2>/dev/null | awk '/^driver/ { print $2 }')
mapfile -t RX_IRQS < <(rx_irqs)

if [ "$CHECK" -eq 1 ]; then
  echo "interface:            $IFACE ($DRIVER)"
  echo "worker cpus:          ${CPUS[*]}"
  echo "queues wanted:        $QUEUES"
  echo "queues max combined:  ${MAX_COMBINED:-n/a}    max rx: ${MAX_RX:-n/a}"
  echo "receive hashing:      ${RXHASH:-unknown}"
  echo "ntuple filters:       ${NTUPLE:-unknown}  (aRFS needs 'on' or settable)"
  echo "rx irqs found:        ${#RX_IRQS[@]} (${RX_IRQS[*]:-none})"
  for irq in "${RX_IRQS[@]}"; do
    printf '  irq %-5s smp_affinity=%s\n' "$irq" "$(cat "/proc/irq/$irq/smp_affinity" 2>/dev/null || echo '?')"
  done
  if systemctl list-unit-files irqbalance.service >/dev/null 2>&1; then
    echo "irqbalance:           $(systemctl is-active irqbalance 2>/dev/null || true) / $(systemctl is-enabled irqbalance 2>/dev/null || true)"
  else
    echo "irqbalance:           not installed"
  fi
  echo "rps_sock_flow_entries: $(cat /proc/sys/net/core/rps_sock_flow_entries 2>/dev/null || echo '?')"
  for q in /sys/class/net/"$IFACE"/queues/rx-*; do
    printf '  %s rps_cpus=%s rps_flow_cnt=%s\n' "$(basename "$q")" "$(cat "$q/rps_cpus")" "$(cat "$q/rps_flow_cnt")"
  done
  echo
  if [ "$HAVE_MULTIQ" -eq 1 ] && [[ "$NTUPLE" != *fixed* ]]; then
    echo "verdict: the full recipe can be applied here."
    exit 0
  fi
  echo "verdict: the hardware recipe cannot be applied here:"
  [ "$HAVE_MULTIQ" -eq 1 ] || echo "  - the NIC exposes at most ${MAX_COMBINED:-${MAX_RX:-1}} RX queue(s); $QUEUES wanted (steps 1 and 3)"
  [[ "$NTUPLE" != *fixed* ]] || echo "  - ntuple filters are fixed off, so accelerated RFS is unavailable (step 4)"
  echo "  the server still runs; use --software-rps to spread the queue(s) over the worker cores with RPS instead."
  exit 2
fi

# ------------------------------------------------------------------ apply ----
if [ "$DRY_RUN" -eq 0 ] && [ "$(id -u)" -ne 0 ]; then
  log "changing NIC queues, IRQ affinity and sysctls needs root (or use --dry-run / --check)"
  exit 77
fi

log "== step 1: size $IFACE to $QUEUES queue pair(s)"
if [[ "$MAX_COMBINED" =~ ^[0-9]+$ ]] && [ "$MAX_COMBINED" -ge "$QUEUES" ]; then
  run ethtool -L "$IFACE" combined "$QUEUES"
elif [[ "$MAX_RX" =~ ^[0-9]+$ ]] && [ "$MAX_RX" -ge "$QUEUES" ]; then
  run ethtool -L "$IFACE" rx "$QUEUES" tx "$QUEUES"
else
  log "   $IFACE supports at most ${MAX_COMBINED:-${MAX_RX:-1}} queue(s); leaving the queue count alone"
fi
# Re-read: the IRQ set changes with the queue count.
mapfile -t RX_IRQS < <(rx_irqs)

log "== step 2: irqbalance off, so the kernel does not move the pins below"
if systemctl list-unit-files irqbalance.service >/dev/null 2>&1; then
  run systemctl stop irqbalance
  run systemctl disable irqbalance
  run systemctl mask irqbalance
else
  log "   irqbalance is not installed; nothing to stop"
fi

log "== step 3: pin RX queue IRQs to worker cores"
if [ "${#RX_IRQS[@]}" -eq 0 ]; then
  log "   no RX IRQs found for $IFACE; skipping"
fi
for ((i = 0; i < ${#RX_IRQS[@]} && i < ${#CPUS[@]}; i++)); do
  irq=${RX_IRQS[i]}; cpu=${CPUS[i]}
  log "   queue $i: irq $irq -> cpu $cpu"
  write "$(cpu_mask "$cpu")" "/proc/irq/$irq/smp_affinity"
done

log "== step 4: accelerated RFS and flow tables"
if [[ "$NTUPLE" == *fixed* ]]; then
  log "   ntuple filters are fixed off on $IFACE; aRFS unavailable, configuring software RFS only"
else
  run ethtool -K "$IFACE" ntuple on
fi
run sysctl -q -w "net.core.rps_sock_flow_entries=$FLOW_ENTRIES"
RXQ=( /sys/class/net/"$IFACE"/queues/rx-* )
PER_QUEUE=$(( FLOW_ENTRIES / ${#RXQ[@]} ))
for q in "${RXQ[@]}"; do
  write "$PER_QUEUE" "$q/rps_flow_cnt"
done

if [ "$SOFTWARE_RPS" -eq 1 ] && [ "${#RXQ[@]}" -lt "${#CPUS[@]}" ]; then
  log "== software RPS: fewer RX queues (${#RXQ[@]}) than workers (${#CPUS[@]}); spreading each queue over the worker cores"
  mask_words=$(( ${CPUS[-1]} / 32 + 1 ))
  # Build one mask covering every worker CPU, word by word.
  WORDS=()
  for ((w = 0; w < mask_words; w++)); do WORDS[w]=0; done
  for cpu in "${CPUS[@]}"; do
    w=$(( cpu / 32 )); WORDS[w]=$(( WORDS[w] | (1 << (cpu % 32)) ))
  done
  ALL_MASK=""
  for ((w = mask_words - 1; w >= 0; w--)); do
    ALL_MASK+=$(printf '%08x' "${WORDS[w]}")
    (( w > 0 )) && ALL_MASK+=","
  done
  for q in "${RXQ[@]}"; do
    write "$ALL_MASK" "$q/rps_cpus"
  done
fi

log "== done. Verify with: watch -n1 'grep $IFACE /proc/interrupts' and ss -lnt (one listener per worker)."
