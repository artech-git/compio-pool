#!/usr/bin/env bash
# Instrumented resource sweep: server resource utilisation vs worker count, single box.
# For each server x worker-count: pin server to W cores, drive with examples/load on the
# remaining cores, and measure the SERVER process with perf (syscalls, context-switches,
# cpu-migrations, page-faults) + /proc (VmHWM, Threads, voluntary/involuntary ctxt, CPU).
set -uo pipefail
REPO=$HOME/compio-pool
PAR=$REPO/target/release/examples
BASE=$REPO/crates/deadpool-baseline/target/release/examples
OUT=${1:-$HOME/resbench-out}; mkdir -p "$OUT"
TMP=$(mktemp -d)
SECS=10
CAP=1024
BYTES=512
CONNS=512
WLIST="4 8 16 24 28 30 32"
SERVERS="compio-pool tokio-per-core tokio-default"
NCPU=$(nproc)
CLK=$(getconf CLK_TCK)
PORT=8100
TSV=$OUT/res.tsv
printf 'server\tworkers\tscpus\tccpus\treqs\trps\tp50_us\tp99_us\tfailed\trss_mb\tthreads\tcpu_pct\tcpu_us_req\tsys_total\tsys_req\tctx_total\tctx_req\tvol_ctx\tinvol_ctx\tmigr\tmigr_req\tpgflt\n' > "$TSV"

bin_for(){ case $1 in compio-pool) echo "$PAR/echo";; tokio-per-core) echo "$BASE/echo_tpc";; tokio-default) echo "$BASE/echo";; esac; }
cpu_jiffies(){ awk '{ s=$0; sub(/^.*\) /,"",s); n=split(s,a," "); print a[12]+a[13] }' /proc/$1/stat 2>/dev/null; }
status_val(){ awk -v k="$2:" '$1==k{print $2; exit}' /proc/$1/status 2>/dev/null; }

run_one(){
  local srv=$1 W=$2 bin scpus ccpus
  bin=$(bin_for "$srv")
  if (( W < NCPU )); then scpus="0-$((W-1))"; ccpus="$W-$((NCPU-1))"; else scpus="0-$((NCPU-1))"; ccpus="0-$((NCPU-1))"; fi
  PORT=$((PORT+1)); local port=$PORT
  taskset -c "$scpus" "$bin" "127.0.0.1:$port" "$CAP" "$W" >"$TMP/s.out" 2>"$TMP/s.err" &
  local SPID=$! i up=0
  for ((i=0;i<120;i++)); do (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && { up=1; break; }; kill -0 $SPID 2>/dev/null || break; sleep 0.05; done
  if (( ! up )); then echo "  $srv W=$W: SERVER FAILED: $(tr '\n' ' ' <"$TMP/s.err" | cut -c1-140)"; kill $SPID 2>/dev/null; wait $SPID 2>/dev/null; return; fi
  sleep 1.0

  local c0 v0 n0
  c0=$(cpu_jiffies $SPID); v0=$(status_val $SPID voluntary_ctxt_switches); n0=$(status_val $SPID nonvoluntary_ctxt_switches)

  sudo perf stat -x, -e context-switches,cpu-migrations,page-faults,raw_syscalls:sys_enter -p $SPID -- sleep $SECS 2>"$TMP/perf.csv" &
  local PERF=$!
  sleep 0.15
  taskset -c "$ccpus" "$PAR/load" "127.0.0.1:$port" --conns $CONNS --seconds $SECS --bytes $BYTES >"$TMP/l.out" 2>&1
  wait $PERF 2>/dev/null

  local c1 v1 n1 rss thr
  c1=$(cpu_jiffies $SPID); v1=$(status_val $SPID voluntary_ctxt_switches); n1=$(status_val $SPID nonvoluntary_ctxt_switches)
  rss=$(status_val $SPID VmHWM); thr=$(status_val $SPID Threads)
  kill $SPID 2>/dev/null; wait $SPID 2>/dev/null

  local reqs rps p50 p99 failed
  reqs=$(awk '/^requests /{print $2}' "$TMP/l.out"); rps=$(awk '/^requests /{r=$3; gsub(/[()]/,"",r); print r}' "$TMP/l.out")
  read -r p50 p99 <<<"$(awk '/^latency us/{print $4, $8}' "$TMP/l.out")"
  failed=$(awk '/^conns /{for(i=1;i<=NF;i++) if($i=="failed") print $(i+1)}' "$TMP/l.out")
  local ctx migr pgf sys
  ctx=$(awk -F, '$3=="context-switches"{print $1}' "$TMP/perf.csv")
  migr=$(awk -F, '$3=="cpu-migrations"{print $1}' "$TMP/perf.csv")
  pgf=$(awk -F, '$3=="page-faults"{print $1}' "$TMP/perf.csv")
  sys=$(awk -F, '$3=="raw_syscalls:sys_enter"{print $1}' "$TMP/perf.csv")

  awk -v srv="$srv" -v W="$W" -v scpus="$scpus" -v ccpus="$ccpus" -v reqs="${reqs:-0}" -v rps="${rps:-0}" \
      -v p50="${p50:-0}" -v p99="${p99:-0}" -v failed="${failed:-0}" -v rss="${rss:-0}" -v thr="${thr:-0}" \
      -v dcpu="$((c1-c0))" -v clk="$CLK" -v secs="$SECS" -v sys="${sys:-0}" -v ctx="${ctx:-0}" \
      -v vol="$((v1-v0))" -v invol="$((n1-n0))" -v migr="${migr:-0}" -v pgf="${pgf:-0}" '
    BEGIN{ r=(reqs>0?reqs:1); cpu_s=dcpu/clk;
      printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%.1f\t%s\t%.0f\t%.2f\t%s\t%.2f\t%s\t%.2f\t%s\t%s\t%s\t%.3f\t%s\n",
        srv,W,scpus,ccpus,reqs,rps,p50,p99,failed, rss/1024, thr, (cpu_s/secs)*100, cpu_s*1e6/r,
        sys, sys/r, ctx, ctx/r, vol, invol, migr, migr/r, pgf }' >> "$TSV"

  echo "  $(printf '%-15s W=%-2s' "$srv" "$W") $(printf '%9s' "${rps:-?}") req/s  rss $(printf '%6.1f' "$(awk -v x=${rss:-0} 'BEGIN{print x/1024}')")MB  thr $(printf '%3s' "${thr:-?}")  sys/req $(awk -v s=${sys:-0} -v r=${reqs:-1} 'BEGIN{printf "%5.2f", s/(r>0?r:1)}')  ctx/req $(awk -v c=${ctx:-0} -v r=${reqs:-1} 'BEGIN{printf "%5.2f", c/(r>0?r:1)}')  migr/req $(awk -v m=${migr:-0} -v r=${reqs:-1} 'BEGIN{printf "%5.3f", m/(r>0?r:1)}')"
}

for W in $WLIST; do echo "== workers $W (server cpus 0-$((W<NCPU?W-1:NCPU-1))) =="; for srv in $SERVERS; do run_one "$srv" "$W"; done; done
echo "DONE -> $TSV"
