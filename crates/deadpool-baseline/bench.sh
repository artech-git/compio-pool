#!/usr/bin/env bash
# bench.sh - compio-pool against tokio, one file, bash + awk + coreutils, one log to upload.
#
#   curl -fsSLO https://raw.githubusercontent.com/artech-git/compio-pool/reuseport-workers/crates/deadpool-baseline/bench.sh
#   chmod +x bench.sh
#   ./bench.sh --dry-run                  # the plan and how long it will take
#   nohup ./bench.sh > bench.out 2>&1 &   # the full matrix; survives a dropped ssh
#   tail -f bench.out
#
# When it finishes (or is interrupted: Ctrl-C and ssh drops still write it) it prints the path of
# ONE text file, bench-<host>-<time>.log: environment, a summary table, and every raw run as TSV.
# Upload that file. Nothing else is needed.
#
# It fetches and builds the code itself (git clone -b reuseport-workers, and Rust into $HOME if
# missing: no sudo, nothing installed system-wide, no kernel setting changed). The repository's
# default branch, `experimental`, is an older design; this script always uses reuseport-workers.
#
# SERVERS, one load generator (examples/load.rs), loopback:
#   tokio-default    multi-threaded work-stealing runtime, one shared listener and pool
#   tokio-per-core   tokio on epoll, one pinned current_thread runtime per core, SO_REUSEPORT
#   compio-pool      io_uring, one pinned ring per core, SO_REUSEPORT
#   compio-defer     compio-pool with IORING_SETUP_DEFER_TASKRUN
#   compio-nocoop    compio-pool with COOP_TASKRUN off
# Any server accepts a capacity suffix: compio-pool:2 is compio-pool at capacity 2 per worker,
# which forces most connections through the fd-handoff path.
#
# PLACEMENT decides what a number means on one machine. In `split` runs the server gets W
# dedicated physical cores (SMT siblings left idle) and the clients every other CPU, so no client
# shares a core, a cache or a wakeup with a server thread: the nearest one machine gets to a
# remote load generator. `shared` pins nothing but the servers' own workers (what a plain
# `cargo run` of both gives). `onecpu` forces server and clients onto one CPU.
# Each run is flagged S (server CPUs >=85% busy), C (client CPUs >=85% busy: the number mostly
# measures the load generator, read it as a lower bound) or B (the box >=90% busy).
#
# VARIATIONS
#   --suites LIST    comma list from: scale512 scale16k payload conns shared onecpu churn numa
#                    custom, or `all` (default; custom is only run when named)
#   --servers LIST   default tokio-default,tokio-per-core,compio-pool
#   --workers "1 2 4 8"   --bytes "64 512 16384"   --conns "64 256 1024"
#                    replace the built-in lists (they drive scale*, payload, conns, custom)
#   --placement split|shared|onecpu    for the custom suite
#   --capacity N     per-worker capacity for the custom suite (default 1024)
#   --reconnect      custom suite: a fresh connection per request
#   --server-cpus L --client-cpus L     explicit taskset lists instead of auto-detected topology
#   --seconds N (5)  --repeats N (3)  --settle S (0.4)  --client-ratio R (1.5)
#   --max-workers N (32)  --max-conns N (1024)
#   --quick          1 repeat of 2 s and small limits: a few-minute smoke test
#   --dry-run        print the plan and the time estimate, run nothing
#   --resume         with --out DIR: skip points already finished there
#   --out DIR        results directory (default ./bench-results/<host>-<utc>)
#   --note TEXT      free text stored in the log (instance type, what you changed, ...)
#   --repo DIR       use this checkout (default: the one this script is in, else ~/compio-pool)
#   --update         git pull --ff-only before building      --rebuild   cargo build again
#   --no-build       fail instead of building                --no-preflight  skip the start check
#
# EXAMPLES
#   ./bench.sh --suites scale512,scale16k --workers "1 2 4 8 16"
#   ./bench.sh --suites payload,conns --servers tokio-per-core,compio-pool,compio-defer
#   ./bench.sh --suites custom --placement onecpu --workers 1 --bytes "512 16384" --conns "4 16 32"
#   ./bench.sh --suites churn --repeats 5
#   ./bench.sh --suites custom --workers 4 --bytes 512 --conns 256 --servers compio-pool:1,compio-pool:2,compio-pool

set -uo pipefail

BENCH_VERSION=1
BRANCH=reuseport-workers
REPO_URL=https://github.com/artech-git/compio-pool

SUITES=all
SERVERS=tokio-default,tokio-per-core,compio-pool
RUN_SECS=5
REPEATS=3
SETTLE=0.4
CLIENT_RATIO=1.5
MAX_WORKERS=32
MAX_CONNS=1024
WORKERS_LIST=""
BYTES_LIST=""
CONNS_LIST=""
PLACEMENT=split
CAPACITY=1024
RECONNECT=0
SERVER_CPUS=""
CLIENT_CPUS=""
QUICK=0
DRY=0
RESUME=0
OUT=""
NOTE=""
REPO=""
UPDATE=0
REBUILD=0
NO_BUILD=0
NO_PREFLIGHT=0

die() { echo "bench.sh: $*" >&2; exit 1; }
usage() { sed -n '2,/^set -uo/p' "${BASH_SOURCE[0]}" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0; }

# ------------------------------------------------------------------------------- arguments

args=()
for a in "$@"; do
    if [[ $a == --*=* ]]; then args+=("${a%%=*}" "${a#*=}"); else args+=("$a"); fi
done
ORIG_ARGS="${args[*]:-}"
set -- ${args[@]+"${args[@]}"}
need_val() { [[ $# -ge 2 ]] || die "$1 needs a value"; }
while (( $# )); do
    case "$1" in
        --suites) need_val "$@"; SUITES=$2; shift 2;;
        --servers) need_val "$@"; SERVERS=$2; shift 2;;
        --workers) need_val "$@"; WORKERS_LIST=$2; shift 2;;
        --bytes) need_val "$@"; BYTES_LIST=$2; shift 2;;
        --conns) need_val "$@"; CONNS_LIST=$2; shift 2;;
        --placement) need_val "$@"; PLACEMENT=$2; shift 2;;
        --capacity) need_val "$@"; CAPACITY=$2; shift 2;;
        --reconnect) RECONNECT=1; shift;;
        --server-cpus) need_val "$@"; SERVER_CPUS=$2; shift 2;;
        --client-cpus) need_val "$@"; CLIENT_CPUS=$2; shift 2;;
        --seconds) need_val "$@"; RUN_SECS=$2; shift 2;;
        --repeats) need_val "$@"; REPEATS=$2; shift 2;;
        --settle) need_val "$@"; SETTLE=$2; shift 2;;
        --client-ratio) need_val "$@"; CLIENT_RATIO=$2; shift 2;;
        --max-workers) need_val "$@"; MAX_WORKERS=$2; shift 2;;
        --max-conns) need_val "$@"; MAX_CONNS=$2; shift 2;;
        --quick) QUICK=1; shift;;
        --dry-run) DRY=1; shift;;
        --resume) RESUME=1; shift;;
        --out) need_val "$@"; OUT=$2; shift 2;;
        --note) need_val "$@"; NOTE=$2; shift 2;;
        --repo) need_val "$@"; REPO=$2; shift 2;;
        --update) UPDATE=1; shift;;
        --rebuild) REBUILD=1; shift;;
        --no-build) NO_BUILD=1; shift;;
        --no-preflight) NO_PREFLIGHT=1; shift;;
        -h|--help) usage;;
        *) die "unknown option $1 (see --help)";;
    esac
done
[[ $PLACEMENT == split || $PLACEMENT == shared || $PLACEMENT == onecpu ]] || die "--placement must be split, shared or onecpu"
[[ $RUN_SECS =~ ^[0-9]+$ && $REPEATS =~ ^[0-9]+$ && $REPEATS -ge 1 ]] || die "--seconds and --repeats must be positive integers"
if (( QUICK )); then
    REPEATS=1; RUN_SECS=2
    (( MAX_WORKERS > 2 )) && MAX_WORKERS=2
    (( MAX_CONNS > 256 )) && MAX_CONNS=256
fi
if [[ -n $SERVER_CPUS || -n $CLIENT_CPUS ]]; then
    [[ -n $SERVER_CPUS && -n $CLIENT_CPUS ]] || die "give both --server-cpus and --client-cpus"
fi

# ------------------------------------------------------------------------------- the code

SELF=$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" 2>/dev/null && pwd || echo "$PWD")
if [[ -z $REPO ]]; then
    if [[ -f $SELF/../../examples/echo.rs ]]; then REPO=$(cd "$SELF/../.." && pwd); else REPO=$HOME/compio-pool; fi
fi
PARENT=$REPO/target/release/examples
BASE=$REPO/crates/deadpool-baseline/target/release/examples

ensure_repo() {
    if [[ ! -f $REPO/examples/echo.rs ]]; then
        command -v git >/dev/null || die "git is needed to fetch the code (or pass --repo DIR)"
        if [[ -e $REPO && -n $(ls -A "$REPO" 2>/dev/null) ]]; then
            die "$REPO exists but is not the $BRANCH checkout; move it or pass --repo"
        fi
        echo "cloning $BRANCH into $REPO"
        git clone -q -b "$BRANCH" "$REPO_URL" "$REPO" || die "git clone failed"
    elif (( UPDATE )); then
        git -C "$REPO" pull --ff-only -q || echo "warning: --update could not fast-forward"
    fi
    local f
    for f in examples/load.rs src/worker.rs crates/deadpool-baseline/examples/echo_tpc.rs; do
        [[ -f $REPO/$f ]] || die "$REPO lacks $f: not the $BRANCH branch (the default branch is an older design). Use: git clone -b $BRANCH $REPO_URL"
    done
    if [[ $SERVERS == *compio-defer* || $SERVERS == *compio-nocoop* ]] && ! grep -q -- '--defer-taskrun' "$REPO/examples/echo.rs"; then
        die "this checkout's echo example predates --defer-taskrun; run with --update --rebuild"
    fi
}

ensure_rust() {
    [[ -f $HOME/.cargo/env ]] && . "$HOME/.cargo/env"
    local need=1.95 have
    have=$(rustc --version 2>/dev/null | awk '{print $2}')
    if [[ -z $have ]] || [[ $(printf '%s\n%s\n' "$need" "$have" | sort -V | head -n1) != "$need" ]]; then
        if command -v rustup >/dev/null; then
            echo "updating Rust (${have:-none} -> stable)"; rustup update stable || die "rustup update failed"
        else
            command -v curl >/dev/null || die "curl is needed to install Rust (or install Rust >= $need yourself)"
            echo "installing Rust with rustup into \$HOME"
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal || die "rustup install failed"
        fi
        . "$HOME/.cargo/env"
    fi
    command -v cc >/dev/null || die "no C linker: install build-essential (Debian/Ubuntu) or your distro's equivalent"
    echo "rustc: $(rustc --version)"
}

ensure_binaries() {
    if (( REBUILD )) || [[ ! -x $PARENT/echo || ! -x $PARENT/load || ! -x $BASE/echo || ! -x $BASE/echo_tpc ]]; then
        (( NO_BUILD )) && die "binaries are missing and --no-build was given"
        ensure_rust
        (cd "$REPO" && cargo build --release --locked --example echo --example load) || die "cargo build failed"
        (cd "$REPO" && cargo build --release --locked --manifest-path crates/deadpool-baseline/Cargo.toml \
            --example echo --example echo_tpc) || die "cargo build (baseline) failed"
    fi
}

# ------------------------------------------------------------------------------- topology

allowed_cpus() {
    local list
    list=$(taskset -pc $$ 2>/dev/null | sed 's/.*: //')
    [[ -n $list ]] || list="0-$(( $(nproc --all) - 1 ))"
    awk -v l="$list" 'BEGIN { n = split(l, p, ","); for (i = 1; i <= n; i++) {
        if (p[i] ~ /-/) { split(p[i], r, "-"); for (c = r[1]; c <= r[2]; c++) print c } else print p[i] } }'
}

# One line per physical core, ordered by node, socket, core: "node|cpu,cpu" (SMT siblings together).
core_lines() {
    local allowed; allowed=$(allowed_cpus | tr '\n' ' ')
    if command -v lscpu >/dev/null && lscpu -p=CPU,CORE,SOCKET,NODE >/dev/null 2>&1; then
        lscpu -p=CPU,CORE,SOCKET,NODE | awk -F, -v allowed="$allowed" '
            BEGIN { n = split(allowed, a, " "); for (i = 1; i <= n; i++) ok[a[i]] = 1 }
            /^#/ { next }
            ($1 in ok) { printf "%d %d %d %d\n", ($4 == "" ? 0 : $4), ($3 == "" ? 0 : $3), ($2 == "" ? $1 : $2), $1 }' |
            sort -n -k1,1 -k2,2 -k3,3 -k4,4 |
            awk '{ k = $1 " " $2 " " $3
                   if (k != last) { if (line != "") print node "|" line; line = $4; node = $1; last = k }
                   else line = line "," $4 }
                 END { if (line != "") print node "|" line }'
    else
        allowed_cpus | awk '{ print "0|" $1 }'
    fi
}

CORE_CPUS=(); CORE_NODE=()
load_topology() {
    local l
    while IFS= read -r l; do CORE_NODE+=("${l%%|*}"); CORE_CPUS+=("${l#*|}"); done < <(core_lines)
    NCORES=${#CORE_CPUS[@]}
    NCPU=$(allowed_cpus | wc -l)
    (( NCORES > 0 )) || die "could not read the CPU topology"
}

join_by_comma() { local IFS=,; echo "$*"; }
ceil_mul() { awk -v r="$1" -v w="$2" 'BEGIN { x = r * w; printf "%d", (x == int(x)) ? x : int(x) + 1 }'; }

# place_split W FIRST -> P_SERVER P_CLIENTS P_NCLI. One thread on each of W physical cores from
# FIRST; clients on every other CPU (so the server cores' SMT siblings stay idle).
place_split() {
    local w=$1 first=${2:-0} i c srv=() cli=()
    (( first + w <= NCORES )) || return 1
    for (( i = 0; i < NCORES; i++ )); do
        IFS=, read -ra c <<<"${CORE_CPUS[i]}"
        if (( i >= first && i < first + w )); then srv+=("${c[0]}"); else cli+=("${c[@]}"); fi
    done
    P_SERVER=$(join_by_comma "${srv[@]}"); P_CLIENTS=$(join_by_comma ${cli[@]+"${cli[@]}"}); P_NCLI=${#cli[@]}
}

pow2_list() {
    local max=$1 w=1 out=()
    while (( w <= max )); do out+=("$w"); w=$(( w * 2 )); done
    (( max > 0 )) && [[ " ${out[*]} " != *" $max "* ]] && out+=("$max")
    echo "${out[*]}"
}

# ------------------------------------------------------------------------------- the plan

POINTS=()
# fields: suite|row|W|conns|bytes|server_cpus|client_cpus|n_client_cpus|box_cpus|extra|servers
add_point() { POINTS+=("$1|$2|$3|$4|$5|$6|$7|$8|$9|${10}|${11}"); }

conns_for() { local c=$(( 32 * $1 )); (( c < 64 )) && c=64; (( c > MAX_CONNS )) && c=$MAX_CONNS; echo "$c"; }

# add_split suite row W conns bytes servers [extra] [first] [clients-override]
add_split() {
    local suite=$1 row=$2 w=$3 conns=$4 bytes=$5 servers=$6 extra=${7:-} first=${8:-0} over=${9:-}
    if [[ -n $SERVER_CPUS ]]; then
        local s=() all; IFS=, read -ra all <<<"$SERVER_CPUS"
        (( w <= ${#all[@]} )) || return 0
        s=("${all[@]:0:w}")
        local ncli; ncli=$(tr ',' '\n' <<<"$CLIENT_CPUS" | wc -l)
        add_point "$suite" "$row" "$w" "$conns" "$bytes" "$(join_by_comma "${s[@]}")" "$CLIENT_CPUS" "$ncli" 0 "$extra" "$servers"
        return 0
    fi
    place_split "$w" "$first" || return 0
    if [[ -n $over ]]; then P_CLIENTS=$over; P_NCLI=$(tr ',' '\n' <<<"$over" | wc -l); fi
    (( P_NCLI >= 1 )) || { echo "note: skipping $suite / $row: no CPU left for the clients" >&2; return 0; }
    add_point "$suite" "$row" "$w" "$conns" "$bytes" "$P_SERVER" "$P_CLIENTS" "$P_NCLI" 0 "$extra" "$servers"
}
add_shared() { add_point "$1" "$2" "$3" "$4" "$5" "" "" 0 "$NCPU" "${7:-}" "$6"; }
add_onecpu() { local c0; c0=${CORE_CPUS[0]%%,*}; add_point "$1" "$2" 1 "$3" "$4" "$c0" "$c0" 1 1 "${6:-}" "$5"; }

gen_plan() {
    local want=",$SUITES,"
    [[ $SUITES == all ]] && want=",scale512,scale16k,payload,conns,shared,onecpu,churn,numa,"
    has() { [[ $want == *",$1,"* ]]; }

    # largest W whose clients still number at least CLIENT_RATIO * W
    local w best=0 need
    for (( w = 1; w <= NCORES && w <= MAX_WORKERS; w++ )); do
        place_split "$w" 0 || break
        need=$(ceil_mul "$CLIENT_RATIO" "$w"); (( need < 1 )) && need=1
        (( P_NCLI >= need )) && best=$w
    done
    WMAX=$best
    local wl wfixed
    if [[ -n $WORKERS_LIST ]]; then wl=$WORKERS_LIST; else wl=$(pow2_list "$WMAX"); fi
    wfixed=$(( WMAX < 4 ? WMAX : 4 ))
    [[ -n $WORKERS_LIST ]] && wfixed=${WORKERS_LIST%% *}
    (( wfixed >= 1 )) || wfixed=1

    local spec
    if has scale512; then for w in $wl; do add_split scale512 "W=$w" "$w" "$(conns_for "$w")" 512 "$SERVERS"; done; fi
    if has scale16k; then for w in $wl; do add_split scale16k "W=$w" "$w" "$(conns_for "$w")" 16384 "$SERVERS"; done; fi
    if has payload; then
        for b in ${BYTES_LIST:-64 512 2048 8192 16384}; do add_split payload "$b B" "$wfixed" "$(conns_for "$wfixed")" "$b" "$SERVERS"; done
    fi
    if has conns; then
        local cl seen=" "
        if [[ -n $CONNS_LIST ]]; then cl=$CONNS_LIST
        else cl="$wfixed $(( 4 * wfixed )) $(( 16 * wfixed )) $(( 64 * wfixed )) $(( 256 * wfixed ))"; fi
        for c in $cl; do
            (( c <= MAX_CONNS )) || continue
            [[ $seen == *" $c "* ]] && continue; seen+="$c "
            add_split conns "$c conns" "$wfixed" "$c" 512 "$SERVERS"
        done
    fi
    if has shared; then
        local sw; if [[ -n $WORKERS_LIST ]]; then sw=$WORKERS_LIST; else sw=$(pow2_list "$(( NCPU / 2 < MAX_WORKERS ? NCPU / 2 : MAX_WORKERS ))"); fi
        for b in ${BYTES_LIST:-512 16384}; do for w in $sw; do
            (( w >= 1 )) && add_shared shared "$b B, W=$w" "$w" "$(conns_for "$w")" "$b" "$SERVERS"
        done; done
    fi
    if has onecpu; then
        if [[ -n $BYTES_LIST || -n $CONNS_LIST ]]; then
            for b in ${BYTES_LIST:-16384}; do for c in ${CONNS_LIST:-16}; do add_onecpu onecpu "$b B x $c conns" "$c" "$b" "$SERVERS"; done; done
        else
            for c in 2 4 8 16 32; do add_onecpu onecpu "16384 B x $c conns" "$c" 16384 "$SERVERS"; done
            for b in 512 2048 4096 8192; do add_onecpu onecpu "$b B x 16 conns" 16 "$b" "$SERVERS"; done
        fi
    fi
    if has churn; then
        local cs=$SERVERS
        [[ ,$SERVERS, == *,compio-pool,* ]] && cs="$SERVERS,compio-pool:2,compio-pool:1"
        add_split churn "reconnect per request" "$wfixed" 64 512 "$cs" "--reconnect 1"
    fi
    if has numa; then
        local nodes n0 n1 i
        nodes=$(printf '%s\n' "${CORE_NODE[@]}" | sort -un | tr '\n' ' ')
        set -- $nodes
        if (( $# >= 2 )); then
            n0=$1; n1=$2
            local a0=-1 loc=() rem=() c
            for (( i = 0; i < NCORES; i++ )); do
                if [[ ${CORE_NODE[i]} == "$n0" ]]; then
                    (( a0 < 0 )) && a0=$i
                    (( i >= a0 + wfixed )) && { IFS=, read -ra c <<<"${CORE_CPUS[i]}"; loc+=("${c[@]}"); }
                elif [[ ${CORE_NODE[i]} == "$n1" ]]; then IFS=, read -ra c <<<"${CORE_CPUS[i]}"; rem+=("${c[@]}"); fi
            done
            local need; need=$(ceil_mul "$CLIENT_RATIO" "$wfixed")
            if (( ${#loc[@]} >= need && ${#rem[@]} >= need )); then
                add_split numa "clients on the server's node" "$wfixed" "$(conns_for "$wfixed")" 512 "$SERVERS" "" "$a0" "$(join_by_comma "${loc[@]}")"
                add_split numa "clients on another node" "$wfixed" "$(conns_for "$wfixed")" 512 "$SERVERS" "" "$a0" "$(join_by_comma "${rem[@]}")"
            fi
        fi
    fi
    if has custom; then
        local cw=${WORKERS_LIST:-1} cb=${BYTES_LIST:-512} cc=${CONNS_LIST:-64} extra=""
        (( RECONNECT )) && extra="--reconnect 1"
        for w in $cw; do for b in $cb; do for c in $cc; do
            local row="W=$w B=$b C=$c"
            case $PLACEMENT in
                split) add_split custom "$row" "$w" "$c" "$b" "$SERVERS" "$extra";;
                shared) add_shared custom "$row" "$w" "$c" "$b" "$SERVERS" "$extra";;
                onecpu) [[ $w == 1 ]] && add_onecpu custom "$row" "$c" "$b" "$SERVERS" "$extra";;
            esac
        done; done; done
    fi
}

plan_minutes() {
    local p f total=0 n
    for p in "${POINTS[@]}"; do
        IFS='|' read -ra f <<<"$p"; n=$(tr ',' '\n' <<<"${f[10]}" | wc -l)
        total=$(awk -v t="$total" -v n="$n" -v r="$REPEATS" -v s="$RUN_SECS" -v x="${f[9]}" 'BEGIN { print t + n * r * (s + 1.8 + (x != "" ? 1.6 : 0)) }')
    done
    awk -v t="$total" 'BEGIN { printf "%d", t / 60 + 0.5 }'
}

# ------------------------------------------------------------------------------- one run

TICK=$(getconf CLK_TCK 2>/dev/null || echo 100)
PORT_NEXT=$(( 20000 + $$ % 1000 ))
SPID=""
next_port() { PORT_NEXT=$(( PORT_NEXT + 1 )); (( PORT_NEXT > 60000 )) && PORT_NEXT=20000; echo "$PORT_NEXT"; }

kill_server() {
    [[ -n $SPID ]] || return 0
    kill "$SPID" 2>/dev/null
    local i; for i in 1 2 3 4 5 6; do kill -0 "$SPID" 2>/dev/null || break; sleep 0.5; done
    kill -9 "$SPID" 2>/dev/null; wait "$SPID" 2>/dev/null; SPID=""
}

snap_srv() {  # pid -> "utime stime voluntary involuntary"
    local pid=$1 st
    st=$(sed 's/^.*) //' "/proc/$pid/stat" 2>/dev/null) || return 1
    [[ -n $st ]] || return 1
    set -- $st
    local ctx; ctx=$(cat /proc/"$pid"/task/*/status 2>/dev/null | awk '/^voluntary_ctxt_switches/ { v += $2 } /^nonvoluntary_ctxt_switches/ { n += $2 } END { print v + 0, n + 0 }')
    echo "${12} ${13} $ctx"
}
snap_sys() { awk '/^cpu / { printf "%s %s %s %s %s %s %s %s ", $2, $3, $4, $5, $6, $7, $8, $9 } /^ctxt/ { print $2 }' /proc/stat; }
child_cpu() { awk 'NR == 2 { for (i = 1; i <= 2; i++) { split($i, a, "m"); sub(/s$/, "", a[2]); t += a[1] * 60 + a[2] } printf "%.3f", t }' "$1"; }

# Sets ROW (one TSV line). Args: suite row spec rep W conns bytes scpus ccpus ncli box extra capture
run_one() {
    local suite=$1 row=$2 spec=$3 rep=$4 w=$5 conns=$6 bytes=$7 scpus=$8 ccpus=$9 ncli=${10} box=${11} extra=${12} capture=${13}
    local base=${spec%%:*} cap=$CAPACITY label=$spec bin sargs=()
    [[ $spec == *:* ]] && { cap=${spec#*:}; label="${base}@cap${cap}"; }
    local common
    common=$(printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' "$suite" "$row" "$label" "$rep" "$w" "$conns" "$bytes" "$cap" "${scpus:--}" "${ccpus:--}")
    fail() { ROW="$common"$'\t'"$1"; local k; for k in {1..20}; do ROW+=$'\t'; done; }
    case $base in
        tokio-default) bin=$BASE/echo;;
        tokio-per-core) bin=$BASE/echo_tpc;;
        compio-pool) bin=$PARENT/echo;;
        compio-defer) bin=$PARENT/echo; sargs=(--defer-taskrun);;
        compio-nocoop) bin=$PARENT/echo; sargs=(--no-coop-taskrun);;
        *) fail "unknown server $base"; return;;
    esac
    local port; port=$(next_port)
    local cmd=("$bin" "127.0.0.1:$port" "$cap" "$w" ${sargs[@]+"${sargs[@]}"})
    [[ -n $scpus ]] && cmd=(taskset -c "$scpus" "${cmd[@]}")
    "${cmd[@]}" >"$TMP/server.out" 2>"$TMP/server.err" &
    SPID=$!
    local i up=0
    for (( i = 0; i < 100; i++ )); do
        (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && { up=1; break; }
        kill -0 "$SPID" 2>/dev/null || break
        sleep 0.05
    done
    if (( ! up )); then
        fail "server did not start: $(tr '\t\n' '  ' <"$TMP/server.err" | cut -c1-200)"; kill_server; return
    fi
    sleep 0.4

    local lcmd=("$PARENT/load" "127.0.0.1:$port" --conns "$conns" --seconds "$RUN_SECS" --bytes "$bytes" $extra)
    [[ -n $ccpus ]] && lcmd=(taskset -c "$ccpus" "${lcmd[@]}")
    local limit=$(( RUN_SECS + 30 + conns / 50 )) out rc S0 S1 X0 X1
    S0=$(snap_srv "$SPID"); X0=$(snap_sys)
    times >"$TMP/t0"
    out=$(timeout -k 3 "$limit" "${lcmd[@]}" 2>&1); rc=$?
    times >"$TMP/t1"
    S1=$(snap_srv "$SPID"); X1=$(snap_sys)
    local stats=""
    if (( capture )); then sleep 1.2; stats=$(grep '^active' "$TMP/server.out" | tail -1 | tr -s ' \t' ' '); fi
    kill_server

    if (( rc == 124 || rc == 137 )); then fail "load did not finish ($conns conns): a client probably failed to connect (backlog or fd limit)"; return; fi
    if [[ -z $S1 ]]; then fail "server died during the run: $(tr '\t\n' '  ' <"$TMP/server.err" | cut -c1-160)"; return; fi
    local parsed
    parsed=$(awk '/^conns / { for (i = 1; i <= NF; i++) if ($i == "failed") f = $(i + 1) }
                  /^requests / { reqs = $2; r = $3; gsub(/[()]/, "", r); rps = r }
                  /^latency us/ { p50 = $4; p90 = $6; p99 = $8; p999 = $10; mx = $12 }
                  END { print reqs + 0, rps + 0, p50 + 0, p90 + 0, p99 + 0, p999 + 0, mx + 0, f + 0 }' <<<"$out")
    set -- $parsed
    if (( $1 == 0 )); then fail "no result from load: $(tr '\t\n' '  ' <<<"$out" | cut -c1-160)"; return; fi
    local c0 c1; c0=$(child_cpu "$TMP/t0"); c1=$(child_cpu "$TMP/t1")
    ROW=$(awk -v common="$common" -v reqs="$1" -v rps="$2" -v p50="$3" -v p90="$4" -v p99="$5" -v p999="$6" -v mx="$7" -v failed="$8" \
        -v s0="$S0" -v s1="$S1" -v x0="$X0" -v x1="$X1" -v c0="$c0" -v c1="$c1" -v tick="$TICK" \
        -v w="$w" -v ncli="$ncli" -v box="$box" -v scpus="$scpus" -v ccpus="$ccpus" -v stats="$stats" 'BEGIN {
            split(s0, a, " "); split(s1, b, " "); split(x0, p, " "); split(x1, q, " ")
            su = (b[1] - a[1]) * 1e6 / tick / reqs; ss = (b[2] - a[2]) * 1e6 / tick / reqs
            vcs = (b[3] - a[3]) / reqs; icsw = (b[4] - a[4]) / reqs
            cli = (c1 - c0) * 1e6 / reqs; syscs = (q[9] - p[9]) / reqs
            tot = 0; for (i = 1; i <= 8; i++) tot += q[i] - p[i]; if (tot == 0) tot = 1
            idle = 100 * (q[4] - p[4]) / tot; steal = 100 * (q[8] - p[8]) / tot
            su_used = (su + ss) * rps / 1e6; cli_used = cli * rps / 1e6
            f = ""
            if (ncli == 0 || scpus == ccpus) { if (su_used + cli_used >= 0.9 * box) f = "B" }
            else { if (su_used >= 0.85 * w) f = f "S"; if (ncli > 0 && cli_used >= 0.85 * ncli) f = f "C" }
            printf "%s\tok\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.3f\t%.3f\t%.4f\t%.4f\t%.3f\t%.3f\t%.1f\t%d\t%.3f\t%.3f\t%s\t%.2f\t%s",
                common, rps, reqs, p50, p90, p99, p999, mx, su, ss, vcs, icsw, cli, syscs, idle, failed, su_used, cli_used, f, steal, stats }')
}

COLS=(suite row server repeat workers conns bytes capacity server_cpus client_cpus status rps reqs p50_us p90_us p99_us p999_us max_us
      srv_user_us_per_req srv_sys_us_per_req srv_vol_cs_per_req srv_invol_cs_per_req cli_cpu_us_per_req sys_cs_per_req
      idle_pct failed_clients srv_cpus_used cli_cpus_used flag steal_pct server_stats)
TSV_HEADER="#$(IFS=$'\t'; echo "${COLS[*]}")"
fld() { awk -F'\t' -v n="$2" '{ print $n }' <<<"$1"; }
show_row() { awk -F'\t' '{ if ($11 == "ok") printf "%s  %s req/s  p50 %sus  p99 %sus  %s", $3, $12, $14, $16, $29; else printf "%s  ERROR %s", $3, $11 }' <<<"$1"; }

# ------------------------------------------------------------------------------- environment

write_env() {
    {
        echo "bench.sh version:  $BENCH_VERSION"
        echo "date (UTC):        $(date -u +%FT%TZ)"
        echo "host:              $(hostname)"
        echo "note:              ${NOTE:-}"
        echo "command line:      $ORIG_ARGS"
        echo "kernel:            $(uname -srvm)"
        echo "virtualization:    $(systemd-detect-virt 2>/dev/null || echo unknown)"
        echo "cpus allowed:      $NCPU ($NCORES physical cores; threads/core $(awk -F, 'NR==1 { print NF }' < <(printf '%s\n' "${CORE_CPUS[0]}")); NUMA nodes $(printf '%s\n' "${CORE_NODE[@]}" | sort -u | wc -l))"
        echo "max split workers: $WMAX (client ratio $CLIENT_RATIO)"
        echo "memory (MiB):      $(free -m 2>/dev/null | awk '/^Mem:/ { print "total " $2 ", available " $7 }')"
        echo "io_uring_disabled: $(cat /proc/sys/kernel/io_uring_disabled 2>/dev/null || echo n/a)"
        echo "cpu governor:      $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
        echo "somaxconn:         $(cat /proc/sys/net/core/somaxconn 2>/dev/null)   tcp_tw_reuse: $(cat /proc/sys/net/ipv4/tcp_tw_reuse 2>/dev/null)   nofile: $(ulimit -n)"
        echo "rustc:             $(rustc --version 2>/dev/null || echo n/a)"
        echo "git:               $(git -C "$REPO" rev-parse --abbrev-ref HEAD 2>/dev/null)@$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null)$( [[ -n $(git -C "$REPO" status --porcelain --untracked-files=no 2>/dev/null) ]] && echo ' (uncommitted changes)')"
        local d
        for d in /sys/devices/system/cpu/cpu0/cache/index*; do
            [[ -d $d ]] && echo "cache cpu0:        L$(cat "$d/level") $(cat "$d/type") $(cat "$d/size")"
        done
        echo "vulnerability mitigations (they change syscall cost):"
        for d in /sys/devices/system/cpu/vulnerabilities/*; do [[ -f $d ]] && echo "  $(basename "$d"): $(cat "$d")"; done
        echo
        echo "--- lscpu ---"; lscpu 2>/dev/null
    } >"$OUT/env.txt"
}

# ------------------------------------------------------------------------------- the summary

summarize() {
    echo "Legend: S = server CPUs >=85% busy (server-bound). C = client CPUs >=85% busy (client-bound:"
    echo "the number mostly measures the load generator; read it as a lower bound). B = whole box"
    echo ">=90% busy (shared / onecpu placement). No flag = neither saturated. '+-N%' is half the"
    echo "min-max spread over repeats. Each cell is the median run by req/s."
    awk -F'\t' '
        function key(s, r, v) { return s SUBSEP r SUBSEP v }
        function ratio(a, b) { return (a + 0 > 0 && b + 0 > 0) ? sprintf("%.2fx", a / b) : "-" }
        /^#/ || NF < 12 { next }
        $11 != "ok" { ne++; err[ne] = $1 " / " $2 " / " $3 " (run " $4 "): " $11; next }
        {
            s = $1; r = $2; v = $3
            if (!(s in ss)) { ss[s] = 1; sord[++ns] = s }
            if (!((s, r) in rs)) { rs[s, r] = 1; nr[s]++; rord[s, nr[s]] = r }
            if (!((s, v) in vs)) { vs[s, v] = 1; nv[s]++; vord[s, nv[s]] = v }
            k = key(s, r, v); c = ++cnt[k]; RPS[k, c] = $12; LN[k, c] = $0; FAIL[k] += $26
        }
        END {
            for (k in cnt) {
                n = cnt[k]
                for (i = 1; i <= n; i++) ix[i] = i
                for (i = 2; i <= n; i++) { x = ix[i]; j = i - 1
                    while (j >= 1 && RPS[k, ix[j]] + 0 > RPS[k, x] + 0) { ix[j + 1] = ix[j]; j-- }
                    ix[j + 1] = x }
                m = ix[int((n + 1) / 2)]; split(LN[k, m], F, "\t")
                M_rps[k] = F[12]; M_flag[k] = F[29]; M_p50[k] = F[14]; M_p99[k] = F[16]
                M_srv[k] = F[19] + F[20]; M_cli[k] = F[23]
                N[k] = n; LO[k] = RPS[k, ix[1]]; HI[k] = RPS[k, ix[n]]
            }
            for (si = 1; si <= ns; si++) {
                s = sord[si]; printf "\n=== %s ===\n", s
                arch = ((s, "tokio-default") in vs) && ((s, "tokio-per-core") in vs)
                back = ((s, "tokio-per-core") in vs) && ((s, "compio-pool") in vs)
                for (blk = 1; blk <= 3; blk++) {
                    printf "\n%s\n", (blk == 1 ? "median req/s [flag] +-spread" : (blk == 2 ? "p50 / p99 latency, us" : "CPU us per request: server(user+sys) / client"))
                    printf "%-30s", ""
                    for (vi = 1; vi <= nv[s]; vi++) printf "%-27s", vord[s, vi]
                    if (blk == 1 && arch) printf "%-13s", "pc/default"
                    if (blk == 1 && back) printf "%-13s", "compio/pc"
                    printf "\n"
                    for (ri = 1; ri <= nr[s]; ri++) {
                        r = rord[s, ri]; printf "%-30s", r
                        for (vi = 1; vi <= nv[s]; vi++) {
                            k = key(s, r, vord[s, vi])
                            if (!(k in cnt)) { printf "%-27s", "-"; continue }
                            if (blk == 1) {
                                cell = M_rps[k] (M_flag[k] != "" ? " [" M_flag[k] "]" : "")
                                if (N[k] > 1 && M_rps[k] > 0) cell = cell sprintf(" +-%d%%", (HI[k] - LO[k]) / 2 / M_rps[k] * 100)
                            } else if (blk == 2) cell = sprintf("%.0f / %.0f", M_p50[k], M_p99[k])
                            else cell = sprintf("%.1f / %.1f", M_srv[k], M_cli[k])
                            printf "%-27s", cell
                        }
                        if (blk == 1 && arch) printf "%-13s", ratio(M_rps[key(s, r, "tokio-per-core")], M_rps[key(s, r, "tokio-default")])
                        if (blk == 1 && back) printf "%-13s", ratio(M_rps[key(s, r, "compio-pool")], M_rps[key(s, r, "tokio-per-core")])
                        printf "\n"
                    }
                }
            }
            nf = 0
            for (k in FAIL) if (FAIL[k] > 0) { split(k, P, SUBSEP); w[++nf] = P[1] " / " P[2] " / " P[3] ": " FAIL[k] " failed clients" }
            if (nf) { printf "\nWARNING: runs with failed clients (results unreliable):\n"; for (i = 1; i <= nf; i++) print "  " w[i] }
            if (ne) { printf "\nERRORS (runs that produced no number):\n"; for (i = 1; i <= ne; i++) print "  " err[i] }
        }' "$OUT/runs.tsv"
}

FINALIZED=0
finalize() {
    (( FINALIZED )) && return; FINALIZED=1
    kill_server
    [[ -n ${TMP:-} ]] && rm -rf "$TMP"
    [[ -n ${OUT:-} && -s ${OUT:-/nonexistent}/runs.tsv ]] || return 0
    summarize >"$OUT/summary.txt" 2>&1
    local final="$OUT/bench-$(hostname)-$STAMP.log"
    {
        echo "##### bench.sh $BENCH_VERSION - upload this file #####"
        echo; echo "##### ENVIRONMENT #####"; cat "$OUT/env.txt"
        echo; echo "##### SUMMARY #####"; cat "$OUT/summary.txt"
        echo; echo "##### RAW RUNS (TSV, one line per run; the columns are in the header line) #####"; cat "$OUT/runs.tsv"
        echo; echo "##### PROGRESS LOG #####"; cat "$OUT/run.log"
    } >"$final"
    cp "$final" "./$(basename "$final")" 2>/dev/null
    echo
    echo "================================================================"
    echo " Upload this file:  $PWD/$(basename "$final")  ($(wc -c <"$final") bytes)"
    echo " (also in $OUT/)"
    echo "================================================================"
}

# ------------------------------------------------------------------------------- main

command -v taskset >/dev/null || die "taskset is required (util-linux)"
command -v awk >/dev/null && command -v timeout >/dev/null || die "awk and timeout (coreutils) are required"
[[ $(uname -s) == Linux ]] || die "Linux only"

(( DRY )) || ensure_repo
load_topology
(( DRY )) || ensure_binaries
gen_plan
(( ${#POINTS[@]} > 0 )) || die "the plan is empty (suite names, worker counts or CPU lists leave nothing to run)"

echo "$NCPU CPUs, $NCORES physical cores, up to $WMAX dedicated server cores at client ratio $CLIENT_RATIO"
echo "plan: ${#POINTS[@]} points, repeats $REPEATS x ${RUN_SECS}s, about $(plan_minutes) minutes"
printf '  %-9s %-34s %-4s %-6s %-6s %-14s %-18s %s\n' suite row W conns bytes server-cpus client-cpus servers
for p in "${POINTS[@]}"; do
    IFS='|' read -ra f <<<"$p"
    printf '  %-9s %-34s %-4s %-6s %-6s %-14.14s %-18.18s %s\n' "${f[0]}" "${f[1]}" "${f[2]}" "${f[3]}" "${f[4]}" "${f[5]:--}" "${f[6]:--}" "${f[10]}"
done
(( DRY )) && { FINALIZED=1; exit 0; }

ulimit -n "$(ulimit -Hn)" 2>/dev/null
[[ $(ulimit -n) == unlimited ]] || (( $(ulimit -n) >= MAX_CONNS * 2 + 256 )) 2>/dev/null || echo "warning: open-file limit $(ulimit -n) is low for --max-conns $MAX_CONNS"

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
[[ -n $OUT ]] || OUT=$PWD/bench-results/$(hostname)-$STAMP
mkdir -p "$OUT" || die "cannot create $OUT"
OUT=$(cd "$OUT" && pwd)
TMP=$(mktemp -d)
trap finalize EXIT
trap 'echo "interrupted: writing what finished so far"; exit 130' INT TERM HUP

(( RESUME )) && [[ -f $OUT/done.keys ]] || : >"$OUT/done.keys"
if [[ ! -s $OUT/runs.tsv ]]; then printf '%s\n' "$TSV_HEADER" >"$OUT/runs.tsv"; fi
[[ -f $OUT/env.txt ]] && (( RESUME )) || write_env
log() { echo "$*" | tee -a "$OUT/run.log"; }
log "results directory: $OUT"

if (( ! NO_PREFLIGHT )); then
    log "preflight: starting each server once ..."
    declare -A seen_pf=(); all_specs=()
    for p in "${POINTS[@]}"; do
        IFS='|' read -ra f <<<"$p"; IFS=, read -ra ss <<<"${f[10]}"
        for spec in "${ss[@]}"; do [[ -n ${seen_pf[$spec]:-} ]] || { seen_pf[$spec]=1; all_specs+=("$spec"); }; done
    done
    seen_pf=()
    for spec in "${all_specs[@]}"; do
        [[ -n ${seen_pf[$spec]:-} ]] && continue; seen_pf[$spec]=1
        run_one preflight p "$spec" 0 1 8 512 "" "" 0 "$NCPU" "" 0
        st=$(fld "$ROW" 11)
        if [[ $st == ok ]]; then log "  $spec: ok ($(fld "$ROW" 12) req/s)"
        else
            log "  $spec: FAILED: $st"
            hint=""; [[ $spec == compio* ]] && hint=" (compio-pool needs io_uring: /proc/sys/kernel/io_uring_disabled must be 0 and no seccomp filter may block it)"
            die "preflight failed for $spec: $st$hint"
        fi
    done
fi

T_START=$(date +%s); idx=0; total=${#POINTS[@]}
for pt in "${POINTS[@]}"; do
    idx=$(( idx + 1 ))
    IFS='|' read -ra f <<<"$pt"
    suite=${f[0]}; row=${f[1]}; w=${f[2]}; conns=${f[3]}; bytes=${f[4]}; scpus=${f[5]}; ccpus=${f[6]}; ncli=${f[7]}; box=${f[8]}; extra=${f[9]}
    if (( RESUME )) && grep -qxF "$suite|$row" "$OUT/done.keys" 2>/dev/null; then log "[$idx/$total] $suite / $row: already done, skipping"; continue; fi
    IFS=, read -ra specs <<<"${f[10]}"
    capture=0; [[ $suite == churn ]] && capture=1
    log "[$idx/$total] $suite / $row   W=$w conns=$conns bytes=$bytes server=${scpus:-any} clients=${ccpus:-any}"
    nspec=${#specs[@]}
    for (( rep = 1; rep <= REPEATS; rep++ )); do
        for (( j = 0; j < nspec; j++ )); do
            spec=${specs[$(( (j + rep - 1) % nspec ))]}     # rotate: drift lands on every server
            run_one "$suite" "$row" "$spec" "$rep" "$w" "$conns" "$bytes" "$scpus" "$ccpus" "$ncli" "$box" "$extra" "$capture"
            printf '%s\n' "$ROW" >>"$OUT/runs.tsv"
            log "    run $rep  $(show_row "$ROW")"
            sleep "$(awk -v s="$SETTLE" -v x="$extra" 'BEGIN { print (x != "") ? s * 4 : s }')"
        done
    done
    echo "$suite|$row" >>"$OUT/done.keys"
    el=$(( $(date +%s) - T_START ))
    log "    point done; $(( el / 60 )) min elapsed, about $(( el * (total - idx) / idx / 60 )) min left"
done
log "finished in $(( ($(date +%s) - T_START) / 60 )) min"
exit 0
