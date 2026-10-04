#!/usr/bin/env bash
# redisbench.sh - compio-redis (compio-pool) vs tokio+deadpool, both fronting a
# real redis-server over the same line protocol, driven by examples/kvload.rs.
#
# Two sweeps:
#   single  - all proxy workers share ONE redis (realistic, backend-bound).
#             Measures proxy latency + CPU-per-request; throughput plateaus when
#             the single redis core saturates (flag R).
#   sharded - one redis per proxy worker, each on its own core and port; the
#             client fans across them. Throughput scales with cores because no
#             single redis is the bottleneck.
#
# Servers:
#   compio-pool      io_uring, thread-per-core, compio-pool Resource pool
#   tokio-percore    tokio current_thread per core, SO_REUSEPORT, deadpool pool
#   tokio-default    tokio multi-thread work-stealing, shared listener + pool
#
# Metrics per run: req/s, latency p50/p90/p99/p99.9/max, proxy user+sys CPU per
# request and cores-used, redis cores-used, voluntary/involuntary ctx switches
# per request, peak proxy RSS, and an S/R/C saturation flag.
#
# Writes ONE uploadable log: redisbench-<host>-<utc>.log  (env + summary + TSV).

set -uo pipefail

SECONDS_PER=4
REPEATS=5
SWEEP=both              # single | sharded | both
SERVERS=compio-pool,tokio-percore,tokio-default
OUT=""
NOTE=""
QUICK=0
REDIS_BIN="$HOME/redis-stable/src/redis-server"
REPO="$HOME/compio-pool"
KEYS=128

die() { echo "redisbench: $*" >&2; exit 1; }

while (( $# )); do
    case "$1" in
        --seconds) SECONDS_PER=$2; shift 2;;
        --repeats) REPEATS=$2; shift 2;;
        --sweep) SWEEP=$2; shift 2;;
        --servers) SERVERS=$2; shift 2;;
        --out) OUT=$2; shift 2;;
        --note) NOTE=$2; shift 2;;
        --redis) REDIS_BIN=$2; shift 2;;
        --repo) REPO=$2; shift 2;;
        --keys) KEYS=$2; shift 2;;
        --quick) QUICK=1; shift;;
        *) die "unknown option $1";;
    esac
done
(( QUICK )) && { REPEATS=2; SECONDS_PER=3; }

COMPIO_BIN="$REPO/crates/compio-redis/target/release/examples/server"
TOKIO_BIN="$REPO/crates/deadpool-baseline/target/release/examples/redis_proxy"
KVLOAD_BIN="$REPO/target/release/examples/kvload"
TICK=$(getconf CLK_TCK 2>/dev/null || echo 100)

for b in "$REDIS_BIN" "$COMPIO_BIN" "$TOKIO_BIN" "$KVLOAD_BIN"; do
    [[ -x $b ]] || die "missing binary: $b (build first)"
done
command -v taskset >/dev/null || die "taskset required"

# --------------------------------------------------------------------- topology
# One physical core per line: "node|cpu,cpu" (SMT siblings together), by node.
core_lines() {
    lscpu -p=CPU,CORE,SOCKET,NODE | awk -F, '
        /^#/ { next }
        { printf "%d %d %d %d\n", ($4==""?0:$4), ($3==""?0:$3), ($2==""?$1:$2), $1 }' |
        sort -n -k1,1 -k2,2 -k3,3 -k4,4 |
        awk '{ k=$1" "$2" "$3
               if (k!=last){ if(line!="") print node"|"line; line=$4; node=$1; last=k }
               else line=line","$4 }
             END{ if(line!="") print node"|"line }'
}
PCORE=(); PNODE=()
while IFS= read -r l; do PNODE+=("${l%%|*}"); PCORE+=("${l#*|}"); done < <(core_lines)
NCORES=${#PCORE[@]}
(( NCORES > 0 )) || die "could not read CPU topology"
t0_of() { echo "${1%%,*}"; }                         # first SMT thread of a core spec

# cpus (both threads) of physical cores [a..b), comma-joined
cpus_of_range() {
    local a=$1 b=$2 i out=""
    for (( i=a; i<b && i<NCORES; i++ )); do out="${out:+$out,}${PCORE[i]}"; done
    echo "$out"
}
count_cpus() { tr ',' '\n' <<<"$1" | grep -c . ; }

# --------------------------------------------------------------------- /proc
cpu_ticks() {  # pids... -> sum(utime+stime) in ticks
    local tot=0 pid st arr
    for pid in "$@"; do
        [[ -r /proc/$pid/stat ]] || continue
        st=$(sed 's/^.*) //' "/proc/$pid/stat" 2>/dev/null) || continue
        read -ra arr <<<"$st"; tot=$(( tot + arr[11] + arr[12] ))
    done
    echo "$tot"
}
ctx_switches() {  # pids... -> "vol invol" summed over all threads
    local pid; { for pid in "$@"; do cat /proc/"$pid"/task/*/status 2>/dev/null; done; } |
        awk '/^voluntary_ctxt_switches/{v+=$2} /^nonvoluntary_ctxt_switches/{n+=$2} END{print v+0, n+0}'
}
rss_kb() {  # pids... -> sum VmHWM (peak RSS) kB
    local tot=0 pid v
    for pid in "$@"; do v=$(awk '/^VmHWM:/{print $2}' /proc/"$pid"/status 2>/dev/null); tot=$(( tot + ${v:-0} )); done
    echo "$tot"
}
snap_sys() { awk '/^cpu /{printf "%s %s %s %s %s %s %s %s",$2,$3,$4,$5,$6,$7,$8,$9}' /proc/stat; }
child_cpu() { awk 'NR==2{for(i=1;i<=2;i++){split($i,a,"m");sub(/s$/,"",a[2]);t+=a[1]*60+a[2]}printf "%.3f",t}' "$1"; }

# --------------------------------------------------------------------- processes
PIDS=()   # everything we start; cleaned up on exit/interrupt
track() { PIDS+=("$1"); }
wait_port() {  # host port -> 0 when accepting
    local i; for (( i=0; i<200; i++ )); do
        (exec 3<>"/dev/tcp/127.0.0.1/$2") 2>/dev/null && { exec 3>&- 3<&-; return 0; }
        kill -0 "$1" 2>/dev/null || return 1
        sleep 0.05
    done
    return 1
}
start_redis() {  # port cpu -> echoes pid, pinned via taskset
    local port=$1 cpu=$2
    taskset -c "$cpu" "$REDIS_BIN" --port "$port" --bind 127.0.0.1 --save '' \
        --appendonly no --protected-mode no --maxclients 20000 --io-threads 1 \
        >/dev/null 2>"$TMP/redis-$port.err" &
    local pid=$!; track "$pid"
    wait_port "$pid" "$port" || { echo "redis on $port failed: $(head -c200 "$TMP/redis-$port.err")" >&2; return 1; }
    echo "$pid"
}
start_proxy() {  # server port cpus workers redis_url -> echoes pid
    local server=$1 port=$2 cpus=$3 workers=$4 url=$5 pid
    case $server in
        compio-pool)   REDIS_URL="$url" taskset -c "$cpus" "$COMPIO_BIN" "127.0.0.1:$port" "$CAP" "$workers" >"$TMP/proxy-$port.out" 2>"$TMP/proxy-$port.err" & ;;
        tokio-percore) REDIS_URL="$url" taskset -c "$cpus" "$TOKIO_BIN" "127.0.0.1:$port" "$CAP" "$workers" percore >"$TMP/proxy-$port.out" 2>"$TMP/proxy-$port.err" & ;;
        tokio-default) REDIS_URL="$url" taskset -c "$cpus" "$TOKIO_BIN" "127.0.0.1:$port" "$CAP" "$workers" default >"$TMP/proxy-$port.out" 2>"$TMP/proxy-$port.err" & ;;
        *) echo "unknown server $server" >&2; return 1;;
    esac
    pid=$!; track "$pid"
    wait_port "$pid" "$port" || { echo "$server on $port failed: $(head -c200 "$TMP/proxy-$port.err")" >&2; return 1; }
    echo "$pid"
}
stop_pids() {
    local p; for p in "$@"; do kill "$p" 2>/dev/null; done
    local i; for i in 1 2 3 4 5 6; do for p in "$@"; do kill -0 "$p" 2>/dev/null && break; done || break; sleep 0.3; done
    for p in "$@"; do kill -9 "$p" 2>/dev/null; wait "$p" 2>/dev/null; done
}
cleanup() { (( ${#PIDS[@]} )) && stop_pids "${PIDS[@]}"; PIDS=(); [[ -n ${TMP:-} ]] && rm -rf "$TMP"; }

# --------------------------------------------------------------------- one run
# Emits one TSV line to runs.tsv. Globals set by caller:
#   PROXY_PIDS REDIS_PIDS ADDRS CCPUS NCLI SRV_CORES NREDIS
run_one() {
    local sweep=$1 server=$2 workload=$3 w=$4 conns=$5 value=$6 pipeline=$7 rep=$8
    local proxy_pids="${PROXY_PIDS[*]}" redis_pids="${REDIS_PIDS[*]}"
    local S0 S1 R0 R1 X0 X1 C0 C1 CS0 CS1 out rc
    S0=$(cpu_ticks $proxy_pids); R0=$(cpu_ticks $redis_pids); X0=$(snap_sys); CS0=$(ctx_switches $proxy_pids)
    times >"$TMP/t0"
    out=$(taskset -c "$CCPUS" "$KVLOAD_BIN" "$ADDRS" --conns "$conns" --seconds "$SECONDS_PER" \
            --workload "$workload" --value "$value" --keys "$KEYS" --pipeline "$pipeline" 2>&1); rc=$?
    times >"$TMP/t1"
    S1=$(cpu_ticks $proxy_pids); R1=$(cpu_ticks $redis_pids); X1=$(snap_sys); CS1=$(ctx_switches $proxy_pids)
    C0=$(child_cpu "$TMP/t0"); C1=$(child_cpu "$TMP/t1")
    local rss; rss=$(rss_kb $proxy_pids)

    local parsed
    parsed=$(awk '/^conns /{for(i=1;i<=NF;i++){if($i=="failed")f=$(i+1); if($i=="mismatches")m=$(i+1)}}
                  /^requests /{reqs=$2; r=$3; gsub(/[()]/,"",r); rps=r}
                  /^latency us/{p50=$4;p90=$6;p99=$8;p999=$10;mx=$12}
                  END{print reqs+0, rps+0, p50+0, p90+0, p99+0, p999+0, mx+0, f+0, m+0}' <<<"$out")
    set -- $parsed
    local reqs=$1 rps=$2 p50=$3 p90=$4 p99=$5 p999=$6 mx=$7 failed=$8 mism=$9
    if (( reqs == 0 )); then
        printf '%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\tERROR\t%s\n' \
            "$sweep" "$server" "$workload" "$w" "$conns" "$value" "$pipeline" "$rep" \
            "$(tr '\t\n' '  ' <<<"$out" | cut -c1-160)" >>"$OUT/runs.tsv"
        return
    fi
    ROW=$(awk -v sweep="$sweep" -v server="$server" -v wl="$workload" -v w="$w" -v conns="$conns" \
        -v value="$value" -v pipe="$pipeline" -v rep="$rep" -v reqs="$reqs" -v rps="$rps" \
        -v p50="$p50" -v p90="$p90" -v p99="$p99" -v p999="$p999" -v mx="$mx" -v failed="$failed" -v mism="$mism" \
        -v s0="$S0" -v s1="$S1" -v r0="$R0" -v r1="$R1" -v x0="$X0" -v x1="$X1" -v c0="$C0" -v c1="$C1" \
        -v cs0="$CS0" -v cs1="$CS1" -v rss="$rss" -v tick="$TICK" -v secs="$SECONDS_PER" \
        -v srvcores="$SRV_CORES" -v nredis="$NREDIS" -v ncli="$NCLI" 'BEGIN{
            su=(s1-s0)*1e6/tick/reqs; ss2=0
            srv_us=(s1-s0)*1e6/tick/reqs
            srv_cpu=(s1-s0)/tick/secs
            red_cpu=(r1-r0)/tick/secs
            split(cs0,a," "); split(cs1,b," ")
            vcs=(b[1]-a[1])/reqs; icsw=(b[2]-a[2])/reqs
            split(x0,p," "); split(x1,q," "); tot=0; for(i=1;i<=8;i++)tot+=q[i]-p[i]; if(tot==0)tot=1
            idle=100*(q[4]-p[4])/tot
            cli_cpu=(c1-c0)/secs - srv_cpu - red_cpu; if(cli_cpu<0)cli_cpu=0
            rssmb=rss/1024
            flag=""
            if(srv_cpu>=0.85*srvcores)flag=flag"S"
            if(red_cpu>=0.85*nredis)flag=flag"R"
            if(ncli>0 && cli_cpu>=0.85*ncli)flag=flag"C"
            printf "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\tok\t%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%.1f\t%.3f\t%.2f\t%.2f\t%.2f\t%.3f\t%.3f\t%.1f\t%.0f\t%d\t%d\t%s",
                sweep,server,wl,w,conns,value,pipe,rep,rps,reqs,p50,p90,p99,p999,mx,
                srv_us,srv_cpu,red_cpu,cli_cpu,vcs,icsw,idle,rssmb,mism,failed,flag }')
    printf '%s\n' "$ROW" >>"$OUT/runs.tsv"
    printf '    %-13s %-5s W=%-2s rps %-8s p50 %-6s p99 %-7s srvcpu %-5s redcpu %-5s %s\n' \
        "$server" "$workload" "$w" "$(fld "$ROW" 10)" "$(fld "$ROW" 12)" "$(fld "$ROW" 14)" \
        "$(fld "$ROW" 18)" "$(fld "$ROW" 19)" "$(fld "$ROW" 27)" | tee -a "$OUT/run.log"
}
fld() { awk -F'\t' -v n="$2" '{print $n}' <<<"$1"; }

COLS=(sweep server workload workers conns value pipeline repeat status rps reqs p50_us p90_us p99_us p999_us max_us
      srv_us_per_req srv_cpus_used redis_cpus_used cli_cpus_used vol_cs_per_req invol_cs_per_req idle_pct rss_mb mismatches failed flag)

# --------------------------------------------------------------------- matrix
conns_for() { local c=$(( 32 * $1 )); (( c < 64 )) && c=64; (( c > 1024 )) && c=1024; echo "$c"; }

# workload rows: "workload value pipeline"
base_workloads() { printf 'get 16 1\nset 16 1\nmix 16 1\n'; }
extra_workloads() { printf 'ping 16 1\nget 256 1\nget 4096 1\nget 16 8\n'; }

sweep_single() {
    local redis_cpu port=7000 rport=6400 redis_pid
    redis_cpu=$(t0_of "${PCORE[0]}")
    log "single sweep: redis on cpu $redis_cpu (1 core), proxy on W cores, clients on the rest"
    redis_pid=$(start_redis "$rport" "$redis_cpu") || die "redis failed"
    REDIS_PIDS=("$redis_pid"); NREDIS=1
    local url="redis://127.0.0.1:$rport"
    local ws="$1" srv w scpus conns
    for w in $ws; do
        (( 1 + w <= NCORES )) || { log "  skip W=$w (needs $((1+w)) cores, have $NCORES)"; continue; }
        scpus=""; local i
        for (( i=1; i<=w; i++ )); do scpus="${scpus:+$scpus,}$(t0_of "${PCORE[i]}")"; done
        CCPUS=$(cpus_of_range $((1+w)) "$NCORES"); NCLI=$(count_cpus "$CCPUS")
        (( NCLI >= 1 )) || { log "  skip W=$w (no client cpus left)"; continue; }
        conns=$(conns_for "$w"); CAP=$conns; SRV_CORES=$w
        ADDRS="127.0.0.1:$((port+1))"
        IFS=$'\n'
        for srv in ${SERVERS//,/$'\n'}; do
            IFS=$' \t\n'
            local pport=$((port+1)); port=$pport
            local ppid; ppid=$(start_proxy "$srv" "$pport" "$scpus" "$w" "$url") || { log "  $srv W=$w failed to start"; continue; }
            PROXY_PIDS=("$ppid"); ADDRS="127.0.0.1:$pport"
            # Warm the upstream pool so every timed repeat is steady-state (the
            # first timed run would otherwise pay for lazily dialling the pool).
            taskset -c "$CCPUS" "$KVLOAD_BIN" "$ADDRS" --conns "$conns" --seconds 2 \
                --workload get --value 16 --keys "$KEYS" --pipeline 1 >/dev/null 2>&1 || true
            local wl val pl line wls
            wls=$(base_workloads); (( w == 8 )) && wls="$wls"$'\n'"$(extra_workloads)"
            while IFS=$' ' read -r wl val pl; do
                [[ -n $wl ]] || continue
                local rep
                for (( rep=1; rep<=REPEATS; rep++ )); do
                    run_one single "$srv" "$wl" "$w" "$conns" "$val" "$pl" "$rep"
                done
            done <<<"$wls"
            stop_pids "$ppid"; PROXY_PIDS=()
            IFS=$'\n'
        done
        IFS=$' \t\n'
    done
    stop_pids "$redis_pid"; REDIS_PIDS=()
}

sweep_sharded() {
    local ws="$1" srv w
    local shard_servers="compio-pool,tokio-percore"
    log "sharded sweep: shard = 1 redis core + 1 proxy core (dedicated); client fans across shards"
    for w in $ws; do
        (( 2*w <= NCORES )) || { log "  skip W=$w (needs $((2*w)) cores for shards, have $NCORES)"; continue; }
        CCPUS=$(cpus_of_range $((2*w)) "$NCORES"); NCLI=$(count_cpus "$CCPUS")
        (( NCLI >= 1 )) || { log "  skip W=$w (no client cpus)"; continue; }
        local conns; conns=$(conns_for "$w"); CAP=$(( conns / w + 2 )); SRV_CORES=$w; NREDIS=$w
        IFS=$'\n'
        for srv in ${shard_servers//,/$'\n'}; do
            [[ ,$SERVERS, == *,$srv,* ]] || { continue; }
            IFS=$' \t\n'
            REDIS_PIDS=(); PROXY_PIDS=(); local addrs="" ok=1 i
            for (( i=0; i<w; i++ )); do
                local rc pc rport=$(( 6500 + i )) pport=$(( 7100 + i ))
                rc=$(t0_of "${PCORE[2*i]}"); pc=$(t0_of "${PCORE[2*i+1]}")
                local rpid; rpid=$(start_redis "$rport" "$rc") || { ok=0; break; }
                REDIS_PIDS+=("$rpid")
                local ppid; ppid=$(start_proxy "$srv" "$pport" "$pc" 1 "redis://127.0.0.1:$rport") || { ok=0; break; }
                PROXY_PIDS+=("$ppid")
                addrs="${addrs:+$addrs,}127.0.0.1:$pport"
            done
            if (( ok )); then
                ADDRS="$addrs"
                # Warm every shard's upstream pool before the timed repeats.
                taskset -c "$CCPUS" "$KVLOAD_BIN" "$ADDRS" --conns "$conns" --seconds 2 \
                    --workload get --value 16 --keys "$KEYS" --pipeline 1 >/dev/null 2>&1 || true
                local wl val pl rep
                while IFS=$' ' read -r wl val pl; do
                    [[ -n $wl ]] || continue
                    for (( rep=1; rep<=REPEATS; rep++ )); do
                        run_one sharded "$srv" "$wl" "$w" "$conns" "$val" "$pl" "$rep"
                    done
                done <<<"$(base_workloads)"
            else
                log "  $srv W=$w shard setup failed"
            fi
            (( ${#PROXY_PIDS[@]} )) && stop_pids "${PROXY_PIDS[@]}"
            (( ${#REDIS_PIDS[@]} )) && stop_pids "${REDIS_PIDS[@]}"
            PROXY_PIDS=(); REDIS_PIDS=()
            IFS=$'\n'
        done
        IFS=$' \t\n'
    done
}

# --------------------------------------------------------------------- summary
summarize() {
    awk -F'\t' '
        /^#/ || NF<10 { next }
        $9!="ok" { ne++; err[ne]=$1" "$2" "$3" W="$4" (rep "$8"): "$10; next }
        { key=$1 SUBSEP $2 SUBSEP $3 SUBSEP $4 SUBSEP $6 SUBSEP $7
          c=++cnt[key]; RPS[key,c]=$10; LINE[key,c]=$0
          seen[$1 SUBSEP $3 SUBSEP $4 SUBSEP $6 SUBSEP $7]=1
          srvs[$2]=1 }
        END{
            n=0
            for(k in cnt){ m=cnt[k]
                for(i=1;i<=m;i++)ix[i]=i
                for(i=2;i<=m;i++){x=ix[i];j=i-1;while(j>=1&&RPS[k,ix[j]]+0>RPS[k,x]+0){ix[j+1]=ix[j];j--}ix[j+1]=x}
                md=ix[int((m+1)/2)]; MED[k]=LINE[k,md] }
            # print grouped by sweep + workload + value + pipeline, rows=W, cols=server
            print "Median run by req/s. Flags: S proxy-bound, R redis-bound, C client-bound."
            print "cols per server: req/s[flag] | p50/p99 us | proxy-cpus | redis-cpus | RSS MB"
            PROCINFO_sorted=1
            # collect ordering
        }' "$OUT/runs.tsv"
    # Detailed tables via a second awk (clearer to keep separate)
    awk -F'\t' '
        function med(k,   m,i,j,x){ m=cnt[k]; for(i=1;i<=m;i++)ix[i]=i
            for(i=2;i<=m;i++){x=ix[i];j=i-1;while(j>=1&&RPS[k,ix[j]]+0>RPS[k,x]+0){ix[j+1]=ix[j];j--}ix[j+1]=x}
            return ix[int((m+1)/2)] }
        /^#/||NF<10{next} $9!="ok"{next}
        { sw=$1;srv=$2;wl=$3;w=$4;val=$6;pl=$7
          grp=sw SUBSEP wl SUBSEP val SUBSEP pl
          key=grp SUBSEP w SUBSEP srv
          c=++cnt[key];RPS[key,c]=$10;L[key,c]=$0
          if(!(grp in grpseen)){grpseen[grp]=1;gord[++ng]=grp}
          if(!((grp,w) in wseen)){wseen[grp,w]=1;word[grp,++nw[grp]]=w}
          if(!((grp,srv) in sseen)){sseen[grp,srv]=1;sord[grp,++ns[grp]]=srv}
        }
        END{
          for(gi=1;gi<=ng;gi++){grp=gord[gi]
            split(grp,G,SUBSEP)
            printf "\n=== sweep=%s workload=%s value=%s pipeline=%s ===\n",G[1],G[2],G[3],G[4]
            printf "%-6s","W"
            for(si=1;si<=ns[grp];si++)printf "| %-40s",sord[grp,si]
            printf "\n"
            # numeric sort W
            for(a=1;a<=nw[grp];a++)for(b=a+1;b<=nw[grp];b++)if(word[grp,a]+0>word[grp,b]+0){t=word[grp,a];word[grp,a]=word[grp,b];word[grp,b]=t}
            for(wi=1;wi<=nw[grp];wi++){w=word[grp,wi];printf "%-6s",w
              for(si=1;si<=ns[grp];si++){srv=sord[grp,si];key=grp SUBSEP w SUBSEP srv
                if(!(key in cnt)){printf "| %-40s","-";continue}
                m=med(key);split(L[key,m],F,"\t")
                cell=sprintf("%s%s %s/%s %sc %sr %sMB",F[10],(F[27]!=""?"["F[27]"]":""),F[12],F[14],F[18],F[19],F[24])
                printf "| %-40s",cell }
              printf "\n" }
          }
        }' "$OUT/runs.tsv"
}

# --------------------------------------------------------------------- main
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
[[ -n $OUT ]] || OUT="$PWD/redisbench-$(hostname)-$STAMP"
mkdir -p "$OUT" || die "cannot mkdir $OUT"
OUT=$(cd "$OUT" && pwd)
TMP=$(mktemp -d)
PROXY_PIDS=(); REDIS_PIDS=(); ADDRS=""; CCPUS=""; NCLI=0; SRV_CORES=1; NREDIS=1; CAP=1024

printf '#%s\n' "$(IFS=$'\t'; echo "${COLS[*]}")" >"$OUT/runs.tsv"
log() { echo "$*" | tee -a "$OUT/run.log"; }

ulimit -n "$(ulimit -Hn)" 2>/dev/null

{
    echo "redisbench on $(hostname)  $(date -u +%FT%TZ)"
    echo "note: $NOTE"
    echo "kernel: $(uname -srm)"
    echo "cpus: $(nproc --all) ($NCORES physical cores, $(printf '%s\n' "${PNODE[@]}"|sort -u|wc -l) NUMA nodes)"
    echo "redis: $("$REDIS_BIN" --version 2>/dev/null)"
    echo "rustc: $(. ~/.cargo/env 2>/dev/null; rustc --version 2>/dev/null)"
    echo "git: $(git -C "$REPO" rev-parse --abbrev-ref HEAD 2>/dev/null)@$(git -C "$REPO" rev-parse --short HEAD 2>/dev/null)"
    echo "io_uring_disabled: $(cat /proc/sys/kernel/io_uring_disabled 2>/dev/null)"
    echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
    echo "nofile: $(ulimit -n)   somaxconn: $(cat /proc/sys/net/core/somaxconn 2>/dev/null)"
    echo "config: seconds=$SECONDS_PER repeats=$REPEATS keys=$KEYS servers=$SERVERS sweep=$SWEEP"
    echo; echo "--- lscpu ---"; lscpu 2>/dev/null
} >"$OUT/env.txt"

FINALIZED=0
finalize() {
    (( FINALIZED )) && return; FINALIZED=1
    cleanup
    [[ -s $OUT/runs.tsv ]] || return 0
    summarize >"$OUT/summary.txt" 2>&1
    local final="$OUT/redisbench-$(hostname)-$STAMP.log"
    {
        echo "##### redisbench - upload this file #####"
        echo; echo "##### ENVIRONMENT #####"; cat "$OUT/env.txt"
        echo; echo "##### SUMMARY #####"; cat "$OUT/summary.txt"
        echo; echo "##### RAW RUNS (TSV) #####"; cat "$OUT/runs.tsv"
        echo; echo "##### PROGRESS LOG #####"; cat "$OUT/run.log"
    } >"$final"
    cp "$final" "./$(basename "$final")" 2>/dev/null
    echo; echo "=============================================================="
    echo " Upload: $final  ($(wc -c <"$final") bytes)"
    echo "=============================================================="
}
trap finalize EXIT
trap 'echo; echo "interrupted - writing what finished"; finalize; exit 130' INT TERM HUP

SINGLE_WS="1 2 4 8 16"
SHARD_WS="1 2 4 8"
(( QUICK )) && { SINGLE_WS="1 4"; SHARD_WS="1 4"; }

log "results dir: $OUT"
T0=$(date +%s)
if [[ $SWEEP == single || $SWEEP == both ]]; then sweep_single "$SINGLE_WS"; fi
if [[ $SWEEP == sharded || $SWEEP == both ]]; then sweep_sharded "$SHARD_WS"; fi
log "done in $(( ($(date +%s) - T0) / 60 )) min"
exit 0
