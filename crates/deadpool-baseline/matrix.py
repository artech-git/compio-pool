#!/usr/bin/env python3
"""Comprehensive benchmark matrix: compio-pool against tokio, for a machine with many cores.

Runs `probe.py`'s one-server-one-load measurement over a matrix and writes a JSONL of every
run plus a Markdown report. Three servers, one client (`examples/load.rs`):

    tokio-default    multi-threaded work-stealing runtime, one shared listener and pool
    tokio-per-core   tokio on epoll, one pinned current_thread runtime per core, SO_REUSEPORT
    compio-pool      io_uring, one pinned ring per core, SO_REUSEPORT

Default per-core against compio-pool isolates the I/O backend; default against per-core
isolates the architecture.

PLACEMENT is what makes a single machine's numbers mean something. In the `split` suites the
server gets W dedicated physical cores (one thread each, SMT siblings left idle) and the
clients get every other CPU, so no client thread can share a core, a cache or a wakeup with a
server thread: the closest one machine gets to a remote load generator. The `shared` suite
does the opposite on purpose (nothing pinned apart from the servers' own pinning), because
that is what a plain `cargo run` of both programs gives and it was where earlier results
misled.

Every point reports whether it was bounded by the server (S: the server's CPUs >=85% busy),
by the client (C: the client's CPUs >=85% busy, so the number mostly measures the load
generator), or by the whole machine (B, shared placement). Read a C row as a lower bound.

Suites (select with --only):

    scale512   worker scaling, 512 B echo, split placement
    scale16k   worker scaling, 16 KiB echo, split placement
    payload    payload sweep 64 B .. 16 KiB at a fixed worker count, split
    conns      connection-count sweep at a fixed worker count, split
    shared     the same servers with nothing separated, 512 B and 16 KiB
    churn      a fresh connection per request; compio-pool also at capacity 2 and 1, which
               forces most connections through the fd-handoff path
    numa       only if the machine has more than one NUMA node: clients on the server's node
               against clients on another

Usage, from the repository root, after building (see run-on-vm.sh, which does it all):

    python3 crates/deadpool-baseline/matrix.py --dry-run          # show the plan and the time
    python3 crates/deadpool-baseline/matrix.py                    # everything, ~30-90 minutes
    python3 crates/deadpool-baseline/matrix.py --quick            # smoke test, a few minutes
    python3 crates/deadpool-baseline/matrix.py --report-only DIR  # rebuild report.md from DIR

Linux only; standard library only. Results go to bench-results/<host>-<utc>/ unless --out.
"""
import argparse
import itertools
import json
import math
import os
import platform
import re
import resource
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import probe  # noqa: E402

ALL_SERVERS = ["tokio-default", "tokio-per-core", "compio-pool"]
LABEL = {"tokio-default": "tokio default", "tokio-per-core": "tokio per-core",
         "compio-pool": "compio-pool"}
SUITES = ["scale512", "scale16k", "payload", "conns", "shared", "churn", "numa"]
DESCRIPTION = {
    "scale512": "Worker scaling, 512 B echo. Server on W dedicated cores, clients on the others.",
    "scale16k": "Worker scaling, 16 KiB echo. Server on W dedicated cores, clients on the others.",
    "payload": "Payload sweep at a fixed worker count. Server and clients on separate cores.",
    "conns": "Connection-count sweep at a fixed worker count. Server and clients on separate "
             "cores. Fewer connections than workers leaves workers idle by `SO_REUSEPORT` hash.",
    "shared": "Nothing separated: the clients float over every CPU, the servers pin themselves. "
              "What plain `cargo run` of both programs gives.",
    "churn": "A fresh connection per request (`--reconnect 1`), so each request is connect, "
             "hash, accept, serve, close. `cap=2` and `cap=1` force most connections through "
             "compio-pool's detach, channel and attach handoff.",
    "numa": "Server on one NUMA node; clients on the same node against clients on another.",
}


# ---------------------------------------------------------------------------- topology

def read_topology():
    """Physical cores, each a sorted list of its CPU ids, ordered by NUMA node, socket and
    core id; and a cpu -> node map. Restricted to this process's affinity mask."""
    allowed = set(os.sched_getaffinity(0))
    info = {}
    try:
        out = subprocess.run(["lscpu", "-p=CPU,CORE,SOCKET,NODE"], capture_output=True,
                             text=True, check=True).stdout
        for line in out.splitlines():
            if not line.strip() or line.startswith("#"):
                continue
            p = line.split(",")
            cpu = int(p[0])
            if cpu in allowed:
                core = int(p[1]) if p[1] else cpu
                sock = int(p[2]) if len(p) > 2 and p[2] else 0
                node = int(p[3]) if len(p) > 3 and p[3] else 0
                info[cpu] = (sock, core, node)
    except (OSError, subprocess.CalledProcessError, ValueError):
        info = {}
    for cpu in allowed:  # no topology available: every CPU is its own core
        info.setdefault(cpu, (0, cpu, 0))
    grouped = {}
    for cpu, (sock, core, node) in sorted(info.items()):
        grouped.setdefault((node, sock, core), []).append(cpu)
    cores = [sorted(v) for _, v in sorted(grouped.items())]
    return cores, {cpu: v[2] for cpu, v in info.items()}


def cpulist(cpus):
    return ",".join(str(c) for c in sorted(cpus))


def split(cores, workers, first=0):
    """(server cpus, client cpus): one thread on each of `workers` physical cores from
    `first`; clients on every other CPU, which leaves those cores' SMT siblings idle."""
    chosen = range(first, first + workers)
    if first + workers > len(cores):
        return None
    server = [cores[i][0] for i in chosen]
    clients = [cpu for i, c in enumerate(cores) if i not in chosen for cpu in c]
    return server, clients


def max_workers(cores, ratio, cap):
    best = 0
    for w in range(1, min(len(cores), cap) + 1):
        sp = split(cores, w)
        if sp and len(sp[1]) >= max(1, math.ceil(ratio * w)):
            best = w
    return best


def worker_list(wmax):
    ws = []
    w = 1
    while w <= wmax:
        ws.append(w)
        w *= 2
    if wmax and wmax not in ws:
        ws.append(wmax)
    return ws


# ---------------------------------------------------------------------------- the plan

def build_points(a, cores, node_of, wmax):
    servers = [(LABEL[s], s, 1024) for s in a.servers]
    pts = []

    def conns_for(w):
        return min(a.max_conns, max(64, 32 * w))

    def add(suite, row, w, conns, nbytes, srv=None, split_cpus=True, extra=(), first=0,
            clients=None):
        if split_cpus:
            sp = split(cores, w, first)
            if sp is None:
                return
            server, cli = sp
            if clients is not None:
                cli = clients
            scpus, lcpus = cpulist(server), cpulist(cli)
            ncli = len(cli)
        else:
            scpus = lcpus = ""
            ncli = 0
        pts.append(dict(suite=suite, row=row, workers=w, conns=conns, bytes=nbytes,
                        servers=srv or servers, server_cpus=scpus, load_cpus=lcpus,
                        n_client_cpus=ncli, load_extra=list(extra)))

    want = set(a.only.split(",")) if a.only else set(SUITES)
    wl = worker_list(wmax)
    w_fixed = min(4, wmax) if wmax else 0

    for suite, nbytes in (("scale512", 512), ("scale16k", 16384)):
        if suite in want:
            for w in wl:
                add(suite, f"W={w}", w, conns_for(w), nbytes)
    if "payload" in want and w_fixed:
        for nbytes in (64, 512, 2048, 8192, 16384):
            add("payload", f"{nbytes} B", w_fixed, conns_for(w_fixed), nbytes)
    if "conns" in want and w_fixed:
        seen = []
        for c in (w_fixed, 4 * w_fixed, 16 * w_fixed, 64 * w_fixed, 256 * w_fixed):
            if c <= a.max_conns and c not in seen:
                seen.append(c)
                add("conns", f"{c} conns", w_fixed, c, 512)
    if "shared" in want:
        total = len(os.sched_getaffinity(0))
        ws = worker_list(min(a.max_workers, max(1, total // 2)))
        for nbytes in (512, 16384):
            for w in ws:
                add("shared", f"{nbytes} B, W={w}", w, conns_for(w), nbytes, split_cpus=False)
    if "churn" in want and w_fixed:
        churn_servers = list(servers)
        if "compio-pool" in a.servers:
            churn_servers += [("compio-pool cap=2", "compio-pool", 2),
                              ("compio-pool cap=1", "compio-pool", 1)]
        add("churn", "reconnect per request", w_fixed, 64, 512, srv=churn_servers,
            extra=("--reconnect", "1"))
    if "numa" in want and w_fixed:
        nodes = sorted(set(node_of.values()))
        if len(nodes) >= 2:
            a_idx = [i for i, c in enumerate(cores) if node_of[c[0]] == nodes[0]]
            b_cpus = [cpu for c in cores if node_of[c[0]] == nodes[1] for cpu in c]
            w = min(w_fixed, len(a_idx))
            sp = split(cores, w, a_idx[0])
            if sp:
                server, _ = sp
                local = [cpu for i in a_idx if i >= a_idx[0] + w for cpu in cores[i]]
                need = max(1, math.ceil(a.client_ratio * w))
                if len(local) >= need and len(b_cpus) >= need:
                    add("numa", "clients on the server's node", w, conns_for(w), 512,
                        first=a_idx[0], clients=local)
                    add("numa", "clients on another node", w, conns_for(w), 512,
                        first=a_idx[0], clients=b_cpus)
    order = {s: i for i, s in enumerate(SUITES)}
    pts.sort(key=lambda p: order[p["suite"]])
    return pts


# ---------------------------------------------------------------------------- running

def sh(cmd):
    try:
        return subprocess.run(cmd, shell=True, capture_output=True, text=True,
                              timeout=15).stdout.strip()
    except (OSError, subprocess.SubprocessError) as e:
        return f"(unavailable: {e})"


def read_file(path):
    try:
        return Path(path).read_text().strip()
    except OSError:
        return ""


def env_info(a, cores, node_of, wmax):
    smt = max((len(c) for c in cores), default=1)
    cache = {}
    for i in range(6):
        d = Path(f"/sys/devices/system/cpu/cpu0/cache/index{i}")
        if d.exists():
            cache[f"L{read_file(d / 'level')} {read_file(d / 'type')}"] = read_file(d / "size")
    return {
        "type": "env",
        "date_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "host": platform.node(),
        "kernel": platform.release(),
        "cpu_model": next((l.split(":", 1)[1].strip() for l in
                           read_file("/proc/cpuinfo").splitlines() if l.startswith("model name")),
                          platform.processor()),
        "allowed_cpus": len(os.sched_getaffinity(0)),
        "physical_cores": len(cores),
        "threads_per_core": smt,
        "numa_nodes": len(set(node_of.values())),
        "memory": sh("free -m | sed -n 2p"),
        "virtualization": sh("systemd-detect-virt 2>/dev/null") or "unknown",
        "io_uring_disabled": read_file("/proc/sys/kernel/io_uring_disabled"),
        "cpu_governor": read_file("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
        "somaxconn": read_file("/proc/sys/net/core/somaxconn"),
        "tcp_tw_reuse": read_file("/proc/sys/net/ipv4/tcp_tw_reuse"),
        "nofile_soft_hard": list(resource.getrlimit(resource.RLIMIT_NOFILE)),
        "cache_cpu0": cache,
        "rustc": sh("rustc --version"),
        "git_branch": sh(f"git -C {probe.ROOT} rev-parse --abbrev-ref HEAD"),
        "git_rev": sh(f"git -C {probe.ROOT} rev-parse --short HEAD"),
        "git_dirty": bool(sh(f"git -C {probe.ROOT} status --porcelain --untracked-files=no")),
        "max_split_workers": wmax,
        "args": vars(a),
        "lscpu": sh("lscpu"),
    }


class Ports:
    """Distinct ports so a run never trips over the previous one's TIME_WAIT."""

    def __init__(self, base):
        self.it = itertools.count(base)

    def next(self):
        return 20000 + next(self.it) % 40000


def median_run(runs):
    runs = sorted(runs, key=lambda r: r["rps"])
    return runs[(len(runs) - 1) // 2]


def parse_lat(s):
    m = re.search(r"p50 ([\d.]+)\s+p90 ([\d.]+)\s+p99 ([\d.]+)\s+p99\.9 ([\d.]+)\s+max ([\d.]+)", s)
    return dict(zip(("p50", "p90", "p99", "p999", "max"), map(float, m.groups()))) if m else {}


def run_point(pt, a, ports):
    servers = pt["servers"]
    runs = {disp: [] for disp, _, _ in servers}
    errors = {}
    for r in range(a.repeats):
        k = r % len(servers)
        for disp, binary, cap in servers[k:] + servers[:k]:  # rotate: spread any drift
            if disp in errors:
                continue
            try:
                res = probe.one_run(binary, ports.next(), pt["workers"], pt["server_cpus"],
                                    pt["load_cpus"], pt["conns"], pt["bytes"], a.seconds,
                                    capacity=cap, load_extra=pt["load_extra"],
                                    capture_stats=(binary == "compio-pool" and pt["suite"] == "churn"))
                res.update(parse_lat(res["lat"]))
                runs[disp].append(res)
            except Exception as e:  # keep the sweep going; the record carries the reason
                errors[disp] = str(e)[-400:]
            time.sleep(a.settle * (4 if pt["load_extra"] else 1))
    recs = []
    for disp, binary, cap in servers:
        rec = {k: v for k, v in pt.items() if k != "servers"}
        rec.update(type="point", server=disp, binary=binary, capacity=cap)
        if runs[disp]:
            rec.update(runs=runs[disp], median=median_run(runs[disp]),
                       rps_all=[x["rps"] for x in runs[disp]])
        if disp in errors:
            rec["error"] = errors[disp]
        recs.append(rec)
    return recs


def plan_minutes(pts, a):
    secs = sum(len(p["servers"]) * a.repeats * (a.seconds + 1.6 + (1.6 if p["load_extra"] else 0))
               for p in pts)
    return secs / 60


# ---------------------------------------------------------------------------- report

def flags(rec, total_cpus):
    m = rec["median"]
    rps = m["rps"]
    srv = (m["srv_user_us"] + m["srv_sys_us"]) * rps / 1e6
    cli = m["cli_us"] * rps / 1e6
    out = ""
    if rec["load_cpus"]:
        if srv >= 0.85 * rec["workers"]:
            out += "S"
        if rec["n_client_cpus"] and cli >= 0.85 * rec["n_client_cpus"]:
            out += "C"
    elif srv + cli >= 0.9 * total_cpus:
        out = "B"
    return out


def cell_rps(rec, total_cpus):
    if "median" not in rec:
        return "error"
    rps = rec["rps_all"]
    med = rec["median"]["rps"]
    f = flags(rec, total_cpus)
    spread = f" ±{(max(rps) - min(rps)) / 2 / med * 100:.0f}%" if med and len(rps) > 1 else ""
    return f"{med:,}{' [' + f + ']' if f else ''}{spread}"


def ratio(a, b):
    if a is None or b is None or "median" not in a or "median" not in b or not b["median"]["rps"]:
        return "-"
    return f"{a['median']['rps'] / b['median']['rps']:.2f}×"


def suite_tables(recs, total_cpus):
    rows = []
    for r in recs:
        if r["row"] not in rows:
            rows.append(r["row"])
    cols = []
    for r in recs:
        if r["server"] not in cols:
            cols.append(r["server"])
    cell = {(r["row"], r["server"]): r for r in recs}
    arch = "tokio default" in cols and "tokio per-core" in cols
    back = "tokio per-core" in cols and "compio-pool" in cols
    out = []

    head = ["", *cols] + (["per-core ÷ default"] if arch else []) + (["compio ÷ per-core"] if back else [])
    out.append("| " + " | ".join(head) + " |")
    out.append("|" + "---|" * len(head))
    for row in rows:
        c = [cell.get((row, s)) for s in cols]
        line = [row] + [cell_rps(x, total_cpus) if x else "-" for x in c]
        g = lambda name: cell.get((row, name))
        if arch:
            line.append(ratio(g("tokio per-core"), g("tokio default")))
        if back:
            line.append(ratio(g("compio-pool"), g("tokio per-core")))
        out.append("| " + " | ".join(line) + " |")
    out.append("")
    out.append("p50 / p99 latency, µs:")
    out.append("")
    out.append("| " + " | ".join([""] + cols) + " |")
    out.append("|" + "---|" * (len(cols) + 1))
    for row in rows:
        line = [row]
        for s in cols:
            x = cell.get((row, s))
            m = x.get("median") if x else None
            line.append(f"{m.get('p50', 0):,.0f} / {m.get('p99', 0):,.0f}" if m else "-")
        out.append("| " + " | ".join(line) + " |")
    out.append("")
    out.append("CPU per request, µs (server user+system, client):")
    out.append("")
    out.append("| " + " | ".join([""] + cols) + " |")
    out.append("|" + "---|" * (len(cols) + 1))
    for row in rows:
        line = [row]
        for s in cols:
            x = cell.get((row, s))
            m = x.get("median") if x else None
            line.append(f"{m['srv_user_us'] + m['srv_sys_us']:.1f}, {m['cli_us']:.1f}" if m else "-")
        out.append("| " + " | ".join(line) + " |")
    out.append("")
    # compio-pool's own counters, where they say something (the handoff suites)
    stats = [(r["row"], r["server"], r["median"].get("stats", "")) for r in recs
             if r.get("median") and r["binary"] == "compio-pool" and r["median"].get("stats")]
    if recs and recs[0]["suite"] == "churn" and stats:
        out.append("compio-pool counters at the end of the median run:")
        out.append("")
        for row, s, line in stats:
            out.append(f"* {s}: `{' '.join(line.split())}`")
        out.append("")
    errs = [(r["row"], r["server"], r["error"]) for r in recs if r.get("error")]
    for row, s, e in errs:
        out.append(f"* **{row} / {s}: {e}**")
    if errs:
        out.append("")
    return out


def write_report(path, env, points):
    total = env["allowed_cpus"]
    L = []
    L.append(f"# compio-pool vs tokio: {env['host']}, {env['date_utc']}")
    L.append("")
    L.append(f"* CPU: {env['cpu_model']}; {env['allowed_cpus']} CPUs = {env['physical_cores']} "
             f"physical cores × {env['threads_per_core']} thread(s); {env['numa_nodes']} NUMA node(s)")
    L.append(f"* Memory: `{env['memory']}`; virtualization: {env['virtualization']}; "
             f"kernel {env['kernel']}")
    L.append(f"* io_uring_disabled={env['io_uring_disabled'] or 'n/a'}, governor="
             f"{env['cpu_governor'] or 'n/a'}, somaxconn={env['somaxconn']}, "
             f"tcp_tw_reuse={env['tcp_tw_reuse']}, nofile={env['nofile_soft_hard']}")
    L.append(f"* Cache (cpu0): {', '.join(f'{k} {v}' for k, v in env['cache_cpu0'].items())}")
    L.append(f"* {env['rustc']}; git {env.get('git_branch', '?')}@{env['git_rev']}"
             f"{' (uncommitted changes)' if env['git_dirty'] else ''}")
    ar = env["args"]
    L.append(f"* Each cell is the median of {ar['repeats']} runs of {ar['seconds']} s, repeats "
             f"interleaved across servers; ±N% is half the min-max spread. Split placement uses "
             f"up to {env['max_split_workers']} dedicated server cores.")
    L.append("")
    L.append("Legend: **S** the server's CPUs were at least 85% busy, so the server is the "
             "bottleneck. **C** the client's CPUs were at least 85% busy, so the number mostly "
             "measures the load generator and is a lower bound. **B** the whole machine was at "
             "least 90% busy (shared placement). No flag: neither side was saturated, so "
             "throughput is limited by the closed loop (connections ÷ latency).")
    L.append("")
    by = {}
    for p in points:
        by.setdefault(p["suite"], []).append(p)
    for suite in SUITES:
        if suite not in by:
            continue
        recs = by[suite]
        L.append(f"## {suite}")
        L.append("")
        L.append(DESCRIPTION[suite])
        L.append("")
        first = recs[0]
        L.append(f"Server CPUs `{first['server_cpus'] or 'unpinned'}`, client CPUs "
                 f"`{first['load_cpus'] or 'unpinned'}`" +
                 ("" if suite in ("scale512", "scale16k", "shared") else
                  f"; {first['workers']} worker(s)") + ".")
        L.append("")
        L += suite_tables(recs, total)
    Path(path).write_text("\n".join(L) + "\n")


def load_results(directory):
    env, points = None, []
    for line in (Path(directory) / "results.jsonl").read_text().splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        if rec["type"] == "env":
            env = rec
        else:
            points.append(rec)
    return env, points


# ---------------------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--out", help="results directory (default bench-results/<host>-<utc>)")
    ap.add_argument("--only", default="", help=f"comma-separated suites from {','.join(SUITES)}")
    ap.add_argument("--servers", default=",".join(ALL_SERVERS))
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--seconds", type=int, default=5)
    ap.add_argument("--client-ratio", type=float, default=1.5,
                    help="client CPUs wanted per server worker when sizing the split suites")
    ap.add_argument("--max-workers", type=int, default=32)
    ap.add_argument("--max-conns", type=int, default=1024)
    ap.add_argument("--settle", type=float, default=0.4, help="pause between runs, seconds")
    ap.add_argument("--quick", action="store_true", help="1 repeat, 2 s, small: a smoke test")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and exit")
    ap.add_argument("--resume", action="store_true", help="skip points already in --out")
    ap.add_argument("--skip-preflight", action="store_true")
    ap.add_argument("--report-only", metavar="DIR", help="rebuild DIR/report.md and exit")
    a = ap.parse_args()
    a.servers = a.servers.split(",")
    if a.quick:
        a.repeats, a.seconds = 1, 2
        a.max_workers = min(a.max_workers, 2)
        a.max_conns = min(a.max_conns, 256)

    if a.report_only:
        env, points = load_results(a.report_only)
        write_report(Path(a.report_only) / "report.md", env, points)
        print(f"wrote {Path(a.report_only) / 'report.md'}")
        return

    missing = [str(p) for p in [*probe.SERVERS.values(), probe.LOAD] if not p.exists()]
    if missing:
        sys.exit("not built: " + ", ".join(missing) + "\nrun crates/deadpool-baseline/run-on-vm.sh "
                 "or see the build commands in probe.py --help")

    cores, node_of = read_topology()
    wmax = max_workers(cores, a.client_ratio, a.max_workers)
    if not wmax:
        sys.exit("not enough CPUs to separate even one server core from its clients")
    pts = build_points(a, cores, node_of, wmax)
    print(f"{len(os.sched_getaffinity(0))} CPUs, {len(cores)} physical cores, "
          f"{len(set(node_of.values()))} NUMA node(s); up to {wmax} dedicated server cores")
    print(f"plan: {len(pts)} points, {sum(len(p['servers']) for p in pts)} server configs, "
          f"~{plan_minutes(pts, a):.0f} minutes")
    for p in pts:
        print(f"  {p['suite']:<9}{p['row']:<30} W={p['workers']:<3} conns={p['conns']:<5} "
              f"bytes={p['bytes']:<6} server={p['server_cpus'] or '-':<12.12} "
              f"clients={p['load_cpus'] or '-':<16.16}")
    if a.dry_run:
        return

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < hard:
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    if resource.getrlimit(resource.RLIMIT_NOFILE)[0] < a.max_conns * 2 + 256:
        print("warning: open-file limit is low for --max-conns; raise `ulimit -n` or lower it")

    out = Path(a.out) if a.out else Path("bench-results") / (
        f"{platform.node()}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")
    out.mkdir(parents=True, exist_ok=True)
    results = out / "results.jsonl"
    done = set()
    env = None
    if a.resume and results.exists():
        env, prev = load_results(out)
        done = {(p["suite"], p["row"]) for p in prev}
    if env is None:
        env = env_info(a, cores, node_of, wmax)
        results.write_text(json.dumps(env) + "\n")
        (out / "lscpu.txt").write_text(env["lscpu"] + "\n")
    print(f"results: {out}")

    ports = Ports(os.getpid() % 1000)
    if not a.skip_preflight:
        print("preflight: starting each server once ...")
        for s in a.servers:
            try:
                probe.one_run(s, ports.next(), 1, "", "", 8, 512, 1)
                print(f"  {s}: ok")
            except Exception as e:
                sys.exit(f"preflight failed for {s}: {e}\n(compio-pool needs io_uring: check "
                         "/proc/sys/kernel/io_uring_disabled and any seccomp profile)")

    t0 = time.time()
    todo = [p for p in pts if (p["suite"], p["row"]) not in done]
    for i, pt in enumerate(todo, 1):
        print(f"[{i}/{len(todo)}] {pt['suite']} / {pt['row']}  W={pt['workers']} conns={pt['conns']} "
              f"bytes={pt['bytes']}", flush=True)
        recs = run_point(pt, a, ports)
        with results.open("a") as f:
            for r in recs:
                f.write(json.dumps(r) + "\n")
        for r in recs:
            m = r.get("median")
            print(f"    {r['server']:<18}" + (f"{m['rps']:>10,} req/s  p50 {m.get('p50', 0):>8,.0f}  "
                  f"p99 {m.get('p99', 0):>9,.0f} us  {flags(r, env['allowed_cpus'])}"
                  if m else f"ERROR {r.get('error', '')[:100]}"), flush=True)
    _, points = load_results(out)
    write_report(out / "report.md", env, points)
    print(f"done in {(time.time() - t0) / 60:.1f} min; report: {out / 'report.md'}")


if __name__ == "__main__":
    main()
