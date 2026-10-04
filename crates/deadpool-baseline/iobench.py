#!/usr/bin/env python3
"""iobench.py — the I/O-job matrix with resource accounting. Linux only.

Runs every I/O workload the examples implement — echo, connection churn, TCP
relay, scatter-gather fanout, static files, WAL append+fsync — against three
servers under identical load and placement:

    compio         compio-pool: io_uring, thread-per-core, thread-local pool
    tokio-percore  tokio: epoll, thread-per-core, per-worker deadpool
    tokio-default  tokio: epoll, work-stealing, shared listener + shared deadpool

and records, for every run:

  performance        requests/s and the latency distribution (from the load
                     generator, which is plain std threads and measures only
                     the server)
  CPU                user + system seconds from /proc/<pid>/stat, turned into
                     CPU microseconds per request and a sys-time share
  context switches   voluntary + involuntary, summed over every thread from
                     /proc/<pid>/task/*/status
  syscalls           read/write syscall counts from /proc/<pid>/io (io_uring
                     servers make almost none — that is the point), and, when
                     `sudo perf` works, total syscalls via raw_syscalls:sys_enter
  memory             peak RSS (VmHWM) and the high-water thread count — the
                     tokio servers grow a blocking pool for file I/O, compio
                     does not
  disk               read_bytes / write_bytes actually hitting storage
  saturation         busy share of the server's CPUs and the clients' CPUs
                     over the window, so a client-bound row is never mistaken
                     for a server limit

Placement gives the server dedicated CPUs, the clients the rest, and any
backend echo servers (for relay/fanout) their own CPU, so the three servers
always compete under the same conditions.

    ./iobench.py --dry-run               # the plan
    ./iobench.py                         # the full matrix (~15 min)
    ./iobench.py --quick                 # one repeat, 2 s runs
    ./iobench.py --points wal-sync,file-4k --servers compio,tokio-default

Writes results.tsv (every raw run), report.md (medians, derived costs), and
iobench.log into --out (default ~/iobench/results-<timestamp>).
"""

import argparse
import datetime
import os
import re
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

# ----------------------------------------------------------------------------
# configuration

REPO = Path(__file__).resolve().parent.parent.parent
COMPIO_BIN = REPO / "target/release/examples"
DP_BIN = REPO / "crates/deadpool-baseline/target/release/examples"

SERVER_PORT = 7100
BACKEND_PORTS = [7001, 7002, 7003]

# Every point: name, workload binary stem, load generator + its flags,
# backends needed, extra server env.
#   workload  -> compio example name / deadpool example name (echo maps to
#                echo_tpc + echo on the tokio side)
POINTS = [
    dict(name="echo-512",   workload="echo",        load="load",       largs=["--conns", "64", "--bytes", "512"]),
    dict(name="echo-64k",   workload="echo",        load="load",       largs=["--conns", "32", "--bytes", "65536"]),
    dict(name="churn",      workload="churn",       load="load_churn", largs=["--conns", "64"]),
    dict(name="relay-512",  workload="relay",       load="load",       largs=["--conns", "64", "--bytes", "512"], backends=1),
    dict(name="fanout-512", workload="fanout",      load="load",       largs=["--conns", "32", "--bytes", "512"], backends=3),
    dict(name="file-4k",    workload="file_server", load="load_file",  largs=["--conns", "64", "--bytes", "4096", "--files", "64"]),
    dict(name="file-64k",   workload="file_server", load="load_file",  largs=["--conns", "32", "--bytes", "65536", "--files", "32"]),
    dict(name="wal-sync",   workload="wal",         load="load_wal",   largs=["--conns", "64", "--bytes", "512"], env={"WAL_SYNC": "1"}),
    dict(name="wal-nosync", workload="wal",         load="load_wal",   largs=["--conns", "64", "--bytes", "512"], env={"WAL_SYNC": "0"}),
]

SERVERS = ["compio", "tokio-percore", "tokio-default"]

CLK_TCK = os.sysconf("SC_CLK_TCK")

# ----------------------------------------------------------------------------
# /proc accounting

def proc_times(pid):
    """(utime_s, stime_s), whole process, all threads."""
    with open(f"/proc/{pid}/stat") as f:
        fields = f.read().rsplit(")", 1)[1].split()
    return int(fields[11]) / CLK_TCK, int(fields[12]) / CLK_TCK


def proc_ctx(pid):
    """(voluntary, involuntary) context switches summed over live threads."""
    vol = invol = 0
    try:
        tids = os.listdir(f"/proc/{pid}/task")
    except OSError:
        return 0, 0
    for tid in tids:
        try:
            status = open(f"/proc/{pid}/task/{tid}/status").read()
        except OSError:
            continue
        m = re.search(r"^voluntary_ctxt_switches:\s+(\d+)", status, re.M)
        if m:
            vol += int(m.group(1))
        m = re.search(r"^nonvoluntary_ctxt_switches:\s+(\d+)", status, re.M)
        if m:
            invol += int(m.group(1))
    return vol, invol


def proc_io(pid):
    """dict of /proc/<pid>/io counters (rchar, wchar, syscr, syscw, read_bytes, write_bytes)."""
    out = {}
    try:
        for line in open(f"/proc/{pid}/io"):
            k, v = line.split(":")
            out[k.strip()] = int(v)
    except OSError:
        pass
    return out


def proc_status_kv(pid):
    """dict of /proc/<pid>/status numeric fields we use (VmHWM kB, VmRSS kB, Threads)."""
    out = {}
    try:
        for line in open(f"/proc/{pid}/status"):
            for key in ("VmHWM", "VmRSS", "Threads"):
                if line.startswith(key + ":"):
                    out[key] = int(line.split()[1])
    except OSError:
        pass
    return out


def cpu_busy(cpuset):
    """{cpu: (busy_ticks, total_ticks)} for the cpus in `cpuset`."""
    out = {}
    for line in open("/proc/stat"):
        if not line.startswith("cpu"):
            continue
        name = line.split()[0]
        if name == "cpu":
            continue
        n = int(name[3:])
        if n not in cpuset:
            continue
        vals = [int(x) for x in line.split()[1:]]
        idle = vals[3] + vals[4]  # idle + iowait
        out[n] = (sum(vals) - idle, sum(vals))
    return out


def busy_share(before, after):
    busy = sum(a[0] - b[0] for b, a in ((before[c], after[c]) for c in before))
    total = sum(a[1] - b[1] for b, a in ((before[c], after[c]) for c in before))
    return busy / total if total else 0.0


class ThreadSampler(threading.Thread):
    """Samples the server's live thread count twice a second; the tokio
    blocking pool only exists while file I/O is in flight, so an end-of-run
    read would miss it."""

    def __init__(self, pid):
        super().__init__(daemon=True)
        self.pid = pid
        self.max_threads = 0
        self._stop_event = threading.Event()

    def run(self):
        while not self._stop_event.is_set():
            st = proc_status_kv(self.pid)
            self.max_threads = max(self.max_threads, st.get("Threads", 0))
            self._stop_event.wait(0.5)

    def stop(self):
        self._stop_event.set()
        self.join(timeout=2)
        return self.max_threads


# ----------------------------------------------------------------------------
# process management

def parse_cpuset(spec):
    out = set()
    for part in spec.split(","):
        if "-" in part:
            a, b = part.split("-")
            out.update(range(int(a), int(b) + 1))
        else:
            out.add(int(part))
    return out


def cpuset_arg(cpuset):
    return ",".join(str(c) for c in sorted(cpuset))


def wait_port(port, timeout=15.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def start(cmd, cpus, env=None, log=None):
    full_env = dict(os.environ)
    if env:
        full_env.update(env)
    out = open(log, "ab") if log else subprocess.DEVNULL
    return subprocess.Popen(
        ["taskset", "-c", cpuset_arg(cpus)] + [str(c) for c in cmd],
        stdout=out, stderr=subprocess.STDOUT, env=full_env,
        start_new_session=True,
    )


def stop(proc):
    if proc is None or proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except OSError:
        proc.terminate()
    try:
        proc.wait(timeout=3)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except OSError:
            proc.kill()
        proc.wait()


def server_cmd(workload, server, cap, workers):
    addr = f"127.0.0.1:{SERVER_PORT}"
    if server == "compio":
        return [COMPIO_BIN / workload, addr, cap, workers]
    if workload == "echo":
        binname = "echo_tpc" if server == "tokio-percore" else "echo"
        return [DP_BIN / binname, addr, cap, workers]
    mode = "percore" if server == "tokio-percore" else "default"
    return [DP_BIN / workload, addr, cap, workers, mode]


# ----------------------------------------------------------------------------
# perf (optional)

def perf_works():
    try:
        r = subprocess.run(
            ["sudo", "-n", "perf", "stat", "-e", "raw_syscalls:sys_enter", "-a", "sleep", "0.1"],
            capture_output=True, text=True, timeout=10,
        )
        return bool(re.search(r"[\d,]+\s+raw_syscalls:sys_enter", r.stderr))
    except Exception:
        return False


def perf_start(pid, seconds, outfile):
    return subprocess.Popen(
        ["sudo", "-n", "perf", "stat", "-e", "raw_syscalls:sys_enter",
         "-p", str(pid), "-o", str(outfile), "--", "sleep", str(seconds)],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def perf_count(outfile):
    try:
        m = re.search(r"([\d,]+)\s+raw_syscalls:sys_enter", Path(outfile).read_text())
        return int(m.group(1).replace(",", "")) if m else None
    except OSError:
        return None


# ----------------------------------------------------------------------------
# one measured run

CLIENT_RE = re.compile(r"requests (\d+)\s+\(([\d.]+) req/s\)")
LAT_RE = re.compile(
    r"latency us\s+p50 ([\d.]+)\s+p90 ([\d.]+)\s+p99 ([\d.]+)\s+p99\.9 ([\d.]+)\s+max ([\d.]+)"
)
FAILED_RE = re.compile(r"failed (\d+)")


def run_point(point, server, args, paths, use_perf, logf):
    name, workload = point["name"], point["workload"]
    backends = []
    serverp = None
    perfp = None
    row = dict(point=name, server=server, workers=args.workers,
               seconds=args.seconds, error="")
    try:
        # Backends (compio echo on its own CPU) for relay / fanout.
        nb = point.get("backends", 0)
        upstreams = []
        for i in range(nb):
            port = BACKEND_PORTS[i]
            upstreams.append(f"127.0.0.1:{port}")
            backends.append(start(
                [COMPIO_BIN / "echo", f"127.0.0.1:{port}", 4096, 1],
                args.backend_cpus, log=paths["runlog"],
            ))
        for i in range(nb):
            if not wait_port(BACKEND_PORTS[i]):
                row["error"] = f"backend {BACKEND_PORTS[i]} not up"
                return row

        env = dict(point.get("env", {}))
        env["FILE_DIR"] = str(paths["files"])
        env["WAL_DIR"] = str(paths["wal"])
        if nb == 1:
            env["UPSTREAM_ADDR"] = upstreams[0]
        elif nb > 1:
            env["UPSTREAM_ADDRS"] = ",".join(upstreams)

        # Fresh WAL dir per run so disk use and dirty state never accumulate.
        if workload == "wal":
            shutil.rmtree(paths["wal"], ignore_errors=True)
            paths["wal"].mkdir(parents=True, exist_ok=True)

        # Warm the page cache for file runs so every server reads hot.
        if workload == "file_server" and not args.cold:
            for f in paths["files"].glob("f*.bin"):
                f.read_bytes()
        if args.cold and workload == "file_server":
            subprocess.run(["sudo", "-n", "sh", "-c",
                            "sync; echo 3 > /proc/sys/vm/drop_caches"], check=False)

        serverp = start(server_cmd(workload, server, args.capacity, args.workers),
                        args.server_cpus, env=env, log=paths["runlog"])
        if not wait_port(SERVER_PORT):
            row["error"] = "server not up"
            return row
        time.sleep(0.3)  # let the probe connection drain and workers settle

        pid = serverp.pid
        sampler = ThreadSampler(pid)
        sampler.start()

        u0, s0 = proc_times(pid)
        v0, i0 = proc_ctx(pid)
        io0 = proc_io(pid)
        srv0 = cpu_busy(args.server_cpus)
        cli0 = cpu_busy(args.client_cpus)
        if use_perf:
            perfp = perf_start(pid, args.seconds + 0.5, paths["perf"])

        load_cmd = [COMPIO_BIN / point["load"], f"127.0.0.1:{SERVER_PORT}",
                    "--seconds", args.seconds] + point["largs"]
        client = subprocess.run(
            ["taskset", "-c", cpuset_arg(args.client_cpus)] + [str(c) for c in load_cmd],
            capture_output=True, text=True, env={**os.environ, "FILE_DIR": str(paths["files"])},
            timeout=args.seconds + 120,
        )

        u1, s1 = proc_times(pid)
        v1, i1 = proc_ctx(pid)
        io1 = proc_io(pid)
        srv1 = cpu_busy(args.server_cpus)
        cli1 = cpu_busy(args.client_cpus)
        st = proc_status_kv(pid)
        row["threads_max"] = sampler.stop()
        if perfp:
            perfp.wait(timeout=10)

        out = client.stdout + client.stderr
        print(f"--- {name} {server} ---\n{out}", file=logf, flush=True)
        m = CLIENT_RE.search(out)
        lat = LAT_RE.search(out)
        fail = FAILED_RE.search(out)
        if client.returncode != 0 or not m or not lat:
            row["error"] = f"client rc={client.returncode}"
            return row

        reqs = int(m.group(1))
        row.update(
            requests=reqs,
            rps=float(m.group(2)),
            p50=float(lat.group(1)), p90=float(lat.group(2)),
            p99=float(lat.group(3)), p999=float(lat.group(4)), pmax=float(lat.group(5)),
            failed_conns=int(fail.group(1)) if fail else 0,
            cpu_user_s=u1 - u0, cpu_sys_s=s1 - s0,
            ctx_vol=v1 - v0, ctx_invol=i1 - i0,
            syscr=io1.get("syscr", 0) - io0.get("syscr", 0),
            syscw=io1.get("syscw", 0) - io0.get("syscw", 0),
            rchar=io1.get("rchar", 0) - io0.get("rchar", 0),
            wchar=io1.get("wchar", 0) - io0.get("wchar", 0),
            disk_read=io1.get("read_bytes", 0) - io0.get("read_bytes", 0),
            disk_write=io1.get("write_bytes", 0) - io0.get("write_bytes", 0),
            rss_peak_kb=st.get("VmHWM", 0),
            srv_busy=busy_share(srv0, srv1),
            cli_busy=busy_share(cli0, cli1),
        )
        if use_perf:
            c = perf_count(paths["perf"])
            if c is not None:
                row["syscalls_total"] = c
        if reqs:
            cpu = row["cpu_user_s"] + row["cpu_sys_s"]
            row["cpu_us_per_req"] = cpu * 1e6 / reqs
            row["sys_share"] = row["cpu_sys_s"] / cpu if cpu else 0.0
            row["ctx_per_req"] = (row["ctx_vol"] + row["ctx_invol"]) / reqs
            row["rw_syscalls_per_req"] = (row["syscr"] + row["syscw"]) / reqs
            if "syscalls_total" in row:
                row["syscalls_per_req"] = row["syscalls_total"] / reqs
        return row
    finally:
        if perfp and perfp.poll() is None:
            perfp.terminate()
        stop(serverp)
        for b in backends:
            stop(b)
        time.sleep(0.2)  # let the port close before the next run


# ----------------------------------------------------------------------------
# reporting

TSV_COLS = [
    "point", "server", "workers", "seconds", "requests", "rps",
    "p50", "p90", "p99", "p999", "pmax", "failed_conns",
    "cpu_user_s", "cpu_sys_s", "cpu_us_per_req", "sys_share",
    "ctx_vol", "ctx_invol", "ctx_per_req",
    "syscr", "syscw", "rw_syscalls_per_req", "syscalls_total", "syscalls_per_req",
    "rchar", "wchar", "disk_read", "disk_write",
    "rss_peak_kb", "threads_max", "srv_busy", "cli_busy", "error",
]


def fmt(v):
    if isinstance(v, float):
        return f"{v:.3f}"
    return str(v)


def write_tsv(rows, path):
    with open(path, "w") as f:
        print("\t".join(TSV_COLS), file=f)
        for r in rows:
            print("\t".join(fmt(r.get(c, "")) for c in TSV_COLS), file=f)


def median_rows(rows):
    """One row per (point, server): median over repeats of every numeric field."""
    keys = {}
    for r in rows:
        if r.get("error"):
            continue
        keys.setdefault((r["point"], r["server"]), []).append(r)
    out = []
    for (point, server), rs in keys.items():
        agg = dict(point=point, server=server, runs=len(rs))
        for col in TSV_COLS:
            vals = [r[col] for r in rs if isinstance(r.get(col), (int, float))]
            if vals:
                agg[col] = statistics.median(vals)
        out.append(agg)
    return out


def write_report(rows, meds, args, path, host_info):
    point_order = [p["name"] for p in POINTS]
    server_order = SERVERS
    by_key = {(m["point"], m["server"]): m for m in meds}

    with open(path, "w") as f:
        w = lambda *a: print(*a, file=f)
        w("# I/O-job benchmark: compio-pool vs tokio + deadpool")
        w()
        w(f"{host_info}")
        w(f"Placement: server cpus {cpuset_arg(args.server_cpus)}, clients {cpuset_arg(args.client_cpus)}, "
          f"backends {cpuset_arg(args.backend_cpus)}. {args.workers} workers, {args.seconds} s runs, "
          f"median of {args.repeats}.")
        w()
        w("Every row: one server under one workload. `cpu µs/req` is server user+sys CPU per request.")
        w("`rw sysc/req` is read/write-family syscalls per request from /proc/<pid>/io — socket")
        w("send/recv don't count there, so it isolates **file** I/O: tokio's blocking-pool file reads,")
        w("writes and nothing else; io_uring file ops never appear. `sysc/req` is ALL syscalls (perf")
        w("raw_syscalls:sys_enter, when available) and carries the socket story too. `ctx/req` is")
        w("context switches per request. `threads` is the high-water thread count — for the tokio")
        w("servers that is the blocking pool, for compio it includes io_uring's kernel io-wq workers.")
        w("`busy` flags who saturated: the server's cpus (S), the clients' (C), or both (B) — a C row")
        w("is a lower bound on the server.")
        w()
        for point in point_order:
            have = [s for s in server_order if (point, s) in by_key]
            if not have:
                continue
            w(f"## {point}")
            w()
            w("| server | req/s | p50 µs | p99 µs | cpu µs/req | sys% | ctx/req | rw sysc/req | sysc/req | peak RSS MB | threads | disk w MB/s | busy |")
            w("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
            base = by_key.get((point, server_order[0]))
            for s in have:
                m = by_key[(point, s)]
                flag = ("B" if m.get("srv_busy", 0) > 0.85 and m.get("cli_busy", 0) > 0.85
                        else "S" if m.get("srv_busy", 0) > 0.85
                        else "C" if m.get("cli_busy", 0) > 0.90 else "-")
                rel = ""
                if base and s != server_order[0] and base.get("rps"):
                    rel = f" ({m.get('rps', 0) / base['rps'] * 100:.0f}%)"
                dw = m.get("disk_write", 0) / args.seconds / 1e6
                w(f"| {s} | {m.get('rps', 0):,.0f}{rel} | {m.get('p50', 0):.1f} | {m.get('p99', 0):.1f} "
                  f"| {m.get('cpu_us_per_req', 0):.1f} | {m.get('sys_share', 0) * 100:.0f} "
                  f"| {m.get('ctx_per_req', 0):.2f} | {m.get('rw_syscalls_per_req', 0):.2f} "
                  f"| {m.get('syscalls_per_req', float('nan')):.2f} "
                  f"| {m.get('rss_peak_kb', 0) / 1024:.1f} | {m.get('threads_max', 0):.0f} "
                  f"| {dw:.1f} | {flag} |")
            w()


# ----------------------------------------------------------------------------
# main

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seconds", type=int, default=5)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--capacity", type=int, default=1024)
    ap.add_argument("--server-cpus", default="1-4")
    ap.add_argument("--client-cpus", default="5-11")
    ap.add_argument("--backend-cpus", default="0")
    ap.add_argument("--points", default="", help="comma-separated subset of point names")
    ap.add_argument("--servers", default="", help="comma-separated subset of servers")
    ap.add_argument("--out", default="")
    ap.add_argument("--quick", action="store_true", help="1 repeat, 2 s runs")
    ap.add_argument("--cold", action="store_true", help="drop page caches before file runs (needs sudo)")
    ap.add_argument("--no-perf", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.quick:
        args.repeats, args.seconds = 1, 2
    args.server_cpus = parse_cpuset(args.server_cpus)
    args.client_cpus = parse_cpuset(args.client_cpus)
    args.backend_cpus = parse_cpuset(args.backend_cpus)

    points = POINTS
    if args.points:
        want = set(args.points.split(","))
        points = [p for p in POINTS if p["name"] in want]
        missing = want - {p["name"] for p in points}
        if missing:
            sys.exit(f"unknown points: {', '.join(sorted(missing))}")
    servers = SERVERS
    if args.servers:
        servers = [s for s in args.servers.split(",") if s]
        bad = set(servers) - set(SERVERS)
        if bad:
            sys.exit(f"unknown servers: {', '.join(sorted(bad))}")

    runs = [(p, s) for p in points for s in servers]
    est = len(runs) * args.repeats * (args.seconds + 3)
    print(f"{len(runs)} combinations x {args.repeats} repeats x {args.seconds}s "
          f"~= {est // 60} min {est % 60} s")
    for p, s in runs:
        print(f"  {p['name']:<12} {s}")
    if args.dry_run:
        return

    if sys.platform != "linux":
        sys.exit("iobench.py reads /proc and must run on Linux")
    for binpath in (COMPIO_BIN / "echo", DP_BIN / "echo"):
        if not binpath.exists():
            sys.exit(
                f"missing {binpath} — build first (CARGO_TARGET_DIR pins the "
                "output under each crate even where ~/.cargo/config.toml "
                "redirects it, and keeps the two crates' same-named examples "
                "from overwriting each other):\n"
                f"  CARGO_TARGET_DIR={REPO}/target cargo build --release --examples\n"
                f"  CARGO_TARGET_DIR={REPO}/crates/deadpool-baseline/target "
                "cargo build --release --manifest-path crates/deadpool-baseline/Cargo.toml --examples")

    # File descriptors for many-connection runs.
    try:
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (min(65535, hard), hard))
    except Exception:
        pass

    out_dir = Path(args.out) if args.out else \
        Path.home() / "iobench" / f"results-{datetime.datetime.now():%Y%m%d-%H%M%S}"
    out_dir.mkdir(parents=True, exist_ok=True)
    work = Path.home() / "iobench"
    paths = dict(
        files=work / "files",
        wal=work / "wal",
        runlog=out_dir / "servers.log",
        perf=out_dir / "perf.tmp",
    )
    paths["files"].mkdir(parents=True, exist_ok=True)
    paths["wal"].mkdir(parents=True, exist_ok=True)

    # Pre-create every file size the matrix requests, so first runs are warm.
    for p in points:
        if p["workload"] != "file_server":
            continue
        largs = p["largs"]
        size = int(largs[largs.index("--bytes") + 1])
        count = int(largs[largs.index("--files") + 1])
        body = bytes(i % 251 for i in range(size))
        for i in range(count):
            fp = paths["files"] / f"f{size}-{i}.bin"
            if not fp.exists() or fp.stat().st_size != size:
                fp.write_bytes(body)

    use_perf = not args.no_perf and perf_works()
    print(f"perf syscall counting: {'on' if use_perf else 'off'}")

    host_info = subprocess.run(["uname", "-srm"], capture_output=True, text=True).stdout.strip()
    ncpu = os.cpu_count()
    host_info = f"{host_info}, {ncpu} cpus, {datetime.datetime.now():%Y-%m-%d %H:%M}"

    rows = []
    logf = open(out_dir / "iobench.log", "w")
    print(host_info, file=logf)
    t0 = time.time()
    total = len(runs) * args.repeats
    done = 0
    try:
        for rep in range(args.repeats):
            for p in points:
                for s in servers:
                    done += 1
                    print(f"[{done}/{total}] rep {rep + 1} {p['name']:<12} {s:<14}", end="", flush=True)
                    row = run_point(p, s, args, paths, use_perf, logf)
                    row["rep"] = rep
                    rows.append(row)
                    if row.get("error"):
                        print(f"  ERROR {row['error']}")
                    else:
                        print(f"  {row['rps']:>10,.0f} req/s  p50 {row['p50']:>8.1f} us  "
                              f"cpu {row.get('cpu_us_per_req', 0):>6.1f} us/req  "
                              f"rw-sysc {row.get('rw_syscalls_per_req', 0):>5.2f}/req")
                    write_tsv(rows, out_dir / "results.tsv")
    except KeyboardInterrupt:
        print("\ninterrupted — writing what we have")

    meds = median_rows(rows)
    write_report(rows, meds, args, out_dir / "report.md", host_info)
    write_tsv(rows, out_dir / "results.tsv")
    logf.close()
    mins = (time.time() - t0) / 60
    print(f"\n{len(rows)} runs in {mins:.1f} min")
    print(f"results: {out_dir}/results.tsv")
    print(f"report:  {out_dir}/report.md")


if __name__ == "__main__":
    main()
