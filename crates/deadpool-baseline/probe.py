#!/usr/bin/env python3
"""Throughput *and* cost per request for the three echo servers.

Throughput alone cannot say why one server is faster on a box where the load
generator shares cores with it: it mixes the server's own cost with where the
kernel happens to put 64 client threads. This runs `examples/load.rs` against

    tokio-default    this crate's `echo`      multi-threaded work-stealing runtime
    tokio-per-core   this crate's `echo_tpc`  tokio on epoll, thread-per-core
    compio-pool      the parent crate's `echo`  io_uring, thread-per-core

and, over the load window only, records

  * req/s and the client's latency line
  * server CPU microseconds per request, user + system      (/proc/<pid>/stat)
  * server context switches per request, voluntary / not    (/proc/<pid>/task/*)
  * client CPU microseconds per request                     (wait4 rusage)
  * system-wide idle share and context switches per request (/proc/stat)

CPU per request does not depend on scheduling luck; req/s on a saturated box is
just `cpus / (server + client µs per request)`.

Placement is the lever. `--server-cpus` and `--load-cpus` are `taskset` lists:

    # clients share the server's cores (what plain `cargo run` gives you)
    probe.py --label shared --workers 4 --conns 64 --bytes 16384

    # server on CPU 0, clients kept off it: the closest this gets to a remote client
    probe.py --label split --workers 1 --server-cpus 0 --load-cpus 1,2,3 --bytes 16384

    # everything forced onto one CPU
    probe.py --label one-cpu --workers 1 --server-cpus 0 --load-cpus 0 --conns 16

Build first (from the repository root):

    cargo build --release --example echo --example load
    cargo build --release --manifest-path crates/deadpool-baseline/Cargo.toml \\
        --example echo --example echo_tpc

Linux only; standard library only. Medians of --repeats runs, by req/s.
"""
import argparse
import os
import re
import resource
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASELINE = ROOT / "crates" / "deadpool-baseline" / "target" / "release" / "examples"
PARENT = ROOT / "target" / "release" / "examples"
SERVERS = {
    "tokio-default": BASELINE / "echo",
    "tokio-per-core": BASELINE / "echo_tpc",
    "compio-pool": PARENT / "echo",
}
LOAD = PARENT / "load"
TICK = os.sysconf("SC_CLK_TCK")


def wait_port(port, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def proc_times(pid):
    """(utime, stime) in clock ticks, all threads."""
    with open(f"/proc/{pid}/stat") as f:
        rest = f.read().rsplit(")", 1)[1].split()
    return int(rest[11]), int(rest[12])


def proc_ctx(pid):
    """(voluntary, involuntary) context switches summed over all threads."""
    vol = invol = 0
    for tid in os.listdir(f"/proc/{pid}/task"):
        try:
            status = open(f"/proc/{pid}/task/{tid}/status").read()
        except FileNotFoundError:  # thread exited
            continue
        vol += int(re.search(r"^voluntary_ctxt_switches:\s+(\d+)", status, re.M).group(1))
        invol += int(re.search(r"^nonvoluntary_ctxt_switches:\s+(\d+)", status, re.M).group(1))
    return vol, invol


def sys_stat():
    """(cpu jiffies [user nice system idle iowait irq softirq steal], context switches)."""
    lines = open("/proc/stat").read().splitlines()
    cpu = list(map(int, lines[0].split()[1:9]))
    ctxt = int(next(l for l in lines if l.startswith("ctxt")).split()[1])
    return cpu, ctxt


def one_run(server, port, workers, server_cpus, load_cpus, conns, nbytes, seconds,
            capacity=1024, load_extra=(), capture_stats=False):
    """One server, one load run. `capacity` is the per-worker pool size (ignored by echo_tpc),
    `load_extra` extra `load` flags such as ("--reconnect", "1"), `capture_stats` keeps the
    server's last stats line (compio-pool's counters, the baseline's pool state)."""
    cmd = [str(SERVERS[server]), f"127.0.0.1:{port}", str(capacity), str(workers)]
    if server_cpus:
        cmd = ["taskset", "-c", server_cpus] + cmd
    stats_file = tempfile.TemporaryFile("w+") if capture_stats else None
    errlog = tempfile.TemporaryFile("w+")
    srv = subprocess.Popen(cmd, stdout=stats_file or subprocess.DEVNULL, stderr=errlog)
    try:
        if not wait_port(port):
            errlog.seek(0)
            raise RuntimeError(f"{server} did not start on port {port}: "
                               f"{errlog.read().strip()[-300:] or 'no output'}")
        time.sleep(0.4)
        lcmd = [str(LOAD), f"127.0.0.1:{port}", "--conns", str(conns),
                "--seconds", str(seconds), "--bytes", str(nbytes), *map(str, load_extra)]
        if load_cpus:
            lcmd = ["taskset", "-c", load_cpus] + lcmd

        u0, s0 = proc_times(srv.pid)
        v0, i0 = proc_ctx(srv.pid)
        c0, x0 = sys_stat()
        # stderr merged so a flood of "client failed" lines cannot fill a pipe.
        ru0 = resource.getrusage(resource.RUSAGE_CHILDREN)
        lp = subprocess.Popen(lcmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        try:
            out, _ = lp.communicate(timeout=seconds + 30 + conns * 0.02)
        except subprocess.TimeoutExpired:
            # load.rs returns from a client that fails to connect *before* its start
            # barrier, so every other client then waits forever. Past a backlog or
            # file-descriptor limit that is a hang, not a slow run.
            lp.kill()
            lp.communicate()
            raise RuntimeError(f"load did not finish ({conns} conns): a client most likely failed "
                               "to connect (listen backlog or fd limit) and the rest are stuck")
        ru1 = resource.getrusage(resource.RUSAGE_CHILDREN)
        u1, s1 = proc_times(srv.pid)
        v1, i1 = proc_ctx(srv.pid)
        c1, x1 = sys_stat()
        time.sleep(1.1 if capture_stats else 0)  # let the once-a-second stats line catch up
    finally:
        srv.terminate()
        try:
            srv.wait(timeout=3)
        except subprocess.TimeoutExpired:
            srv.kill()
    stats = ""
    if stats_file is not None:
        stats_file.seek(0)
        lines = [l for l in stats_file.read().splitlines() if l.startswith("active")]
        stats = lines[-1] if lines else ""
        stats_file.close()

    m = re.search(r"requests (\d+)\s+\((\d+) req/s\)", out)
    if not m:
        raise RuntimeError(f"unparseable load output:\n{out}")
    reqs, rps = int(m.group(1)), int(m.group(2))
    dcpu = [b - a for a, b in zip(c0, c1)]
    total = sum(dcpu) or 1
    us = lambda ticks: ticks * 1e6 / TICK
    return dict(
        rps=rps,
        failed=int(re.search(r"failed (\d+)", out).group(1)),
        lat=re.search(r"latency us\s+(.*)", out).group(1),
        srv_user_us=us(u1 - u0) / reqs,
        srv_sys_us=us(s1 - s0) / reqs,
        srv_vol_cs=(v1 - v0) / reqs,
        srv_invol_cs=(i1 - i0) / reqs,
        cli_us=((ru1.ru_utime - ru0.ru_utime) + (ru1.ru_stime - ru0.ru_stime)) * 1e6 / reqs,
        sys_cs=(x1 - x0) / reqs,
        idle_pct=100.0 * dcpu[3] / total,
        reqs=reqs,
        secs=float(re.search(r"seconds (\d+)", out).group(1)),
        stats=stats,
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--label", default="run")
    ap.add_argument("--servers", default=",".join(SERVERS))
    ap.add_argument("--workers", type=int, required=True)
    ap.add_argument("--server-cpus", default="", help="taskset list for the server, e.g. 0 or 0,1")
    ap.add_argument("--load-cpus", default="", help="taskset list for the load generator")
    ap.add_argument("--conns", type=int, default=64)
    ap.add_argument("--bytes", type=int, default=512)
    ap.add_argument("--seconds", type=int, default=5)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--port", type=int, default=7700, help="first port; each run takes the next")
    a = ap.parse_args()

    missing = [str(p) for p in [*SERVERS.values(), LOAD] if not p.exists()]
    if missing:
        sys.exit("not built: " + ", ".join(missing) + "\nsee the build commands in --help")

    print(f"## {a.label}: workers={a.workers} server_cpus={a.server_cpus or 'any'} "
          f"load_cpus={a.load_cpus or 'any'} conns={a.conns} bytes={a.bytes} "
          f"{a.repeats} x {a.seconds}s")
    print(f"{'server':<15}{'req/s':>9}  {'srv us/req usr+sys':>20}  {'srv cs/req v/inv':>17}  "
          f"{'srv cpu':>7}  {'cli us/req':>10}  {'sys cs/req':>10}  {'idle%':>5}  failed  latency us")
    port = a.port
    for server in a.servers.split(","):
        runs = []
        for _ in range(a.repeats):
            runs.append(one_run(server, port, a.workers, a.server_cpus, a.load_cpus,
                                a.conns, a.bytes, a.seconds))
            port += 1
            time.sleep(0.3)
        runs.sort(key=lambda r: r["rps"])
        m = runs[(len(runs) - 1) // 2]
        srv_cpu = (m["srv_user_us"] + m["srv_sys_us"]) * m["rps"] / 1e6  # CPUs' worth
        print(f"{server:<15}{m['rps']:>9}  "
              f"{m['srv_user_us']:>9.2f}+{m['srv_sys_us']:<10.2f}  "
              f"{m['srv_vol_cs']:>8.2f}/{m['srv_invol_cs']:<8.2f}  "
              f"{srv_cpu:>7.2f}  {m['cli_us']:>10.2f}  {m['sys_cs']:>10.2f}  "
              f"{m['idle_pct']:>5.1f}  {sum(r['failed'] for r in runs):>6}  {m['lat']}"
              f"   [{runs[0]['rps']}-{runs[-1]['rps']}]")


if __name__ == "__main__":
    main()
