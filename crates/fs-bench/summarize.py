#!/usr/bin/env python3
"""Turns a `run.sh` JSONL file into the tables the report is built from.

Reps are combined with a median, not a mean: `docs/performance.md` records that
a single point can swing by more than 2x between runs of one binary, and a mean
lets one such excursion move the number a reader will quote.

Usage:
    crates/fs-bench/summarize.py results.jsonl            # markdown tables
    crates/fs-bench/summarize.py results.jsonl --json out.json
"""

import json
import statistics
import sys
from collections import defaultdict

ARMS = ["compio", "deadpool", "tokio"]
CASES = [
    "stat",
    "open_close",
    "acquire_read4k",
    "read_4k_rand",
    "read_64k_seq",
    "write_4k_fsync",
    "write_1m_buffered",
]


def load(path):
    """Groups every record by the point it belongs to."""
    # point -> metric -> [value per rep]
    points = defaultdict(lambda: defaultdict(list))
    floors, meta = {}, {}
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        d = json.loads(line)
        kind = d["kind"]
        if kind == "floor":
            floors[d["case"]] = d
            continue
        if kind == "meta":
            meta[d.get("arm", "?")] = d
            continue
        key = (d["surface"], d["panel"], d["case"], d["arm"], d["threads"], d["depth"])
        if kind == "stat":
            for k in ("p50", "p95", "p99", "p999", "max", "min", "mean", "n"):
                points[key][k].append(d[k])
        else:
            points[key][d["unit"]].append(d["value"])
    return points, floors, meta


def med(points, key, metric):
    v = points.get(key, {}).get(metric)
    return statistics.median(v) if v else None


def fmt(v, unit=""):
    if v is None:
        return "—"
    if unit == "ns":
        if v >= 1_000_000:
            return f"{v / 1e6:.2f} ms"
        if v >= 1_000:
            return f"{v / 1e3:.2f} µs"
        return f"{v:.0f} ns"
    if unit == "ops":
        if v >= 1e6:
            return f"{v / 1e6:.2f} M"
        if v >= 1e3:
            return f"{v / 1e3:.0f} k"
        return f"{v:.0f}"
    if unit == "x":
        return f"{v:.1f}×"
    if unit == "mib":
        return f"{v:,.0f}"
    return f"{v:,.2f}"


def table(points, surface, panel, case, axis_name, axis_values, fixed):
    """One case, one panel: arms as column groups, the swept axis as rows."""
    rows = []
    for a in axis_values:
        threads, depth = (a, fixed) if axis_name == "threads" else (fixed, a)
        row = {axis_name: a}
        for arm in ARMS:
            k = (surface, panel, case, arm, threads, depth)
            row[arm] = {
                "p50": med(points, k, "p50"),
                "p99": med(points, k, "p99"),
                "p999": med(points, k, "p999"),
                "max": med(points, k, "max"),
                "ops": med(points, k, "ops/s"),
                "mib": med(points, k, "MiB/s"),
                "cpu": med(points, k, "cpu_ns/op"),
                "sys": med(points, k, "sys_ns/op"),
                "vcsw": med(points, k, "vcsw/op"),
                "ivcsw": med(points, k, "ivcsw/op"),
                "threads_os": med(points, k, "os_threads"),
                "n": med(points, k, "n"),
                "setup": med(points, k, "setup_ms"),
            }
        rows.append(row)
    return rows


def axis_values(points, surface, panel, case, axis):
    vals = {
        k[4] if axis == "threads" else k[5]
        for k in points
        if k[0] == surface and k[1] == panel and k[2] == case
    }
    return sorted(vals)


def main():
    path = sys.argv[1]
    points, floors, meta = load(path)

    surfaces = sorted({k[0] for k in points})
    out = {"floors": floors, "meta": meta, "panels": []}

    for surface in surfaces:
        for case in CASES:
            keys = [k for k in points if k[0] == surface and k[1] == "threads" and k[2] == case]
            if not keys:
                continue
            depth = keys[0][5]
            out["panels"].append(
                {
                    "surface": surface,
                    "panel": "threads",
                    "case": case,
                    "axis": "threads",
                    "fixed": depth,
                    "rows": table(
                        points, surface, "threads", case, "threads",
                        axis_values(points, surface, "threads", case, "threads"), depth,
                    ),
                }
            )
        for case in CASES:
            keys = [k for k in points if k[0] == surface and k[1] == "depth" and k[2] == case]
            if not keys:
                continue
            threads = keys[0][4]
            out["panels"].append(
                {
                    "surface": surface,
                    "panel": "depth",
                    "case": case,
                    "axis": "depth",
                    "fixed": threads,
                    "rows": table(
                        points, surface, "depth", case, "depth",
                        axis_values(points, surface, "depth", case, "depth"), threads,
                    ),
                }
            )

    if "--json" in sys.argv:
        dest = sys.argv[sys.argv.index("--json") + 1]
        with open(dest, "w") as f:
            json.dump(out, f, indent=1)
        print(f"wrote {dest}")

    print("\n## Reference constants (this machine)\n")
    print("| what | p50 | p99 |")
    print("|---|---|---|")
    for c, d in floors.items():
        print(f"| `{c}` | {fmt(d['p50'], 'ns')} | {fmt(d['p99'], 'ns')} |")

    for p in out["panels"]:
        print(f"\n## {p['surface']} · {p['case']} · by {p['axis']} ({p['axis']} swept, "
              f"{'depth' if p['axis'] == 'threads' else 'threads'}={p['fixed']})\n")
        print("| " + p["axis"] + " | arm | p50 | p99 | p99.9 | ops/s | MiB/s | cpu ns/op | vcsw/op | OS threads |")
        print("|---|---|---|---|---|---|---|---|---|---|")
        for row in p["rows"]:
            for arm in ARMS:
                v = row[arm]
                print(
                    f"| {row[p['axis']]} | {arm} | {fmt(v['p50'], 'ns')} | {fmt(v['p99'], 'ns')} | "
                    f"{fmt(v['p999'], 'ns')} | {fmt(v['ops'], 'ops')} | {fmt(v['mib'], 'mib')} | "
                    f"{fmt(v['cpu'])} | {fmt(v['vcsw'])} | {fmt(v['threads_os'], 'mib')} |"
                )


if __name__ == "__main__":
    main()
