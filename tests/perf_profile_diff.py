#!/usr/bin/env python3
"""Diff the CDC profile of a fast run against a slow one.

Usage:  perf_profile_diff.py <fast.log> <slow.log> [...more logs]

Each log is a perf_<driver>_cdc.log dump. Prints, per instrumented stage, the total wall time
and the share of the measured window, so the stage that differs between regimes stands out.
"""
import re
import sys

RE_TOTAL = re.compile(r"\[profile-total\] t=([\d.]+)s go\{([^}]*)\} (.*)")
RE_TICK = re.compile(r"\[profile\] t=([\d.]+)s go\{([^}]*)\} (.*)")
RE_CONST = re.compile(r"\[profile-const\] (.*)")
RE_SPAN = re.compile(r"([\w.]+)=([\dhms.µn]+)/(\d+)(?:/u(\d+)\(([\d.]+)\))?")
RE_SPEED = re.compile(r'"Speed":\s*"([\d.]+) rps"')


def dur_s(text):
    """Parse a Go duration such as 1h2m3.4s / 512ms / 900µs into seconds."""
    total, num = 0.0, ""
    units = {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "µs": 1e-6, "us": 1e-6, "ns": 1e-9}
    i = 0
    while i < len(text):
        c = text[i]
        if c.isdigit() or c == ".":
            num += c
            i += 1
            continue
        unit = text[i:i + 2] if text[i:i + 2] in units else c
        total += float(num or 0) * units.get(unit, 0)
        num = ""
        i += len(unit)
    return total


def parse(path):
    consts, ticks, total, window, speed = [], [], None, 0.0, None
    for line in open(path, errors="ignore"):
        if m := RE_CONST.search(line):
            consts.append(m.group(1).strip())
        if m := RE_SPEED.search(line):
            speed = float(m.group(1))
        for regex, sink in ((RE_TICK, ticks), (RE_TOTAL, None)):
            if m := regex.search(line):
                spans = {}
                for name, d, calls, units, per in RE_SPAN.findall(m.group(3)):
                    spans[name] = {
                        "s": dur_s(d),
                        "calls": int(calls),
                        "units": int(units) if units else 0,
                        "per": float(per) if per else 0.0,
                    }
                entry = (float(m.group(1)), m.group(2), spans)
                if sink is None:
                    total, window = spans, float(m.group(1))
                else:
                    sink.append(entry)
    # A container killed at the window never reaches the clean-exit total, so rebuild it by
    # summing the interval lines. Costs at most one un-emitted tick of counters.
    if total is None and ticks:
        total, window = {}, ticks[-1][0]
        for _, _, spans in ticks:
            for name, v in spans.items():
                acc = total.setdefault(name, {"s": 0.0, "calls": 0, "units": 0, "per": 0.0})
                acc["s"] += v["s"]
                acc["calls"] += v["calls"]
                acc["units"] += v["units"]
        for v in total.values():
            v["per"] = v["units"] / v["calls"] if v["calls"] else 0.0
    return {"consts": consts, "ticks": ticks, "total": total, "window": window,
            "speed": speed, "path": path}


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    runs = [parse(p) for p in sys.argv[1:]]

    for r in runs:
        if not r["total"]:
            print(f"!! {r['path']}: no [profile-total] line "
                  "(OLAKE_TIMING unset, or the container was killed before the final tick)")
    runs = [r for r in runs if r["total"]]
    if not runs:
        sys.exit(1)

    names = sorted({n for r in runs for n in r["total"]})
    labels = [r["path"].split("/")[-1][:26] for r in runs]

    print("\n=== per-run constants ===")
    for r, lab in zip(runs, labels):
        spd = r["speed"] if r["speed"] else float("nan")
        print("%-28s window=%.0fs speed=%.0f rps" % (lab, r["window"], spd))
        for c in r["consts"]:
            print(f"{'':28s}   {c}")

    print("\n=== stage wall time (share of window) ===")
    print(f"{'stage':32s}" + "".join(f"{lab:>28s}" for lab in labels))
    for n in names:
        row = f"{n:32s}"
        for r in runs:
            v = r["total"].get(n)
            if not v:
                row += f"{'-':>28s}"
                continue
            pct = 100 * v["s"] / r["window"] if r["window"] else 0
            cell = "%8.1fs %5.1f%% n=%d" % (v["s"], pct, v["calls"])
            row += "%28s" % cell
        print(row)

    print("\n=== mean latency per call (ms) and units per call ===")
    print(f"{'stage':32s}" + "".join(f"{lab:>28s}" for lab in labels))
    for n in names:
        row = f"{n:32s}"
        for r in runs:
            v = r["total"].get(n)
            if not v or not v["calls"]:
                row += f"{'-':>28s}"
                continue
            cell = "%9.3fms u/c=%.0f" % (1000 * v["s"] / v["calls"], v["per"])
            row += "%28s" % cell
        print(row)

    stream_timeline(runs)

    print("\n=== ratio vs first run (>1 means slower in that run) ===")
    base = runs[0]
    for n in names:
        b = base["total"].get(n)
        if not b or b["s"] <= 0:
            continue
        cells = []
        for r in runs[1:]:
            v = r["total"].get(n)
            cells.append(f"{v['s'] / b['s']:6.2f}x" if v else "     -")
        print(f"{n:32s}" + "".join(f"{c:>12s}" for c in cells))


def stream_timeline(runs):
    """Which CDC stream is flowing in each interval.

    mysql writes one 15M-row transaction per table, so if the binlog serves each table as one
    contiguous block only one of the two writers is ever busy — that shows up here as one stream
    holding ~100% of an interval for a long stretch.
    """
    for r in runs:
        names = sorted({n for _, _, sp in r["ticks"] for n in sp if n.startswith("stream.")})
        if len(names) < 1:
            continue
        print(f"\n=== per-interval stream mix — {r['path'].split('/')[-1]} ===")
        print("  " + "  ".join(n.replace("stream.", "")[-18:] for n in names))
        for t, _, sp in r["ticks"]:
            counts = [sp.get(n, {}).get("calls", 0) for n in names]
            tot = sum(counts) or 1
            bars = "  ".join("%5d %3.0f%%" % (c, 100 * c / tot) for c in counts)
            print("  t=%-5.0f %s" % (t, bars))


if __name__ == "__main__":
    main()
