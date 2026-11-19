"""Summarize benchmark timings from results/ dir.
usage: summarize.py <results_dir> <tag> <full_rows> <rows_after_cdc>"""
import sys, os, json, re
R = sys.argv[1]; TAG = sys.argv[2]; FULL = int(sys.argv[3]); AFTER = int(sys.argv[4])
def rd(f):
    p = os.path.join(R, f); return float(open(p).read().strip()) if os.path.exists(p) else None
def mem_gb(s):
    m = re.match(r"([\d.]+)(GiB|MiB|KiB|B)", s.strip()); 
    return float(m.group(1)) * {"GiB": 1, "MiB": 1/1024, "KiB": 1/1048576, "B": 0}[m.group(2)] if m else 0
out = []
for pipe in ["olake", "dbz_kc", "flink", "dbz_spark"]:
    for phase, base, target in [("full", 0, FULL), ("cdc", FULL, AFTER)]:
        n = f"{pipe}_{TAG}_{phase}"
        start = rd(n + ".start")
        pts = []
        for line in open(os.path.join(R, n + ".rows.csv")):
            a = line.strip().split(",")
            if len(a) == 2 and a[1].lstrip("-").isdigit(): pts.append((float(a[0]), int(a[1])))
        done = next(t for t, c in pts if c >= target)
        first = next((t for t, c in pts if c > base), None)
        secs = done - start
        cpu = {}; mem = {}; avg_cores = None; avg_mem = None; samples = 0
        sp = os.path.join(R, n + ".stats.csv")
        if os.path.exists(sp):
            snap = {}
            for line in open(sp):
                a = line.strip().split(",")
                if len(a) < 4: continue
                ts, name, c, m = a[0], a[1], a[2], a[3]
                try: cv = float(c.rstrip("%"))
                except ValueError: continue
                d = snap.setdefault(ts, [0.0, 0.0]); d[0] += cv; d[1] += mem_gb(m.split("/")[0])
            if snap:
                cpu = max(v[0] for v in snap.values()); mem = max(v[1] for v in snap.values())
            # averages only over samples taken while the run was in progress
            run = [v for t, v in snap.items() if start <= float(t) <= done + 10]
            if run:
                samples = len(run)
                avg_cores = sum(v[0] for v in run) / samples / 100
                avg_mem = sum(v[1] for v in run) / samples
        rows = target - base
        out.append({"pipeline": pipe, "phase": phase, "rows": rows, "seconds": round(secs, 1),
                    "rows_per_sec": round(rows / secs), "first_commit_s": round(first - start, 1) if first else None,
                    "peak_cpu_pct": round(cpu) if cpu else None, "peak_mem_gb": round(mem, 1) if mem else None,
                    "avg_cores": round(avg_cores, 1) if avg_cores else None,
                    "avg_mem_gb": round(avg_mem, 1) if avg_mem else None,
                    # CPU cost proxy: cores x seconds on the benchmark host, per 1M rows
                    "core_seconds_per_million_rows": round(avg_cores * secs / (rows / 1e6), 1) if avg_cores else None,
                    "cpu_samples": samples})
print(json.dumps(out, indent=1))
