"""Poll an Iceberg table in Glue and log committed row count over time.
usage: count.py <db.table> <target_rows|+delta> <timeout_seconds> [poll_seconds]
Prints CSV lines: epoch_seconds,total_records. Exits 0 when total_records >= target.
A target written as +N means "N more rows than the first value seen".
total_records counts rows in data files; rows removed by delete files are still counted."""
import os, sys, time
from pyiceberg.catalog import load_catalog

ident, target_arg, timeout = sys.argv[1], sys.argv[2], int(sys.argv[3])
interval = float(sys.argv[4]) if len(sys.argv) > 4 else 5.0
target = None if target_arg.startswith("+") else int(target_arg)
REGION = os.environ["AWS_REGION"]
cat = load_catalog("glue", **{"type": "glue", "glue.region": REGION, "s3.region": REGION})
start = time.time()
last = None
while True:
    n = -1
    try:
        snap = cat.load_table(ident).current_snapshot()
        if snap is None:
            n = 0
        else:
            props = getattr(snap.summary, "additional_properties", None) or {}
            n = int(props.get("total-records", 0))
    except Exception as e:  # table not created yet, or transient catalog error
        n = -1
    if target is None and n >= 0:
        target = n + int(target_arg[1:])
    if n != last:
        print(f"{time.time():.1f},{n}", flush=True)
        last = n
    if target is not None and n >= target:
        sys.exit(0)
    if time.time() - start > timeout:
        print(f"{time.time():.1f},TIMEOUT", flush=True)
        sys.exit(2)
    time.sleep(interval)
