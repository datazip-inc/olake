"""Validate Iceberg tables against source aggregates. usage: validate.py <db> <table>..."""
import os, sys, json
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
REGION = os.environ["AWS_REGION"]
cat = load_catalog("glue", **{"type": "glue", "glue.region": REGION, "s3.region": REGION})
db = sys.argv[1]
for name in sys.argv[2:]:
    try:
        t = cat.load_table(f"{db}.{name}")
        s = t.current_snapshot().summary.additional_properties
        meta = {"data_files": int(s.get("total-data-files", 0)), "delete_files": int(s.get("total-delete-files", 0)),
                "position_deletes": int(s.get("total-position-deletes", 0)), "equality_deletes": int(s.get("total-equality-deletes", 0)),
                "size_gb": round(int(s.get("total-files-size", 0)) / 1e9, 2), "snapshots": len(t.snapshots())}
    except Exception as e:
        print(json.dumps({"table": name, "error": f"{type(e).__name__}: {e}"[:300]}), flush=True); continue
    try:
        a = t.scan(selected_fields=("c_custkey", "c_acctbal")).to_arrow()
        k = a["c_custkey"]
        out = {"table": name, "rows": a.num_rows, "distinct_keys": pc.count_distinct(k).as_py(),
               "sum_custkey": pc.sum(pc.cast(k, "int64")).as_py(),
               "sum_acctbal": round(float(pc.sum(pc.cast(a["c_acctbal"], "float64")).as_py()), 2)}
    except Exception as e:   # for example: reader does not support the delete files in this table
        out = {"table": name, "read_error": f"{type(e).__name__}: {e}"[:300]}
    out.update(meta)
    print(json.dumps(out), flush=True)
