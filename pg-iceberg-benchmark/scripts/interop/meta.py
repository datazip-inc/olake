import os, sys, json
from pyiceberg.catalog import load_catalog
R = os.environ["AWS_REGION"]
cat = load_catalog("glue", **{"type": "glue", "glue.region": R, "s3.region": R})
for name in sys.argv[2:]:
    t = cat.load_table(f"{sys.argv[1]}.{name}")
    s = t.current_snapshot().summary.additional_properties
    print(json.dumps({"table": name, "format_version": t.metadata.format_version,
        "identifier_fields": [t.schema().find_field(i).name for i in t.schema().identifier_field_ids],
        "types": {f.name: str(f.field_type) + ("" if f.required else "?") for f in t.schema().fields},
        "partition_spec": str(t.spec()), "delete_files": s.get("total-delete-files"), "eq_deletes": s.get("total-equality-deletes"),
        "props": {k: v for k, v in t.metadata.properties.items()}}))
