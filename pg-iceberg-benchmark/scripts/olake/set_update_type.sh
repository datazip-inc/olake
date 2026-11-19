#!/bin/bash
# Switch an existing OLake run to another delete mode. The next sync rewrites existing equality deletes.
# usage: set_update_type.sh <tag> eq|pos|dv
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/common.sh" || exit 1
TAG="${1:?usage: set_update_type.sh <tag> eq|pos|dv}"; UPD="${2:?eq|pos|dv}"
docker run --rm -v "$BENCH/olake/$TAG:/c" python:3.12-slim python -c "
import json
p='/c/streams_bench.json'; c=json.load(open(p))
for ns in c['selected_streams'].values():
    for s in ns: s['update_type']='$UPD'
json.dump(c,open(p,'w'),indent=1); print('update_type =', '$UPD')"
