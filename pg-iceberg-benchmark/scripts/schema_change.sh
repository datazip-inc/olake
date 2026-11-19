#!/bin/bash
# Schema change test: add a column on the source while pipelines are parked, write rows that use it, resume.
# usage: schema_change.sh <tag>     (needs a finished upsert_all.sh run with the same tag)
# Changes only the upsert test table created by this benchmark.
cd "$(dirname "${BASH_SOURCE[0]}")" && export WRITE_MODE=upsert SCHEMA_EVOLVE=1 && source ./common.sh || exit 1
TAG="${1:?usage: schema_change.sh <tag>}"; SRC="bench_upsert_$TAG"; export POLL_TIMEOUT="${POLL_TIMEOUT:-420}"
psqlc -q <<SQL
ALTER TABLE public."$SRC" ADD COLUMN c_tier int;
UPDATE public."$SRC" SET c_tier = 1 WHERE c_custkey BETWEEN 300001 AND 301000;
INSERT INTO public."$SRC" SELECT 200000000+c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, 2 FROM $BASE_TABLE WHERE c_custkey BETWEEN 1 AND 1000;
SQL
echo "=== source: rows | rows with c_tier"; psqlc -At -F ' | ' -c "select count(*), count(c_tier) from public.\"$SRC\"" | tee "$R/schema_$TAG.source.txt"
run() { echo "=== $(date -u +%FT%TZ) $*"; "$@"; echo "=== rc=$? $*"; }      # keep going on failure: failure is a result here
run ./olake/run.sh cdc "$SRC" "$TAG" +2000
run ./dbz/run.sh kc cdc "$SRC" "$TAG" +2000
run ./dbz/run.sh spark cdc "$SRC" "$TAG" +2000
(cd dbz && docker compose stop)
run ./flink/run.sh cdc "$SRC" "$TAG" +2000
(cd flink && docker compose stop)
T="olake_$TAG dbz_kc_$TAG flink_$TAG dbz_spark_$TAG"; O=$BENCH/interop
echo "=== columns per table"
docker run --rm --network host $AWS_ENV -v $O:/t --entrypoint python bench-poller -u /t/meta.py $GLUE_DB $T > $R/schema_$TAG.meta.jsonl 2>&1
python3 - "$R/schema_$TAG.meta.jsonl" <<'PY'
import json, sys
for l in open(sys.argv[1]):
    try: r = json.loads(l)
    except ValueError: print(l.strip()[:200]); continue
    print(r["table"], "c_tier" in r["types"], r["types"].get("c_tier"))
PY
echo "=== trino: rows | rows with c_tier (error = column absent)"
docker rm -f trino_interop >/dev/null 2>&1
docker run -d --name trino_interop --network host $AWS_ENV -v $O/trino/iceberg.properties:/etc/trino/catalog/iceberg.properties trinodb/trino:476 >/dev/null
for i in $(seq 1 60); do docker exec trino_interop trino --execute "select 1" >/dev/null 2>&1 && break; sleep 3; done
for t in $T; do docker exec trino_interop trino --execute "select '$t', count(*), count(c_tier) from iceberg.$GLUE_DB.$t" 2>&1 | grep -v WARNING | tail -1 | cut -c1-200; done | tee $R/schema_$TAG.trino.txt
docker stop trino_interop >/dev/null
echo SCHEMA_DONE
