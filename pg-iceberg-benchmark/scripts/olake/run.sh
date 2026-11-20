#!/bin/bash
# OLake pipeline. usage: run.sh full|cdc <source_table> <tag> <target_rows>
set -u; source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/common.sh" || exit 1
PHASE=$1; SRC=$2; TAG=$3; TARGET=$4
NAME=olake_${TAG}_${PHASE}; D=$BENCH/olake/$TAG; SLOT=bench_olake_${TAG}; IMG=$OLAKE_IMAGE
THREADS=$PARALLELISM
# OLAKE_UPDATE_TYPE: how OLake records deletes in upsert mode. eq = equality deletes (default),
# pos = position deletes, dv = deletion vectors. pos and dv use a row index kept in $D/index.
UPD=${OLAKE_UPDATE_TYPE:-eq}
mkdir -p $D
if [ "$PHASE" = full ]; then
  psqlc -At -c "select pg_create_logical_replication_slot('$SLOT','pgoutput') where not exists (select 1 from pg_replication_slots where slot_name='$SLOT')"
  cat > $D/source.json <<JSON
{"host":"$PGHOST","port":$PGPORT,"database":"$PGDATABASE","username":"$PGUSER","password":"$PGPASSWORD",
 "jdbc_url_params":{},"ssl":{"mode":"$PGSSLMODE"},"max_threads":$THREADS,"retry_count":3,
 "update_method":{"replication_slot":"$SLOT","publication":"$PUBLICATION","initial_wait_time":120}}
JSON
  cat > $D/destination.json <<JSON
{"type":"ICEBERG","writer":{"catalog_type":"glue","iceberg_s3_path":"$WAREHOUSE","aws_region":"$AWS_REGION"}}
JSON
  docker run --rm --network host $AWS_ENV -v $D:/mnt/config $IMG discover --config /mnt/config/source.json > $L/$NAME.discover.log 2>&1
  python3 - "$D" "$SRC" "$GLUE_DB" "olake_$TAG" "$WRITE_MODE" "$UPD" <<'PY'
import json, sys
d, src, db, tbl, mode, upd = sys.argv[1:7]
c = json.load(open(f"{d}/streams.json"))
sel = [x for x in c["selected_streams"]["public"] if x["stream_name"] == src]
assert sel, "stream not discovered"
sel[0]["update_type"] = upd
sel[0]["append_mode"] = (mode != "upsert")   # append: insert only; upsert: OLake default, writes delete files
c["selected_streams"] = {"public": sel}
c["streams"] = [x for x in c["streams"] if x["stream"]["name"] == src and x["stream"]["namespace"] == "public"]
s = c["streams"][0]["stream"]
s["sync_mode"] = "cdc"; s["destination_database"] = db; s["destination_table"] = tbl
json.dump(c, open(f"{d}/streams_bench.json", "w"), indent=1)
PY
  STATE=""
else
  STATE="--state /mnt/config/state.json"
fi
start_poller $NAME olake_$TAG $TARGET
start_stats $NAME
mark $NAME.start
docker run --rm --name olake_bench --memory ${MEM_GB}g --network host $AWS_ENV -e OLAKE_INDEX_DB_DIR=/mnt/config/index -v $D:/mnt/config $IMG sync --config /mnt/config/source.json --catalog /mnt/config/streams_bench.json --destination /mnt/config/destination.json $STATE > $L/$NAME.log 2>&1
echo "olake exit=$?"
mark $NAME.process_exit
wait_poller $NAME; RC=$?; stop_stats $NAME; exit $RC
