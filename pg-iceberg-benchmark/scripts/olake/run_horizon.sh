#!/bin/bash
# OLake pipeline with Snowflake Horizon as the Iceberg catalog (REST). Same flow as olake/run.sh, other destination.
# usage: run_horizon.sh full|cdc <source_table> <tag>
# Needs .snowflake_bench with SNOWFLAKE_ORG_ACCOUNT, SNOWFLAKE_PAT and the one-time setup from the OLake docs
# (database, role and service user). The Glue row-count poller cannot read this catalog, so the run is timed
# from sync start to sync process exit.
set -u; export WRITE_MODE="${WRITE_MODE:-upsert}"; source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/common.sh" || exit 1
set -a; . "$BENCH/.snowflake_bench"; set +a
PHASE=$1; SRC=$2; TAG=$3
SF_DB="${SNOWFLAKE_HORIZON_DB:-OLAKE_BENCH_HZ}"; SF_SCHEMA="${SNOWFLAKE_HORIZON_SCHEMA:-bench_hz}"; SF_ROLE="${SNOWFLAKE_HORIZON_ROLE:-OLAKE_ROLE}"
NAME=olake_${TAG}_${PHASE}; D=$BENCH/olake/$TAG; SLOT=bench_olake_${TAG}; IMG=olakego/source-postgres:latest
THREADS=${OLAKE_THREADS:-16}; UPD=${OLAKE_UPDATE_TYPE:-pos}
mkdir -p $D
if [ "$PHASE" = full ]; then
  psqlc -At -c "select pg_create_logical_replication_slot('$SLOT','pgoutput') where not exists (select 1 from pg_replication_slots where slot_name='$SLOT')"
  cat > $D/source.json <<JSON
{"host":"$PGHOST","port":$PGPORT,"database":"$PGDATABASE","username":"$PGUSER","password":"$PGPASSWORD",
 "jdbc_url_params":{},"ssl":{"mode":"$PGSSLMODE"},"max_threads":$THREADS,"retry_count":3,
 "update_method":{"replication_slot":"$SLOT","publication":"$PUBLICATION","initial_wait_time":120}}
JSON
  cat > $D/destination.json <<JSON
{"type":"ICEBERG","writer":{"catalog_type":"horizon","catalog_name":"horizon",
 "rest_catalog_url":"https://$SNOWFLAKE_ORG_ACCOUNT.snowflakecomputing.com/polaris/api/catalog",
 "iceberg_s3_path":"$SF_DB","aws_region":"${SNOWFLAKE_REGION:-AWS_AP_SOUTH_1}","scope":"session:role:$SF_ROLE",
 "rest_auth_type":"Programmatic Access Token (PAT)","credential":"$SNOWFLAKE_PAT","arrow_writes":false}}
JSON
  chmod 600 $D/source.json $D/destination.json
  docker run --rm --network host -v $D:/mnt/config $IMG check --destination /mnt/config/destination.json > $L/$NAME.check.log 2>&1; echo "destination check: $(grep -o -E 'SUCCEEDED|FAILED' $L/$NAME.check.log | tail -1)"
  docker run --rm --network host -v $D:/mnt/config $IMG discover --config /mnt/config/source.json > $L/$NAME.discover.log 2>&1
  python3 - "$D" "$SRC" "$SF_SCHEMA" "olake_$TAG" "$WRITE_MODE" "$UPD" <<'PY'
import json, sys
d, src, db, tbl, mode, upd = sys.argv[1:7]
c = json.load(open(f"{d}/streams.json"))
sel = [x for x in c["selected_streams"]["public"] if x["stream_name"] == src]
assert sel, "stream not discovered"
sel[0]["update_type"] = upd
sel[0]["append_mode"] = (mode != "upsert")
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
start_stats $NAME
mark $NAME.start
docker run --rm --name olake_hz --network host -e OLAKE_INDEX_DB_DIR=/mnt/config/index -v $D:/mnt/config $IMG sync --config /mnt/config/source.json --catalog /mnt/config/streams_bench.json --destination /mnt/config/destination.json $STATE > $L/$NAME.log 2>&1
RC=$?; echo "olake exit=$RC"
mark $NAME.process_exit; stop_stats $NAME
echo "seconds=$(python3 -c "print(round(float(open('$R/$NAME.process_exit').read())-float(open('$R/$NAME.start').read()),1))") $(grep -o 'Total records read: [0-9]*' $L/$NAME.log | tail -1)"
exit $RC
