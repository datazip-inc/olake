#!/bin/bash
# Flink (CDC source + Iceberg sink, one job): postgres-cdc connector -> Iceberg Flink connector (Glue), no Kafka. usage: run.sh full|cdc <source_table> <tag> <target_rows>
set -u; source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/common.sh" || exit 1
PHASE=$1; SRC=$2; TAG=$3; TARGET=$4
NAME=flink_${TAG}_${PHASE}; D=$BENCH/flink; TABLE=flink_$TAG; SLOT=bench_flink_$TAG
mkdir -p $D/state $D/work; chmod 777 $D/state $D/work
cd $D && docker compose up -d --build > $L/$NAME.compose.log 2>&1
for i in $(seq 1 60); do curl -sf localhost:8081/overview >/dev/null && break; sleep 2; done
RESTORE=""
# upsert mode: keyed changelog, so updates and deletes work without REPLICA IDENTITY FULL on the source table
CHANGELOG=""; [ "$WRITE_MODE" = upsert ] && CHANGELOG="'changelog-mode'='upsert',"
if [ "$PHASE" = cdc ]; then
  JOB=$(cat $D/work/$TAG.jobid)
  # newest checkpoint that finished writing (has _metadata); a crash can leave a half-written one
  CK=$(for d in $(ls -d $D/state/ckpt/$JOB/chk-* | sort -t- -k2 -n -r); do [ -f "$d/_metadata" ] && { echo "$d"; break; }; done)
  RESTORE="SET 'execution.savepoint.path'='file:///state/ckpt/$JOB/$(basename $CK)';"
fi
cat > $D/work/$TAG.sql <<SQL
SET 'pipeline.name'='bench-flink-$TAG';
$RESTORE
CREATE TABLE src (
  c_custkey INT, c_name STRING, c_address STRING, c_nationkey INT, c_phone STRING,
  c_acctbal DECIMAL(15,2), c_mktsegment STRING, c_comment STRING,
  PRIMARY KEY (c_custkey) NOT ENFORCED
) WITH (
  'connector'='postgres-cdc','hostname'='$PGHOST','port'='$PGPORT','username'='$PGUSER','password'='$PGPASSWORD',
  'database-name'='$PGDATABASE','schema-name'='public','table-name'='$SRC',
  'slot.name'='$SLOT','decoding.plugin.name'='pgoutput', $CHANGELOG
  'debezium.publication.name'='$PUBLICATION','debezium.publication.autocreate.mode'='disabled',
  'debezium.database.sslmode'='$PGSSLMODE',
  'scan.incremental.snapshot.enabled'='true','scan.incremental.snapshot.backfill.skip'='true',
  'scan.incremental.snapshot.chunk.size'='100000','scan.snapshot.fetch.size'='10000'
);
CREATE CATALOG glue WITH ('type'='iceberg','catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',
  'warehouse'='$WAREHOUSE','io-impl'='org.apache.iceberg.aws.s3.S3FileIO','client.region'='$AWS_REGION');
CREATE DATABASE IF NOT EXISTS glue.$GLUE_DB;
CREATE TABLE IF NOT EXISTS glue.$GLUE_DB.$TABLE (
  c_custkey INT, c_name STRING, c_address STRING, c_nationkey INT, c_phone STRING,
  c_acctbal DECIMAL(15,2), c_mktsegment STRING, c_comment STRING,
  PRIMARY KEY (c_custkey) NOT ENFORCED
) WITH ('format-version'='2');
INSERT INTO glue.$GLUE_DB.$TABLE SELECT * FROM src;
SQL
start_poller $NAME $TABLE $TARGET
start_stats $NAME
mark $NAME.start
docker compose exec -T jobmanager ./bin/sql-client.sh -f /work/$TAG.sql > $L/$NAME.submit.log 2>&1
NEWJOB=$(grep -oE "Job ID: [0-9a-f]{32}" $L/$NAME.submit.log | tail -1 | cut -d" " -f3)
echo "flink job: $NEWJOB"; [ -n "$NEWJOB" ] || { tail -30 $L/$NAME.submit.log; stop_stats $NAME; exit 1; }
wait_poller $NAME; RC=$?
mark $NAME.end
sleep 35   # let one more checkpoint complete so restore point covers all committed data
curl -s -X PATCH "localhost:8081/jobs/$NEWJOB?mode=cancel" > $L/$NAME.cancel.log 2>&1; sleep 5
echo $NEWJOB > $D/work/$TAG.jobid
docker compose logs > $L/$NAME.flink.log 2>&1
stop_stats $NAME
exit $RC
