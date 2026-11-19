#!/bin/bash
# Debezium -> Kafka -> {Iceberg Kafka Connect sink | Spark Structured Streaming}.
# usage: run.sh kc|spark full|cdc <source_table> <tag> <target_rows>
set -u; source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/common.sh" || exit 1
SINK=$1; PHASE=$2; SRC=$3; TAG=$4; TARGET=$5
P=dbz${SINK}${TAG}                         # topic prefix, unique per pipeline + tag
NAME=dbz_${SINK}_${TAG}_${PHASE}; TOPIC=$P.public.$SRC; TABLE=dbz_${SINK}_${TAG}
C=localhost:8083; D=$BENCH/dbz
# upsert mode: Debezium adds __op and emits deletes as rows; sink applies them by key
EXTRA_SRC=""; EXTRA_SINK=""
if [ "$WRITE_MODE" = upsert ]; then
  EXTRA_SRC='"transforms.unwrap.add.fields":"op","transforms.unwrap.delete.tombstone.handling.mode":"rewrite",'
  EXTRA_SINK='"iceberg.tables.cdc-field":"__op","iceberg.tables.default-id-columns":"c_custkey",'
fi
# SCHEMA_EVOLVE=1: let the Iceberg sink add new source columns to the table
[ "${SCHEMA_EVOLVE:-0}" = 1 ] && EXTRA_SINK="$EXTRA_SINK\"iceberg.tables.evolve-schema-enabled\":\"true\","
cd $D && docker compose up -d >/dev/null 2>&1
for i in $(seq 1 90); do curl -sf $C/connector-plugins >/dev/null && break; sleep 2; done
put() { curl -s -o /dev/null -w "%{http_code} $1\n" -X PUT -H 'Content-Type: application/json' $C/connectors/$1/config -d @"$2"; }
src_cfg() { cat > $D/$P-src.json <<JSON
{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","tasks.max":"1",
 "database.hostname":"$PGHOST","database.port":"$PGPORT","database.user":"$PGUSER","database.password":"$PGPASSWORD",
 "database.dbname":"$PGDATABASE","database.sslmode":"$PGSSLMODE",
 "topic.prefix":"$P","plugin.name":"pgoutput","slot.name":"bench_${P}","publication.name":"$PUBLICATION",
 "publication.autocreate.mode":"disabled","table.include.list":"public.$SRC","snapshot.mode":"initial",
 "decimal.handling.mode":"double","max.batch.size":"8192","max.queue.size":"32768","snapshot.fetch.size":"20000",
 "transforms":"unwrap","transforms.unwrap.type":"io.debezium.transforms.ExtractNewRecordState",
 $EXTRA_SRC
 "key.converter":"org.apache.kafka.connect.json.JsonConverter","key.converter.schemas.enable":"true",
 "value.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter.schemas.enable":"true",
 "topic.creation.default.partitions":"16","topic.creation.default.replication.factor":"1",
 "producer.override.compression.type":"lz4","producer.override.linger.ms":"20","producer.override.batch.size":"524288"}
JSON
}
sink_cfg() { cat > $D/$P-sink.json <<JSON
{"connector.class":"io.tabular.iceberg.connect.IcebergSinkConnector","tasks.max":"16","topics":"$TOPIC",
 "iceberg.tables":"$GLUE_DB.$TABLE","iceberg.tables.auto-create-enabled":"true",
 $EXTRA_SINK
 "iceberg.catalog.catalog-impl":"org.apache.iceberg.aws.glue.GlueCatalog","iceberg.catalog.warehouse":"$WAREHOUSE",
 "iceberg.catalog.io-impl":"org.apache.iceberg.aws.s3.S3FileIO","iceberg.catalog.client.region":"$AWS_REGION",
 "iceberg.control.topic":"control-$P","iceberg.control.commit.interval-ms":"30000","iceberg.control.commit.timeout-ms":"60000",
 "key.converter":"org.apache.kafka.connect.json.JsonConverter","key.converter.schemas.enable":"true",
 "value.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter.schemas.enable":"true",
 "consumer.override.max.poll.records":"5000"}
JSON
}
spark_start() {
  mkdir -p $BENCH/spark/ckpt-$P $BENCH/spark/ivy
  docker rm -f spark_bench >/dev/null 2>&1
  docker run -d --name spark_bench --user 0 --network host $AWS_ENV -v $BENCH/spark:/work apache/spark:3.5.3 \
    /opt/spark/bin/spark-submit --master 'local[16]' --driver-memory 32g --conf spark.jars.ivy=/work/ivy \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.iceberg:iceberg-aws-bundle:1.7.1 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
    --conf spark.sql.catalog.glue.warehouse=$WAREHOUSE \
    --conf spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.glue.client.region=$AWS_REGION \
    /work/job.py "$TOPIC" "$GLUE_DB" "$TABLE" "/work/ckpt-$P" "$WRITE_MODE" >/dev/null
}
K="docker compose exec -T kafka /opt/kafka/bin"
start_poller $NAME $TABLE $TARGET
start_stats $NAME
if [ "$PHASE" = full ]; then
  src_cfg
  if [ "$SINK" = kc ]; then
    $K/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic control-$P --partitions 1 --replication-factor 1 >/dev/null 2>&1
    sink_cfg
  fi
  mark $NAME.start
  put $P-src $D/$P-src.json
  [ "$SINK" = kc ] && put $P-sink $D/$P-sink.json
  [ "$SINK" = spark ] && spark_start
else
  # re-apply config files first (same result if unchanged), then resume from stored offsets
  src_cfg; put $P-src $D/$P-src.json
  if [ "$SINK" = kc ]; then sink_cfg; put $P-sink $D/$P-sink.json; fi
  mark $NAME.start
  curl -s -X PUT $C/connectors/$P-src/resume
  [ "$SINK" = kc ] && curl -s -X PUT $C/connectors/$P-sink/resume
  [ "$SINK" = spark ] && spark_start
fi
wait_poller $NAME; RC=$?
mark $NAME.end
# park this pipeline: offsets stay in Kafka, slot stays on source
curl -s -X PUT $C/connectors/$P-src/stop
[ "$SINK" = kc ] && curl -s -X PUT $C/connectors/$P-sink/stop
if [ "$SINK" = spark ]; then docker logs spark_bench > $L/$NAME.spark.log 2>&1; docker stop -t 60 spark_bench >/dev/null; fi
docker compose logs connect > $L/$NAME.connect.log 2>&1
stop_stats $NAME
exit $RC
