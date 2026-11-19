#!/bin/bash
# Cross-engine read test: every benchmark table is read by PyIceberg, Spark, Trino and Flink.
# usage: run.sh <tag>     output: results/interop_<tag>/
cd "$(dirname "${BASH_SOURCE[0]}")/.." && source ./common.sh || exit 1
TAG="${1:?usage: interop/run.sh <tag>}"; T="olake_$TAG dbz_kc_$TAG flink_$TAG dbz_spark_$TAG"; I=$BENCH/interop; O=$R/interop_$TAG; mkdir -p $O; trino_props $O/trino
echo "=== pyiceberg $(date -u +%T)"
docker run --rm --network host $AWS_ENV -v $BENCH/tools:/t -v $O:/o --entrypoint python bench-poller -u /t/validate.py $GLUE_DB $T 2>&1 | tee $O/pyiceberg.out
docker run --rm --network host $AWS_ENV -v $I:/t --entrypoint python bench-poller -u /t/meta.py $GLUE_DB $T > $O/meta.jsonl 2>&1
echo "=== spark $(date -u +%T)"
SP="docker run --rm --user 0 --network host $AWS_ENV -v $BENCH/spark:/work -v $O:/o apache/spark:3.5.3 /opt/spark/bin/spark-sql --master local[16] --driver-memory 16g --conf spark.jars.ivy=/work/ivy --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.iceberg:iceberg-aws-bundle:1.7.1 --conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue.warehouse=$WAREHOUSE --conf spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue.client.region=$AWS_REGION"
: > $O/spark.out
for t in $T; do   # one run per table so a table one engine cannot read does not hide the others
  $SP -e "select '$t' as tbl, count(*) as n, count(distinct c_custkey) as k, cast(sum(c_acctbal) as decimal(20,2)) as s from glue.$GLUE_DB.$t" >> $O/spark.out 2> $O/spark.$t.err || echo "$t	READ_ERROR $(grep -m1 -E 'Exception|Error' $O/spark.$t.err | cut -c1-200)" >> $O/spark.out
done; cat $O/spark.out
echo "=== trino $(date -u +%T)"
docker rm -f trino_interop >/dev/null 2>&1
docker run -d --name trino_interop --network host $AWS_ENV -v $O/trino/iceberg.properties:/etc/trino/catalog/iceberg.properties trinodb/trino:476 >/dev/null
for i in $(seq 1 60); do docker exec trino_interop trino --execute "select 1" >/dev/null 2>&1 && break; sleep 3; done
for t in $T; do docker exec trino_interop trino --execute "select '$t', count(*), count(distinct c_custkey), cast(sum(c_acctbal) as decimal(20,2)) from iceberg.$GLUE_DB.$t" 2>&1 | tail -2; done | tee $O/trino.out
docker stop trino_interop >/dev/null
echo "=== flink $(date -u +%T)"
(cd flink && docker compose up -d >/dev/null 2>&1); for i in $(seq 1 60); do curl -sf localhost:8081/overview >/dev/null && break; sleep 2; done
{ echo "SET 'execution.runtime-mode'='batch';"; echo "SET 'sql-client.execution.result-mode'='tableau';"
  echo "CREATE CATALOG glue WITH ('type'='iceberg','catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog','warehouse'='$WAREHOUSE','io-impl'='org.apache.iceberg.aws.s3.S3FileIO','client.region'='$AWS_REGION');"
  for t in $T; do echo "select '$t' as tbl, count(*) as n, count(distinct c_custkey) as k, cast(sum(c_acctbal) as decimal(20,2)) as s from glue.$GLUE_DB.$t;"; done; } > flink/work/interop.sql
docker compose -f flink/docker-compose.yml exec -T jobmanager ./bin/sql-client.sh -f /work/interop.sql > $O/flink.out 2>&1; grep -E "^\| +[a-z_]+_$TAG|ERROR|Exception" $O/flink.out | cut -c1-200
(cd flink && docker compose stop >/dev/null 2>&1)
echo INTEROP_DONE
