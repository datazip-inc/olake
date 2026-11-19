#!/bin/bash
# Iceberg table maintenance with Spark: rewrite data files so earlier delete files no longer apply,
# and drop delete files that have become dangling. This is a separate job, not part of any writer.
# usage: maintain.sh <glue_table>...
cd "$(dirname "${BASH_SOURCE[0]}")/.." && source ./common.sh || exit 1
O=$BENCH/interop; : > $O/maintain.sql
for t in "$@"; do echo "CALL glue.system.rewrite_data_files(table => '$GLUE_DB.$t', options => map('rewrite-all','true','remove-dangling-deletes','true'));" >> $O/maintain.sql; done
docker run --rm --user 0 --network host $AWS_ENV -v $BENCH/spark:/work -v $O:/o apache/spark:3.5.3 /opt/spark/bin/spark-sql --master "local[8]" --driver-memory 8g --conf spark.jars.ivy=/work/ivy \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.iceberg:iceberg-aws-bundle:1.7.1 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.glue.warehouse=$WAREHOUSE --conf spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue.client.region=$AWS_REGION \
  -f /o/maintain.sql 2> $O/maintain.err
echo "rc=$?"; grep -m3 -E "Exception|mismatched" $O/maintain.err | cut -c1-250
