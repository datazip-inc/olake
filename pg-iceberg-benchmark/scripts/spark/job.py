"""Kafka (Debezium flattened JSON with schema envelope) -> Iceberg (Glue).
args: <topic> <glue_db> <table> <checkpoint_dir> [append|upsert] [trigger_seconds]
append: insert every record. upsert: MERGE by c_custkey per micro-batch (merge-on-read, writes position deletes)."""
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, get_json_object, row_number

topic, db, table, ckpt = sys.argv[1:5]
mode = sys.argv[5] if len(sys.argv) > 5 else "append"
trigger = f"{int(sys.argv[6]) if len(sys.argv) > 6 else 30} seconds"
spark = SparkSession.builder.appName("dbz-spark-iceberg").getOrCreate()
cols = ["c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"]
schema = ("c_custkey INT, c_name STRING, c_address STRING, c_nationkey INT, c_phone STRING, "
          "c_acctbal DOUBLE, c_mktsegment STRING, c_comment STRING")
props = ""
if mode == "upsert":
    props = (" TBLPROPERTIES ('format-version'='2','write.delete.mode'='merge-on-read',"
             "'write.update.mode'='merge-on-read','write.merge.mode'='merge-on-read')")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS glue.{db}")
spark.sql(f"CREATE TABLE IF NOT EXISTS glue.{db}.{table} ({schema}) USING iceberg{props}")
rows = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 4000000)
        .option("failOnDataLoss", "false")
        .load()
        .select(from_json(get_json_object(col("value").cast("string"), "$.payload"), schema + ", __op STRING").alias("r"),
                col("offset").alias("_off"))
        .select("r.*", "_off")
        .where("c_custkey IS NOT NULL"))

def merge_batch(batch, _id):
    # keep the last change per key (a key always lands in one Kafka partition, so offset orders it)
    w = Window.partitionBy("c_custkey").orderBy(col("_off").desc())
    batch.withColumn("_rn", row_number().over(w)).where("_rn = 1").createOrReplaceTempView("changes")
    sets = ", ".join(f"t.{c} = s.{c}" for c in cols)
    batch.sparkSession.sql(
        f"MERGE INTO glue.{db}.{table} t USING changes s ON t.c_custkey = s.c_custkey "
        f"WHEN MATCHED AND s.__op = 'd' THEN DELETE "
        f"WHEN MATCHED THEN UPDATE SET {sets} "
        f"WHEN NOT MATCHED AND (s.__op IS NULL OR s.__op <> 'd') THEN INSERT ({', '.join(cols)}) "
        f"VALUES ({', '.join('s.' + c for c in cols)})")

if mode == "upsert":
    (rows.writeStream.trigger(processingTime=trigger).option("checkpointLocation", ckpt)
     .foreachBatch(merge_batch).start().awaitTermination())
else:
    (rows.select(*cols).writeStream.format("iceberg").outputMode("append")
     .trigger(processingTime=trigger).option("checkpointLocation", ckpt)
     .option("fanout-enabled", "true").toTable(f"glue.{db}.{table}").awaitTermination())
