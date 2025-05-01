# Iceberg Writer

The **Iceberg Writer** receives data from Olake sources and writes it to **Apache Iceberg** tables.  
It understands **append** as well as **upsert (equality-delete)** workflows and supports JDBC, AWS Glue, Hive and REST catalogs.

---

## How It Works

Iceberg Writer produces **equality-delete** files, so **Spark is *not* required** for upserts.

| Scenario              | Behaviour                                                                                |
| --------------------- | ---------------------------------------------------------------------------------------- |
| **Backfill**          | Appends rows (simple **append** mode).                                                   |
| **CDC / Incremental** | Performs **upserts** by creating equality-delete files, then inserting the new versions. |

---

## Current Architecture

```
Golang code ──gRPC──▶ Java (this project) ──writes──▶ S3  +  Iceberg Catalog
```

> **Why Java?**  
> The Go-Iceberg library does not yet support equality deletes.  
> Until it does (or until Iceberg-Rust becomes production-ready), we delegate the final write step to a lightweight Java gRPC service for full feature-parity.

**Execution flow**

1. The entry-point is **`iceberg.go`**.  
2. It spawns a **Java gRPC server** as a subprocess.  
3. Records are sent in **256 MB** (in-memory) batches &nbsp;*(size refers to the marshalled objects, not to Parquet blocks)*.

### Java Iceberg Writer / Sink

The Java implementation lives in **`debezium-server-iceberg-sink`**.  
See **`./debezium-server-iceberg-sink/README.md`** for details on running it in standalone mode.

---

## How to Run

> All examples below assume you have already created a valid **`streams.json`** and **`source.json`** and that you invoke the Olake CLI exactly as in the *Getting Started* guide.

### Local MinIO + JDBC Catalog (quick test setup)

> **Prerequisite:** Docker Desktop or Docker Engine

```bash
cd writers/iceberg/local-test
docker compose up      # starts Postgres (JDBC catalog), MinIO and Spark
```

This spins up

1. **Postgres** – JDBC catalog  
2. **MinIO** – S3-compatible object store  
3. **Spark**  – convenient engine for ad-hoc queries

Create **`destination.json`**:

```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "jdbc",

    "jdbc_url":      "jdbc:postgresql://localhost:5432/iceberg",
    "jdbc_username": "iceberg",
    "jdbc_password": "password",

    "normalization": false,

    "iceberg_s3_path": "s3a://warehouse",

    "s3_endpoint":     "http://localhost:9000",
    "s3_use_ssl":      false,
    "s3_path_style":   true,

    "aws_access_key": "admin",
    "aws_secret_key": "password",

    "iceberg_db": "olake_iceberg"
  }
}
```

Run your **sync** command; once it finishes you can inspect the data:

```bash
# open a shell in the Spark container
docker exec -it spark-iceberg bash
spark-sql                       # retry once if it fails to start

-- inside spark-sql
-- SELECT  *  FROM  <catalog>.<database>.<table>
SELECT *
FROM   olake_iceberg.olake_iceberg.table_name;
```

---

### AWS S3 + Glue Catalog

Create **`destination.json`**:

```json
{
  "type": "ICEBERG",
  "writer": {
    "normalization":   false,

    "iceberg_s3_path": "s3://bucket_name/olake_iceberg/test_olake",

    "aws_region":   "ap-south-1",
    "aws_access_key": "XXX",
    "aws_secret_key": "XXX",

    "iceberg_db": "olake_iceberg",

    "grpc_port":            50051,
    "sink_rpc_server_host": "localhost"
  }
}
```

> **Field reference**  
> * `iceberg_s3_path` – root folder for data & metadata  
> * `aws_region`      – bucket + Glue region  
> * IAM user needs **full Glue + S3** permissions.

---

### REST Catalog

```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "rest",
    "normalization": false,

    "rest_catalog_url": "http://localhost:8181/catalog",

    "iceberg_s3_path": "warehouse",
    "iceberg_db":      "ICEBERG_DATABASE_NAME"
  }
}
```

---

### Hive Catalog

```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "hive",
    "normalization": false,

    "iceberg_s3_path": "s3a://warehouse/",
    "aws_region":      "us-east-1",

    "aws_access_key": "admin",
    "aws_secret_key": "password",

    "s3_endpoint":   "http://localhost:9000",
    "s3_use_ssl":    false,
    "s3_path_style": true,

    "hive_uri":         "http://localhost:9083",
    "hive_clients":     5,
    "hive_sasl_enabled": false,

    "iceberg_db": "olake_iceberg"
  }
}
```

> ⚠️ Replace the placeholder values with real credentials before running.

For an exhaustive list of catalog options, see the [Catalog docs](https://olake.io/docs/writers/iceberg/catalog/overview).

---

## Partitioning Support

Partitioning boosts query performance by pruning irrelevant files.  
Set per-stream partitions through **`partition_regex`** in **`streams.json`**.

### Syntax

```text
"/{field_name, transform}/{another_field, transform}"
```

* **field_name** – column to partition by  
* **transform**  – one of  
  `identity` · `hour` · `day` · `month` · `year` · `bucket[N]` · `truncate[N]`

When a partition field is missing, the writer substitutes **`null`** so the record is still ingested.

### Supported Transforms

The Iceberg writer supports the following transforms:
- `identity`: Use the raw value (good for categorical data)
- `year`, `month`, `day`, `hour`: Time-based transforms (good for timestamp columns)
- `bucket[N]`: Hash the value into N buckets (for high-cardinality fields)
- `truncate[N]`: Truncate the string to N characters (for string fields)

For more details on partition transforms, see the [Iceberg Partition Transform Specification](https://iceberg.apache.org/spec/#partition-transforms).


### Examples

| Goal                                       | `partition_regex`                                |
| ------------------------------------------ | ------------------------------------------------ |
| Yearly buckets on `created_at`             | `"/{created_at, year}"`                          |
| Daily partitions                           | `"/{event_date, day}"`                           |
| Customer ID + month                        | `"/{customer_id, identity}/{event_time, month}"` |
| Daily partitions using *current* timestamp | `"/{now(), day}"`                                |

### Stream configuration sample

```json
{
  "selected_streams": {
    "my_namespace": [
      {
        "stream_name":     "my_stream",
        "partition_regex": "/{timestamp_col, day}/{region, identity}"
      }
    ]
  }
}
```

#### After a sync you can query efficiently

```sql
SELECT *
FROM olake_iceberg.olake_iceberg.my_stream
WHERE timestamp_col = DATE '2023-05-01'
  AND region        = 'us-east';
```

> Iceberg will only scan the partitions that match these predicates.

---

## Additional Resources

* [OLake → Iceberg partitioning guide](https://olake.io/docs/writers/iceberg/partitioning)    

