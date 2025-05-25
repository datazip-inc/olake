# Iceberg-writer

Writes to apache-iceberg. This writer takes configuration of catalog and s3 buckets and writes data received from the Olake source.

## How It Works 

Iceberg writer writes data primarily as a equality delete which doesn't requires spark. 

For backfill --> Works in append mode

For CDC --> Works in Upsert mode by creating delete files

### Current Architecture :

> Golang Code  --gRPC-->  Java (This Project)  --Write to Iceberg-->  S3 + Iceberg Catalog

Important reason why we are using Java to write is, current Golang-iceberg project doesn't support Equality delete writes. We plan to move this to iceberg-rust to improve on memory footprint.

1. Flow starts with iceberg.go file. 
2. We create Java rpc server by a process call.
3. Send records via rpc in batches of 256 mb (in-memory object size, not a real parquet size)

### Java Iceberg Writer/sink : 

Its based in the directory debezium-server-iceberg-sink. Read more ./debezium-server-iceberg-sink/README.md on how to run it in standalone mode and test.


## How to run 

### Local Minio + JDBC Catalog (Local test setup):

Make sure you have docker installed before you run this

```shell
cd writers/iceberg/local-test
docker compose up
```
This will create 
1. postgres --> JDBC catalog
2. Minio --> For AWS S3 like filesystem setup on your local
3. Spark --> Querying Iceberg data.

Now create a writer.json for iceberg writer as follows : 
```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "jdbc",
    "jdbc_url": "jdbc:postgresql://localhost:5432/iceberg",
    "jdbc_username": "iceberg",
    "jdbc_password": "password",
    "iceberg_s3_path": "s3a://warehouse",
    "s3_endpoint": "http://localhost:9000",
    "s3_use_ssl": false,
    "s3_path_style": true,
    "aws_access_key": "admin",
    "aws_secret_key": "password",
    "iceberg_db": "olake_iceberg"
  }
}  
```
And run the sync normally as mentioned in the getting started doc.

> Now how to see if the data is actually populated?
Run : 
```shell
# Connect to the spark-iceberg container
docker exec -it spark-iceberg bash

# Start spark-sql (rerun it if error occurs)
spark-sql

# Query in format select * from catalog_name.iceberg_db_name.table_name
select * from olake_iceberg.olake_iceberg.table_name;
```


### AWS S3 + Glue Catalog
Create a json for writer config (Works for S3 as storage and AWS Glue as a catalog) : 
```json
{
    "type": "ICEBERG",
    "writer": {
      "iceberg_s3_path": "s3://bucket_name/olake_iceberg/test_olake",
      "aws_region": "ap-south-1",
      "aws_access_key": "XXX",
      "aws_secret_key": "XXX",
      "iceberg_db": "olake_iceberg",
      "grpc_port": 50051,
      "sink_rpc_server_host": "localhost"
    }
  }  
```

And run the sync normally as mentioned in the getting started doc.

* `iceberg_s3_path` -> Stores the relevant iceberg data/metadata files
* `aws_region` -> Region for AWS bucket and catalog
* `aws_access_key` -> AWS access key which has full access to glue & AWS S3
* `aws_secret_key` -> AWS secret key
* `iceberg_db` -> database you want to create in glue.

### REST Catalog
Create a json for writer config (writer.json)
```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "rest",
    "rest_catalog_url": "http://localhost:8181/catalog",
    "iceberg_s3_path": "warehouse",
    "iceberg_db": "ICEBERG_DATABASE_NAME"
  }
}
```

### Hive Catalog
Create a json for writer config (writer.json)
```json
{
    "type": "ICEBERG",
    "writer": {
        "catalog_type": "hive",
        "iceberg_s3_path": "s3a://warehouse/",
        "aws_region": "us-east-1",
        "aws_access_key": "admin",
        "aws_secret_key": "password",
        "s3_endpoint": "http://localhost:9000",
        "hive_uri": "http://localhost:9083",
        "s3_use_ssl": false,
        "s3_path_style": true,
        "hive_clients": 5,
        "hive_sasl_enabled": false,
        "iceberg_db": "olake_iceberg"
    }
}
```

Please change the above to real credentials to make it work.

For detailed catalog configs and usage, refer [here.](https://olake.io/docs/category/catalogs)

## Partitioning Support

The Iceberg writer supports partitioning data based on field values, which can significantly improve query performance when filtering on partition columns. Partitioning is configured at the stream level using the `PartitionRegex` field in `StreamMetadata`.

### Partition Configuration Format

Partitions are specified using the following format:

```
/{field_name, transform}/{another_field, transform}
```

Where:
- `field_name`: Name of the column to partition by
- `transform`: Iceberg partition transform to apply (e.g., `identity`, `hour`, `day`, `month`, `year`, `bucket[N]`, `truncate[N]`)

### Example Partition Configurations

1. Partition by year from a timestamp column:
```
/{created_at, year}
```

2. Partition by day:
```
/{event_date, day}
```

3. Multiple partitions (by customer ID and month):
```
/{customer_id, identity}/{event_time, month}
```

4. Using a current timestamp (special case):
```
/{now(), day}
```

### Supported Transforms

The Iceberg writer supports the following transforms:
- `identity`: Use the raw value (good for categorical data)
- `year`, `month`, `day`, `hour`: Time-based transforms (good for timestamp columns)
- `bucket[N]`: Hash the value into N buckets (for high-cardinality fields)
- `truncate[N]`: Truncate the string to N characters (for string fields)

For more details on partition transforms, see the [Iceberg Partition Transform Specification](https://iceberg.apache.org/spec/#partition-transforms).

### Handling Missing Partition Fields

When a partition field is missing from a record, the writer will automatically set the field to `nil`, which Iceberg treats as a null value. This ensures that records with missing partition fields can still be processed correctly.

### Example Usage

To include partitioning in your sync **streams.json**:

1. Specify the partitioning in your stream configuration:
```json
{
  "selected_streams": {
    "my_namespace": [
      {
        "stream_name": "my_stream",
        "partition_regex": "/{timestamp_col, day}/{region, identity}"
      }
    ]
  }
}
```
2. Run your sync as usual, and the Iceberg writer will create the appropriate partitioned table structure.

After syncing, you can query the data efficiently by filtering on partition columns:

```sql
-- This query will only scan relevant partitions
select * from olake_iceberg.olake_iceberg.my_stream 
where timestamp_col = '2023-05-01' and region = 'us-east';
```
