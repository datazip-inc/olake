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

Its based in the directory olake-iceberg-java-writer. Read more ./olake-iceberg-java-writer/README.md on how to run it in standalone mode and test.

## Environment Variables

### OLAKE_DEBUG_MODE

Set this environment variable to enable debug mode for the Java Iceberg writer. When enabled, the Java process will start with remote debugging options, allowing you to attach a debugger on port 5005.

**Note:** Debug mode is only enabled during sync operations (not during check operations) to avoid blocking the connection check process.

```bash
# Enable debug mode
export OLAKE_DEBUG_MODE=1

# Run your sync command
olake sync --config source.json --destination destination.json --catalog catalog.json
```

## How to run 

### Local Minio + JDBC Catalog (Local test setup):

Make sure you have docker installed before you run this

```shell
cd destination/iceberg/local-test
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
    "aws_secret_key": "password"
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

### REST Catalog
Create a json for writer config (writer.json)
```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "rest",
    "rest_catalog_url": "http://localhost:8181/catalog",
    "iceberg_s3_path": "warehouse"
  }
}
```

**Authentication Fields (optional):**
- `token` - Bearer token for token-based authentication
- `oauth2_uri` - OAuth2 server URI for OAuth2 authentication
- `rest_auth_type` - Authentication type (e.g., "oauth2")
- `credential` - Client secret or credential for OAuth2 (Usually id:secret)
- `scope` - OAuth2 scopes (space-separated)
- `rest_signing_name` - Service name for AWS Signature V4 (e.g., "s3tables")
- `rest_signing_region` - Region for AWS Signature V4 signing
- `rest_signing_v_4` - Enable AWS Signature V4 signing (boolean)

### S3 Table Bucket
Create a json for writer config (writer.json)
```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "rest",
    "rest_catalog_url": "https://s3tables.us-east-1.amazonaws.com/iceberg",
    "iceberg_s3_path": "arn:aws:s3tables:<REGION>:<ACCOUNT_ID>:bucket/<BUCKET_NAME>",
    "aws_access_key": "",
    "aws_secret_key": "",
    "aws_region": "<REGION>",
    "rest_signing_name": "s3tables",
    "rest_signing_region": "<REGION>",
    "rest_signing_v_4": true
  }
}
```

change the placeholders with actual values
* `REGION` -> Region for AWS bucket and catalog
* `NAMESPACE` -> This will be your s3 table bucket namespace
* `ACCOUNT_ID` -> AWS account identifier
* `BUCKET_NAME` -> Table Bucket Name

### Unity Catalog support (Rest)
```json
{
  "type": "ICEBERG",
  "writer": {
    "catalog_type": "rest",
    "normalization": true,
    "rest_catalog_url": "https://<DATABRICK_WORKSPACE_URL>/api/2.1/unity-catalog/iceberg-rest",
    "iceberg_s3_path": "<CATALOG_NAME>",
    "token": "<DATABRICK_USER_PERSONAL_ACCESS_TOKEN>",
    "no_identifier_fields": true
  }
}
```

change the placeholders with actual values
* `DATABRICK_WORKSPACE_URL` -> Databricks workplace URL (URL that you used to access your Databricks console)
* `CATALOG_NAME` -> Catalog name (ex, workspace)
* `NAMESPACE` -> Namespace name inside catalog (ex, default)
* `DATABRICK_USER_PERSONAL_ACCESS_TOKEN` -> Go to settings > developer > create PAT
* no_identifier_fields -> true (This is needed for environments which doesn't support Equality delete based updates. Ex Databricks Unity managed iceberg tables)

Note : Auth can be done using Oauth2 as well.

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
        "hive_sasl_enabled": false
    }
}
```

To use Iceberg with GCP, configure the writer to connect to a Hive Metastore hosted on Dataproc Metastore and write data to Dataproc generated bucket
```json
{
  "type": "ICEBERG",
  "writer": {
      "catalog_type": "hive",
      "hive_uri": "thrift://<hive-dataproc-metastore-ip>:9083",
      "hive_clients": 10,
      "hive_sasl_enabled": false,
      "iceberg_s3_path": "gs://gcp-Dataproc-bucket/hive-warehouse",
      "aws_region": "us-central1"
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