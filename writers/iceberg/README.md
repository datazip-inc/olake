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
    "normalization": false,
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
      "normalization": false,
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
    "normalization": false,
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
        "normalization": false,
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
