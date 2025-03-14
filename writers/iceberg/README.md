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

Create a json for writer config (Works for S3 as storage and AWS Glue as a catalog) : 
```json
{
    "type": "ICEBERG",
    "writer": {
      "normalization": false,
      "s3_path": "s3://bucket_name/olake_iceberg/test_olake",
      "aws_region": "ap-south-1",
      "aws_access_key": "XXX",
      "aws_secret_key": "XXX",
      "database": "olake_iceberg",
      "grpc_port": 50051,
      "server_host": "localhost"
    }
  }  
```

And run the sync normally as mentioned in the getting started doc.

* s3_path -> Stores the relevant iceberg data/metadata files
* aws_region -> Region for AWS bucket and catalog
* aws_access_key -> AWS access key which has full access to glue & AWS S3
* aws_secret_key -> AWS secret key
* database -> database you want to create in glue.

Please change the above to real credentials to make it work.