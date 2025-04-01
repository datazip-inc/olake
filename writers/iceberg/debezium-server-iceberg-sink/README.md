
# Java Iceberg Sink

This project is a fork and modified version of the [debezium-server-iceberg](https://github.com/memiiso/debezium-server-iceberg) project, originally used to dump data from Debezium Server into Iceberg. The modifications make it compatible with Olake by sending data in Debezium format.

## Architecture

The data flow in this project is as follows:


Golang Code  --gRPC-->  Java (This Project)  --Write to Iceberg-->  S3 + Iceberg Catalog

(Check out the Olake Iceberg Writer code to understand how data is sent to Java via gRPC.)

## Prerequisites

- **Java 17** must be installed.
- **Docker** & Docker compose.

## Running the Project

### Setup local storage and catalog (Minio + JDBC catalog)

```shell
cd writers/iceberg/local-test
docker compose up
```
This will create 
1. postgres --> JDBC catalog
2. Minio --> For AWS S3 like filesystem setup on your local
3. Spark --> Querying Iceberg data.

### VSCode Debug Configuration

Set up the following configuration in your VSCode debug console:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "type": "java",
            "name": "OlakeRpcServer",
            "request": "launch",
            "mainClass": "io.debezium.server.iceberg.OlakeRpcServer",
            "projectName": "iceberg-sink",
            "args": [
                "{\"catalog-impl\":\"org.apache.iceberg.jdbc.JdbcCatalog\",\"catalog-name\":\"olake_iceberg\",\"io-impl\":\"org.apache.iceberg.aws.s3.S3FileIO\",\"jdbc.password\":\"password\",\"jdbc.user\":\"iceberg\",\"port\":\"50051\",\"s3.access-key-id\":\"admin\",\"s3.endpoint\":\"http://localhost:9000\",\"s3.path-style-access\":\"true\",\"s3.secret-access-key\":\"password\",\"s3.ssl-enabled\":\"false\", \"s3.region\":\"us-east-1\",\"table-namespace\":\"olake_iceberg\",\"table-prefix\":\"\",\"upsert\":\"false\",\"upsert-keep-deletes\":\"true\",\"uri\":\"jdbc:postgresql://localhost:5432/iceberg\",\"warehouse\":\"s3a://warehouse\",\"write.format.default\":\"parquet\"}"
            ]
        }
    ]
}
```
> warehouse = path where data will be stored in s3.

### Functionality

When you run the project, it will:
- Spin up a gRPC server ready to accept records in Debezium format.
- Store the data in local Minio & postgres JDBC catalog.

## Testing in Standalone Mode

To test the project independently, follow these steps:

1. Download the test files as a ZIP from [this gist](https://gist.github.com/shubham19may/b820daf21fdfae2c648204889ab62fc7).
2. Unzip the downloaded file.
3. After starting the Java server, run the Golang main file using the following command:

   ```shell
   go run main.go messaging.pb.go messaging_grpc.pb.go
   ```

This test will:
- Create an Iceberg table named `incr1111` in the `olake_iceberg` database.
- Insert one record into the table.

> Now how to see if the data is actually populated?
Run : 
```shell
# Connect to the spark-iceberg container
docker exec -it spark-iceberg bash

# Start spark-sql (rerun it if error occurs)
spark-sql

# Query in format select * from catalog_name.iceberg_db_name.table_name
select * from olake_iceberg.olake_iceberg.incr1111;
```