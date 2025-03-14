
# Java Iceberg Sink

This project is a fork and modified version of the [debezium-server-iceberg](https://github.com/memiiso/debezium-server-iceberg) project, originally used to dump data from Debezium Server into Iceberg. The modifications make it compatible with Olake by sending data in Debezium format.

## Architecture

The data flow in this project is as follows:


Golang Code  --gRPC-->  Java (This Project)  --Write to Iceberg-->  S3 + Iceberg Catalog

Check out the Olake Iceberg Writer code to understand how data is sent to Java via gRPC.

## Prerequisites

- **Java 17** must be installed.

## Running the Project

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
                "{\"jdbc.password\":\"my_password\",\"s3.path-style-access\":\"true\",\"jdbc.user\":\"my_user\",\"io-impl\":\"org.apache.iceberg.aws.s3.S3FileIO\",\"catalog-impl\":\"org.apache.iceberg.aws.glue.GlueCatalog\",\"upsert\":\"true\",\"table-namespace\":\"olake_iceberg\",\"catalog-name\":\"olake_iceberg\",\"warehouse\":\"s3://bucket-name/olake_iceberg/test_olake\",\"uri\":\"jdbc_db_url\",\"glue.region\":\"ap-south-1\",\"s3.secret-access-key\":\"XXX\",\"s3.access-key-id\":\"XXX\",\"upsert-keep-deletes\":\"true\",\"write.format.default\":\"parquet\",\"table-prefix\":\"\"}"
            ]
        }
    ]
}
```

> **Important:** Replace the following fields with your actual configuration values:
> - `s3.secret-access-key`
> - `s3.access-key-id`
> - `warehouse`

This configuration is designed for AWS Glue and S3-based Iceberg settings. Ensure that the generated credentials have full access to both Glue and the specified S3 bucket.

### Functionality

When you run the project, it will:
- Spin up a gRPC server ready to accept records in Debezium format.

## Testing in Standalone Mode

To test the project independently, follow these steps:

1. Download the test files as a ZIP from [this gist](https://gist.github.com/shubham19may/b820daf21fdfae2c648204889ab62fc7).
2. Unzip the downloaded file.
3. After starting the Java server, run the Golang main file using the following command:

   ```bash
   go run main.go messaging.pb.go messaging_grpc.pb.go
   ```

This test will:
- Create an Iceberg table named `incr1111` in the `olake_iceberg` database.
- Insert one record into the table.