# Oracle Driver
The Oracle Driver enables data synchronization from Oracle to your desired destination. It supports **Full Refresh** mode.

---

## Supported Modes
1. **Full Refresh**
   Fetches the complete dataset from Oracle.

---

## Setup and Configuration
To run the Oracle Driver, configure the following files with your specific credentials and settings:

- **`config.json`**: oracle connection details.
- **`streams.json`**: List of collections and fields to sync (generated using the *Discover* command).
- **`write.json`**: Configuration for the destination where the data will be written.

Place these files in your project directory before running the commands.

### Config File
Add Oracle credentials in following format in `config.json` file. [More details.](https://olake.io/docs/connectors/oracle/config)
   ```json
   {
    "host": "oracle-host",
    "username": "oracle-user",
    "password": "oracle-password",
    "service_name": "oracle-service-name",
    "port": 1521,
    "max_threads": 10,
    "retry_count": 0,
    "jdbc_url_params": {},
    "ssl": {
        "mode": "disable"
    }
  }
```


## Commands

### Discover Command

The *Discover* command generates json content for `streams.json` file, which defines the schema of the collections to be synced.

#### Usage
To run the Discover command, use the following syntax
   ```bash
   ./build.sh driver-oracle discover --config /oracle/examples/config.json
   ```

#### Example Response (Formatted)
After executing the Discover command, a formatted response will look like this:
```json
{
  "type": "CATALOG",
  "catalog": {
      "selected_streams": {
         "public": [
               {
                  "partition_regex": "",
                  "stream_name": "table_1",
                  "normalization": false
               }
         ]
      },
      "streams": [
         {
         "stream": {
            "name": "table_1",
            "namespace": "public",
            ...
         }
         }
      ]
  }
}
```

#### Configure Streams
Before running the Sync command, the generated `streams.json` file must be configured. Follow these steps:
- Remove Unnecessary Streams:<br>
   Remove streams from selected streams.
- Add Partition based on Column Value
   Modify partition_regex field to partition destination data based on column value

- Modify Each Stream:<br>
   For each stream you want to sync:<br>
   - Add the following properties:
      ```json
      "sync_mode": "full_refresh",
      ```

- Final Streams Example
<br> `normalization` determines that level 1 flattening is required. <br>
<br> The `append_only` flag determines whether records can be written to the iceberg delete file. If set to true, no records will be written to the delete file. Know more about delete file: [Iceberg MOR and COW](https://olake.io/iceberg/mor-vs-cow)<br>
   ```json
   {
      "selected_streams": {
         "public": [
               {
                  "partition_regex": "",
                  "stream_name": "table_1",
                  "normalization": false,
                  "append_only": false
               }
         ]
      },
      "streams": [
         {
            "stream": {
               "name": "table_1",
               "namespace": "public",
               ...
               "sync_mode": "cdc"
            }
         }
      ]
   }
   ```

### Writer File
The Writer file defines the configuration for the destination where data needs to be added.<br>
Example (For Local):
   ```
   {
      "type": "PARQUET",
      "writer": {
         "local_path": "./examples/reader"
      }
   }
   ```
Example (For S3):
   ```
   {
      "type": "PARQUET",
      "writer": {
         "s3_bucket": "olake",
         "s3_region": "",
         "s3_access_key": "",
         "s3_secret_key": "",
         "s3_path": ""
      }
   }
   ```

Example (For AWS S3 + Glue Configuration)
  ```
  {
      "type": "ICEBERG",
      "writer": {
        "s3_path": "s3://{bucket_name}/{path_prefix}/",
        "aws_access_key": "XXX",
        "aws_secret_key": "XXX",
        "database": "olake_iceberg",
        "grpc_port": 50051,
        "server_host": "localhost"
      }
  }
  ```

Example (Local Test Configuration (JDBC + Minio))
  ```
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

Find more about writer docs [here.](https://olake.io/docs/category/destinations-writers)

### Sync Command
The *Sync* command fetches data from Oracle and ingests it into the destination.

```bash
./build.sh driver-oracle sync --config /oracle/examples/config.json --catalog /oracle/examples/streams.json --destination /oracle/examples/write.json
```

Find more at [Oracle Docs](https://olake.io/docs/category/oracle)