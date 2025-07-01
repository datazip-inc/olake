# MySql Driver
The MySql Driver enables data synchronization from MySql to your desired destination. It supports both **Full Refresh** and **CDC (Change Data Capture)** modes.

---

## Supported Modes
1. **Full Refresh**
   Fetches the complete dataset from MySql.
2. **CDC (Change Data Capture)**
   Tracks and syncs incremental changes from MySql in real time.
3. **Strict CDC (Change Data Capture)**
   Tracks only new changes from the current position in the MySQL binlog, without performing an initial backfill.

---

## Setup and Configuration
To run the MySql Driver, configure the following files with your specific credentials and settings:

- **`config.json`**: MySql connection details.
- **`streams.json`**: List of collections and fields to sync (generated using the *Discover* command).
- **`write.json`**: Configuration for the destination where the data will be written.

Place these files in your project directory before running the commands.

### Config File
Add MySql credentials in following format in `config.json` file. [More details.](https://olake.io/docs/connectors/mysql/config)
   ```json
  {
    "hosts": "mysql-host",
    "username": "mysql-user",
    "password": "mysql-password",
    "database": "mysql-database",
    "port": 3306,
    "update_method": {
      "intial_wait_time": 10
     },
    "tls_skip_verify": true,
    "max_threads":10,
    "backoff_retry_count": 2,
    "jdbc_url_params": {
      "connectTimeout": "30000",
      "socketTimeout": "30000",
      "useSSL": "true"
    },
    "ssl_config": {
      "mode": "verify-ca",
      "server_ca": "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
      "client_cert": "-----BEGIN CERTIFICATE-----\nMIID...\n-----END CERTIFICATE-----",
      "client_key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----"
    }
  }
```

#### JDBC URL Parameters

The `jdbc_url_params` field allows you to specify custom connection parameters for the MySQL JDBC connection. These parameters will be appended to the connection string. Common parameters include:

- `connectTimeout`: Connection timeout in milliseconds
- `socketTimeout`: Socket timeout in milliseconds
- `useSSL`: Whether to use SSL for the connection
- `autoReconnect`: Whether to automatically reconnect if connection is lost
- `characterEncoding`: Character encoding to use
- `serverTimezone`: Server timezone to use

#### SSL Configuration

The `ssl_config` field provides advanced SSL configuration options:

- `mode`: SSL mode to use. Options include:
  - `disable`: No SSL
  - `require`: Use SSL, but don't verify the server certificate
  - `verify-ca`: Verify that the server certificate is signed by a trusted CA
  - `verify-full`: Verify that the server certificate is signed by a trusted CA and the server hostname matches the certificate

When using `verify-ca` or `verify-full` modes, you must provide:
- `server_ca`: The CA certificate in PEM format
- `client_cert`: The client certificate in PEM format
- `client_key`: The client private key in PEM format

For simple SSL connections without certificate verification, you can use the `tls_skip_verify` option instead of the full SSL configuration.

## Commands

### Discover Command

The *Discover* command generates json content for `streams.json` file, which defines the schema of the collections to be synced.

#### Usage
To run the Discover command, use the following syntax
   ```bash
   ./build.sh driver-mysql discover --config /mysql/examples/config.json
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
                  "normalization": false,
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
      "sync_mode": "cdc",
      ```
   - Specify the cursor field (only for incremental syncs):
      ```json
      "cursor_field": "<cursor field from available_cursor_fields>"
      ```
   - To enable `append_only` mode, explicitly set it to `true` in the selected stream configuration. \
      Similarly, for `chunk_column`, ensure it is defined in the stream settings as required.
      ```json
         "selected_streams": {
            "public": [
                  {
                     "partition_regex": "",
                     "stream_name": "table_1",
                     "normalization": false,
                     "append_only": false,
                     "chunk_column":""
                  }
            ]
         },
      ```

- Final Streams Example
<br> `normalization` determines that level 1 flattening is required. <br>
<br> The `append_only` flag determines whether records can be written to th iceberg delete file. If set to true, no records will be written to the delete file. Know more about delete file: [Iceberg MOR and COW](https://olake.io/iceberg/mor-vs-cow)<br>
<br>The `chunk_column` used to divide data into chunks for efficient parallel querying and extraction from the database.<br>
   ```json
   {
      "selected_streams": {
         "public": [
               {
                  "partition_regex": "",
                  "stream_name": "table_1",
                  "normalization": false,
                  "append_only": false,
                  "chunk_column":""
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
        "aws_region": "ap-south-1",
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
The *Sync* command fetches data from MySql and ingests it into the destination.

```bash
./build.sh driver-mysql sync --config /mysql/examples/config.json --catalog /mysql/examples/streams.json --destination /mysql/examples/write.json
```

To run sync with state
```bash
./build.sh driver-mysql sync --config /mysql/examples/config.json --catalog /mysql/examples/streams.json --destination /mysql/examples/write.json --state /mysql/examples/state.json
```


### State File
The State file is generated by the CLI command at the completion of a batch or the end of a sync. This file can be used to save the sync progress and later resume from a specific checkpoint.
#### State File Format
You can save the state in a `state.json` file using the following format:
```json
{
    "type": "GLOBAL",
    "global": {
        "server_id": 7000,
        "state": {
            "position": {
                "Name": "mysql-bin.000040",
                "Pos": 7688
            }
        },
        "streams": [
            "mydatabase.table_1",
            "mydatabase.table_2"
        ]
    },
    "streams": [
        {
            "stream": "table_1",
            "namespace": "mydatabase",
            "sync_mode": "",
            "state": {
                "chunks": []
            }
        },
        {
            "stream": "table_2",
            "namespace": "mydatabase",
            "sync_mode": "",
            "state": {
                "chunks": []
            }
        }
    ]
}
```

Find more at [MySql Docs](https://olake.io/docs/category/mysql)