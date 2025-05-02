# Postgres Driver
The Postgres Driver enables data synchronization from Postgres to your desired destination. It supports both **Full Refresh** and **CDC (Change Data Capture)** modes.

---

## Supported Modes
1. **Full Refresh**  
   Fetches the complete dataset from Postgres.
2. **CDC (Change Data Capture)**  
   Tracks and syncs incremental changes from Postgres in real time.

---

## Setup and Configuration
To run the Postgres Driver, configure the following files with your specific credentials and settings:

- **`config.json`**: postgres connection details.  
- **`streams.json`**: List of collections and fields to sync (generated using the *Discover* command).  
- **`write.json`**: Configuration for the destination where the data will be written.

Place these files in your project directory before running the commands.

### Config File 
Add Postgres credentials in following format in `config.json` file. [More details.](https://olake.io/docs/connectors/postgres/config)
   ```json
   {
    "host": "postgres-host",
    "port": 5432,
    "database": "postgres_db",
    "username": "postgres_user",
    "password": "postgres_pass",
    "jdbc_url_params": {},
    "ssl": {
        "mode": "disable"
    },
    "update_method": { 
        "replication_slot": "postgres_slot",
        "intial_wait_time":10
    },
    "reader_batch_size": 100000,
    "default_mode":"cdc",
    "max_threads" :50,
  }
```


## Commands

### Discover Command

The *Discover* command generates json content for `streams.json` file, which defines the schema of the collections to be synced.

#### Usage
To run the Discover command, use the following syntax
   ```bash
   ./build.sh driver-postgres discover --config /postgres/examples/config.json 
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
                  "split_column":"",
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
- Add split column (primary key) based on which full load chunks can be created 

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
- Final Streams Example
<br> `normalization` determines that level 1 flattening is required. <br>
   ```json
   {
      "selected_streams": {
         "public": [
               {
                  "partition_regex": "",
                  "stream_name": "table_1",
                  "split_column":"",
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
The *Sync* command fetches data from Postgres and ingests it into the destination.

```bash
./build.sh driver-postgres sync --config /postgres/examples/config.json --catalog /postgres/examples/streams.json --destination /postgres/examples/write.json
```

To run sync with state 
```bash
./build.sh driver-postgres sync --config /postgres/examples/config.json --catalog /postgres/examples/streams.json --destination /postgres/examples/write.json --state /postgres/examples/state.json
```


### State File 
The State file is generated by the CLI command at the completion of a batch or the end of a sync. This file can be used to save the sync progress and later resume from a specific checkpoint.
#### State File Format
You can save the state in a `state.json` file using the following format:
```json
{
    "type": "GLOBAL",
    "global": {
        "state": {
            "lsn": "2D9/AD00445A"
        },
        "streams": [
            "public.table_1",
            "public.table_2"
        ]
    },
    "streams": [
        {
            "stream": "table_1",
            "namespace": "public",
            "sync_mode": "",
            "state": {
                "chunks": []
            }
        },
        {
            "stream": "table_2",
            "namespace": "public",
            "sync_mode": "",
            "state": {
                "chunks": []
            }
        }
    ]
}
```

Find more at [Postgres Docs](https://olake.io/docs/category/postgres)