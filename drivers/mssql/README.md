# MSSQL Driver
The MSSQL Driver enables data synchronization from Microsoft SQL Server to your desired destination. It supports **Full Refresh**, **Incremental**, and **CDC (Change Data Capture)** modes.

---

## Supported Modes
1. **Full Refresh**
   Fetches the complete dataset from SQL Server with chunked snapshotting for efficient parallel processing.
2. **CDC (Change Data Capture)**
   Tracks and syncs incremental changes from SQL Server in real time using native SQL Server CDC with LSN (Log Sequence Number) tracking.
3. **Strict CDC (Change Data Capture)**
   Tracks only new changes from the current position in the SQL Server CDC log, without performing an initial backfill.
4. **Incremental**
   Syncs only new or modified records which have cursor value greater than or equal to the saved position.

---

## Setup and Configuration
To run the MSSQL Driver, configure the following files with your specific credentials and settings:

- **`config.json`**: SQL Server connection details.
- **`streams.json`**: List of tables and fields to sync (generated using the *Discover* command).
- **`write.json`**: Configuration for the destination where the data will be written.

Place these files in your project directory before running the commands.

### Config File
Add SQL Server credentials in following format in `config.json` file. [More details.](https://olake.io/docs/connectors/mssql/config)
   ```json
   {
    "host": "sql-server-host",
    "port": 1433,
    "database": "database_name",
    "username": "sql_user",
    "password": "sql_password",
    "max_threads": 3,
    "retry_count": 3,
    "ssl": {
        "mode": "disable"
    }
  }
```

### Enabling CDC for SQL Server

CDC (Change Data Capture) must be enabled at both the database level and table level for CDC sync modes to work.

#### Enable CDC at Database Level

Run the following SQL command on your SQL Server database:

```sql
USE [your_database_name];
GO
EXEC sys.sp_cdc_enable_db;
GO
```

To verify CDC is enabled at the database level:

```sql
SELECT is_cdc_enabled 
FROM sys.databases 
WHERE name = 'your_database_name';
```

This should return `1` if CDC is enabled.

#### Enable CDC at Table Level

For each table you want to sync using CDC, enable CDC on that specific table:

```sql
USE [your_database_name];
GO
EXEC sys.sp_cdc_enable_table
    @source_schema = N'schema_name',
    @source_name = N'table_name',
    @role_name = NULL;  -- Optional: specify a role name to control access
GO
```

To verify CDC is enabled for a specific table:

```sql
SELECT c.capture_instance
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN cdc.change_tables c ON t.object_id = c.source_object_id
WHERE s.name = 'schema_name' AND t.name = 'table_name';
```

If CDC is enabled, this query will return the capture instance name.

> **Note**: You must be a member of the `db_owner` fixed database role to enable CDC. CDC requires SQL Server Enterprise, Developer, or Standard edition (Standard edition has limitations).

---

## Commands

### Discover Command

The *Discover* command generates json content for `streams.json` file, which defines the schema of the tables to be synced.

#### Usage
To run the Discover command, use the following syntax
   ```bash
   ./build.sh driver-mssql discover --config /path/to/config.json
   ```

To run discover and save output directly to streams file:
   ```bash
   ./build.sh driver-mssql discover --config /path/to/config.json > /path/to/streams.json
   ```

#### Example Response (Formatted)
After executing the Discover command, a formatted response will look like this:
```json
{
  "type": "CATALOG",
  "catalog": {
      "selected_streams": {
         "dbo": [
               {
                  "partition_regex": "",
                  "stream_name": "table_1",
                  "normalization": false,
                  "append_mode": false,
                  "chunk_column": ""
               }
         ]
      },
      "streams": [
         {
         "stream": {
            "name": "table_1",
            "namespace": "dbo",
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
   - To enable `append_mode` mode, explicitly set it to `true` in the selected stream configuration. \
      Similarly, for `chunk_column`, ensure it is defined in the stream settings as required.
      ```json
         "selected_streams": {
            "dbo": [
                  {
                     "partition_regex": "",
                     "stream_name": "table_1",
                     "normalization": false,
                     "append_mode": false,
                     "chunk_column": ""
                  }
            ]
         },
      ```
      
   - Add `cursor_field` from set of `available_cursor_fields` in case of incremental sync. This column will be used to track which rows from the table must be synced. If the primary cursor field is expected to contain `null` values, a fallback cursor field can be specified after the primary cursor field using a colon separator. The system will use the fallback cursor when the primary cursor is `null`.
        > **Note**: For incremental sync to work correctly, the primary cursor field (and fallback cursor field if defined) must contain at least one non-null value. Defined cursor fields cannot be entirely null.
      ```json
         "sync_mode": "incremental",
         "cursor_field": "UPDATED_AT:CREATED_AT" // UPDATED_AT is the primary cursor field, CREATED_AT is the fallback cursor field (which can be skipped if the primary cursor is not expected to contain null values)
      ```

   - The `filter` mode under selected_streams allows you to define precise criteria for selectively syncing data from your source.
      ```json
         "selected_streams": {
            "dbo": [
                  {
                     "partition_regex": "",
                     "stream_name": "table_1",
                     "normalization": false,
                     "filter": "id > 1 and created_at <= \"2025-05-27T11:43:40.497+00:00\""
                  }
            ]
         },
      ```
      For primitive types, directly provide the value without using any quotes.

- Final Streams Example
<br> `normalization` determines that level 1 flattening is required. <br>
<br> The `append_mode` flag determines whether records can be written to the iceberg delete file. If set to true, no records will be written to the delete file. Know more about delete file: [Iceberg MOR and COW](https://olake.io/iceberg/mor-vs-cow)<br>
<br>The `chunk_column` used to divide data into chunks for efficient parallel querying and extraction from the database.<br>
   ```json
   {
      "selected_streams": {
         "dbo": [
               {
                  "partition_regex": "",
                  "stream_name": "table_1",
                  "normalization": false,
                  "append_mode": false,
                  "chunk_column": ""
               }
         ]
      },
      "streams": [
         {
            "stream": {
               "name": "table_1",
               "namespace": "dbo",
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
      "aws_secret_key": "password"
    }
  }
  ```
Find more about writer docs [here.](https://olake.io/docs/category/destinations-writers)

### Sync Command
The *Sync* command fetches data from SQL Server and ingests it into the destination.

```bash
./build.sh driver-mssql sync --config /path/to/config.json --catalog /path/to/streams.json --destination /path/to/write.json
```

To run sync with state
```bash
./build.sh driver-mssql sync --config /path/to/config.json --catalog /path/to/streams.json --destination /path/to/write.json --state /path/to/state.json
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
            "lsn": "000001d4:00000a28:0001"
        },
        "streams": [
            "dbo.table_1",
            "dbo.table_2"
        ]
    },
    "streams": [
        {
            "stream": "table_1",
            "namespace": "dbo",
            "sync_mode": "",
            "state": {
                "chunks": []
            }
        },
        {
            "stream": "table_2",
            "namespace": "dbo",
            "sync_mode": "",
            "state": {
                "chunks": []
            }
        }
    ]
}
```
