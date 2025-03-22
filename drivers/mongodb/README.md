# MongoDB Driver
The MongoDB Driver enables data synchronization from MongoDB to your desired destination. It supports both **Full Refresh** and **CDC (Change Data Capture)** modes.

---

## Supported Modes
1. **Full Refresh**  
   Fetches the complete dataset from MongoDB.
2. **CDC (Change Data Capture)**  
   Tracks and syncs incremental changes from MongoDB in real time.

---

## Setup and Configuration
To run the MongoDB Driver, configure the following files with your specific credentials and settings:

- **`config.json`**: MongoDB connection details.  
- **`catalog.json`**: List of collections and fields to sync (generated using the *Discover* command).  
- **`write.json`**: Configuration for the destination where the data will be written.

Place these files in your project directory before running the commands.

### Config File 
Add MongoDB credentials in following format in `config.json` file. To check more about config [visit docs](https://olake.io/docs/connectors/mongodb/config)
   ```json
   {
      "hosts": [
         "host1:27017",
         "host2:27017",
         "host3:27017"
      ],
      "username": "test",
      "password": "test",
      "authdb": "admin",
      "replica-set": "rs0",
      "read-preference": "secondaryPreferred",
      "srv": false,
      "server-ram": 16,
      "database": "database",
      "max_threads": 50,
      "default_mode" : "cdc",
      "backoff_retry_count": 2,
      "partition_strategy":""
   }
```

## Commands
### Discover Command
The *Discover* command generates json content for `catalog.json` file, which defines the schema of the collections to be synced.

#### Usage
To run the Discover command, use the following syntax
   ```bash
   ./build.sh driver-mongodb discover --config /mongodb/examples/config.json 
   ```

#### Example Response (Formatted)
After executing the Discover command, a formatted response will look like this:
```json
{
  "type": "CATALOG",
  "catalog": {
      "selected_streams": {
         "namespace": [
               {
                  "partition_regex": "",
                  "stream_name": "incr"
               }
         ]
      },
      "streams": [
         {
         "stream": {
            "name": "tweets",
            "namespace": "twitter_data",
            ...
         }
         }
      ]
  }
}
```

#### Configure Catalog
Before running the Sync command, the generated `catalog.json` file must be configured. Follow these steps:
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
- Final Catalog Example
   ```json
   {
      "selected_streams": {
         "namespace": [
               {
                  "partition_regex": "",
                  "stream_name": "incr"
               }
         ]
      },
      "streams": [
         {
            "stream": {
               "name": "incr2",
               "namespace": "incr",
               ...
               "sync_mode": "cdc"
            }
         }
      ]
   }
   ```




### Writer File 
The Writer file defines the configuration for the destination where data needs to be added.<br>
`normalization` determine that Level 1 flattening is required. <br>
Example (For Local):
   ```
   {
      "type": "PARQUET",
      "writer": {
         "normalization":true,
         "local_path": "./examples/reader"
      }
   }
   ```
Example (For S3):
   ```
   {
      "type": "PARQUET",
      "writer": {
         "normalization":false,
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
        "normalization": false,
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

For Detailed overview check [here.](https://olake.io/docs/category/destinations-writers)

### Sync Command
The *Sync* command fetches data from MongoDB and ingests it into the destination.

```bash
./build.sh driver-mongodb sync --config /mongodb/examples/config.json --catalog /mongodb/examples/catalog.json --destination /mongodb/examples/write.json
```

To run sync with state 
```bash
./build.sh driver-mongodb sync --config /mongodb/examples/config.json --catalog /mongodb/examples/catalog.json --destination /mongodb/examples/write.json --state /mongodb/examples/state.json
```


### State File 
The State file is generated by the CLI command at the completion of a batch or the end of a sync. This file can be used to save the sync progress and later resume from a specific checkpoint.
#### State File Format
You can save the state in a `state.json` file using the following format:
```json
{
    "type": "STREAM",
    "streams": [
        {
            "stream":"stream_8",
            "namespace":"otter_db",
            "sync_mode":"cdc",
            "state": {
                "resume_token": {"_data": "82673F82FE000000022B0429296E1404"}
            }
        },
        {
            "stream":"stream_0",
            "namespace":"otter_db",
            "sync_mode":"cdc",
            "state": {
                "resume_token": {"_data": "82673F82FE000000022B0429296E1404"}
            }
        }
    ]
}
```

### Speed Comparison: Full Load Performance
For a collection of 230 million rows (664.81GB) from [Twitter data](https://archive.org/details/archiveteam-twitter-stream-2017-11), here's how Olake compares to other tools:

| Tool                    | Full Load Time             | Performance    |
| ----------------------- | -------------------------- | -------------- |
| **Olake**               | 46 mins                    | X times faster |
| **Fivetran**            | 4 hours 39 mins (279 mins) | 6x slower      |
| **Airbyte**             | 16 hours (960 mins)        | 20x slower     |
| **Debezium (Embedded)** | 11.65 hours (699 mins)     | 15x slower     |


### Incremental Sync Performance

| Tool                    | Incremental Sync Time | Records per Second (r/s) | Performance    |
| ----------------------- | --------------------- | ------------------------ | -------------- |
| **Olake**               | 28.3 sec              | 35,694 r/s               | X times faster |
| **Fivetran**            | 3 min 10 sec          | 5,260 r/s                | 6.7x slower    |
| **Airbyte**             | 12 min 44 sec         | 1,308 r/s                | 27.3x slower   |
| **Debezium (Embedded)** | 12 min 44 sec         | 1,308 r/s                | 27.3x slower   |

Cost Comparison: (Considering 230 million first full load & 50 million rows incremental rows per month) as dated 30th September 2025

### Testing Infrastructure

Virtual Machine: `Standard_D64as_v5`

- CPU: `64` vCPUs
- Memory: `256` GiB RAM
- Storage: `250` GB of shared storage

### MongoDB Setup:
  - 3 Nodes running in a replica set configuration:
  - 1 Primary Node (Master) that handles all write operations.
  - 2 Secondary Nodes (Replicas) that replicate data from the primary node.

Find more at [MongoDB Docs](https://olake.io/docs/category/mongodb)