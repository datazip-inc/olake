# Kafka Driver
The Kafka Driver enables data synchronization from Kafka to your desired destination. It supports both **Full Refresh** and **Incremental** modes.

---

## Supported Modes
1. **Full Refresh**
   Fetches the complete dataset from Kafka.
2. **Incremental**
   Tracks and syncs incremental changes or messages from Kafka in real time streaming mode.

---

## Setup and Configuration
To run the Kafka Driver, configure the following files with your specific credentials and settings:

- **`source.json`**: Kafka connection details.
- **`streams.json`**: List of topics to sync (generated using the *Discover* command).
- **`destination.json`**: Configuration for the destination where the data will be written.

Place these files in your project directory before running the commands.

### Source File
Add Kafka credentials in following format in `source.json` file. To check more about config [visit docs](https://olake.io/docs/connectors/Kafka/config)
   ```json
    {
        "bootstrap_servers": "localhost:9092, localhost:9093",
        "protocol": {
            "security_protocol": "PLAINTEXT"
        },
        "default_mode": "incremental",
        "consumer_group": "test-consumer",
        "auto_offset_reset": "earliest"
    }
```
- There are 3 security protocols:<br>
    - `PLAINTEXT`
        - Kafka cluster without any authentication and encryption.
    - `SASL_PLAINTEXT`
        - Kafka cluster with Simple Authentication and Security Layer i.e. authentication but no encription.
        - Requires SASL mechanism and SASL JAAS Configuration string. Supported are: 
            - PLAIN
            ```json
            "protocol": {
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "PLAIN",
                "sasl_jaas_config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"TEST-USER\" password=\"TEST-PASS\";"
            },
            ```
            - SCRAM-SHA-512
            ```json
            "protocol": {
                "security_protocol": "SASL_PLAINTEXT",
                "sasl_mechanism": "SCRAM-SHA-512",
                "sasl_jaas_config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"TEST-USER\" password=\"TEST-PASS\";"
            },
            ```
    - `SASL_SSL`
        - Kafka cluster with Simple Authentication and Security Layer i.e. authentication and encryption using Secure Sockets Layer.
        - Requires SASL mechanism and SASL JAAS Configuration string. Supported are: 
            - PLAIN
            ```json
            "protocol": {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "PLAIN",
                "sasl_jaas_config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"TEST-USER\" password=\"TEST-PASS\";"
            },
            ```
            - SCRAM-SHA-512
            ```json
            "protocol": {
                "security_protocol": "SASL_SSL",
                "sasl_mechanism": "SCRAM-SHA-512",
                "sasl_jaas_config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"TEST-PASS\" password=\"TEST-PASS\";"
            },
            ```

## Commands
### Discover Command
The *Discover* command generates json content for `streams.json` file, which defines the schema of the topics to be synced.

#### Usage
To run the Discover command, use the following syntax
   ```bash
   ./build.sh driver-kafka discover --config /kafka/examples/source.json
   ```

#### Example Response (Formatted)
After executing the Discover command, a formatted response will look like this:
```json
{
  "type": "CATALOG",
  "catalog": {
      "selected_streams": {
          "": [
            {
                "partition_regex": "",
                "stream_name": "Temp-Topic-1",
                "normalization": false
            }
        ]
      },
      "streams": [
        {
            "stream": {
                "name": "DukePartitionTest",
                "type_schema": { "properties": {...} },
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

- Modify Each Stream:<br>
   For each stream you want to sync:<br>
   - Add the following properties:
      ```json
      "sync_mode": "incremental",
      ```
   - Specify the cursor field (only for incremental syncs):
      ```json
      "cursor_field": "<cursor field from available_cursor_fields>"
      ```
   - To enable `append_only` mode, explicitly set it to `true` in the selected stream configuration.
      ```json
        "selected_streams": {
            "namespace": [
                {
                    "partition_regex": "",
                    "stream_name": "topic-1",
                    "normalization": false,
                    "append_only": false
                }
            ]
        },
      ```

- Final Streams Example
<br> [To Do] `normalization` determines that level 1 flattening is required. <br>
<br> The `append_only` flag determines whether records can be written to the iceberg delete file. If set to true, no records will be written to the delete file. Know more about delete file: [Iceberg MOR and COW](https://olake.io/iceberg/mor-vs-cow) <br>
   ```json
   {
      "selected_streams": {
         "namespace": [
               {
                  "partition_regex": "",
                  "stream_name": "topic-1",
                  "normalization": false,
                  "append_only": false
               }
         ]
      },
      "streams": [
         {
            "stream": {
               "name": "incr2",
               ...
               "sync_mode": "full_refresh"
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

For Detailed overview check [here.](https://olake.io/docs/category/destinations-writers)

### Sync Command
The *Sync* command fetches data from Kafka and ingests it into the destination.
Currently 2 modes are supported: Full_Refresh and Incremental. 
- In case of Full_Refresh, all the messages from the selected topic will be synced from start offset till the latest offset. 
- In case of Incremental :
   - For this sync, cursor field from the available cursor field needs to be provided.
   ```json
   "cursor_field" = "offset"
   ```
   - if `auto_offset_reset = earliest` then all the unread messages by the consumer will be synced from the start offset, with any of new incoming messages.
   - if `auto_offset_reset = latest` then all the new incoming messages will be synced.

```bash
./build.sh driver-kafka sync --config /Kafka/examples/source.json --catalog /Kafka/examples/streams.json --destination /Kafka/examples/destination.json
```

To run sync with state
```bash
./build.sh driver-kafka sync --config /Kafka/examples/source.json --catalog /Kafka/examples/streams.json --destination /Kafka/examples/destination.json --state /Kafka/examples/state.json
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
            "stream": "Test-Topic",
            "namespace": "",
            "sync_mode": "",
            "state": {
                "partition_LastPartitionNumber_offset": 0
            }
        }
    ]
}
```

Find more at [Olake Docs](https://olake.io/docs)