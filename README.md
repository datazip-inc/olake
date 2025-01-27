<h1 align="center" style="border-bottom: none">
    <a href="https://datazip.io/olake" target="_blank">
        <img alt="olake" src="https://github.com/user-attachments/assets/d204f25f-5289-423c-b3f2-44b2194bdeaf" width="100" height="100"/>
    </a>
    <br>OLake
</h1>

<p align="center">Fastest open-source tool for replicating Databases to Apache Iceberg or Data Lakehouse. ⚡ Efficient, quick and scalable data ingestion for real-time analytics. Starting with MongoDB. Visit <a href="https://datazip.io/olake" target="_blank">datazip.io/olake</a> for the full documentation, and benchmarks</p>

<p align="center">
    <img alt="GitHub issues" src="https://img.shields.io/github/issues/datazip-inc/olake"> </a>
    <a href="https://twitter.com/intent/tweet?text=Use%20the%20fastest%20open-source%20tool,%20OLake,%20for%20replicating%20Databases%20to%20S3%20and%20Apache%20Iceberg%20or%20Data%20Lakehouse.%20It%E2%80%99s%20Efficient,%20quick%20and%20scalable%20data%20ingestion%20for%20real-time%20analytics.%20Check%20at%20https://datazip.io/%20%23opensource%20%23olake%20via%20%40datazipio">
        <img alt="tweet" src="https://img.shields.io/twitter/url/http/shields.io.svg?style=social"></a> 
    <a href="https://join.slack.com/t/getolake/shared_invite/zt-2utw44do6-g4XuKKeqBghBMy2~LcJ4ag">
        <img alt="slack" src="https://img.shields.io/badge/Join%20Our%20Community-Slack-blue"> 
    </a> 
</p>
  
  
<h3 align="center">
  <a href="https://datazip.io/olake/docs"><b>Documentation</b></a> &bull;
  <a href="https://twitter.com/datazipio"><b>Twitter</b></a>
</h3>


![undefined](https://github.com/user-attachments/assets/fe37e142-556a-48f0-a649-febc3dbd083c)

Connector ecosystem for Olake, the key points Olake Connectors focuses on are these
- **Integrated Writers to avoid block of reading, and pushing directly into destinations**
- **Connector Autonomy**
- **Avoid operations that don't contribute to increasing record throughput**

# Getting Started with OLake

Follow the steps below to get started with OLake:

1. Set Up Your Folder and Configuration Files

    First, create a folder on your system where you'll store the necessary configuration files. We'll refer to this folder as `olake_folder_path`.

    Inside `olake_folder_path`, create the following files:

    - `config.json`: This file contains the configuration settings for OLake. You can find the file format and detailed information [here](https://github.com/datazip-inc/olake/tree/master/drivers/mongodb#config-file).
  
    - `write.json`: This file is used to specify the destination and write settings. Ensure that the `local_path` field is set to `/mnt/config`.

    ### Example Structure of `write.json`:
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
3. Run the discovery process to generate the catalog:  
    ```bash
   docker run -v olake_folder_path:/mnt/config olakego/source-mongodb:latest discover --config /mnt/config/config.json
    ```

4. Run the sync process to replicate data:  
    ```bash
   docker run -v olake_folder_path:/mnt/config olakego/source-mongodb:latest sync --config /mnt/config/config.json --catalog /mnt/config/catalog.json --destination /mnt/config/write.json

    ```

5. For incremental sync with state, run:  
    ```bash
    docker run -v olake_folder_path:/mnt/config olakego/source-mongodb:latest sync --config /mnt/config/config.json --catalog /mnt/config/catalog.json --destination /mnt/config/write.json --state /mnt/config/state.json

    ```

For more details, refer to the [documentation](https://datazip.io/olake/docs).



## Benchmark Results: Refer this doc for complete information

### Speed Comparison: Full Load Performance

For a collection of 230 million rows (664.81GB) from [Twitter data](https://archive.org/details/archiveteam-twitter-stream-2017-11), here's how Olake compares to other tools:

| Tool              | Full Load Time    | Performance          |
|-------------------|-------------------|----------------------|
| **Olake**         | 46 mins           | X times faster       |
| **Fivetran**      | 4 hours 39 mins (279 mins) | 6x slower          |
| **Airbyte**       | 16 hours (960 mins) | 20x slower         |
| **Debezium (Embedded)** | 11.65 hours (699 mins) | 15x slower     |


### Incremental Sync Performance

| Tool                 | Incremental Sync Time | Records per Second (r/s) | Performance      |
|----------------------|------------------------|---------------------------|------------------|
| **Olake**            | 28.3 sec              | 35,694 r/s                | X times faster   |
| **Fivetran**         | 3 min 10 sec          | 5,260 r/s                 | 6.7x slower      |
| **Airbyte**          | 12 min 44 sec         | 1,308 r/s                 | 27.3x slower     |
| **Debezium (Embedded)** | 12 min 44 sec       | 1,308 r/s                 | 27.3x slower     |

Cost Comparison: (Considering 230 million first full load & 50 million rows incremental rows per month) as dated 30th September: Find more [here](https://datazip.io/olake/docs/olake/mongodb/benchmark).



### Testing Infrastructure

Virtual Machine: `Standard_D64as_v5`

- CPU: `64` vCPUs
- Memory: `256` GiB RAM
- Storage: `250` GB of shared storage

### MongoDB Setup:

- 3 Nodes running in a replica set configuration:
  - 1 Primary Node (Master) that handles all write operations.
  - 2 Secondary Nodes (Replicas) that replicate data from the primary node.

Find more [here](https://datazip.io/olake/docs/olake/mongodb/benchmark).


## Components
### Drivers

Drivers aka Connectors/Source that includes the logic for interacting with database. Upcoming drivers being planned are
- [x] MongoDB ([Documentation](https://github.com/datazip-inc/olake/tree/master/drivers/mongodb))
- [ ] Kafka
- [ ] Postgres
- [ ] DynamoDB



### Writers

Writers are directly integrated into drivers to avoid blockage of writing/reading into/from os.StdOut or any other type of I/O. This enables direct insertion of records from each individual fired query to the destination.

Writers are being planned in this order
- [x] Parquet Writer (Writes Parquet files on Local/S3)
- [ ] S3 Iceberg Parquet
- [ ] Snowflake
- [ ] BigQuery
- [ ] RedShift

### Core

Core or framework is the component/logic that has been abstracted out from Connectors to follow DRY. This includes base CLI commands, State logic, Validation logic, Type detection for unstructured data, handling Config, State, Catalog, and Writer config file, logging etc.

Core includes http server that directly exposes live stats about running sync such as
- Possible finish time
- Concurrently running processes
- Live record count

Core handles the commands to interact with a driver via these
- spec command: Returns render-able JSON Schema that can be consumed by rjsf libraries in frontend
- check command: performs all necessary checks on the Config, Catalog, State and Writer config
- discover command: Returns all streams and their schema
- sync command: Extracts data out of Source and writes into destinations


### SDKs

SDKs are libraries/packages that can orchestrate the connector in two environments i.e. Docker and Kubernetes. These SDKs can be directly consumed by users similar to PyAirbyte, DLT-hub.

(Unconfirmed) SDKs can interact with Connectors via potential GRPC server to override certain default behavior of the system by adding custom functions to enable features like Transformation, Custom Table Name via writer, or adding hooks.

### Olake

Olake will be built on top of SDK providing persistent storage and a user interface that enables orchestration directly from your machine with default writer mode as `S3 Iceberg Parquet`