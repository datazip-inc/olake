<h1 align="center" style="border-bottom: none">
    <a href="https://datazip.io/olake" target="_blank">
        <img alt="olake" src="https://github.com/user-attachments/assets/d204f25f-5289-423c-b3f2-44b2194bdeaf" width="100" height="100"/>
    </a>
    <br>OLake
</h1>

<p align="center">Fastest open-source tool for replicating Databases to Apache Iceberg or Data Lakehouse. ⚡ Efficient, quick and scalable data ingestion for real-time analytics. Starting with MongoDB. Visit <a href="https://olake.io/" target="_blank">olake.io/docs</a> for the full documentation, and benchmarks</p>

<p align="center">
    <img alt="GitHub issues" src="https://img.shields.io/github/issues/datazip-inc/olake"> </a>
    <a href="https://twitter.com/intent/tweet?text=Use%20the%20fastest%20open-source%20tool,%20OLake,%20for%20replicating%20Databases%20to%20S3%20and%20Apache%20Iceberg%20or%20Data%20Lakehouse.%20It%E2%80%99s%20Efficient,%20quick%20and%20scalable%20data%20ingestion%20for%20real-time%20analytics.%20Check%20at%20https://olake.io/%20%23opensource%20%23olake%20via%20%40_olake">
        <img alt="tweet" src="https://img.shields.io/twitter/url/http/shields.io.svg?style=social"></a> 
    <a href="https://join.slack.com/t/getolake/shared_invite/zt-2utw44do6-g4XuKKeqBghBMy2~LcJ4ag">
        <img alt="slack" src="https://img.shields.io/badge/Join%20Our%20Community-Slack-blue"> 
    </a> 
</p>
  

<h3 align="center">
  <a href="https://olake.io/docs"><b>Documentation</b></a> &bull;
  <a href="https://twitter.com/_olake"><b>Twitter</b></a> &bull;
  <a href="https://www.youtube.com/@olakeio"><b>YouTube</b></a> &bull;
  <a href="https://meetwaves.com/library/olake"><b>Slack Knowledgebase</b></a>  &bull;
  <a href="https://olake.io/blog"><b>Blogs</b></a>
</h3>


![undefined](https://github.com/user-attachments/assets/fe37e142-556a-48f0-a649-febc3dbd083c)

Connector ecosystem for Olake, the key points Olake Connectors focuses on are these
- **Integrated Writers to avoid block of reading, and pushing directly into destinations**
- **Connector Autonomy**
- **Avoid operations that don't contribute to increasing record throughput**

# Getting Started with OLake

Follow the steps below to get started with OLake:

1. ### Prepare Your Folder

    1. Create a folder on your computer. Let’s call it `olake_folder_path`.
        <div style="background-color: #f9f9f9; border-left: 6px solid #007bff; padding: 10px; color: black;">

        💡 **Note:** In below configurations replace `olake_folder_path` with the newly created folder path.

        </div>
    2. Inside this folder, create two files:
       - config.json: This file contains your connection details. You can find examples and instructions [here](https://github.com/datazip-inc/olake/tree/master/drivers/mongodb#config-file).
       - writer.json: This file specifies where to save your data (local machine or S3).
    
    #### Example Structure of `writer.json` :
    Example (For Local): 
    ```json
    {
      "type": "PARQUET",
         "writer": {
           "normalization":false, // to enable/disable level one flattening
           "local_path": "/mnt/config/{olake_reader}" // replace olake_reader with desired folder name
      }
    }
    ```
    Example (For S3):
    ```json
    {
      "type": "PARQUET",
         "writer": {
           "normalization":false, // to enable/disable level one flattening
           "s3_bucket": "olake",  
           "s3_region": "",
           "s3_access_key": "", 
           "s3_secret_key": "", 
           "s3_path": ""
       }
    }
    ```
2. ### Generate a Catalog File

   Run the discovery process to identify your MongoDB data:  
    ```bash
   docker run -v olake_folder_path:/mnt/config olakego/source-mongodb:latest discover --config /mnt/config/config.json
    ```
    This will create a catalog.json file in your folder. The file lists the data streams from your MongoDB
    ```json
        {
         "selected_streams": {
                "namespace": [
                    {
                        "partition_regex": "/{col_1, default_value, granularity}",
                        "stream_name": "table1"
                    },
                    {
                        "partition_regex": "",
                        "stream_name": "table2"
                    }
                ]
            },
            "streams": [
                {
                    "stream": {
                        "name": "table1",
                        "namespace": "namespace",
                        // ...
                        "sync_mode": "cdc"
                    }
                },
                {
                    "stream": {
                        "name": "table2",
                        "namespace": "namespace",
                        // ...
                        "sync_mode": "cdc"
                    }
                }
            ]
        }
    ```
    #### (Optional) Partition Destination Folder based on Columns
    Partition data based on column value. Read more in the documentation about [S3 partitioning](https://olake.io/docs/writers/s3#s3-data-partitioning).
    ```json
         "partition_regex": "/{col_1, default_value, granularity}",
    ```
    `col_1`: Partitioning Column. Supports `now()` as a value for the current date.<br>
    `default_value`: if the column value is null or not parsable then the default will be used.<br>
    `granularity` (Optional): Support for time-based columns. Supported Values: `HH`,`DD`,`WW`,`MM`,`YY`.
    #### (Optional) Exclude Unwanted Streams
    To exclude streams, edit catalog.json and remove them from selected_streams. <br>
    #### Example (For Exclusion of table2) 
    **Before**
    ```json
     "selected_streams": {
        "namespace": [
            {
                "partition_regex": "/{col_1, default_value, granularity}",
                "stream_name": "table1"
            },
            {
                "partition_regex": "",
                "stream_name": "table2"
            }
        ]
    }
    ```
    **After Exclusion of table2**
    ```json
    "selected_streams": {
        "namespace": [
            {
                "partition_regex": "/{col_1, default_value, granularity}",
                "stream_name": "table1"
            }
        ]
    }
    ```
3. ### Sync Data
   Run the following command to sync data from MongoDB to your destination:
    
    ```bash
   docker run -v olake_folder_path:/mnt/config olakego/source-mongodb:latest sync --config /mnt/config/config.json --catalog /mnt/config/catalog.json --destination /mnt/config/writer.json

    ```

4. ### Sync with State: 
   If you’ve previously synced data and want to continue from where you left off, use the state file:
    ```bash
    docker run -v olake_folder_path:/mnt/config olakego/source-mongodb:latest sync --config /mnt/config/config.json --catalog /mnt/config/catalog.json --destination /mnt/config/writer.json --state /mnt/config/state.json

    ```

For more details, refer to the [documentation](https://olake.io/docs).



## Benchmark Results: Refer to this doc for complete information

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

Cost Comparison: (Considering 230 million first full load & 50 million rows incremental rows per month) as dated 30th September 2025: Find more [here](https://olake.io/docs/connectors/mongodb/benchmarks).



### Testing Infrastructure

Virtual Machine: `Standard_D64as_v5`

- CPU: `64` vCPUs
- Memory: `256` GiB RAM
- Storage: `250` GB of shared storage

### MongoDB Setup:

- 3 Nodes running in a replica set configuration:
  - 1 Primary Node (Master) that handles all write operations.
  - 2 Secondary Nodes (Replicas) that replicate data from the primary node.

Find more [here](https://olake.io/docs/connectors/mongodb/benchmarks).



Detailed roadmap can be found on [GitHub OLake Roadmap 2024-25](https://github.com/orgs/datazip-inc/projects/5)

## Source Connector Level Functionalities Supported

| Connector Functionalities | MongoDB [(docs)](https://olake.io/docs/connectors/mongodb/overview) | Postgres [(docs)](https://olake.io/docs/connectors/postgres/overview) | MySQL [(docs)](https://olake.io/docs/connectors/mysql/overview) |
| ------------------------- | ------- | -------- | ------------------------------------------------------------ |
| Full Refresh Sync Mode    | ✅       | ✅        | ✅                                                            |
| Incremental Sync Mode     | ❌       | ❌        | ❌                                                            |
| CDC Sync Mode             | ✅       | ✅        | ✅                                                            |
| Full Parallel Processing  | ✅       | ✅        | ✅                                                            |
| CDC Parallel Processing   | ✅       | ❌        | ❌                                                            |
| Resumable Full Load       | ✅       | ✅        | ✅                                                            |
| CDC Heart Beat            | ❌       | ❌        | ❌                                                            |

We have additionally planned the following sources -  [AWS S3](https://github.com/datazip-inc/olake/issues/86) |  [Kafka](https://github.com/datazip-inc/olake/issues/87) 


## Writer Level Functionalities Supported

| Features/Functionality          | Local Filesystem [(docs)](https://olake.io/docs/writers/local) | AWS S3 [(docs)](https://olake.io/docs/writers/s3/overview) | Iceberg (WIP) |
| ------------------------------- | ---------------------- | --- | ------------- |
| Flattening & Normalization (L1) | ✅                      | ✅             |             |
| Partitioning                    | ✅                      | ✅             |             |
| Schema Changes                  | ✅                      | ✅             |             |
| Schema Evolution                | ✅                      | ✅             |             |


## Catalogue Support

| Catalogues                 | Support                                                                                                  |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| Glue Catalog               | [WIP](https://github.com/datazip-inc/olake/pull/113)                                                     |
| Hive Meta Store            | Upcoming                                                                                                 |
| JDBC Catalogue             | Upcoming                                                                                                 |
| REST Catalogue - Nessie    | Upcoming                                                                                                 |
| REST Catalogue - Polaris   | Upcoming                                                                                                 |
| REST Catalogue - Unity     | Upcoming                                                                                                 |
| REST Catalogue - Gravitino | Upcoming                                                                                                 |
| Azure Purview              | Not Planned, [submit a request](https://github.com/datazip-inc/olake/issues/new?template=new-feature.md) |
| BigLake Metastore          | Not Planned, [submit a request](https://github.com/datazip-inc/olake/issues/new?template=new-feature.md) |



See [Roadmap](https://github.com/orgs/datazip-inc/projects/5) for more details.


### Core

Core or framework is the component/logic that has been abstracted out from Connectors to follow DRY. This includes base CLI commands, State logic, Validation logic, Type detection for unstructured data, handling Config, State, Catalog, and Writer config file, logging etc.

Core includes http server that directly exposes live stats about running sync such as:
- Possible finish time
- Concurrently running processes
- Live record count

Core handles the commands to interact with a driver via these:
- `spec` command: Returns render-able JSON Schema that can be consumed by rjsf libraries in frontend
- `check` command: performs all necessary checks on the Config, Catalog, State and Writer config
- `discover` command: Returns all streams and their schema
- `sync` command: Extracts data out of Source and writes into destinations

Find more about how OLake works [here.](https://olake.io/docs/category/understanding-olake)

### SDKs

SDKs are libraries/packages that can orchestrate the connector in two environments i.e. Docker and Kubernetes. These SDKs can be directly consumed by users similar to PyAirbyte, DLT-hub.

(Unconfirmed) SDKs can interact with Connectors via potential GRPC server to override certain default behavior of the system by adding custom functions to enable features like Transformation, Custom Table Name via writer, or adding hooks.

### Olake

Olake will be built on top of SDK providing persistent storage and a user interface that enables orchestration directly from your machine with default writer mode as `S3 Iceberg Parquet`



## Contributing

We ❤️ contributions big or small. Please read [CONTRIBUTING.md](CONTRIBUTING.md) to get started with making contributions to OLake.

- To contribute to Frontend, go to [OLake Frontend GitHub repo](https://github.com/datazip-inc/olake-frontend/).

- To contribute to OLake website and documentation (olake.io), go to [OLake Frontend GitHub repo](https://github.com/datazip-inc/olake-docs).

Not sure how to get started? Just ping us on `#contributing-to-olake` in our [slack community](https://olake.io/slack)

## [Documentation](olake.io/docs)


If you need any clarification or find something missing, feel free to raise a GitHub issue with the label `documentation` at [olake-docs](https://github.com/datazip-inc/olake-docs/) repo or reach out to us at the community slack channel.




## Community

Join the [slack community](https://olake.io/slack) to know more about OLake, future roadmaps and community meetups, about Data Lakes and Lakehouses, the Data Engineering Ecosystem and to connect with other users and contributors.

Checkout [OLake Roadmap](https://olake.io/docs/roadmap) to track and influence the way we build it, your expert opinion is always welcomed for us to build a best class open source offering in Data space.

If you have any ideas, questions, or any feedback, please share on our [Github Discussions](https://github.com/datazip-inc/olake/discussions) or raise an issue.

As always, thanks to our amazing [contributors!](https://github.com/datazip-inc/olake/graphs/contributors)
