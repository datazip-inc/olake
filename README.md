# Olake

![undefined](https://github.com/user-attachments/assets/fe37e142-556a-48f0-a649-febc3dbd083c)

Connector ecosystem for Olake, the key points Olake Connectors focuses on are these
- **Integrated Writers to avoid block of reading, and pushing directly into destinations**
- **Connector Autonomy**
- **Avoid operations that don't contribute to increasing record throughput**


## Olake Framework Structure
![diagram](/.github/assets/Olake.jpg)

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
- [x] MongoDB
- [ ] Kafka
- [ ] Postgres
- [ ] DynamoDB

### Writers

Writers are directly integrated into drivers to avoid blockage of writing/reading into/from os.StdOut or any other type of I/O. This enables direct insertion of records from each individual fired query to the destination.

Writers are being planned in this order
- [ ] Local Parquet
- [ ] S3 Simple Parquet
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
