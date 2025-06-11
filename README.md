<h1 align="center" style="border-bottom: none">
    <a href="https://datazip.io/olake" target="_blank">
        <img alt="olake" src="https://github.com/user-attachments/assets/d204f25f-5289-423c-b3f2-44b2194bdeaf" width="100" height="100"/>
    </a>
    <br>OLake
</h1>

<p align="center">The fastest open-source tool for replicating databases to Apache Iceberg. OLake, an easy-to-use web interface and a CLI for efficient, scalable, & real-time data ingestion. Visit <a href="https://olake.io/" target="_blank">olake.io/docs</a> for the full documentation, and benchmarks</p>

<p align="center">
    <a href="https://github.com/datazip-inc/olake/issues"><img alt="GitHub issues" src="https://img.shields.io/github/issues/datazip-inc/olake"/></a> <a href="https://olake.io/docs"><img alt="Documentation" height="22" src="https://img.shields.io/badge/view-Documentation-blue?style=for-the-badge"/></a>
    <a href="https://join.slack.com/t/getolake/shared_invite/zt-2utw44do6-g4XuKKeqBghBMy2~LcJ4ag"><img alt="slack" src="https://img.shields.io/badge/Join%20Our%20Community-Slack-blue"/></a>
</p>

## 🚀 Getting Started with OLake UI (Recommended)

OLake UI is a web-based interface for managing OLake jobs, sources, destinations, and configurations. You can run the entire OLake stack (UI, Backend, and all dependencies) using Docker Compose. This is the recommended way to get started.

### Quick Start

1. **Clone the OLake UI repository:**

    ```bash
    git clone https://github.com/datazip-inc/olake-ui.git
    cd olake-ui
    ```

2. **Edit persistence/config paths (required):**

   - The docker-compose.yml uses `/your/chosen/host/path/olake-data` as a placeholder for the host directory where OLake's persistent data and configuration will be stored. You **must** replace this with an actual path on your system before starting the services. You can change this by editing the `x-app-defaults` section at the top of `docker-compose.yml`:

   ```yaml
   x-app-defaults:
   host_persistence_path: &hostPersistencePath /your/host/path
   ```

   - Make sure the directory exists and is writable by the user running Docker (see how to change [file permissions for Linux/macOS](https://wiki.archlinux.org/title/File_permissions_and_attributes#Changing_permissions)).

3. **Start all services:**

    ```bash
    docker compose up -d
    ```

4. **Access the UI:**

      * **OLake UI:** [http://localhost:8000](http://localhost:8000)
      * Log in with default credentials: `admin` / `password`.

Detailed getting started using OLake UI can be found [here](https://olake.io/docs/getting-started/olake-ui).

![olake-ui](https://github.com/user-attachments/assets/6081e9ad-7aef-465f-bde1-5b41b19ec6cd)

### Creating Your First Job

With the UI running, you can create a data pipeline in a few steps:

1. **Create a Job:** Navigate to the **Jobs** tab and click **Create Job**.
2. **Configure Source:** Set up your source connection (e.g., PostgreSQL, MySQL, MongoDB).
3. **Configure Destination:** Set up your destination (e.g., Apache Iceberg with a Glue, REST, Hive, or JDBC catalog).
4. **Select Streams:** Choose which tables to sync and configure their sync mode (`CDC` or `Full Refresh`).
5. **Configure & Run:** Give your job a name, set a schedule, and click **Create Job** to finish.

For a detailed walkthrough, refer to the [Create a Job documentation](https://olake.io/docs/jobs/create-jobs).

## Performance Benchmarks*

OLake is engineered for high-throughput data replication.

1. **Postgres Connector to Apache Iceberg:** ([See Detailed Benchmark](https://olake.io/docs/connectors/postgres/benchmarks))

      * **Full load:** Syncs at **46,262 RPS** for 4 billion rows. (101x Airbyte, 11.6x Estuary, 3.1x Debezium)
      * **CDC:** Syncs at **36,982 RPS** for 50 million changes. (63x Airbyte, 12x Estuary, 2.7x Debezium)

2. **MongoDB Connector to Apache Iceberg:** ([See Detailed Benchmark](https://olake.io/docs/connectors/mongodb/benchmarks))

      * Syncs **35,694 records/sec**, replicating a 664 GB dataset (230 million rows) in 46 minutes. (20× Airbyte, 15× Debezium, 6× Fivetran)

**These are preliminary results. Fully reproducible benchmark scores will be published soon.*

## Getting Started with OLake

### Install OLake

Below are different ways you can run OLake:

1. [OLake UI (Recommended)](https://olake.io/docs/getting-started/olake-ui)
2. [Stand-alone Docker container](https://olake.io/docs/install/docker)
3. [Airflow on EC2](https://olake.io/blog/olake-airflow-on-ec2?utm_source=chatgpt.com)
4. [Airflow on Kubernetes](https://olake.io/blog/olake-airflow)

### Source / Connectors

1. [Getting started Postgres -> Writers](https://github.com/datazip-inc/olake/tree/master/drivers/postgres) | [Postgres Docs](https://olake.io/docs/connectors/postgres/overview)
2. [Getting started MongoDB -> Writers](https://github.com/datazip-inc/olake/tree/master/drivers/mongodb) | [MongoDB Docs](https://olake.io/docs/connectors/mongodb/overview)
3. [Getting started MySQL -> Writers](https://github.com/datazip-inc/olake/tree/master/drivers/mysql)  | [MySQL Docs](https://olake.io/docs/connectors/mysql/overview)

### Writers / Destination

1. [Apache Iceberg Docs](https://olake.io/docs/writers/iceberg/overview) 
   1. [AWS Glue Catalog](https://olake.io/docs/writers/iceberg/catalog/glue)
   2. [REST Catalog](https://olake.io/docs/writers/iceberg/catalog/rest)
      1. S3 Tables Supported
   3. [JDBC Catalog](https://olake.io/docs/writers/iceberg/catalog/jdbc)
   4. [Hive Catalog](https://olake.io/docs/writers/iceberg/catalog/hive)
2. [AWS S3 Docs](https://olake.io/docs/writers/s3/overview) 
3. [Azure ADLS Gen2](https://olake.io/docs/writers/azure-adls/overview)
4. [Google Cloud Storage (GCS)](https://olake.io/docs/writers/gcs/overview)
5. [MinIO](https://olake.io/docs/writers/iceberg/docker-compose#local-catalog-test-setup)
6. [Local FileSystem Docs](https://olake.io/docs/writers/local)

### Source Connectors

| Functionality                 | MongoDB | Postgres | MySQL |
| :---------------------------- | :-----: | :------: | :---: |
| Full Refresh Sync             |    ✅    |    ✅     |   ✅   |
| Incremental Sync              |    WIP    |    WIP     |   WIP   |
| CDC Sync                      |    ✅    |    ✅     |   ✅   |
| Full Load Parallel Processing |    ✅    |    ✅     |   ✅   |
| CDC Parallel Processing       |    ✅    |    ❌     |   ❌   |
| Resumable Full Load           |    ✅    |    ✅     |   ✅   |
| CDC Heartbeat (Planned)                 |    -    |    -     |   -   |

### Destination Writers

| Functionality              | Local Filesystem | AWS S3 | Apache Iceberg |
| :------------------------- | :--------------: | :----: | :------------: |
| Flattening & Normalization |        ✅         |   ✅    |       ✅        |
| Partitioning               |        ✅         |   ✅    |       ✅        |
| Schema Data Type Changes   |        ✅         |   ✅    |      WIP        |
| Schema Evolution           |        ✅         |   ✅    |       ✅        |

### Supported Catalogs For Iceberg Writer

| Catalog               | Status                                                                                                   |
| :-------------------- | :------------------------------------------------------------------------------------------------------- |
| **Glue Catalog**      | Supported                                                                                                |
| **Hive Metastore**    | Supported                                                                                                |
| **JDBC Catalog**      | Supported                                                                                                |
| **REST Catalog**      | Supported                                                                                                |
| **Azure Purview**     | Not Planned, [submit a request](https://github.com/datazip-inc/olake/issues/new?template=new-feature.md) |
| **BigLake Metastore** | Not Planned, [submit a request](https://github.com/datazip-inc/olake/issues/new?template=new-feature.md) |

## ⚙️ Core Framework & CLI

For advanced users and automation, OLake's core logic is exposed via a powerful CLI. The core framework handles state management, configuration validation, logging, and type detection. It interacts with drivers using four main commands:

* `spec`: Returns a render-able JSON Schema for a connector's configuration.
* `check`: Validates connection configurations for sources and destinations.
* `discover`: Returns all available streams (e.g., tables) and their schemas from a source.
* `sync`: Executes the data replication job, extracting from the source and writing to the destination.

**Find out more about how OLake works [here](https://olake.io/docs).**

## Playground

1. [OLake + Apache Iceberg + REST Catalog + Presto](https://olake.io/docs/playground/olake-iceberg-presto)
2. [OLake + Apache Iceberg + AWS Glue + Trino](https://olake.io/iceberg/olake-iceberg-trino)
3. [OLake + Apache Iceberg + AWS Glue + Athena](https://olake.io/iceberg/olake-iceberg-athena)
4. [OLake + Apache Iceberg + AWS Glue + Snowflake](https://olake.io/iceberg/olake-glue-snowflake)


## 🗺️ Roadmap

Check out our [GitHub Project Roadmap](https://github.com/orgs/datazip-inc/projects/5) and the [Upcoming OLake Roadmap](https://olake.io/docs/roadmap) to track what's next. If you have ideas or feedback, please share them in our [GitHub Discussions](https://github.com/datazip-inc/olake/discussions) or by opening an issue.

## ❤️ Contributing

We ❤️ contributions, big or small! Check out our [Bounty Program](https://olake.io/docs/community/issues-and-prs#goodies). A huge thanks to all our amazing [contributors!](https://github.com/datazip-inc/olake/graphs/contributors)

* To contribute to the **OLake core**, see [CONTRIBUTING.md](https://www.google.com/search?q=CONTRIBUTING.md).
* To contribute to the **UI**, visit the [OLake UI Repository](https://github.com/datazip-inc/olake-ui).
* To contribute to our **website and documentation**, visit the [Olake Docs Repository](https://github.com/datazip-inc/olake-docs/).

