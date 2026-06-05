
<h1 align="center" style="border-bottom: none">
    <a href="https://datazip.io/olake" target="_blank">
        <img alt="olake" src="https://github.com/user-attachments/assets/d204f25f-5289-423c-b3f2-44b2194bdeaf" width="100" height="100"/>
    </a>
    <br>OLake Go
</h1>

<p align="center">
  <strong>OLake Go</strong> is a high-performance, open-source data ingestion engine for replicating databases, S3, and Kafka into
  <strong>Apache Iceberg</strong> (or plain Parquet).
  <br/>
  Built for scalable, real-time pipelines, OLake provides a simple web UI and CLI - used to ingest into vendor-lock-in free Iceberg tables supporting all the query-engines/warehouses.
  <br/><br/>
  Read the docs and benchmarks at
  <a href="https://olake.io/docs" target="_blank">olake.io/docs</a>.
  Join our active community on
  <a href="https://olake.io/slack/" target="_blank">Slack</a>.
</p>



<p align="center">
    <a href="https://github.com/datazip-inc/olake/issues">
    <img alt="GitHub issues" src="https://img.shields.io/github/issues/datazip-inc/olake"/>
    </a> 
    <a href="https://olake.io/docs">
    <img alt="Documentation" src="https://img.shields.io/badge/view-Documentation-white"/>
    </a>
    <a href="https://olake.io/slack/">
    <img alt="slack" src="https://img.shields.io/badge/Join%20Our%20Community-Slack-blue"/>
    </a>
    <a href="https://olake.io/docs/community/contributing/">
        <img alt="Contribute to OLake" src="https://img.shields.io/badge/Contribute-OLake-2563eb"/>
    </a>
</p>

 > 🎉 **OLake Fusion is now live!** — Automate your Apache Iceberg Table Maintenance. Check it out here → [github.com/datazip-inc/olake-fusion](https://github.com/datazip-inc/olake-fusion) 🎉

## OLake Go — Super-fast Sync to Apache Iceberg

**OLake Go** supports replication from **transactional databases** such as **PostgreSQL, MySQL, MongoDB, Oracle, DB2, and MSSQL**, **event-streaming systems like Apache Kafka** and **Object-store like S3**, into open data lakehouse formats such as **Apache Iceberg** or **Plain Parquet** — delivering blazing-fast performance with minimal infrastructure cost.

<h1 align="center" style="border-bottom: none">
    <a href="https://datazip.io/olake" target="_blank">
        <img width="1440" height="720" alt="pic" src="https://github.com/user-attachments/assets/4b17c1f7-9a24-46d4-894e-bf8c5e71b64c" />
    </a>
</h1>


---

### 🚀 Why OLake Go?

- 🧠 **Smart sync**: Full + CDC replication with automatic schema discovery & schema evolution 
- ⚡ **High throughput**: 580K RPS (Postgres) & 338K RPS (MySQL)
- ➡️ **Exactly once delivery & Arrow writes**: Accuracy with speed.
- 💾 **Iceberg-native**: Supports Glue, Hive, JDBC, REST catalogs  
- 🖥️ **Self-serve UI**: Deploy via Docker Compose and sync in minutes  
- 💸 **Infra-light**: No Spark, no Flink, no Kafka, no Debezium

---

### 📊 Benchmarks

#### Full Load

| Source → Destination | Full Load       | Relative Performance (Full Load)    | Full Report                                                  |
|----------------------|-----------------|--------------------------------------|--------------------------------------------------------------|
| Postgres → Iceberg   | 5,80,113 RPS    | 12.5× faster than Fivetran            | [Full Report](https://olake.io/docs/benchmarks?tab=postgres) |
| MySQL → Iceberg      | 1,39,773 RPS    | 1.91× faster than Fivetran           | [Full Report](https://olake.io/docs/benchmarks/?tab=mysql)   |
| MongoDB → Iceberg    | 37,879 RPS              | -                                    | [Full Report](https://olake.io/docs/benchmarks/?tab=mongodb) |
| Oracle → Iceberg     | 5,26,337 RPS  | -                                    | [Full Report](https://olake.io/docs/benchmarks/?tab=oracle)  |
| Kafka → Iceberg      | 2,09,065 MPS (Bounded Incremental) | 1.23x slower than Flink                                    | [Full Report](https://olake.io/docs/benchmarks/?tab=kafka)   |

#### CDC

| Source → Destination | CDC             | Relative Performance (CDC)          | Full Report                                                  |
|----------------------|-----------------|--------------------------------------|--------------------------------------------------------------|
| Postgres → Iceberg   | 55,555 RPS      | 2× faster than Fivetran              | [Full Report](https://olake.io/docs/benchmarks?tab=postgres) |
| MySQL → Iceberg      | 59,951 RPS      | 1.52× faster than Fivetran           | [Full Report](https://olake.io/docs/benchmarks/?tab=mysql)   |
| MongoDB → Iceberg    | 10,692 RPS      | -                                    | [Full Report](https://olake.io/docs/benchmarks/?tab=mongodb) |
| Oracle → Iceberg     | -               | -                                    | [Full Report](https://olake.io/docs/benchmarks/?tab=oracle)  |

---

### 🔧 Supported Sources and Destinations


#### Sources (Databases and S3)


| Source        | Full Load    |  CDC          | Incremental       | Notes                       | Documentation               |
|---------------|--------------|---------------|-------------------|-----------------------------|-----------------------------|
| PostgreSQL    | ✅           | ✅ `pgoutput` | ✅                 |`wal2json` deprecated       |[Postgres Docs](https://olake.io/docs/connectors/postgres/overview) |
| MySQL         | ✅           | ✅            | ✅                | Binlog-based CDC            | [MySQL Docs](https://olake.io/docs/connectors/mysql/overview) |
| MongoDB       | ✅           | ✅            | ✅                | Oplog-based CDC             |[MongoDB Docs](https://olake.io/docs/connectors/mongodb/overview) |
| Oracle        | ✅           | WIP  | ✅                |  JDBC based Full Load & Incremental                |  [Oracle Docs](https://olake.io/docs/connectors/oracle/overview) |
| DB2          | ✅           | -    | ✅                | JDBC based Full Load & Incremental                 | [DB2 Docs](https://olake.io/docs/connectors/db2/) |
| MSSQL        | ✅           | ✅   | ✅                | Full Load, CDC & Incremental                        | [MSSQL Docs](https://olake.io/docs/connectors/mssql/) |
| S3           | ✅           | -    | ✅                | Ingests from Amazon S3 or S3-compatible (MinIO, LocalStack) | [S3 Docs](https://olake.io/docs/connectors/s3/) |

#### Sources (Kafka)


| Source | Bounded Incremental | Notes                             | Documentation |
|--------|--------------------|-----------------------------------|---------------|
| Kafka  | ✅                 | Latest offset bounded incremental sync | [Kafka Docs](https://olake.io/docs/connectors/kafka) |


#### Destinations


| Destination    | Format    | Supported Catalogs                                            |
|----------------|-----------|---------------------------------------------------------------|
| Iceberg        | ✅         | Glue, Hive, JDBC, REST (Nessie, Polaris, Unity, Lakekeeper, AWS S3 tables)  |
| Parquet        | ✅         | Filesystem                                                   |


##### Writer Docs

1. Apache Iceberg Docs
    1. Catalogs
       1. [AWS Glue Catalog](https://olake.io/docs/writers/iceberg/catalog/glue)
       2. [REST Catalog](https://olake.io/docs/writers/iceberg/catalog/rest)
          - Generic
          - Lakekeeper
          - Nessie
          - S3 Tables
          - Unity
          - Apache Polaris
       3. [JDBC Catalog](https://olake.io/docs/writers/iceberg/catalog/jdbc)
       4. [Hive Catalog](https://olake.io/docs/writers/iceberg/catalog/hive)
    2. [Azure ADLS Gen2](https://olake.io/docs/writers/iceberg/azure)
    3. [Google Cloud Storage (GCS)](https://olake.io/docs/writers/iceberg/gcp/)
    4. [MinIO (local)](https://olake.io/docs/writers/iceberg/troubleshooting-local/?view=local#local-testing)

2. Parquet Writer
   1. [AWS S3 Docs](https://olake.io/docs/writers/parquet/config/)
   2. [Google Cloud Storage (GCS)](https://olake.io/docs/writers/parquet/config/#using-gcs-compatible-s3-credentials)
   3. Local FileSystem Docs

---

### 🧪 Quickstart (UI + Docker)

OLake UI is a web-based interface for managing OLake Go jobs, sources, destinations and configurations. You can run the entire OLake Go stack (UI, Backend, and all dependencies) using Docker Compose. This is the recommended way to get started.
Run the UI, connect your source DB, and start syncing in minutes. 

```sh
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose.yml | docker compose -f - up -d
```

**Access the UI:**
- **OLake UI:** [http://localhost:8000](http://localhost:8000)
- Log in with default credentials: `admin` / `password`

Detailed getting started using OLake UI can be found [here](https://olake.io/docs/getting-started/quickstart/).

![olake-ui](https://github.com/user-attachments/assets/691eb8fb-3c81-4ae1-83ee-e4854a98c15a)

#### Creating Your First Job

With the UI running, you can create a data pipeline in a few steps:

1. **Configure Source:** Navigate to **Source** tab and click **Create Source**. Set up your source connection (e.g., PostgreSQL, MySQL, MongoDB).
2. **Configure Destination:** Navigate to **Destination** tab and click **Create Destination**. Set up your destination (e.g., Apache Iceberg with a Glue, REST, Hive, or JDBC catalog).
3. **Create a Job:** Navigate to the **Jobs** tab and click **Create Job**. 
4. **Configure & Run:** Give your job a name, set a schedule, select your source and destination and click **Next** to finish.
5. **Select Streams:** Choose which tables to sync and configure their sync mode (`CDC` or `Full Refresh`).


For a detailed walkthrough, refer to the [Jobs documentation](https://olake.io/docs/getting-started/creating-first-pipeline/).

---

### 🛠️ CLI Usage (Advanced)

For advanced users and automation, OLake Go's core logic is exposed via a powerful CLI. The core framework handles state management, configuration validation, logging, and type detection. It interacts with drivers using four main commands:

* `spec`: Returns a render-able JSON Schema for a connector's configuration.
* `check`: Validates connection configurations for sources and destinations.
* `discover`: Returns all available streams (e.g., tables) and their schemas from a source.
* `sync`: Executes the data replication job, extracting from the source and writing to the destination.

**Find out more about CLI [here](https://olake.io/docs/install/docker-cli/).**

---

#### Install OLake Go

Below are other different ways you can run OLake Go:

1. [OLake Go UI (Recommended)](https://olake.io/docs/getting-started/quickstart/)
2. [Kubernetes using Helm](https://olake.io/docs/install/kubernetes)
3. [Standalone Docker container](https://olake.io/docs/install/docker-cli)
4. [Airflow on EC2](https://olake.io/blog/olake-airflow-on-ec2/)
5. [Airflow on Kubernetes](https://olake.io/blog/olake-airflow) 

---

### Playground

1. [OLake Go + Apache Iceberg + REST Catalog + Presto](https://olake.io/docs/playground/olake-iceberg-presto)
2. [OLake Go + Apache Iceberg + AWS Glue + Trino](https://olake.io/iceberg/olake-iceberg-trino)
3. [OLake Go + Apache Iceberg + AWS Glue + Athena](https://olake.io/iceberg/olake-iceberg-athena)
4. [OLake Go + Apache Iceberg + AWS Glue + Snowflake](https://olake.io/iceberg/olake-glue-snowflake)
5. [OLake Go + Apache Iceberg + REST Catalog + Spark](https://olake.io/docs/getting-started/playground/)


---

### 🌍 Use Cases

- ✅ Migrate from OLTP to Iceberg without Spark or Flink
- ✅ Enable BI over fresh CDC data using Athena, StarRocks, Trino, Presto, Dremio, Databricks, Snowflake and more!
- ✅ Build near real-time data lake-house on cost-efficient cloud object stores
- ✅ Move away from vendor-lock-in warehouse or tools with open data lake-house
- ✅ Single copy for both analytics & machine learning
  
---

### 🧭 Roadmap Highlights

- [x] Oracle Full Load Support
- [x] Oracle Incremental
- [x] Filters for Full Load and Incremental
- [ ] Iceberg V3 Support

📌 Check out our [GitHub Project Roadmap](https://github.com/orgs/datazip-inc/projects/5) and the [Upcoming OLake Roadmap](https://github.com/datazip-inc/olake#-roadmap-highlights) to track what's next. If you have ideas or feedback, please share them in our [GitHub Discussions](https://github.com/datazip-inc/olake/discussions) or by opening an issue.

---

### 🤝 Contributing

We ❤️ contributions, big or small!

Check out our [Bounty Program](https://olake.io/docs/community/issues-and-prs#goodies). A huge thanks to all our amazing [contributors!](https://github.com/datazip-inc/olake/graphs/contributors)

* To contribute to the **OLake Go**, see [CONTRIBUTING.md](https://github.com/datazip-inc/olake/blob/master/CONTRIBUTING.md).
* To contribute to the **UI**, visit the [OLake UI Repository](https://github.com/datazip-inc/olake-ui).
* To contribute to the **OLake Helm**, visit the [OLake Helm Repository](https://github.com/datazip-inc/olake-helm).
* To contribute to our **website and documentation**, visit the [Olake Docs Repository](https://github.com/datazip-inc/olake-docs/).
