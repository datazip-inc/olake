<h1 align="center" style="border-bottom: none">
    <a href="https://datazip.io/olake" target="_blank">
        <img alt="olake" src="https://github.com/user-attachments/assets/d204f25f-5289-423c-b3f2-44b2194bdeaf" width="100" height="100"/>
    </a>
    <br>OLake
</h1>

<p align="center">Fastest open-source tool for replicating Databases to Apache Iceberg or Data Lakehouse. ⚡ Efficient, quick and scalable data ingestion for real-time analytics. Starting with MongoDB. Visit <a href="https://olake.io/" target="_blank">olake.io/docs</a> for the full documentation, and benchmarks</p>

<p align="center">
    <a href="https://github.com/datazip-inc/olake/issues"><img alt="GitHub issues" src="https://img.shields.io/github/issues/datazip-inc/olake"/></a> <a href="https://olake.io/docs"><img alt="Documentation" height="22" src="https://img.shields.io/badge/view-Documentation-blue?style=for-the-badge"/></a>
    <a href="https://join.slack.com/t/getolake/shared_invite/zt-2utw44do6-g4XuKKeqBghBMy2~LcJ4ag"><img alt="slack" src="https://img.shields.io/badge/Join%20Our%20Community-Slack-blue"/></a>
</p>


![undefined](https://github.com/user-attachments/assets/fe37e142-556a-48f0-a649-febc3dbd083c)

Connector ecosystem for Olake, the key points Olake Connectors focuses on are these
- **Integrated Writers to avoid block of reading, and pushing directly into destinations**
- **Connector Autonomy**
- **Avoid operations that don't contribute to increasing record throughput**

## Performance Benchmarks*
1. **MongoDB Connector:** Syncs 35,694 records/sec; 230 million rows in 46 minutes for a 664 GB dataset (20× Airbyte, 15× Embedded Debezium, 6× Fivetran) -> ([See Detailed Benchmark](https://olake.io/docs/connectors/mongodb/benchmarks))  
2. **Postgres Connector:**  Syncs 1,000,000 records/sec for 50GB -> ([See Detailed Benchmark](https://olake.io/docs/connectors/postgres/benchmarks))  
3. **MySQL Connector:** Syncs 1,000,000 records/sec for 10GB; ~209 mins for 100+GB ->  ([See Detailed Benchmark](https://olake.io/docs/connectors/mysql/benchmarks))  

*These are preliminary performances, we'll published fully reproducible benchmark scores soon.

## Getting Started with OLake

### Source / Connectors
1. [Getting started Postgres -> Writers](https://github.com/datazip-inc/olake/tree/master/drivers/postgres) | [Postgres Docs](https://olake.io/docs/category/postgres)
2. [Getting started MongoDB -> Writers](https://github.com/datazip-inc/olake/tree/master/drivers/mongodb) | [MongoDB Docs](https://olake.io/docs/category/mongodb)
3. [Getting started MySQL -> Writers](https://github.com/datazip-inc/olake/tree/master/drivers/mysql)  | [MySQL Docs](https://olake.io/docs/category/mysql)

### Writers / Destination
1. [Apache Iceberg Docs](https://olake.io/docs/category/apache-iceberg) 
2. [AWS S3 Docs](https://olake.io/docs/category/aws-s3) 
3. [Local FileSystem Docs](https://olake.io/docs/writers/local) 


## Source/Connector Functionalities
|  Functionality | MongoDB | Postgres | MySQL |
| ------------------------- | ------- | -------- | ----- |
| Full Refresh Sync Mode    | ✅       | ✅        | ✅     |
| Incremental Sync Mode     | ❌       | ❌        | ❌     |
| CDC Sync Mode             | ✅       | ✅        | ✅     |
| Full Parallel Processing  | ✅       | ✅        | ✅     |
| CDC Parallel Processing   | ✅       | ❌        | ❌     |
| Resumable Full Load       | ✅       | ✅        | ✅     |
| CDC Heart Beat            | ❌       | ❌        | ❌     |

We have additionally planned the following sources -  [AWS S3](https://github.com/datazip-inc/olake/issues/86) |  [Kafka](https://github.com/datazip-inc/olake/issues/87) 


## Writer Functionalities
| Functionality          | Local Filesystem | AWS S3 | Apache Iceberg |
| ------------------------------- | ---------------- | ------ | -------------- |
| Flattening & Normalization (L1) | ✅                | ✅      |  ✅              |
| Partitioning                    | ✅                | ✅      |                |
| Schema Changes                  | ✅                | ✅      |                |
| Schema Evolution                | ✅                | ✅      |                |

## Supported Catalogs For Iceberg Writer
| Catalog                 | Status                                                                                                  |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| Glue Catalog               | Supported                                                                                                      |
| Hive Meta Store            | Upcoming                                                                                                 |
| JDBC Catalogue             | Supported                                                                                                 |
| REST Catalogue             | Supported                                                                                                 |
| Azure Purview              | Not Planned, [submit a request](https://github.com/datazip-inc/olake/issues/new?template=new-feature.md) |
| BigLake Metastore          | Not Planned, [submit a request](https://github.com/datazip-inc/olake/issues/new?template=new-feature.md) |

## Core
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

## Roadmap
Checkout [GitHub Project Roadmap](https://github.com/orgs/datazip-inc/projects/5) and [Upcoming OLake Roadmap](https://olake.io/docs/roadmap) to track and influence the way we build it. 
If you have any ideas, questions, or any feedback, please share on our [Github Discussions](https://github.com/datazip-inc/olake/discussions) or raise an issue.

## Contributing
We ❤️ contributions big or small check our [Bounty Program](https://olake.io/docs/community/issues-and-prs#goodies). As always, thanks to our amazing [contributors!](https://github.com/datazip-inc/olake/graphs/contributors).
- To contribute to Olake Check [CONTRIBUTING.md](CONTRIBUTING.md)
- To contribute to UI, visit [OLake UI Repository](https://github.com/datazip-inc/olake-frontend/).
- To contribute to OLake website and documentation (olake.io), visit [Olake Docs Repository][GITHUB_DOCS].

<!----variables---->
[GITHUB_DOCS]: https://github.com/datazip-inc/olake-docs/
