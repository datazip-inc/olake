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
  <a href="https://olake.io/blog"><b>Blogs</b></a>
</h3>


![undefined](https://github.com/user-attachments/assets/fe37e142-556a-48f0-a649-febc3dbd083c)

Connector ecosystem for Olake, the key points Olake Connectors focuses on are these
- **Integrated Writers to avoid block of reading, and pushing directly into destinations**
- **Connector Autonomy**
- **Avoid operations that don't contribute to increasing record throughput**

## Getting Started with OLake

### Source / Connectors
1. [Getting started Postgres -> Writers](https://olake.io/docs/getting-started/postgres) | [Postgres Docs](https://olake.io/docs/category/postgres)
2. [Getting started MongoDB -> Writers](https://olake.io/docs/getting-started/mongodb) | [MongoDB Docs](https://olake.io/docs/category/mongodb)
3. [Getting started MySQL -> Writers](https://olake.io/docs/getting-started/mysql)  | [MySQL Docs](https://olake.io/docs/category/mysql)

### Writers / Destination
1. [Apache Iceberg Docs](https://olake.io/docs/category/apache-iceberg) 
2. [AWS S3 Docs](https://olake.io/docs/category/aws-s3) 
3. [Local FileSystem Docs](https://olake.io/docs/writers/local) 


## Source Connector Level Functionalities Supported

| Connector Functionalities | MongoDB | Postgres | MySQL |
| ------------------------- | ------- | -------- | ----- |
| Full Refresh Sync Mode    | ✅       | ✅        | ✅     |
| Incremental Sync Mode     | ❌       | ❌        | ❌     |
| CDC Sync Mode             | ✅       | ✅        | ✅     |
| Full Parallel Processing  | ✅       | ✅        | ✅     |
| CDC Parallel Processing   | ✅       | ❌        | ❌     |
| Resumable Full Load       | ✅       | ✅        | ✅     |
| CDC Heart Beat            | ❌       | ❌        | ❌     |

We have additionally planned the following sources -  [AWS S3](https://github.com/datazip-inc/olake/issues/86) |  [Kafka](https://github.com/datazip-inc/olake/issues/87) 


## Writer Level Functionalities Supported

| Features/Functionality          | Local Filesystem | AWS S3 | Apache Iceberg |
| ------------------------------- | ---------------- | ------ | -------------- |
| Flattening & Normalization (L1) | ✅                | ✅      |                |
| Partitioning                    | ✅                | ✅      |                |
| Schema Changes                  | ✅                | ✅      |                |
| Schema Evolution                | ✅                | ✅      |                |

## Catalogue Support

| Catalogues                 | Support                                                                                                  |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| Glue Catalog               | WIP                                                                                                      |
| Hive Meta Store            | Upcoming                                                                                                 |
| JDBC Catalogue             | Upcoming                                                                                                 |
| REST Catalogue - Nessie    | Upcoming                                                                                                 |
| REST Catalogue - Polaris   | Upcoming                                                                                                 |
| REST Catalogue - Unity     | Upcoming                                                                                                 |
| REST Catalogue - Gravitino | Upcoming                                                                                                 |
| Azure Purview              | Not Planned, [submit a request](https://github.com/datazip-inc/olake/issues/new?template=new-feature.md) |
| BigLake Metastore          | Not Planned, [submit a request](https://github.com/datazip-inc/olake/issues/new?template=new-feature.md) |

See [GitHub Project Roadmap](https://github.com/orgs/datazip-inc/projects/5) and [OLake Upcoming Roadmap](https://olake.io/docs/roadmap) for more details.


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

- To contribute to OLake website and documentation (olake.io), go to [OLake Website GitHub repo](https://github.com/datazip-inc/olake-docs).

Not sure how to get started? Just ping us on `#contributing-to-olake` in our [slack community](https://olake.io/slack)

## Documentation

If you need any clarification or find something missing, feel free to raise a GitHub issue with the label `documentation` at [olake-docs](https://github.com/datazip-inc/olake-docs/) repo or reach out to us at the community slack channel.

## Community

Join the [slack community](https://olake.io/slack) to know more about OLake, future roadmaps and community meetups, about Data Lakes and Lakehouses, the Data Engineering Ecosystem and to connect with other users and contributors.

Checkout [OLake Roadmap](https://olake.io/docs/roadmap) to track and influence the way we build it, your expert opinion is always welcomed for us to build a best class open source offering in Data space.

If you have any ideas, questions, or any feedback, please share on our [Github Discussions](https://github.com/datazip-inc/olake/discussions) or raise an issue.

As always, thanks to our amazing [contributors!](https://github.com/datazip-inc/olake/graphs/contributors)
