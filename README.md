<h1 align="center" style="border-bottom: none"> <a href="https://datazip.io/olake" target="_blank"> <img alt="OLake" src="https://github.com/user-attachments/assets/d204f25f-5289-423c-b3f2-44b2194bdeaf" width="100" height="100"/> </a> <br>OLake </h1><p align="center"> Fastest open-source database replication tool for Apache Iceberg & Data Lakehouses<br> ⚡ Efficient, scalable data ingestion for real-time analytics. MongoDB-first implementation. </p><p align="center"> <a href="https://olake.io/docs"><img alt="Documentation" src="https://img.shields.io/badge/docs-olake.io-blue?style=flat-square"></a> <a href="https://github.com/datazip-inc/olake/issues"><img alt="GitHub Issues" src="https://img.shields.io/github/issues/datazip-inc/olake?style=flat-square"></a> <a href="https://join.slack.com/t/getolake/shared_invite/zt-2utw44do6-g4XuKKeqBghBMy2~LcJ4ag"><img alt="Slack Community" src="https://img.shields.io/badge/community-slack-blue?style=flat-square"></a> </p>
OLake Architecture

Key Features
Integrated Writers: Direct data pipeline from source to destination

Massive Parallel Processing: 10-100x faster than traditional ETL

Schema Evolution: Automatic handling of schema changes

Resumable Syncs: Continue from last successful state

Getting Started
Connectors
Source	Documentation	Status
MongoDB	MongoDB Guide	Production Ready
PostgreSQL	Postgres Guide	Beta
MySQL	MySQL Guide	Beta
Destinations
Target	Documentation	Status
Apache Iceberg	Iceberg Guide	Production Ready
AWS S3	S3 Guide	Production Ready
Local Filesystem	Local FS Guide	Production Ready
Feature Matrix
Source Capabilities
Feature	MongoDB	PostgreSQL	MySQL
Full Refresh Sync	✔️	✔️	✔️
Change Data Capture	✔️	◻️	◻️
Parallel Processing	✔️	✔️	✔️
Resumable Loads	✔️	✔️	✔️
Writer Capabilities
Feature	Iceberg	AWS S3	Local FS
Schema Evolution	✔️	✔️	✔️
Time Travel	✔️	◻️	◻️
Partition Management	✔️	✔️	✔️
ACID Compliance	✔️	◻️	◻️
Iceberg Catalog Support
Catalog Type	Status
AWS Glue	Production
Hive Metastore	Beta
JDBC	Development
REST (Nessie/Polaris)	Planned
Core Architecture
mermaid
Copy
graph TD
    A[Source Connector] --> B{OLake Core}
    B --> C[Stream Processor]
    C --> D[Schema Manager]
    D --> E[Parallel Writer]
    E --> F[(Destination)]
    
    B --> G[State Manager]
    G --> H[Checkpoint Service]
    B --> I[Monitoring API]
Roadmap
Q3 2024: Kafka Source Connector

Q4 2024: Unity Catalog Support

Q1 2025: Snowflake Destination

Contribution
We welcome contributions through:

GitHub Issues for bug reports

Pull Requests for code changes

Documentation improvements
