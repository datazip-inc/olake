# Olake Examples

This directory contains self-contained, end-to-end demo stacks for Olake. Each example is a complete combination of source database, storage, catalog, and query engine that runs alongside the [base OLake stack](https://raw.githubusercontent.com/datazip-inc/olake-ui/refs/heads/master/docker-compose.yml).

## How it works

- First run the base Olake stack (OLake UI, Temporal worker, Temporal service, and dependencies).
- Then start one example from this directory.

## Available examples

- `presto-tabularest-minio-mysql/`  
  - MySQL → Olake → Iceberg (Tabular REST) on MinIO → Presto

## Quick start pattern

```bash
# 1) Start base Olake stack
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose.yml | docker compose -f - up -d

# 2) Start an example
cd examples/presto-tabularest-minio-mysql
docker compose up -d

# 3) Follow suggested steps in README.md for the example
```

Each example’s `README.md` includes:
- Required ports and endpoints
- Service access URLs
- Pipeline setup steps in Olake
- Sample queries and troubleshooting

## Contributing a new example

- Naming: `(<query-engine>)-(<catalog>)-(<storage>)-(<source>)`
  - Example: `trino-lakekeeperest-minio-postgresql`
- Include:
  - `docker-compose.yml` using the external network `olake-network`
  - `README.md` with:
    - Prerequisite base-stack command
    - Port availability section (list host ports)
    - Step-by-step pipeline setup in Olake
    - Access URLs and sample queries
- Prefer minimal images and clear, reproducible dataset bootstrapping.
- Test the full flow end-to-end before submitting a PR.
