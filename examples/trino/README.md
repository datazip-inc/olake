# Trino + OLake Integration Guide

This guide explains how to run Trino with Iceberg tables using Docker Compose.

## Prerequisites

- Docker and Docker Compose installed
- All configuration files in place (see file structure below)

## File Structure
```
trino/
├── docker-compose.yml
├── README.md
└── etc/
    ├── config.properties
    ├── node.properties
    ├── jvm.config
    ├── log.properties
    └── catalog/
        ├── iceberg.properties
        ├── jmx.properties
        └── tpch.properties
```

## Quick Start

1. **Start the stack:**
   ```bash
   docker-compose up -d
   ```

2. **Verify services are running:**
   ```bash
   docker-compose ps
   curl http://localhost:8080/v1/info
   ```

3. **Connect to Trino:**
   ```bash
   # Using Trino CLI
   docker exec -it trino-trino-coordinator-1 trino
   
   # Or use DBeaver/other SQL clients:
   # Host: localhost, Port: 8080, User: admin (no password)
   # DBeaver setup: https://dbeaver.com/docs/dbeaver/Database-driver-Trino/
   ```

## Example Queries

```sql
SHOW CATALOGS;
SHOW SCHEMAS IN tpch;
SELECT * FROM tpch.tiny.nation LIMIT 5;
```

## Service URLs

- **Trino**: http://localhost:8080
- **MinIO Console**: http://localhost:9001 (minio/minio_password)

## Troubleshooting

```bash
# Check logs if something fails
docker logs trino-trino-coordinator-1

# Restart everything
docker-compose down
docker-compose up -d
```
