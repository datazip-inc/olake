# Trino + OLake Integration Guide

This guide explains how to run Trino with the OLake Docker stack to query Iceberg tables.

## 1. Prerequisites
Ensure the following before you begin:

- The OLake Docker stack is running.
- The `iceberg-rest` and `minio` services are healthy.
- Iceberg tables exist with sample data.

## 2. Start the Trino container
From the OLake project root (`olake/`), start Trino with:

```bash
docker run -d \
  --name olake-trino-coordinator \
  --network app-network \
  -p 8888:8080 \
  -v "$(pwd)/examples/trino/etc:/opt/trino-server/etc" \
  trinodb/trino:latest

  # Verify startup (wait 30 seconds)
curl -I http://localhost:8888/v1/info
```

## 3. Run Queries using a Client

Connect to the Trino server at [http://localhost:8888](http://localhost:8888)(user: admin, no password)

Use the Trino CLI for a quick start, or DBeaver for a richer user interface.

- For DBeaver: Follow the [DBeaver Official Guide](https://dbeaver.com/docs/dbeaver/Database-driver-Trino/) to connect to localhost:8888.  

- For Trino CLI: Run `docker exec -it olake-trino-coordinator trino` to start querying.


#### Example Queries

```
SHOW CATALOGS;
SHOW SCHEMAS IN iceberg;
SELECT * FROM iceberg.weather.weather LIMIT 10;
```

## 5. Troubleshooting
Check Trino logs:
```bash
# Check logs
docker logs -f olake-trino-coordinator

# Verify network
docker network inspect app-network

# Restart if needed
docker restart olake-trino-coordinator
```