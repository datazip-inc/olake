# OLake Demo: Trino + Tabular REST + MinIO + MySQL

This example demonstrates an end-to-end data lakehouse pipeline:
- **MySQL** → **OLake** → **Iceberg (Tabular REST) on MinIO** → **Trino**

## Prerequisites

* [Docker](https://docs.docker.com/get-docker/) installed and running (Docker Desktop recommended for Mac/Windows)
* [Docker Compose](https://docs.docker.com/compose/) (comes with Docker Desktop)
* **Port Availability:** The following ports must be available on your system:
   - **8000** - OLake UI
   - **8088** - Trino query engine UI
   - **3306** - MySQL database
   - **8181** - Iceberg REST catalog API  
   - **8443** - MinIO console UI
   - **9090** - MinIO server API

**Note:** If any of these ports are in use, stop the conflicting services or modify the port mappings in the docker-compose.yml file.

## Quick Start

### 1. Start the Base OLake Stack

```bash
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose.yml | docker compose -f - up -d
```

### 2. Start the Demo Stack

```bash
# Navigate to this example directory
cd examples/trino-tablurarest-minio-mysql

# Start all services
docker compose up -d
```

### 3. Accessing Services

1.  **Log in** to the OLake UI at [http://localhost:8000](http://localhost:8000) with credentials `admin`/`password`.

2. **Verify Source Data:**
      - Access the MySQL CLI:
        ```bash
        docker exec -it primary_mysql mysql -u root -ppassword
        ```
      - Select the `weather` database and query the table:
        ```sql
        USE weather;
        SELECT * FROM weather LIMIT 10;
        ```
        This will display the first 10 rows of the `weather` table.

3.  **Create and Configure a Job:**
    Create a Job to define and run the data pipeline:
    * On the main page, click on the **"Create your first Job"** button.

    * **Set up the Source:**
        * **Connector:** `MySQL`
        * **Version:** chose the latest available version
        * **Name of your source:** `olake_mysql`
        * **Host:** `host.docker.internal`
        * **Port:** `3306`
        * **Database:** `weather`
        * **Username:** `root`
        * **Password:** `password`

    * **Set up the Destination:**
        * **Connector:** `Apache Iceberg`
        * **Catalog:** `REST catalog`
        * **Name of your destination:** `olake_iceberg`
        * **Version:** chose the latest available version
        * **Iceberg REST Catalog URI:** `http://host.docker.internal:8181`
        * **Iceberg S3 Path:** `s3://warehouse/weather/`
        * **Iceberg Database:** `weather`
        * **S3 Endpoint (for Iceberg data files written by OLake workers):** `http://host.docker.internal:9090`
        * **AWS Region:** `us-east-1`
        * **S3 Access Key:** `minio`
        * **S3 Secret Key:** `minio123`
    
    * **Select Streams to sync:**
        * Select the weather table using checkbox to sync from Source to Destination.
        * Click on the weather table and set Normalisation to `true` using the toggle button.

    * **Configure Job:**
        * Set job name and replication frequency.

    * **Save and Run the Job:**
        * Save the job configuration.
        * Run the job manually from the UI to initiate the data pipeline from MySQL to Iceberg by clicking **Sync now**.

### 4. Query Data with Trino

1. **Access Trino UI:** [http://localhost:8088](http://localhost:8088)

2. **Run Queries via CLI:**
   ```bash
   docker run --rm -it --network olake-network trinodb/trino:latest trino \
     --server http://trino:8080 \
     --catalog iceberg --schema weather \
     --execute "SHOW TABLES;"
   ```

3. **Query example:**
     ```sql
     SELECT station_state, AVG(temperature_avg) as avg_temp
     FROM iceberg.weather.weather 
     GROUP BY station_state 
     ORDER BY avg_temp DESC 
     LIMIT 10;
     ```

## Services & Ports

| Service | URL | Description |
|---------|-----|-------------|
| OLake UI | `http://localhost:8000` | Data pipeline management |
| Trino UI | `http://localhost:8088` | Query interface |
| MinIO Console | `http://localhost:8443` | Object storage management |
| MySQL | `localhost:3306` | Source database |
| Iceberg REST | `http://localhost:8181` | Catalog API |

## Troubleshooting

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f trino
```

### Check Service Status
```bash
docker compose ps
```

### Verify Data Load
```bash
# Connect to MySQL
docker exec -it primary_mysql mysql -u root -ppassword weather

# Check weather table
SELECT COUNT(*) FROM weather;
SELECT * FROM weather LIMIT 5;
```

### Test Trino Connection
```bash
# Check if Trino can see Iceberg tables
docker run --rm -it --network olake-network trinodb/trino:latest trino \
  --server http://olake-trino-coordinator:8080 \
  --catalog iceberg --schema weather \
  --execute "SHOW TABLES;"
```

### Common Issues

**Trino can't connect to Iceberg:**
- Ensure the data pipeline in OLake has run successfully
- Check that MinIO bucket contains data: `http://localhost:8443`
- Verify Iceberg REST catalog is responding: `http://localhost:8181/v1/namespaces`

**MySQL connection issues:**
- Wait for `init-mysql-tasks` to complete data loading
- Check MySQL logs: `docker compose logs primary_mysql`

**MinIO access issues:**
- Check MinIO credentials in docker-compose.yml match OLake destination config
- Verify bucket permissions in MinIO console

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│    MySQL    │───▶│    OLake    │───▶│   MinIO     │
│  (Source)   │    │ (Pipeline)  │    │ (Storage)   │
└─────────────┘    └─────────────┘    └─────────────┘
                           │                  │
                           ▼                  │
                   ┌─────────────┐            │
                   │   Iceberg   │◀───────────┘
                   │ REST Catalog│
                   └─────────────┘
                           │
                           ▼
                   ┌─────────────┐
                   │    Trino    │
                   │ (Query UI)  │
                   └─────────────┘
```

## Cleanup

```bash
# Stop this example
docker compose down

# Stop base OLake stack
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose.yml | docker compose -f - down
```
