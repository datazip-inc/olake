# OLake Demo: Spark + Tabular REST + MinIO + MySQL

This example demonstrates an end-to-end data pipeline to Apache Iceberg using Apache Spark as the query engine:

**MySQL** → **OLake** → **Iceberg (Tabular REST) on MinIO** → **Spark**

## Prerequisites

* [Docker](https://docs.docker.com/get-docker/) installed and running (Docker Desktop recommended for Mac/Windows)
* [Docker Compose](https://docs.docker.com/compose/) (comes with Docker Desktop)
* **Port Availability:** The following ports must be available on your system:
   - **8000** - OLake UI
   - **8088** - Spark UI
   - **8888** - Jupyter Notebook (Spark)
   - **3306** - MySQL database
   - **8181** - Iceberg REST catalog API
   - **8443** - MinIO console UI
   - **9090** - MinIO server API

**Note:** If any of these ports are in use, stop the conflicting services or modify the port mappings in the docker-compose.yml file.

## Quick Start

### 1. Start OLake

```bash
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose.yml | docker compose -f - up -d
```

### 2. Start the Demo Stack

Navigate to this example directory
```bash
cd examples/spark-tablurarest-minio-mysql
```
Start the services
```
docker compose up -d
```

### 3. Accessing Services

1. **Verify OLake** is running at [http://localhost:8000](http://localhost:8000) with credentials `admin`/`password`.

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

3. **Create and Configure a Job:**
   Create a Job to define and run the data pipeline:
   * On the main page, click on the **"Create your first Job"** button. Set job name and frequency.

   * **Set up the Source:**
       * **Connector:** `MySQL`
       * **OLake Version:** chose the latest available version
       * **Name of your source:** `olake_mysql`
       * **MySQL Host:** `host.docker.internal`
       * **Database:** `weather`
       * **Username:** `root`
       * **Password:** `password`
       * **Port:** `3306`
       * **SSH Config:** `No Tunnel`
       * **Update Method:** `Standalone`
   
   Let the Retry Count be `3` by default, and Skip TLS Verification as `True`

   * **Set up the Destination:**
       * **Connector:** `Apache Iceberg`
       * **OLake Version:** chose the latest available version
       * **Name of your destination:** `olake_iceberg`
       * **Catalog Type:** `REST`
       * **REST Catalog URI:** `http://host.docker.internal:8181`
       * **S3 Path:** `s3://warehouse/weather/`
       * **S3 Endpoint (for Iceberg data files written by Olake workers):** `http://host.docker.internal:9090`
       * **S3 Access Key:** `minio`
       * **S3 Secret Key:** `minio123`
       * **AWS Region:** `us-east-1`

   Let the other fields be at their default values.

   * **Select Streams to sync:**
       * Make sure that the `weather` table has been selected for the sync.
       * Click on the weather table and make sure that the Normalisation is set to `true` using the toggle button.

   * **Save and Run the Job:**
       * Save the job configuration.
       * Run the job manually from the UI to initiate the data pipeline from MySQL to Iceberg by clicking **Sync now**.

### 4. Query Data with Spark

#### Option 1: Using Spark SQL Shell

1. **Access Spark SQL:**
   ```bash
   docker exec -it olake-spark /opt/spark/bin/spark-sql
   ```

2. **Run Queries:**
   ```sql
   SHOW NAMESPACES IN iceberg;
   ```
   Check for the namespace: `{job_name}_weather`, this is created by OLake
   ```sql
   SHOW TABLES IN iceberg.{job_name}_weather;
   ```
   Now, query the iceberg table
   ```sql
   SELECT * FROM iceberg.{job_name}_weather.weather LIMIT 10;
   ```

#### Option 2: Using Jupyter Notebook

1. **Access Jupyter:** [http://localhost:8888](http://localhost:8888) (no password required)

2. Create a new python notebook or use an existing one, and run these following commands in a cell

     ```python
     %%sql

     SHOW NAMESPACES IN iceberg;
     ```
     ```python
     %%sql

     SHOW TABLES IN iceberg.{job_name}_weather;
     ```
      ```python
     %%sql

     SELECT * FROM iceberg.{job_name}_weather.weather LIMIT 10;
     ```

## Troubleshooting

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f spark
```

### Check Iceberg REST Catalog
```bash
# List all namespaces
curl http://localhost:8181/v1/namespaces

# List tables in a namespace (replace {job_name}_weather with your namespace)
curl http://localhost:8181/v1/namespaces/{job_name}_weather/tables
```

### Common Issues

**Spark can't connect to Iceberg:**
- Check that MinIO bucket contains data: [http://localhost:8443](http://localhost:8443) (login: minio/minio123)
- Verify Iceberg REST catalog is responding: `http://localhost:8181/v1/namespaces`

**MySQL connection issues:**
- Wait for `init-mysql-tasks` to complete data loading
- Check MySQL logs: `docker compose logs primary_mysql`

**MinIO access issues:**
- Check MinIO credentials in docker-compose.yml match OLake destination config
- Verify bucket permissions in MinIO console

**Port conflicts:**
- If port 8088 is already in use, modify the Spark port mapping in docker-compose.yml
- Common conflict: Hadoop/Spark services running on port 8088

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│    MySQL    │───▶│    OLake    │───▶│   MinIO     │
│  (Source)   │    │ (Pipeline)  │    │ (Storage)   │
└─────────────┘    └─────────────┘    └─────────────┘
                           │                  │
                           ▼                  │
                   ┌─────────────┐            │
                   │ Iceberg     │◀───────────┘
                   │ REST Catalog│
                   └─────────────┘
                           │
                           ▼
                   ┌─────────────┐
                   │    Spark    │
                   │  (Query UI  │
                   │ + Notebook) │
                   └─────────────┘
```

## Cleanup

Stop this example
```bash
docker compose down
```

Stop OLake
```bash
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose.yml | docker compose -f - down
```
