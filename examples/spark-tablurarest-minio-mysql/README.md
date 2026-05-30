# OLake Demo: Spark + Tabular REST + MinIO + MySQL

This example demonstrates an end-to-end data lakehouse pipeline:
- **MySQL** → **OLake** → **Iceberg (Tabular REST) on MinIO** → **Spark**

## Prerequisites

* [Docker](https://docs.docker.com/get-docker/) installed and running (Docker Desktop recommended for Mac/Windows)
* [Docker Compose](https://docs.docker.com/compose/) (comes with Docker Desktop)
* **Port Availability:** The following ports must be available on your system:
   - **8000** - OLake UI (from separate stack)
   - **8888** - Jupyter Notebook (Spark)
   - **3306** - MySQL database
   - **8181** - Iceberg REST catalog API
   - **9000** - MinIO server API
   - **9091** - MinIO console UI

**Note:** If any of these ports are in use, stop the conflicting services or modify the port mappings in the docker-compose.yml file.

## Quick Start

### 1. Start OLake Tech Stack

```bash
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose-v1.yml | docker compose -f - up -d
```

### 2. Start the Demo Stack

```bash
cd examples/spark-tablurarest-minio-mysql
```
```bash
docker compose up -d
```

### 3. Accessing Services

1. **Verify Source Data:**
   - Access the MySQL CLI:
     ```bash
     docker exec -it primary_mysql mysql -u root -ppassword
     ```
   - Select the `weather` database and query the table:
     ```sql
     USE weather;
     SELECT * FROM weather LIMIT 10;
     ```

2. **Log in** to the OLake UI at [http://localhost:8000](http://localhost:8000) with credentials `admin`/`password`.

3. **Create and Configure a Job:**
   Create a Job to define and run the data pipeline:
   * On the main page, click on the **"Create Job"** button.

   * **Job Configuration:**
       * **Job Name:** `job` (Any name can be configured)
       * Let the frequency be "Every Minute"

   * **Set up your Source:** Set up a new source
       * **Connector:** `MySQL`
       * **Version:** choose the latest available version
       * **Name of your source:** `olake_mysql`
       * **Host:** `host.docker.internal`
       * **Database:** `weather`
       * **Username:** `root`
       * **Password:** `password`

   * **Set up your Destination:**
       * **Connector:** `Apache Iceberg`
       * **Version:** choose the latest available version
       * **Name of your destination:** `olake_iceberg`
       * **Catalog Type:** `REST`
       * **REST Catalog URI:** `http://host.docker.internal:8181`
       * **S3 Path:** `s3://warehouse/weather`
       * **S3 Endpoint:** `http://host.docker.internal:9000`
       * **S3 Access Key:** `minio`
       * **S3 Secret Key:** `minio123`
       * **AWS Region:** `us-east-1`

   * **Streams Selection**
       * Make sure that the weather table has been selected for the sync.
       * Click on the weather table and make sure that the Normalisation is set to `true` using the toggle button, and then click on `Create Job` button.

   You can check the job status in the **Jobs** section.

### 3. Query Data with Spark

#### Option 1: Using Spark SQL Shell

1. **Access Spark SQL:**
   ```bash
   docker exec -it olake-spark /opt/spark/bin/spark-sql
   ```

2. **Run Queries:**
   ```sql
   SHOW CATALOGS;
   ```
   You will see the `olake_iceberg` catalog available.

   ```sql
   SHOW NAMESPACES IN olake_iceberg;
   ```
   You will be able to see the namespace created for your table as `{job_name}_weather`

   Now check the tables available in the namespace:
   ```sql
   SHOW TABLES IN {job_name}_weather;
   ```

   Now, query the iceberg table
   ```sql
   SELECT * FROM olake_iceberg.{job_name}_weather.weather LIMIT 10;
   ```

#### Option 2: Using Jupyter Notebook

1. **Access Jupyter:** [http://localhost:8888](http://localhost:8888) (no password required)

2. Create a new python notebook and run these commands in cells:

   ```python
   %%sql
   SHOW CATALOGS;
   ```

   ```python
   %%sql
   SHOW NAMESPACES IN olake_iceberg;
   ```

   ```python
   %%sql
   SHOW TABLES IN {job_name}_weather;
   ```

   ```python
   %%sql
   SELECT * FROM olake_iceberg.{job_name}_weather.weather LIMIT 10;
   ```

### Common Issues

**Spark can't connect to Iceberg:**
- Ensure the data pipeline in OLake has run successfully
- Check that MinIO bucket contains data: `http://localhost:9091` (credentials: minio/minio123)
- Verify Iceberg REST catalog is responding: `http://localhost:8181/v1/namespaces`

**MinIO access issues:**
- Check MinIO credentials in docker-compose-v1.yml match OLake destination config
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
                   │    Spark    │
                   │ (Query UI)  │
                   └─────────────┘
```

## Cleanup

```bash
# Stop and remove all data
docker compose down -v

# Stop base OLake stack (if running separately)
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose-v1.yml | docker compose -f - down
```
