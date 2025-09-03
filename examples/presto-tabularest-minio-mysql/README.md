# Olake Demo: Presto + Tabular REST + MinIO + MySQL

This example demonstrates a complete data pipeline using:
- **Olake** for data pipeline orchestration
- **MySQL** as the source database with sample weather data
- **MinIO** for S3-compatible object storage
- **Tabular Iceberg REST Catalog** for table metadata management
- **Presto** as the query engine for data analysis

## Prerequisites

* [Docker](https://docs.docker.com/get-docker/) installed and running (Docker Desktop recommended for Mac/Windows)
* [Docker Compose](https://docs.docker.com/compose/) (comes with Docker Desktop)
* **Port Availability:** The following ports must be available on your system:
   - **8000** - OLake UI
   - **8088** - Presto query engine UI
   - **3306** - MySQL database
   - **8181** - Iceberg REST catalog API  
   - **8443** - MinIO console UI
   - **9090** - MinIO server API

**Note:** If any of these ports are in use, stop the conflicting services or modify the port mappings in the docker-compose.yml file.

## Quick Start

### 1. Start the Base Olake Stack

```bash
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose.yml | docker compose -f - up -d
```

### 2. Start the Demo Stack

```bash
# Navigate to this example directory
cd examples/presto-tabularest-minio-mysql

# Start services
docker compose up -d
```

### 3. Accessing Services
1.  **Log in** to the Olake UI at [http://localhost:8000](http://localhost:8000) with credentials `admin`/`password`.

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
        * **Iceberg REST Catalog URL:** `http://host.docker.internal:8181`
        * **Iceberg S3 Path (example):** `s3://warehouse/weather/`
        * **Iceberg Database (example):** `weather`
        * **S3 Endpoint (for Iceberg data files written by Olake workers):** `http://host.docker.internal:9090`
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

### 4. Query Data with Presto

1. **Access Presto UI:** `http://localhost:80`

2. **Run Queries:**
   - Click **SQL CLIENT**
   - Select **Catalog:** `iceberg`, **Schema:** `weather`
   - Query example:
     ```sql
     SELECT station_state, AVG(temperature_avg) as avg_temp
     FROM iceberg.weather.weather 
     GROUP BY station_state 
     ORDER BY avg_temp DESC 
     LIMIT 10;
     ```

## Sample Queries

Once data is synchronized, try these queries in Presto:

### Basic Data Exploration
```sql
-- Count total records
SELECT COUNT(*) FROM iceberg.weather.weather;

-- Show table schema
DESCRIBE iceberg.weather.weather;

-- Sample data
SELECT * FROM iceberg.weather.weather LIMIT 10;
```

## Troubleshooting

### Check Service Status
```bash
docker compose ps
```

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f presto
```

### Verify Data Load
```bash
# Connect to MySQL
docker exec -it primary_mysql mysql -u root -ppassword weather

# Check weather table
SELECT COUNT(*) FROM weather;
SELECT * FROM weather LIMIT 5;
```

### Test Presto Connection
```bash
# Check if Presto can see Iceberg tables
docker exec -it olake-presto-coordinator presto --catalog iceberg --schema weather --execute "SHOW TABLES;"
```

### Common Issues

**Presto can't connect to Iceberg:**
- Ensure the data pipeline in Olake has run successfully
- Check that MinIO bucket contains data: `http://localhost:8443`
- Verify Iceberg REST catalog is responding: `http://localhost:8181/v1/namespaces`

**MySQL connection issues:**
- Wait for `init-mysql-tasks` to complete data loading
- Check MySQL logs: `docker compose logs primary_mysql`

**MinIO access issues:**
- Check MinIO credentials in docker-compose.yml match Olake destination config
- Verify bucket permissions in MinIO console

## Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│    MySQL    │───▶│    Olake    │───▶│   MinIO     │
│  (Source)   │    │ (Pipeline)  │    │ (Storage)   │
└─────────────┘    └─────────────┘    └─────────────┘
                           │                  │
                           ▼                  │
                   ┌─────────────┐            │
                   │  Iceberg    │◀───────────┘
                   │ REST Catalog│
                   └─────────────┘
                           │
                           ▼
                   ┌─────────────┐
                   │   Presto    │
                   │ (Query UI)  │
                   └─────────────┘
```

## Cleanup

```bash
# Stop this example
docker compose down

# Stop base Olake stack
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose.yml | docker compose -f - down
```