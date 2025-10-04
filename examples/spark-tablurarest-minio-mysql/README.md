# OLake Demo: Spark + Tabular REST + MinIO + MySQL

This example demonstrates an end-to-end data lakehouse pipeline using Apache Spark with Jupyter Notebook for interactive data analysis:

**MySQL** → **OLake** → **Iceberg (Tabular REST) on MinIO** → **Spark + Jupyter**

## Prerequisites

* [Docker](https://docs.docker.com/get-docker/) installed and running (Docker Desktop recommended for Mac/Windows)
* [Docker Compose](https://docs.docker.com/compose/) (comes with Docker Desktop)
* **Port Availability:** The following ports must be available on your system:
   - **8000** - OLake UI
   - **8888** - Jupyter Notebook UI (for querying data)
   - **4040** - Spark Application UI
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
cd examples/spark-tablurarest-minio-mysql

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
        * **Version:** choose the latest available version
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
        * **Version:** choose the latest available version
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

### 4. Query and Analyze Data with Jupyter Notebook

**This is the GUI interface for querying data - no command line needed!**

1. **Access Jupyter Notebook:** [http://localhost:8888](http://localhost:8888)
   - **Token:** `olake123`
   - Simply paste this token when prompted

2. **Open the Weather Analysis Notebook:**
   - In Jupyter, navigate to the `work` folder
   - Open `Weather_Data_Analysis.ipynb`

3. **Run the Notebook:**
   - Click on **Run → Run All Cells** or run cells one by one using `Shift+Enter`
   - The notebook will:
     - Connect to Iceberg REST catalog
     - Query the weather data synced from MySQL
     - Perform various analytics and aggregations
     - Display results in an interactive format

4. **Example Queries in the Notebook:**
   - View all weather data
   - Calculate average temperature by state
   - Find cities with highest precipitation
   - Analyze wind speed patterns
   - Create custom queries

## Project Structure

```
spark-tablurarest-minio-mysql/
├── conf/                          # Configuration files
│   ├── catalog/
│   │   └── iceberg.properties     # Iceberg REST catalog configuration
│   ├── spark-defaults.conf        # Spark + Iceberg settings
│   ├── core-site.xml              # Hadoop S3A configuration
│   └── jupyter_notebook_config.py # Jupyter server settings
├── scripts/
│   └── init-spark.sh              # Initialization script
├── notebooks/
│   └── Weather_Data_Analysis.ipynb # Main query interface
├── docker-compose.yml             # Service definitions
└── README.md                      # Documentation
```

## Features

✅ **Configuration Management**
   - **`conf/spark-defaults.conf`** - Spark and Iceberg settings (REST catalog URI, S3 config, JARs)
   - **`conf/core-site.xml`** - Hadoop S3A filesystem configuration
   - **`conf/catalog/iceberg.properties`** - Iceberg catalog properties
   - **`conf/jupyter_notebook_config.py`** - Jupyter server configuration
   - All configs are mounted read-only into containers for security

✅ **Automated Initialization**
   - **`scripts/init-spark.sh`** - Verifies catalog connectivity, installs dependencies, sets up directories
   - Runs automatically before Jupyter starts

✅ **Web-Based Interface**
   - **OLake UI** (port 8000) - Pipeline configuration
   - **Jupyter Notebook** (port 8888) - Interactive data querying and analysis
   - **MinIO Console** (port 8443) - Storage inspection
   - **Spark UI** (port 4040) - Job monitoring

✅ **Interactive Analysis**
   - Live code execution with instant results
   - Rich data visualization capabilities
   - Step-by-step documentation in the notebook
   - Easy modification of queries without restarting services
   - Configuration loaded from external files

✅ **Production-Ready Architecture**
   - Apache Spark for distributed processing
   - Iceberg REST catalog for table metadata
   - PostgreSQL for catalog metadata persistence
   - MinIO for S3-compatible object storage
   - External configuration files (separation of concerns)
   - Proper service dependencies and health checks

## Troubleshooting

### View Logs
```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f jupyter
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

### Jupyter Not Loading?
```bash
# Check Jupyter logs
docker compose logs jupyter

# Restart Jupyter if needed
docker compose restart jupyter
```

### Common Issues

**Jupyter notebook connection issues:**
- Ensure token `olake123` is entered correctly
- Check that port 8888 is not in use by another application
- Wait 30 seconds after `docker compose up` for packages to install

**Spark can't connect to Iceberg:**
- Ensure the data pipeline in OLake has run successfully
- Check that MinIO bucket contains data: `http://localhost:8443` (login: minio/minio123)
- Verify Iceberg REST catalog is responding: `http://localhost:8181/v1/namespaces`

**MySQL connection issues:**
- Wait for `init-mysql-tasks` to complete data loading
- Check MySQL logs: `docker compose logs primary_mysql`

**Notebook shows "No tables found":**
- Make sure you've run the OLake sync job and it completed successfully
- Check the OLake UI to verify the job status
- Run the catalog verification cells in the notebook to debug

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
                   │(PostgreSQL) │
                   └─────────────┘
                           │
                           ▼
                   ┌─────────────┐
                   │   Jupyter   │
                   │  Notebook   │
                   │   (Spark)   │
                   └─────────────┘
```

## Technology Stack

- **Query Interface:** Jupyter Notebook - web-based interactive computing
- **Processing Engine:** Apache Spark for distributed data processing
- **Catalog:** Iceberg REST catalog with PostgreSQL backend
- **Storage:** MinIO (S3-compatible object storage)
- **Source Database:** MySQL 8.0 with CDC capabilities
- **Programming:** SQL and Python (PySpark) support

## Cleanup

```bash
# Stop this example
docker compose down

# Stop base OLake stack
curl -sSL https://raw.githubusercontent.com/datazip-inc/olake-ui/master/docker-compose.yml | docker compose -f - down
```

## Need Help?

- Check the Jupyter notebook for step-by-step instructions
- Review the troubleshooting section above
- Verify all services are running: `docker compose ps`
