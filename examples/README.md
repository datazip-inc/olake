# Olake End-to-End Demo Stack with Docker Compose

This Docker Compose setup provides a comprehensive environment for demonstrating and exploring Olake's capabilities. It includes a pre-configured MySQL database with the "weather" sample dataset, MinIO for S3-compatible storage, an Iceberg REST catalog, Temporal for workflow management, and the Olake application itself.

## Features

* **Olake Application (`olake-app`):** The core application for defining and managing data pipelines.
* **MySQL (`primary_mysql`):**
    * Pre-loaded with the "weather" sample database.
    * Change Data Capture (CDC) enabled via binary logs.
* **Iceberg REST Catalog (`rest`):** Manages metadata for Iceberg tables, using PostgreSQL as its backend.
## Prerequisites

* **Docker:** Latest version installed and running.
* **Docker Compose:** Latest version installed (usually included with Docker Desktop).
* **Resources:** This stack runs multiple services and loads a large dataset. Allocate sufficient memory and CPU resources to Docker (e.g., 8GB+ RAM recommended).

## Running the Stack
1. **Clone the repository:**

   ```bash
   git clone https://github.com/datazip-inc/olake.git
   cd olake/examples
   ```

2.  **Start the Services:**
    ```bash
    docker compose up -d
    ```
    On the first run, Docker will download all the necessary images, and the `init-mysql-tasks` service will clone the "weather" CSV and load it into MySQL. **This initial setup, especially the docker image download part, can take some amount of time (potentially 5-10 minutes or more depending on internet speed and machine performance).**

## Accessing Services

Once the stack is up and running (especially after `init-mysql-tasks` and `olake-app` are healthy/started):

* **Olake Application UI:** `http://localhost:8000`
    * Default credentials:
        * Username: `admin`
        * Password: `password`
* **MySQL (`primary_mysql`):**
    * Verify Source Data:
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

## Interacting with Olake

1.  Log in to the Olake UI at `http://localhost:8000` using the default credentials.
2.  **Configure Data Source and Destination:**

    * Set up a **Source** connection to the `primary_mysql` database within Olake:
        * **Host:** `host.docker.internal`
        * **Port:** `3306`
        * **Database:** `weather`
        * **User:** `root`
        * **Password:** `password`

    * Set up a **Destination** connection for Apache Iceberg within Olake:
        * **Iceberg REST Catalog URL:** `http://host.docker.internal:8181`
        * **Iceberg S3 Path (example):** `s3://warehouse/weather/`
        * **Iceberg Database (example):** `weather`
        * **S3 Endpoint (for Iceberg data files written by Olake workers):** `http://host.docker.internal:9090`
        * **AWS Region:** `us-east-1`
        * **S3 Access Key:** `minio`
        * **S3 Secret Key:** `minio123`

3.  **Create and Configure a Job:**
    Once the Source (MySQL) and Destination (Iceberg) are successfully configured and tested in Olake, create a Job to define and run the data pipeline:
    * Navigate to the **"Jobs"** tab in the Olake UI.
    * Click on the **"Create Job"** button.

    * **Set up the Source:**
        * Use and existing source -> Connector: MySQL -> Select the source from the dropdown list -> Next.

    * **Set up the Destination:**
        * Use and existing destination -> Conector: Apache Iceberg -> Catalog: REST Catalog -> Select the destination from the dropdown list -> Next.
    
    * **Select Streams to sync:**
        * Select the weather table using checkbox to sync from Source to Destination.
        * Click on the weather table and set Normalisation to `true` using the toggle button.

    * **Configure Job:**
        * Set job name and replication frequency.

    * **Save and Run the Job:**
        * Save the job configuration.
        * Run the job manually from the UI to initiate the data pipeline from MySQL to Iceberg by selecting **Sync now**.

## Querying Iceberg Tables with External Engines

Once Olake has processed data and created Iceberg tables, the tables can be queried using various external SQL query engines leveraging the power of engines like Presto, Trino, DuckDB, DorisDB, and others to analyze the data.

Example configurations and detailed setup instructions for specific query engines are provided in their respective subdirectories within this example:

* **Presto:**
    * Sample configuration files are located in the `./presto/etc/` directory.
    * For detailed setup instructions, please refer to the [**Presto Setup Guide (`./presto/README.md`)**](./presto/README.md).

* **(Future) Trino:**
    * Coming soon...

* **(Future) DuckDB:**
    * Coming soon...

* **(Future) DorisDB:**
    * Coming soon...

### Optional Configuration

**Custom Admin User:**

The stack automatically creates an initial admin user on first startup. To change the default credentials, edit the `x-signup-defaults` section in `docker-compose.yml`:

```yaml
x-signup-defaults:
username: &defaultUsername "custom-username"
password: &defaultPassword "secure-password"
email: &defaultEmail "email@example.com"
```

**Custom Data Directory:**

The docker-compose.yml uses `${PWD}/olake-data` for the host directory where Olake's persistent configuration, states and metadata will be stored. This could be replaced with any other path on host system before starting the services. Change this by editing the `x-app-defaults` section at the top of `docker-compose.yml`:
```yaml
x-app-defaults:
  host_persistence_path: &hostPersistencePath /alternate/host/path
```
Make sure the directory exists and is writable by the user running Docker (see how to change [file permissions for Linux/macOS](https://wiki.archlinux.org/title/File_permissions_and_attributes#Changing_permissions)).

## Troubleshooting

### Viewing Logs

- **All services:**
  ```bash
  docker compose logs -f
  ```

- **Specific service:**
  ```bash
  docker compose logs -f <service_name>
  ```

### Checking Service Status

- **Service status:**
  ```bash
  docker compose ps
  ```

### Common Commands

- **Restart a service:**
  ```bash
  docker compose restart <service_name>
  ```

- **Stop all services and remove volumes:**
  ```bash
  docker compose down -v
  ```