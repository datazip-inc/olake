# Olake End-to-End Demo Stack with Docker Compose

This Docker Compose setup provides a comprehensive environment for demonstrating and exploring Olake's capabilities. It includes a pre-configured MySQL database with the "employees" sample dataset, MinIO for S3-compatible storage, an Iceberg REST catalog, Temporal for workflow management, and the Olake application itself.

## Features

* **Olake Application (`olake-app`):** The core application for defining and managing data pipelines.
* **MySQL (`primary_mysql`):**
    * Pre-loaded with the extensive "employees" sample database.
    * Change Data Capture (CDC) enabled via binary logs.
* **Iceberg REST Catalog (`rest`):** Manages metadata for Iceberg tables, using PostgreSQL as its backend.
## Prerequisites

* **Docker:** Latest version installed and running.
* **Docker Compose:** Latest version installed (usually included with Docker Desktop).
* **Resources:** This stack runs multiple services and loads a large dataset. Allocate sufficient memory and CPU resources to Docker (e.g., 8GB+ RAM recommended).

## Setup
1. **Clone the repository:**

   ```bash
   git clone https://github.com/datazip-inc/olake.git
   cd olake/examples
   ```

2. **Edit persistence/config paths (required):**

   - The docker-compose.yml uses `/your/chosen/host/path/olake-data` as a placeholder for the host directory where Olake's persistent data and configuration will be stored. You **must** replace this with an actual path on your system before starting the services. You can change this by editing the `x-app-defaults` section at the top of `docker-compose.yml`:
     ```yaml
     x-app-defaults:
       host_persistence_path: &hostPersistencePath /your/host/path
     ```
   - Make sure the directory exists and is writable by the user running Docker (see how to change [file permissions for Linux/macOS](https://wiki.archlinux.org/title/File_permissions_and_attributes#Changing_permissions)).

3. **Customizing Admin User (optional):**

   The stack automatically creates an initial admin user on first startup. The default credentials are:

   - Username: "admin"
   - Password: "password"
   - Email: "test@example.com"

   To change these defaults, edit the `x-signup-defaults` section in your `docker-compose.yml`:

   ```yaml
   x-signup-defaults:
   username: &defaultUsername "your-custom-username"
   password: &defaultPassword "your-secure-password"
   email: &defaultEmail "your-email@example.com"
   ```


## Running the Stack

1.  **Navigate to the Directory:** Open a terminal and change to the directory where you saved the `docker-compose.yml` file.

2.  **Start the Services:**
    ```bash
    docker compose up -d
    ```
    * The first time you run this, Docker will download all the necessary images, and the `init-mysql-tasks` service will clone the "employees" database repository and load it into MySQL. **This initial setup, especially the data loading part, can take a significant amount of time (potentially 10-30 minutes or more depending on your internet speed and machine performance).** Please be patient.
    * You can monitor the progress of the `init-mysql-tasks` service:
        ```bash
        docker compose logs -f init-mysql-tasks
        ```
    * Once `init-mysql-tasks` completes, other dependent services like `olake-app` will start.

3.  **View Logs:** To view logs for all services:
    ```bash
    docker compose logs -f
    ```
    Or for a specific service:
    ```bash
    docker compose logs -f <service_name> 
    # e.g., docker compose logs -f olake-app
    ```

4.  **Stopping the Stack:**
    ```bash
    docker compose down
    ```

5.  **Stopping and Removing Volumes (for a clean restart):**
    If you want to remove all data (MySQL data, MinIO buckets, PostgreSQL data, etc.) and start fresh:
    ```bash
    docker compose down -v
    ```

## Accessing Services

Once the stack is up and running (especially after `init-mysql-tasks` and `olake-app` are healthy/started):

* **Olake Application UI:** `http://localhost:8000`
    * A default user is created by `signup-init`:
        * Username: `admin`
        * Password: `password`
        * Email: `admin@example.com`
* **MinIO Console (S3 Storage):** `http://localhost:8443`
    * Access Key: `minio`
    * Secret Key: `minio123`
    * You should find a `warehouse` bucket created by the `mc` service, which will store Iceberg data.
* **MySQL (`primary_mysql`):**
    * Host: `localhost`
    * Port: `3306`
    * Root Password: `password`
    * Databases: `main` (default), `employees` (loaded by `init-mysql-tasks`).
* **Iceberg REST Catalog API:** `http://localhost:8181` (Primarily for programmatic access or use by query engines).

## Interacting with Olake

1.  Log in to the Olake UI at `http://localhost:8000` using the default credentials.
2.  **Configure Data Source and Destination:**

    > **⚠️ Important Network Configuration for Olake Pipelines:**
    > The Olake pipelines, when executed by Temporal, may spin up worker containers that operate outside the main `app-network` defined in this Docker Compose setup. For these workers to successfully connect to services like MySQL, the Iceberg REST Catalog, and MinIO S3:
    > * You **MUST** use `host.docker.internal` as the hostname in your Olake Source and Destination configurations. `host.docker.internal` is a special DNS name that resolves to your host machine's internal IP address from within any Docker container.
    > * This functionality is available out-of-the-box with Docker Desktop (Mac/Windows). For Docker on Linux (if not using Docker Desktop), you might need to ensure your Docker daemon or the worker containers are configured to resolve `host.docker.internal` (e.g., by adding `--add-host=host.docker.internal:host-gateway` to the Docker run command for the workers, which would be managed by the Olake/Temporal setup).
    > * The required ports for these services (`3306` for MySQL, `8181` for Iceberg REST, `9090` for MinIO S3) are already exposed to the host in the provided `docker-compose.yml`.

    * Set up a **Source** connection to the `primary_mysql` database within Olake:
        * **Host:** `host.docker.internal`
        * **Port:** `3306`
        * **Database:** `employees`
        * **User:** `root`
        * **Password:** `password`

    * Set up a **Destination** connection for Apache Iceberg within Olake:
        * **Iceberg REST Catalog URL:** `http://host.docker.internal:8181`
        * **Iceberg S3 Path (example):** `s3://warehouse/employees/`
        * **Iceberg Database (example):** `employees`
        * **S3 Endpoint (for Iceberg data files written by Olake workers):** `http://host.docker.internal:9090`
        * **AWS Region:** `us-east-1`
        * **S3 Access Key:** `minio`
        * **S3 Secret Key:** `minio123`

3.  **Create and Configure a Job:**
    Once your Source (MySQL) and Destination (Iceberg) are successfully configured and tested in Olake, you can create a Job to define and run your data pipeline:
    * Navigate to the **"Jobs"** tab in the Olake UI.
    * Click on the **"Create Job"** button.

    * **Set up your Source:**
        * In the "Source" configuration section of the new job.
        * Select the option to **"Use an existing source"** (or similar wording).
        * Choose **"MySQL"** as the Connector type.
        * Under **"Select existing source:"** select the pre-configured MySQL source connection you created.

    * **Set up your Destination:**
        * In the "Destination" configuration section of the new job.
        * Select the option to **"Use an existing destination"** (or similar wording).
        * Choose **"Apache Iceberg"** as the Connector type.
        * Choose **"REST Catalog"** as the Catalog.
        * Under **"Select existing destination:"** select the pre-configured Iceberg destination you created.
    
    * **Select Streams to sync:**
        * Select the tables you want to sync from Source to Destination.

    * **Configure Job:**
        * Set job name and replication frequency.

    * **Save and Run the Job:**
        * Save the job configuration.
        * You can then typically run the job manually from the UI to initiate the data pipeline from MySQL to Iceberg by selecting **Sync now**

## Querying Iceberg Tables with External Engines

Once Olake has processed data and created Iceberg tables, you can query these tables using various external SQL query engines. This allows you to leverage the power of engines like Presto, Trino, DuckDB, DorisDB, and others to analyze your data.

Example configurations and detailed setup instructions for specific query engines are provided in their respective subdirectories within this example:

* **Presto / Trino:**
    * Sample configuration files are located in the `./presto/etc/` directory.
    * For detailed setup instructions, please refer to the [**Presto/Trino Setup Guide (`./presto/README.md`)**](./presto/README.md).

* **(Future) DuckDB:**
    * Coming soon...

* **(Future) DorisDB:**
    * Coming soon...

Before attempting to connect a query engine, ensure the main Olake Docker Compose stack is up and running, and Olake has successfully populated Iceberg tables.