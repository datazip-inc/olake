# Querying Olake-Managed Iceberg Tables with Presto/Trino

This guide explains how to run Presto (or Trino) as an external Docker container to query Apache Iceberg tables created by the main Olake Docker Compose stack.

It assumes:
1.  The main Olake Docker Compose stack (from the parent directory's `docker-compose.yml`) is already up and running.
2.  Services like the Iceberg REST Catalog (`rest`) and MinIO (`minio`) are active on the `app-network`.
3.  You have populated Iceberg tables using Olake.
4.  The necessary Presto configuration files are present in the `./etc/` subdirectory (relative to this `presto` directory).

## Steps to Run Presto

1.  **Navigate to the Main Example Directory:**
    * Open your terminal. All `docker run` or `docker compose` commands mentioned below should ideally be executed from the **root directory of the main Olake example** (i.e., the directory containing the main `docker-compose.yml` and this `presto` subdirectory). This ensures relative paths for volume mounts work as intended. For example, if this `presto` directory is inside `olake-example/presto/`, you should be in `olake-example/` to run the `docker run` command below.

2.  **Run the Presto Docker Container:**
    * Execute the following command from the main example's root directory:

    ```bash
    docker run -d --name my-olake-presto-coordinator \
      --network app-network \
      -p 80:8080 \
      -v "$(pwd)/etc:/opt/presto-server/etc" \
      prestodb/presto:latest
    ```

    * **Explanation of the command:**
        * `-d`: Runs the container in detached mode (in the background).
        * `--name my-olake-presto-coordinator`: Assigns a recognizable name to your Presto container.
        * `--network app-network`: **This is crucial.** It connects the Presto container to the same Docker network (`app-network`) used by the Olake services. This allows Presto to resolve and communicate with services like `rest` (Iceberg Catalog) and `minio` (S3 storage) using their service names.
        * `-p 80:8080`: Maps port `8080` inside the Presto container (Presto's default HTTP port) to port `80` on your host machine. You can change `8088` to any other available port on your host if needed (e.g., `-p 8080:8080` to use the default HTTP port, or `-p 8090:8080`).
        * `-v "$(pwd)/etc:/opt/presto-server/etc"`: This mounts the Presto configuration files located in the `./etc` subdirectory (relative to where you run the command) into the expected location within the Presto container.
            * If you are on Windows Command Prompt, replace `$(pwd)` with `"%cd%"`.
            * If you are on Windows PowerShell, replace `$(pwd)` with `${PWD}`.
        * `prestodb/presto:latest`: Specifies the Docker image for Presto. You can replace this with a specific Presto version tag or a Trino image (e.g., `trinodb/trino:latest`). Ensure the configuration files in `./presto/etc/` are compatible with the chosen image and version.

3.  **Access Presto UI & Query Data:**
    * Once the Presto container is running (it might take a few seconds to initialize), you can access the Presto web UI in your browser at `http://localhost:80` (or whichever host port you mapped).
    * You can use the Presto CLI, a SQL IDE (like DBeaver, DataGrip), or any application compatible with Presto/Trino to connect to `localhost:80`.
    * You should be able to query the `iceberg` catalog defined in your `./presto/etc/catalog/iceberg.properties` file. Example queries:
        ```sql
        SHOW CATALOGS;
        -- Expected output should include 'iceberg' and 'system'

        SHOW SCHEMAS FROM iceberg;
        -- This will list the namespaces/schemas within your Iceberg catalog 
        -- (e.g., 'default', or schemas created by Olake).

        -- Assuming Olake created tables in a schema named 'olake_output_schema'
        -- SHOW TABLES FROM iceberg.olake_output_schema;

        -- Query a specific table
        -- SELECT * FROM iceberg.olake_output_schema.your_iceberg_table LIMIT 10;
        ```

## Presto Configuration Files (Located in `./etc/`)

The `./etc/` directory (within this `presto` folder) should contain the following configuration files, pre-configured to work with the Olake stack:

* **`./etc/config.properties`**:
    * Main Presto coordinator configuration (e.g., setting it as a coordinator, HTTP port).
    ```properties
    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8080
    query.max-memory=4GB 
    query.max-memory-per-node=2GB 
    discovery-server.enabled=true
    discovery.uri=http://localhost:8080 
    ```

* **`./etc/jvm.config`**:
    * JVM settings for Presto (e.g., memory allocation).
    ```
    -server
    -Xmx4G 
    -XX:+UseG1GC
    # ... other JVM options
    ```

* **`./etc/catalog/iceberg.properties`**:
    * **This is the most critical file for connecting to your Olake-managed Iceberg data.**
    * It defines the `iceberg` catalog in Presto.
    * It must correctly point to the Iceberg REST Catalog service (`rest`) and the MinIO S3 service (`minio`) running within the `app-network`.
    ```properties
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://rest:8181 
    iceberg.s3.endpoint=http://minio:9090
    iceberg.s3.path-style-access=true
    iceberg.s3.aws-access-key=minio
    iceberg.s3.aws-secret-key=minio123
    ```

**Troubleshooting Presto Connection:**
* **Network:** Ensure Presto is on the `app-network`. Use `docker network inspect app-network` to see connected containers.
* **Service Resolution:** From within the Presto container (`docker exec -it my-olake-presto-coordinator sh`), try to `ping rest` or `ping minio` (you might need to install ping: `apk add iputils`).
* **Presto Logs:** Check Presto container logs: `docker logs my-olake-presto-coordinator`.
* **Iceberg Catalog Logs:** Check the `rest` service logs: `docker compose logs -f rest`.
* **MinIO Logs:** Check the `minio` service logs: `docker compose logs -f minio`.
* **Configuration:** Double-check all URIs, ports, and credentials in `iceberg.properties`.