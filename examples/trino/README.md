# Querying Olake-Managed Iceberg Tables with Trino

This guide explains how to run Trino as an external Docker container to query Apache Iceberg tables created by the main Olake Docker Compose stack.

It assumes:
1.  The main Olake Docker Compose stack (from the parent directory's `docker-compose.yml`) is already up and running.
2.  Services like the Iceberg REST Catalog (`rest`) and MinIO (`minio`) are active on the `app-network`.
3.  You have populated Iceberg tables using Olake.

## Steps to Run Trino

1. **Navigate to the Trino Directory:**
   ```bash
   cd trino
   ```

2. **Run the Trino Docker Conainer:**
   ```bash
   docker run -d --name olake-trino-coordinator \
     --network app-network \
     -p 80:8080 \
     -v "$(pwd)/etc:/opt/trino-server/etc" \
     trinodb/trino:latest
   ```

3. **Query Data Using Trino UI:**

   1. **Access the Trino UI:**
      - Open your browser and go to `http://localhost:80`.
      - If prompted for credentials, use 'admin' as the username.

   3. **Select the Catalog and Schema:**
      - Choose **Catalog: iceberg** and **Schema: weather** from the dropdown menus.

   4. **Run the Query:**
      - Enter the following query in the SQL editor:
        ```sql
        SELECT * FROM iceberg.weather.weather LIMIT 10;
        ```
      - Click **Run** to execute the query.

This will display the first 10 rows of the `weather` table, allowing you to verify the data loaded into the Iceberg catalog.

**Troubleshooting Trino Connection:**
* **Network:** Ensure Trino is on the `app-network`. Use `docker network inspect app-network` to see connected containers.
* **Trino Logs:** Check Trino container logs: `docker logs my-olake-trino-coordinator`.
