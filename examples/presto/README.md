# Querying Olake-Managed Iceberg Tables with Presto

This guide explains how to run Presto as an external Docker container to query Apache Iceberg tables created by the main Olake Docker Compose stack.

It assumes:
1.  The main Olake Docker Compose stack (from the parent directory's `docker-compose.yml`) is already up and running.
2.  Services like the Iceberg REST Catalog (`rest`) and MinIO (`minio`) are active on the `app-network`.
3.  You have populated Iceberg tables using Olake.
4.  The necessary Presto configuration files are present in the `./etc/` subdirectory (relative to this `presto` directory).

## Steps to Run Presto

1. **Navigate to the Presto Directory:**
   ```bash
   cd presto
   ```

2. **Run the Presto Docker Container:**
   ```bash
   docker run -d --name olake-presto-coordinator \
     --network app-network \
     -p 80:8080 \
     -v "$(pwd)/etc:/opt/presto-server/etc" \
     prestodb/presto:latest
   ```

3. **Query Data Using Presto UI:**

   1. **Access the Presto UI:**
      - Open your browser and go to `http://localhost:80`.

   2. **Navigate to the SQL Client:**
      - Click on **SQL CLIENT** located at the top right of the UI.

   3. **Select the Catalog and Schema:**
      - Choose **Catalog: iceberg** and **Schema: weather** from the dropdown menus.

   4. **Run the Query:**
      - Enter the following query in the SQL editor:
        ```sql
        SELECT * FROM iceberg.weather.weather LIMIT 10;
        ```
      - Click **Run** to execute the query.

This will display the first 10 rows of the `weather` table, allowing you to verify the data loaded into the Iceberg catalog.

**Troubleshooting Presto Connection:**
* **Network:** Ensure Presto is on the `app-network`. Use `docker network inspect app-network` to see connected containers.
* **Presto Logs:** Check Presto container logs: `docker logs my-olake-presto-coordinator`.