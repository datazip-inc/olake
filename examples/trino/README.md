Trino with OLake Integration Guide
This guide details how to use Trino to query Iceberg tables managed by the main OLake Docker stack.

Prerequisites: Ensure the main OLake Docker stack is running, including the iceberg-rest-catalog and minio services, and that you have loaded some data into your Iceberg tables.

1. Trino and Iceberg Configuration
The Trino example is pre-configured with the necessary iceberg.properties file at examples/trino/etc/catalog/iceberg.properties.

2. Running Trino and Querying Data
Follow these steps from the examples directory of your OLake project:

Navigate to the Trino example directory:

cd trino

Start the Trino container:
This command launches the Trino coordinator and connects it to your existing OLake services.

docker run -d \
  --name olake-trino-coordinator \
  --network app-network \
  -p 80:8080 \
  -v "$(pwd)/etc:/opt/trino-server/etc" \
  trinodb/trino:latest

Run a query:

Using the Trino UI:

Open your web browser and go to http://localhost:80.

Log in with the username admin.

In the UI, select Catalog: iceberg and Schema: weather.

Using DBeaver:

If the Trino UI dropdowns are not visible, you can connect directly using a SQL client like DBeaver. Refer to the official documentation here: https://dbeaver.com/docs/dbeaver/Database-driver-Trino/.

Execute a query to verify your data, for example:

SELECT * FROM iceberg.weather.weather LIMIT 10;

3. Trino Container Troubleshooting
If you encounter issues with the Trino container, these commands can help:

View Trino container logs:

docker logs -f olake-trino-coordinator

Check Trino container status:

docker ps -f name=olake-trino-coordinator

Restart the Trino container:

docker restart olake-trino-coordinator

