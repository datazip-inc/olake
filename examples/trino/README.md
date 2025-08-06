Trino with OLake Integration Guide
This guide details how to use Trino to query your Iceberg tables, which are managed by the main OLake Docker stack.

Prerequisites: This guide assumes you have already set up and are running the main OLake Docker stack as described in the main examples/README.md. This includes active Iceberg REST Catalog (iceberg-rest-catalog) and MinIO (minio) services, and that you have loaded some data into your Iceberg tables.

1. Trino and Iceberg Configuration
Your Trino setup relies on the iceberg.properties file located at examples/trino/etc/catalog/iceberg.properties. This file is pre-configured to connect Trino to your Iceberg catalog and MinIO.

The content of iceberg.properties should be:

# Connects Trino to the Iceberg catalog.
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest-catalog:8181

# Connects Trino to MinIO where the actual data is stored.
iceberg.s3.endpoint=http://minio:9000
iceberg.s3.access-key=minioadmin
iceberg.s3.secret-key=minioadmin
iceberg.s3.path-style-access=true
iceberg.s3.region=us-east-1

2. Running Trino and Querying Data
Follow these steps from the examples directory of your OLake project:

Navigate to the Trino example directory:

cd trino

Start the Trino container:
This command launches the Trino coordinator, connecting it to your existing OLake services and mounting the pre-configured iceberg.properties file.

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

If the Trino UI dropdowns are not visible, you can connect directly using a SQL client like DBeaver. Refer to the official Trino documentation for DBeaver setup.

In your chosen query editor, execute a query to verify your data, for example:

SELECT * FROM iceberg.weather.weather LIMIT 10;

3. Trino Container Troubleshooting
If you encounter issues with the Trino container, these commands can help:

View Trino container logs:

docker logs -f olake-trino-coordinator

Check Trino container status:

docker ps -f name=olake-trino-coordinator

Restart the Trino container:

docker restart olake-trino-coordinator