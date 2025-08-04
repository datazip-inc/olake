Guide to Trino with Olake Integrations
This guide walks you through how to use Trino to query your Iceberg tables, which are managed by the main Olake Docker stack.

1. Setting up Olake
Here are some common things you might need to set up for your main Olake project.

1.1 Changing the Admin User

The default admin user is admin. To change the username and password, edit the docker-compose.yml file:

# Change these values to set your custom admin credentials.
x-signup-defaults:
  username: &defaultUsername "custom-username"
  password: &defaultPassword "secure-password"
  email: &defaultEmail "email@example.com"

1.2 Changing the Data Directory

The docker-compose.yml file saves your data to olake-data by default. To change this, update the host_persistence_path value:

# Update the path in quotes to a new location on your computer.
x-app-defaults:
  host_persistence_path: &hostPersistencePath /alternate/host/path

Quick Tip: Make sure the folder you choose exists and has the right permissions.

2. Using Trino to Query Your Iceberg Tables
This section shows you how to get Trino running and connected to your data.

2.1 What You Need First

Just make sure these things are ready:

Your main Olake Docker stack is already up and running.

The Iceberg REST Catalog (iceberg-rest-catalog) and MinIO (minio) services are active.

You've already loaded some data into your Iceberg tables using Olake.

2.2 The iceberg.properties File

This file is crucial for connecting Trino to your data. Your examples/trino/etc/catalog/iceberg.properties file must look like this:

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

2.3 How to Run Trino and See Your Data

Follow these steps from the main olake folder:

Go into the Trino folder:

cd examples/trino

Start the Trino container:
This command starts Trino, connects it to your other services, and mounts your iceberg.properties file.

docker run -d \
  --name olake-trino-coordinator \
  --network app-network \
  -p 80:8080 \
  -v "$(pwd)/etc:/opt/trino-server/etc" \
  trinodb/trino:latest

Run a query:

Trino UI: Go to http://localhost:80 and log in with the username admin. In the UI, select Catalog: iceberg and Schema: weather.

DBeaver: If the UI dropdowns don't work, you can use a tool like DBeaver to connect directly to Trino and run your query.

In the query editor, run a query like this to check your data:

SELECT * FROM iceberg.weather.weather LIMIT 10;

3. General Help and Troubleshooting
3.1 Looking at the Logs

For all services:

docker compose logs -f

For a single service:

docker compose logs -f <service_name>

3.2 Checking if Services are Running

To see the status of all your services:

docker compose ps

3.3 Useful Docker Commands

To restart a service:

docker compose restart <service_name>

To stop everything and delete your data (use with caution):

docker compose down -v
