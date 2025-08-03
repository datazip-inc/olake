Guide to Trino with Olake Integrations
This guide walks you through how to use Trino to query your Iceberg tables, which are managed by the main Olake Docker stack. We'll cover the basic setup, what you need to know, and some common fixes.

1. Setting up Olake
Here are some common things you might need to set up for your main Olake project.

1.1 Changing the Admin User

Olake automatically makes an admin user when it starts. The default username is admin. If you want to change the default username and password, you can edit this section in your docker-compose.yml file:

x-signup-defaults:
username: &defaultUsername "custom-username"
password: &defaultPassword "secure-password"
email: &defaultEmail "email@example.com"






1.2 Changing the Data Directory

The docker-compose.yml file usually saves your data to a folder called olake-data. You can change this to a different folder on your computer if you want:

x-app-defaults:
  host_persistence_path: &hostPersistencePath /alternate/host/path






Quick Tip: Make sure the folder you choose actually exists and that you have permission to write to it. If you're on Linux or macOS, you might need to change file permissions.

2. Using Trino to Query Your Iceberg Tables
This is the main part. It tells you how to get Trino running and connected so you can see your data.

2.1 What You Need First

Just make sure these things are ready before you start:

Your main Olake Docker stack is already up and running.

The Iceberg REST Catalog (iceberg-rest-catalog) and MinIO (minio) services are active in your Docker app-network.

You've already put some data into your Iceberg tables using Olake.

2.2 The Super Important iceberg.properties File

This file is the key to connecting Trino to your data. If something is wrong here, it won't work. To make sure Trino can find your Iceberg catalog and get data from MinIO, your examples/trino/etc/catalog/iceberg.properties file must look like this:

connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest-catalog:8181
iceberg.s3.endpoint=http://minio:9000
iceberg.s3.access-key=minioadmin
iceberg.s3.secret-key=minioadmin
iceberg.s3.path-style-access=true
iceberg.s3.region=us-east-1






iceberg.rest-catalog.uri: This is the address for your Iceberg Catalog. http://iceberg-rest-catalog:8181 is usually the right one.

iceberg.s3.endpoint: This is the address for MinIO, which is where your actual data is stored.

iceberg.s3.access-key and iceberg.s3.secret-key: These are the login details for MinIO.

2.3 How to Run Trino and See Your Data

Just follow these steps from the main olake folder:

Go into the Trino folder:

cd examples/trino






Start the Trino container:
This command starts Trino, connects it to your other services, and uses the iceberg.properties file you just set up.

docker run -d --name olake-trino-coordinator \
  --network app-network \
  -p 80:8080 \
  -v "$(pwd)/etc:/opt/trino-server/etc" \
  trinodb/trino:latest






Use the Trino website to run a query:

Open the Trino UI:

Go to http://localhost:80 in your web browser.

The username is admin.

Pick your Catalog and Schema:

In the dropdown menus, choose Catalog: iceberg and Schema: weather.

Quick Tip: If you don't see the dropdown menus, it likely means there is a connectivity issue. In that case, you can use a tool like DBeaver to connect directly to Trino and run your queries.

Run a query:

Type your query in the editor. For example, to check your data:

SELECT * FROM iceberg.weather.weather LIMIT 10;






Click Run.

If it all works, you'll see your data right there!

3. General Help and Troubleshooting
3.1 Looking at the Logs

For all services:

docker compose logs -f






For just one service (like Trino):

docker compose logs -f <service_name>






(To see the Trino logs specifically, you can also use docker logs -f olake-trino-coordinator)

3.2 Checking if Services are Running

To see the status of all your services:

docker compose ps






3.3 Useful Docker Commands

To restart a service:

docker compose restart <service_name>






To stop everything and delete your data (be careful!):

docker compose down -v






