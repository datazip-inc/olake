# Contributing to Olake Iceberg Java Writer

Following instructions specifically apply to contributing to Iceberg writer code in this folder.

## 1. Architecture

The data flow in this project is as follows:

```
Golang Code  --gRPC-->  Java (This Project)  --Write to Iceberg-->  S3 + Iceberg Catalog
```

The Java component serves as a bridge between the Go code in Olake and Apache Iceberg. When changes are made to schemas or data, the Go code sends these through gRPC to the Java writer, which then manages the storage in Apache Iceberg tables.

## 2. Prerequisites

Before you begin development on the Java component, ensure you have:

- JDK 17 or higher
- Maven 3.6.3 or higher
- An IDE with Java support (VSCode with Java extensions recommended)
- Docker & Docker compose (for local testing)

## 3. Setting up the Olake Project

Before working on the Java component, you need to set up the main Olake project:

1. Follow the installation instructions in the [main Olake documentation](https://olake.io/docs).

2. Make sure you can build and run the Olake project successfully before proceeding with Java component development.

This setup is essential because the Java component is called from the Go code, and you'll need a working Olake installation to test your changes.

## 4. Development Environment Setup for Java project

### Setup

In this mode, you'll run the Go code, which will start the Java process, and then attach a debugger to the Java process.

1. **Open java code as a project in a separate VSCode window** for easier development and debugging.
2. When testing with Go code integration, configure only one stream in your `streams.json` source configuration. This is important because a separate Java process is created per stream.

#### Debugging Steps

1. Enable debug mode in your Iceberg writer configuration by adding `"debug_mode": true` to your writer config:
   ```json
   {
     "type": "ICEBERG",
     "writer": {
       "debug_mode": true,
       "catalog_type": "jdbc",
       // other config options...
     }
   }
   ```

2. Create a VSCode launch profile in your Java project:
   ```json
   {
       "type": "java",
       "name": "Attach Java (port 5005)",
       "request": "attach",
       "hostName": "localhost",
       "port": 5005,
       "projectName": "olake-iceberg-java-writer"
   }
   ```

3. Start the Go application. The Go process will start the Java process in debug mode and wait for a debugger to attach (Step 5).

4. Set breakpoints in your Java code where needed.

5. In your Java VSCode window, run the "Attach Java" debug configuration to connect to the waiting Java process.

6. Once the debugger is attached, the Java process will continue execution and hit your breakpoints.

#### After Making Changes to Java code

##### If you are running Go vscode-debug mode + java vscode-debug mode

1. Delete the JAR file `olake-iceberg-java-writer.jar` from the Go project's base directory.
3. After making changes to the Java code, rebuild the JAR using:
   ```
   mvn clean package -DskipTests
   ```
4. Run your Go code with the VSCode debugger and attach java debugger same way.

##### If you are running Go via `./build sync ...` command + java vscode-debug mode
1. Delete the JAR file `olake-iceberg-java-writer.jar` from the Go project's base directory.
2. Delete the `target` folder from `writers/iceberg/olake-iceberg-java-writer`.
3. Run `./build sync ...` command again to rebuild everything (including the jar again) and attach java debugger same way.

## 5. Testing

For testing the Java component, you'll need to set up a local environment.

### Testing Setup

1. Run the local test environment:
   ```shell
   cd writers/iceberg/local-test
   docker compose up
   ```

2. Configure JDBC catalog in your writer.json:
   ```json
   {
     "type": "ICEBERG",
     "writer": {
       "catalog_type": "jdbc",
       "jdbc_url": "jdbc:postgresql://localhost:5432/iceberg",
       "jdbc_username": "iceberg",
       "jdbc_password": "password",
       "iceberg_s3_path": "s3a://warehouse",
       "s3_endpoint": "http://localhost:9000",
       "s3_use_ssl": false,
       "s3_path_style": true,
       "aws_access_key": "admin",
       "aws_secret_key": "password",
       "iceberg_db": "olake_iceberg",
       "debug_mode": true
     }
   }
   ```

3. Run the sync

### Verifying Data

To verify the data was properly stored:
```shell
# Connect to the spark-iceberg container
docker exec -it spark-iceberg bash

# Start spark-sql (rerun it if error occurs)
spark-sql

# Query in format select * from catalog_name.iceberg_db_name.table_name
select * from olake_iceberg.olake_iceberg.<table_name>;
```

## 6. Getting Help

For any questions or issues specifically related to the Iceberg Java Writer, you can:

1. Check the additional testing information in `./olake-iceberg-java-writer/README.md`
2. Join the [Olake Slack channel](https://join.slack.com/t/getolake/shared_invite/zt-2usyz3i6r-8I8c9MtfcQUINQbR7vNtCQ) for community support
3. Submit an issue on the [GitHub repository](https://github.com/datazip-inc/olake/issues)

