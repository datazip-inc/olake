# Java Iceberg Sink

This project is a fork and modified version of the [debezium-server-iceberg](https://github.com/memiiso/debezium-server-iceberg) project, originally used to dump data from Debezium Server into Iceberg.

## Architecture

The data flow in this project is as follows:


Golang Code  --gRPC-->  Java (This Project)  --Write to Iceberg-->  S3 + Iceberg Catalog

(Check out the Olake Iceberg Writer code to understand how data is sent to Java via gRPC.)

## Development and Testing

For detailed instructions on setting up the development environment, prerequisites, running, debugging, and testing this component, please refer to the [CONTRIBUTING.md](./CONTRIBUTING.md) file.