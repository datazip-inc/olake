# Java Iceberg Sink

This project is a standalone gRPC writer that OLake uses to write data into Iceberg. It does not use or depend on Debezium. It was originally derived from [debezium-server-iceberg](https://github.com/memiiso/debezium-server-iceberg) (Apache License 2.0) and has since been rewritten for OLake; see `Olake-changes-notice.txt` for attribution details.

## Architecture

The data flow in this project is as follows:


Golang Code  --gRPC-->  Java (This Project)  --Write to Iceberg-->  S3 + Iceberg Catalog

(Check out the Olake Iceberg Writer code to understand how data is sent to Java via gRPC.)

## Development and Testing

For detailed instructions on setting up the development environment, prerequisites, running, debugging, and testing this component, please refer to the [CONTRIBUTING.md](./CONTRIBUTING.md) file.