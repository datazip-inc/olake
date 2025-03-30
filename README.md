# OLake Documentation

## Overview
OLake is an open-source tool designed for efficient replication of databases to Apache Iceberg or Data Lakehouses. It supports databases like PostgreSQL, MongoDB, and MySQL, making it easy to sync and manage data in modern storage solutions.

## Features
- **Database Replication**: Seamlessly replicate data from various databases.
- **Apache Iceberg Support**: Enables efficient storage and querying of data.
- **Data Lakehouse Integration**: Supports modern data architectures.
- **High Performance**: Optimized for fast and reliable data transfer.
- **Open Source**: Community-driven and actively maintained.

## Installation
### Prerequisites
Ensure you have the following installed:
- Docker
- Python 3.x
- PostgreSQL, MongoDB, or MySQL (as required)

### Steps
1. Clone the OLake repository:
   ```sh
   git clone https://github.com/datazip-inc/olake.git
   cd olake
   ```
2. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```
3. Run the application:
   ```sh
   docker-compose up
   ```

## Usage
### Basic Configuration
Update the `config.yaml` file with your database credentials and target storage details.

### Running OLake
To start data replication, run:
```sh
python olake.py --config config.yaml
```

## Troubleshooting Guide

This guide provides solutions to common issues encountered while using OLake. If you experience a problem not listed here, please refer to our [GitHub Issues Page](https://github.com/datazip-inc/olake/issues) or contact our support team.

### 1. Parquet File Not Closing Properly
**Issue:** During data replication, you may encounter an error indicating that a Parquet file is not closed when a new one is being created.

**Solution:**
- Ensure that OLake has the necessary permissions to write and close files in the target directory.
- Verify that there is sufficient disk space available.
- If the issue persists, consider updating to the latest version of OLake, as this may be a known issue addressed in recent updates.

*Reference: [GitHub Issue #184](https://github.com/datazip-inc/olake/issues/184)*

### 2. Slow Iceberg Writer Performance in Docker
**Issue:** When running OLake in a Docker environment, the Iceberg writer operates significantly slower than expected.

**Solution:**
- Allocate more resources (CPU and memory) to the Docker container running OLake.
- Optimize the Docker environment by ensuring that the host machine has adequate resources and that other containers are not consuming excessive resources.
- Check for any network latency issues that might affect data transfer rates.

*Reference: [GitHub Issue #172](https://github.com/datazip-inc/olake/issues/172)*

### 3. Using `documentKey` Instead of `_id` in Sharded MongoDB
**Issue:** In a sharded MongoDB setup, change event keys may not be correctly identified when using the `_id` field.

**Solution:**
- Modify the OLake configuration to use the `documentKey` field instead of `_id` for change event keys in sharded environments.
- Ensure that the `documentKey` field is correctly indexed in your MongoDB collections to improve performance.

*Reference: [GitHub Issue #171](https://github.com/datazip-inc/olake/issues/171)*

### 4. Incremental Sync Not Supported
**Issue:** Incremental synchronization for MongoDB and PostgreSQL is not currently supported.

**Solution:**
- Monitor the [GitHub Issues Page](https://github.com/datazip-inc/olake/issues) for updates on the implementation of incremental sync features.
- Consider contributing to the development of this feature by following our [Contributing Guidelines](https://github.com/datazip-inc/olake/blob/master/CONTRIBUTING.md).

*References: [GitHub Issues #161](https://github.com/datazip-inc/olake/issues/161) and [#160](https://github.com/datazip-inc/olake/issues/160)*

### 5. Postgres Inconsistency in CDC and Backfill Data Types
**Issue:** There may be inconsistencies in data types between Change Data Capture (CDC) and backfill processes in PostgreSQL.

**Solution:**
- Review and standardize the data type mappings used in both CDC and backfill processes.
- Test the data replication process with a subset of data to identify and resolve type mismatches before performing a full replication.

*Reference: [GitHub Issue #152](https://github.com/datazip-inc/olake/issues/152)*

## Contributing
We welcome contributions from the community! To contribute:
1. Fork the repository.
2. Create a new branch for your feature/fix.
3. Submit a pull request with a clear description of your changes.

## Need Assistance?
If you have any questions or uncertainties about setting up OLake, contributing to the project, or troubleshooting issues, we're here to help:

- **Email Support:** Reach out to our team at [hello@olake.io](mailto:hello@olake.io) for prompt assistance.
- **Join our Slack Community:** Engage with other users and the development team to discuss roadmaps, report bugs, and seek debugging help.
- **Schedule a Call:** If you prefer a one-on-one conversation, schedule a call with our CTO and team.

Your success with OLake is our priority. Don’t hesitate to contact us if you need any help or further clarification!

## License
OLake is released under the MIT License. See `LICENSE` for details.

## Contact
For support and discussion, open an issue on GitHub or reach out via the OLake community forum.
