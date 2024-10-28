# colake

Connector ecosystem for Olake, the key points Olake Connectors focusing on
are these
- **Integrated Writers to avoid block of reading and pushing directly into destinations**
- **Connector Autonomy**
- **Avoid operations that doesn't contribute into increasing record throughput**


## Components
### Drivers

Drivers aka Connectors/Source that includes the logic for interacting with database. Upcoming drivers being planned are
- [x] MongoDB
- [ ] Postgres
- [ ] DynamoDB

### Writers

Writers are directly intergrated into drivers to avoid blockage of writing/reading into/from os.StdOut or any other type of I/O. This enables direct insertion of records from each individual fired query to the destination.

Writers are being planned in this order
- [ ] Local
- [ ] Iceberg
- [ ] S3
- [ ] ClickHouse

### Core

Core or framework is the component/logic that has been abstracted out from Connectors to follow DRY. This includes base CLI commands, State logic, Validation logic, Type detection for unstructured data, handling Config, State, Catalog, and Writer config file, etc.

Core includes http server that directly exposes live stats about running sync such as
- Possible finish time
- Conncurrently running processes
- Live record count

Core handles the commands to interact with a driver via these
- spec command: Returns render-able JSON Schema that can be consumed by rjsf libraries in frontend
- check command: performs all necessary checks on the Config, Catalog, State and Writer config
- discover command: Returns all streams and their schema
- sync command: Extracts data out of Source and writes into destinations
- plan command (Unconfirmed): Returns a plan for distributed execution by analyzing State and Catalog


### SDKs

SDKs are libraries/packages that can orchestrate the connector in two environments i.e. Docker and Kubernetes. These SDKs can be directly consumed by users similar to PyAirbyte, DLT-hub.

(Unconfirmed) SDKs can interact with with Connectors via potential GRPC server to override certain default behavior of the system by adding custom functions to enables feature like Transformation, Custom Table Name via writer, or adding hooks.

In Kubernetes mode Golang SDK can orchestrate plan command and horizontally scale and run drivers separately, or vertically scale if failed with OOM.


### Olake

Olake will be built on top of SDK providing persistent storage and a user interface with enabling deployment directly into your machine with default storage mode as Iceberg