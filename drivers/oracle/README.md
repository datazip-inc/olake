# Oracle Driver for Olake

This directory contains the Oracle database driver implementation for Olake, providing integration with Oracle databases for data synchronization and CDC (Change Data Capture).

## Features

- **Full Refresh Sync**: Complete table synchronization
- **CDC Support**: Change Data Capture for real-time data changes
- **Schema Discovery**: Automatic table and column discovery
- **Type Mapping**: Oracle data type to Olake type conversion
- **Connection Pooling**: Efficient database connection management

## Configuration

The Oracle driver supports the following configuration parameters:

```json
{
  "hosts": "localhost",
  "username": "system",
  "password": "oracle",
  "database": "XE",
  "port": 1521,
  "service_name": "XE",
  "sid": "",
  "tls_skip_verify": false,
  "max_threads": 4,
  "backoff_retry_count": 3,
  "update_method": {
    "intial_wait_time": 10
  }
}
```

### Configuration Parameters

- `hosts`: Oracle database host (default: localhost)
- `username`: Database username (required)
- `password`: Database password (required)
- `database`: Database name (default: XE)
- `port`: Database port (default: 1521)
- `service_name`: Oracle service name (default: XE)
- `sid`: Oracle SID (alternative to service_name)
- `tls_skip_verify`: Skip TLS verification (default: false)
- `max_threads`: Maximum number of concurrent connections (default: 4)
- `backoff_retry_count`: Number of retry attempts (default: 3)
- `update_method.intial_wait_time`: CDC initialization wait time in seconds (default: 10)

## Supported Data Types

The Oracle driver maps Oracle data types to Olake data types as follows:

| Oracle Type | Olake Type |
|-------------|------------|
| VARCHAR2, NVARCHAR2, CHAR, NCHAR, CLOB, NCLOB, LONG | String |
| NUMBER, FLOAT, BINARY_FLOAT, BINARY_DOUBLE | Float |
| DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH LOCAL TIME ZONE | DateTime |
| BLOB, BFILE, RAW, LONG RAW, BINARY, VARBINARY | Binary |
| XMLTYPE, JSON | String |
| BOOLEAN | Boolean |

## Testing

### Prerequisites

1. Docker and Docker Compose installed
2. Oracle database running (can be started via Docker Compose)

### Running Tests

1. Start the Oracle database:
   ```bash
   cd drivers/oracle
   docker-compose up -d
   ```

2. Wait for the database to be ready (check health status)

3. Run the integration tests:
   ```bash
   go test -v ./internal/
   ```

### Test Coverage

The integration tests cover:
- **Setup**: Database connection and configuration validation
- **Discover**: Table and schema discovery
- **Read**: Full refresh and CDC data reading

## CDC Implementation

The Oracle driver includes placeholder implementations for CDC functionality. In a production environment, you would need to implement:

1. **Oracle Streams**: For capturing database changes
2. **GoldenGate**: For real-time data replication
3. **Redo Log Monitoring**: For direct redo log access
4. **Change Record Processing**: Converting Oracle-specific change records to Olake format

### TODO for Production CDC

- [ ] Implement Oracle Streams integration
- [ ] Add GoldenGate support
- [ ] Implement redo log monitoring
- [ ] Add change record conversion logic
- [ ] Implement CDC state management
- [ ] Add proper error handling and recovery

## Development

### Building

```bash
cd drivers/oracle
go build -o oracle-driver main.go
```

### Running

```bash
./oracle-driver
```

## Troubleshooting

### Common Issues

1. **Connection Refused**: Ensure Oracle database is running and accessible
2. **Authentication Failed**: Verify username/password and user permissions
3. **Service Not Found**: Check service_name or SID configuration
4. **Port Already in Use**: Ensure port 1521 is available

### Logs

Enable debug logging by setting the appropriate log level in your configuration.

## Contributing

When contributing to the Oracle driver:

1. Follow the existing code style and patterns
2. Add tests for new functionality
3. Update documentation for configuration changes
4. Ensure compatibility with supported Oracle versions

## License

This driver is part of the Olake project and follows the same licensing terms. 