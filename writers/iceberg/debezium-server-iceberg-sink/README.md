# Debezium Server Iceberg Sink

This project provides an Iceberg sink for Debezium Server, allowing change data capture (CDC) events to be written to Iceberg tables.

## File Size Management Options

This project offers two approaches to optimize Iceberg table writes based on Parquet file size:

### 1. Dynamic File Size Management (Recommended)

The `iceberg-dynamic` consumer leverages Iceberg's native file size management capabilities:

```properties
# Use the dynamic file size consumer
debezium.sink.type=iceberg-dynamic

# Set the target file size in MB (default: 256)
debezium.sink.iceberg.target-file-size-mb=256

# Set the commit interval in milliseconds (default: 60000)
debezium.sink.iceberg.commit-interval-ms=60000
```

#### How It Works

1. **Native Iceberg Integration**: Sets the `write.target-file-size-bytes` property directly on Iceberg tables.
2. **Time-based Commits**: Flushes data based on a configurable time interval.
3. **Accumulation**: Maintains buffers of records per table that are committed at appropriate intervals.
4. **Let Iceberg Handle It**: Relies on Iceberg's built-in mechanisms to manage file sizes.

#### Benefits

- **Fully Dynamic**: Uses Iceberg's native file size management
- **More Resilient**: Less complex batching logic reduces potential issues
- **Better Integration**: Works directly with Iceberg's internal mechanisms
- **Tunable**: Can be adjusted through simple configuration

### 2. Estimated Size-based Batching (Alternative)

The `ParquetSizeWait` approach (for use with the standard `iceberg` consumer) estimates file sizes and controls batch timing:

```properties
# Use the standard consumer with ParquetSizeWait
debezium.sink.type=iceberg
debezium.sink.batch.batch-size-wait=ParquetSizeWait

# Set the target Parquet file size in MB (default: 256)
debezium.sink.batch.parquet-size-wait.target-size-mb=256

# Set the estimated average record size in bytes (default: 1024)
debezium.sink.batch.parquet-size-wait.avg-record-size-bytes=1024

# Optionally configure the target file size directly for Iceberg (in MB)
debezium.sink.iceberg.target-file-size-mb=256
```

#### How It Works

1. **Size Estimation**: Accumulates records and estimates total size based on configured average record size.
2. **Write Triggering**: Writes to Iceberg when either:
   - Estimated size reaches target (default 256MB)
   - No more records are available in queue

## Considerations for Choosing an Approach

- **Dynamic Approach**: Best for production environments where you want Iceberg to handle file size management 
  natively without external estimation.

- **Estimated Approach**: Useful when you need more control over exactly when commits happen, but relies on 
  accurate size estimation.

For most use cases, the dynamic approach is recommended as it provides better integration with Iceberg's 
internal mechanisms.

## Other Configuration

For other configuration options, please refer to the Debezium Server and Iceberg documentation. 