# Olake S3 Source Driver

Production-ready S3 source connector for Olake that enables ingesting data from Amazon S3 or S3-compatible storage (MinIO, LocalStack, etc.).

## Features

### ✅ Multi-Format Support
- **CSV**: Plain and gzip compressed, with header detection and delimiter configuration
- **JSON**: JSONL, JSON Array, and Single Object formats (plain and gzip)
- **Parquet**: Native columnar format with full type support
- **Compression**: Automatic gzip detection based on file extension (`.gz`)

### ✅ Advanced Sync Capabilities
- **Folder Grouping**: Group files by folder prefix into logical streams
- **Incremental Sync**: Track changes via S3 `LastModified` timestamp
- **State Persistence**: Reliable state management for resumable syncs
- **Parallel Processing**: Configurable concurrent file processing

### ✅ Production Ready
- **Schema Inference**: Intelligent schema detection from file content
- **Error Handling**: Graceful handling of missing files and malformed data
- **S3 Compatible**: Works with AWS S3, MinIO, LocalStack, and other S3-compatible storage
- **Comprehensive Testing**: Full test suite with Docker-based integration tests

## Configuration

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `bucket_name` | string | S3 bucket name |
| `region` | string | AWS region (e.g., "us-east-1") |
| `file_format` | string | File format: `csv`, `json`, or `parquet` |

### Authentication Fields (Optional)

| Field | Type | Description |
|-------|------|-------------|
| `access_key_id` | string | AWS access key ID (optional - see note below) |
| `secret_access_key` | string | AWS secret access key (optional - see note below) |

**Authentication Note**: Both `access_key_id` and `secret_access_key` are **optional**. If omitted, the driver uses AWS default credential chain (IAM roles, environment variables, instance profiles, ECS task roles, etc.). If you provide one, you must provide both.

### Optional Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `path_prefix` | string | `""` | S3 path prefix to filter files |
| `endpoint` | string | AWS S3 | Custom S3 endpoint (MinIO/LocalStack) |
| `max_threads` | integer | `10` | Number of concurrent file processors |
| `retry_count` | integer | `3` | Number of retries for failed operations |

**Note**: Stream grouping is always enabled at level 1 (first folder after path_prefix). This groups all files in the same top-level folder into one stream.

### CSV-Specific Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `delimiter` | string | `","` | CSV field delimiter |
| `has_header` | boolean | `true` | Whether CSV has header row |
| `skip_rows` | integer | `0` | Number of rows to skip at beginning |
| `quote_character` | string | `"\""` | Quote character for fields |

### Compression Handling

**Automatic Detection**: Gzip compression is automatically detected based on file extension:
- `.csv.gz` → Gzipped CSV
- `.json.gz` or `.jsonl.gz` → Gzipped JSON
- Plain files processed without decompression

**No configuration required** - the driver handles this transparently.

## Quick Start

### Automated Setup (Recommended)

The fastest way to test the S3 driver locally:

```bash
cd drivers/s3/examples
./quickstart.sh
```

This automated script will:
1. ✅ Start MinIO and PostgreSQL (Iceberg catalog)
2. ✅ Generate test data (Parquet, CSV, JSON with gzip)
3. ✅ Upload data to MinIO buckets
4. ✅ Build the S3 driver
5. ✅ Run discovery for all formats
6. ✅ Display next steps for syncing

**Access Points**:
- MinIO Console: http://localhost:9001 (admin/password)
- MinIO API: http://localhost:9000
- PostgreSQL: localhost:5432 (postgres/postgres)

### Manual Setup

See the comprehensive guides in `examples/`:
- **[examples/README.md](examples/README.md)** - Overview and quick reference
- **[examples/TESTING_GUIDE.md](examples/TESTING_GUIDE.md)** - Detailed step-by-step guide
- **[examples/SETUP_SUMMARY.md](examples/SETUP_SUMMARY.md)** - Configuration reference

### Testing Individual Formats

#### Parquet Files
```bash
cd drivers/s3/examples

# Discover schemas
../olake discover --config source-parquet.json > catalog_parquet.json

# Sync data
../olake sync \
  --config source-parquet.json \
  --catalog catalog_parquet.json \
  --destination destination.json \
  --state state.json
```

#### CSV Files
```bash
# Discover and sync CSV files (including gzipped)
../olake discover --config source-csv.json > catalog_csv.json
../olake sync --config source-csv.json --catalog catalog_csv.json \
  --destination destination.json --state state.json
```

#### JSON Files
```bash
# Discover and sync JSON files (JSONL, Arrays, gzipped)
../olake discover --config source-json.json > catalog_json.json
../olake sync --config source-json.json --catalog catalog_json.json \
  --destination destination.json --state state.json
```

## Example Configurations

### AWS S3 with Parquet (IAM Role Authentication)

```json
{
  "bucket_name": "my-data-warehouse",
  "region": "us-east-1",
  "path_prefix": "data/",
  "file_format": "parquet",
  "max_threads": 10
}
```

**Note**: No credentials needed when using IAM roles, environment variables, or instance profiles.

### AWS S3 with Static Credentials

```json
{
  "bucket_name": "my-data-warehouse",
  "region": "us-east-1",
  "path_prefix": "data/",
  "access_key_id": "<YOUR_AWS_ACCESS_KEY_ID>",
  "secret_access_key": "<YOUR_AWS_SECRET_ACCESS_KEY>",
  "file_format": "parquet",
  "max_threads": 10
}
```

**Alternative**: Use environment variables:
```bash
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
```
Then omit credentials from config - the driver uses environment variables automatically.

**Folder Structure**:
```
data/
├── users/
│   ├── 2024-01-01/user_data.parquet
│   └── 2024-01-02/user_data.parquet
└── orders/
    ├── 2024-01-01/order_data.parquet
    └── 2024-01-02/order_data.parquet
```
**Result**: Creates 2 streams: `users` and `orders`

### MinIO with CSV (Mixed Compression)

```json
{
  "bucket_name": "analytics",
  "region": "us-east-1",
  "access_key_id": "minioadmin",
  "secret_access_key": "minioadmin",
  "endpoint": "http://localhost:9000",
  "file_format": "csv",
  "csv": {
    "has_header": true,
    "delimiter": ","
  }
}
```

Handles both `.csv` and `.csv.gz` files automatically!

### S3 with JSON (Incremental Sync)

```json
{
  "bucket_name": "event-logs",
  "region": "us-west-2",
  "path_prefix": "events/",
  "access_key_id": "<YOUR_AWS_ACCESS_KEY_ID>",
  "secret_access_key": "<YOUR_AWS_SECRET_ACCESS_KEY>",
  "file_format": "json",
  "max_threads": 5
}
```

**Note**: Automatically detects JSONL, JSON Array, and Single Object formats. Supports `.json` and `.json.gz` files.

## Project Structure

```
drivers/s3/
├── internal/                    # Core implementation
│   ├── s3.go                   # Main driver (discovery, setup)
│   ├── config.go               # Configuration validation
│   ├── backfill.go             # Backfill logic (chunking, file processing)
│   ├── incremental.go          # Incremental sync logic
│   ├── types.go                # Type definitions
│   └── s3_test.go              # Unit tests (154+ test cases)
├── pkg/parser/                  # Reusable parser package
│   ├── parser.go               # Parser interface
│   ├── csv.go                  # CSV parser with schema inference
│   ├── json.go                 # JSON parser (JSONL/Array/Object)
│   └── parquet.go              # Parquet parser with streaming
├── resources/
│   └── spec.json               # Configuration specification
├── examples/                    # Testing and examples
│   ├── docker-compose.yml      # MinIO + PostgreSQL setup
│   ├── quickstart.sh           # Automated testing script
│   ├── generate_test_data.py   # Test data generator
│   ├── upload_to_minio.sh      # MinIO upload script
│   ├── source-*.json           # Example source configs (3)
│   ├── destination.json        # Iceberg destination config
│   ├── catalog_*.json          # Example catalogs (3)
│   ├── README.md               # Examples overview
│   ├── TESTING_GUIDE.md        # Detailed testing guide
│   └── SETUP_SUMMARY.md        # Setup reference
├── testdata/
│   └── create_parquet.go       # Parquet test file generator
├── go.mod                       # Go module definition
├── main.go                      # Driver entry point
└── README.md                    # This file
```

## How It Works

### 1. Discovery Phase
- Lists S3 objects in specified bucket/prefix
- Filters by file format (`.csv`, `.json`, `.jsonl`, `.parquet`)
- Groups files by first folder level after `path_prefix`
- Each top-level folder becomes a stream (e.g., `users/`, `orders/`)

### 2. Schema Inference
- Downloads first file from each stream
- Parses content based on format:
  - **CSV**: Reads header and samples rows for type detection (uses AND logic for type inference)
  - **JSON**: Auto-detects JSONL/Array/Object format, infers field types (uses AND logic)
  - **Parquet**: Reads native schema from file metadata using S3 range requests
- Generates `types.Stream` with columns and data types
- Adds `_last_modified_time` field as cursor for incremental sync

### 3. Backfill/Sync
- Groups files into 2GB chunks for efficient processing
- Large files (>2GB) processed individually with streaming
- Processes files in parallel using configurable `max_threads`
- Reads records in batches (format-specific parsing)
- Injects `_last_modified_time` into every record
- Sends batches to destination (Iceberg)
- Handles compression transparently (`.gz` files)

### 4. Incremental Sync
- Uses S3 `LastModified` timestamp as cursor
- Compares with previous sync state
- Only processes files with `LastModified > last_synced_timestamp`
- Updates state after successful sync
- Supports both full refresh and incremental modes

## State Management

**Stream-Level State**: The driver implements `StateType()` returning `types.StreamType` for proper framework integration.

**Cursor Tracking**:
- **Cursor Field**: `_last_modified_time` (S3 timestamp)
- **Granularity**: Per-stream (folder)
- **Storage**: JSON state file with stream cursors
- **Behavior**: Only sync files modified since last run

**Example State**:
```json
{
  "users": {
    "_last_modified_time": "2024-01-15T10:30:00Z"
  },
  "orders": {
    "_last_modified_time": "2024-01-15T11:45:00Z"
  }
}
```

## Sync Modes

1. **Full Refresh**: Syncs all files every time (no state tracking)
2. **Incremental**: Syncs only new/modified files (state-based)

Configure in `streams.json`:
```json
{
  "stream": "users",
  "sync_mode": "incremental",
  "cursor_field": "_last_modified_time"
}
```

## Performance Considerations

- **Parallel Processing**: Configure `max_threads` for concurrent file processing
- **Discovery Time**: Proportional to stream count × file size (schema inference)
- **Incremental Efficiency**: Only processes changed files (can be 100x faster)
- **Large Files**: Each file processed as single chunk (no partial reads yet)

## Known Limitations

1. **Schema Evolution**: Manual re-discovery needed if file schema changes
2. **Compression**: Only gzip supported (no zip/bzip2)
3. **Stream Grouping**: Fixed at level 1 (first folder only)

## Troubleshooting

### Connection Issues

**MinIO Not Accessible**:
```bash
# Check MinIO is running
docker ps | grep minio

# Test MinIO health
curl http://localhost:9000/minio/health/live

# Restart services
cd examples/
docker compose down && docker compose up -d
```

**AWS S3 Authentication Errors**:
- Verify access key and secret key are correct
- Check IAM permissions include `s3:ListBucket` and `s3:GetObject`
- Ensure region matches bucket location

### Discovery Issues

**No Streams Found**:
- Check `path_prefix` matches your folder structure
- Verify `file_format` matches file extensions (`.csv`, `.json`, `.parquet`)
- Ensure files are organized in folders (e.g., `bucket/prefix/stream_name/files`)
- Check bucket and prefix exist: `aws s3 ls s3://bucket-name/prefix/`

**Files Missing from Stream**:
- Files must have correct extension for format
- Gzip files must have `.gz` suffix
- Ensure files aren't empty (0 bytes)

### Schema Inference Errors

**CSV Parsing Failed**:
- Verify `has_header` setting matches file structure
- Check `delimiter` is correct (comma, tab, pipe, etc.)
- Ensure all rows have same column count
- Try with plain CSV before gzipped version

**JSON Format Detection Failed**:
- Check JSON is valid: `jq . < file.json`
- Ensure consistent field names across records
- Verify not mixing JSONL and JSON Array in same folder
- Try decompressing `.gz` files manually first

**Parquet Schema Error**:
- Verify file is valid Parquet: `parquet-tools schema file.parquet`
- Check file wasn't corrupted during upload
- Ensure compatible Parquet version (tested with Parquet 1.0+)

### Sync Issues

**Incremental Sync Not Working**:
- Verify `state.json` file is being persisted
- Check `sync_mode: "incremental"` in catalog
- Ensure `cursor_field: "_last_modified_time"` is set
- Check state file permissions (must be writable)

**Out of Memory**:
- Reduce `max_threads` (fewer concurrent files)
- Split large files before upload
- Process fewer streams at once

### Debug Mode

Enable detailed logging:
```bash
export LOG_LEVEL=debug
../olake sync --config source.json --catalog catalog.json \
  --destination destination.json --state state.json
```

Check logs in `examples/logs/` directory.

## Testing

### Run Unit Tests
```bash
cd drivers/s3
go test ./internal/... -v
```

Expected output:
```
ok  	github.com/datazip-inc/olake/drivers/s3/internal	1.116s
```

### Integration Tests
See [examples/TESTING_GUIDE.md](examples/TESTING_GUIDE.md) for comprehensive integration testing.

## Contributing

When adding new features:

1. **Write Tests**: Add unit tests in `internal/*_test.go`
2. **Update Docs**: Update this README and relevant guides
3. **Test Locally**: Use `examples/quickstart.sh` for end-to-end testing
4. **Follow Patterns**: Match code style of existing drivers
5. **Document Changes**: Update this README

## Additional Resources

- **[examples/TESTING_GUIDE.md](examples/TESTING_GUIDE.md)** - Comprehensive testing guide
- **[examples/README.md](examples/README.md)** - Examples overview and quick reference
- **[examples/SETUP_SUMMARY.md](examples/SETUP_SUMMARY.md)** - Configuration reference
