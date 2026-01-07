# Olake S3 Source Driver
Production-ready S3 source connector for Olake that ingests data directly from AWS S3 or S3-compatible storage (MinIO, LocalStack, etc.).

## Highlights
- **Multi-format**: CSV (plain or `.gz`), JSON (JSONL/array/object), and Parquet with schema inference.
- **Incremental sync**: Tracks `_last_modified_time` per stream and processes only newer files.
- **Parallel processing**: Chunked downloads and configurable `max_threads` keep throughput high.
- **Stateful**: Stream-level state keeps cursor information so you can resume syncs reliably.

## Configuration
### Required fields
| Field | Type | Description |
| --- | --- | --- |
| `bucket_name` | string | Target S3 bucket name |
| `region` | string | AWS region (e.g., `us-east-1`) |
| `file_format` | string | `csv`, `json`, or `parquet` |
| `path_prefix` | string | Prefix used to group files into streams |

### Optional fields
| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `access_key_id` | string | — | Static AWS access key (pair with `secret_access_key`) |
| `secret_access_key` | string | — | Static AWS secret key (pair with `access_key_id`) |
| `endpoint` | string | AWS S3 | Override S3 endpoint (MinIO/LocalStack) |
| `max_threads` | integer | 10 | Concurrent file download/parsing workers |
| `retry_count` | integer | 3 | Retries for transient failures |
| `compression` | string | — | Override auto-detected compression (`gzip` or `none`)

**Authentication note**: Omitting credentials lets the driver fall back to the AWS default credential chain (environment variables, IAM roles, instance profiles, etc.). If you provide one static credential, include the other as well.

### CSV-specific tuning
Use the nested `csv` block to customize parsing.
| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `delimiter` | string | `","` | Field delimiter |
| `has_header` | boolean | `true` | Whether the first row is a header |
| `skip_rows` | integer | 0 | Rows to skip before parsing |
| `quote_character` | string | `\"\"\"` | Quote character for fields |

### JSON-specific tuning
Use the `json` block to override parsing of JSON files.
| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `line_delimited` | boolean | `true` | When true, treat each line as a separate record; set to false for JSON arrays or single objects |

## Commands
Run the driver binaries through the repository root `build.sh` helper:
```
./build.sh driver-s3 discover --config /path/to/source.json
./build.sh driver-s3 sync --config /path/to/source.json --catalog /path/to/catalog.json --destination /path/to/destination.json --state /path/to/state.json
```
- `discover` generates a `streams.json` catalog describing each folder and inferred columns.
- `sync` processes files in ~2 GB chunks, injects `_last_modified_time` as the cursor, and pushes records to the destination.
- The `state.json` file records `_last_modified_time` per stream so subsequent runs only process changed files.

## Example configuration snippets
Use these as a starting point; substitute your bucket, path, and authentication values.
### JSON stream example
```json
{
  "bucket_name": "your-bucket",
  "region": "us-east-1",
  "path_prefix": "data/json/",
  "file_format": "json",
  "json": { "line_delimited": true },
  "compression": "gzip",
  "max_threads": 5,
  "retry_count": 3
}
```
### CSV stream example
```json
{
  "bucket_name": "your-bucket",
  "region": "us-east-1",
  "path_prefix": "data/csv/",
  "file_format": "csv",
  "csv": { "has_header": true, "delimiter": "," },
  "max_threads": 5,
  "retry_count": 3
}
```
### Parquet stream example
```json
{
  "bucket_name": "your-bucket",
  "region": "us-east-1",
  "path_prefix": "data/parquet/",
  "file_format": "parquet",
  "compression": "none",
  "max_threads": 5,
  "retry_count": 3
}
```

## Catalog guidance (streams)
- `selected_streams` selects which folders (streams) to sync; each entry maps to a stream name under your prefix (e.g., `users`).
- Each `streams[]` entry includes the inferred schema, available sync modes (`full_refresh`, `incremental`), and the cursor field (`_last_modified_time`).
- `_last_modified_time` is added to every stream so you can configure incremental syncs per folder.

Example catalog structure:
```json
{
  "selected_streams": {
    "data": [
      { "stream_name": "users", "partition_regex": "" },
      { "stream_name": "orders", "partition_regex": "" }
    ]
  },
  "streams": [
    {
      "stream": {
        "name": "users",
        "namespace": "data",
        "supported_sync_modes": ["full_refresh", "incremental"],
        "cursor_field": "_last_modified_time",
        "sync_mode": "incremental"
      }
    }
  ]
}
```

## State guidance
The `state.json` structure mirrors the catalog streams and records the latest `_last_modified_time` per stream.
```json
{
  "type": "STREAM",
  "streams": [
    { "stream": "users", "state": { "_last_modified_time": "2025-01-01T00:00:00Z", "chunks": [] } }
  ]
}
```
Use this file when re-running syncs to resume from the last `_last_modified_time` per stream.

Find more at [Postgres Docs](https://olake.io/docs/category/s3)
