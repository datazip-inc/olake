# S3 Driver Testing Guide

Complete guide for testing the S3 driver with Parquet, CSV, and JSON files (including gzip compression support).

## Prerequisites

Before starting, ensure you have:
- Docker and Docker Compose installed
- Python 3.7+ with `pandas` and `pyarrow`
- Go 1.25+ (for building OLake)

Install Python dependencies:
```bash
pip install pandas pyarrow
```

## Quick Start (Automated)

Run the complete setup in one command:

```bash
cd drivers/s3/examples
./quickstart.sh
```

This will:
1. ✅ Start MinIO and PostgreSQL services
2. ✅ Generate test data (Parquet, CSV, JSON with gzip)
3. ✅ Upload data to MinIO
4. ✅ Build S3 driver
5. ✅ Run discovery for all formats

---

## Manual Setup (Step by Step)

### 1. Start Services

```bash
cd drivers/s3/examples
docker compose up -d
```

This starts:
- **MinIO** (S3 API on port 9000, Console on port 9001)
- **PostgreSQL** (Iceberg catalog on port 5432)

Verify:
```bash
docker ps | grep olake
```

### 2. Generate Test Data

```bash
python3 generate_test_data.py
```

Creates **18 test files** in multiple formats:

```
test_data/
├── parquet/          # Parquet with Snappy compression
│   ├── users/        → 2 files (220 records)
│   ├── orders/       → 2 files (330 records)
│   └── products/     → 1 file (50 records)
│
├── csv/              # Plain CSV + Gzipped CSV
│   ├── users/        → 2 files: .csv + .csv.gz (170 records)
│   ├── orders/       → 2 files: .csv + .csv.gz (260 records)
│   └── products/     → 1 file: .csv.gz (40 records)
│
├── json/             # JSON arrays + JSONL (plain + gzipped)
│   ├── users/        → 2 files: .jsonl + .jsonl.gz (135 records)
│   ├── orders/       → 2 files: .json + .json.gz (210 records)
│   └── products/     → 1 file: .jsonl.gz (30 records)
│
└── mixed/            # Mixed formats for testing
    └── transactions/ → 3 files: .parquet, .csv.gz, .jsonl (130 records)
```

### 3. Upload to MinIO

```bash
./upload_to_minio.sh
```

Verify in MinIO Console: http://localhost:9001
- Username: `admin`
- Password: `password`

---

## Testing Different Formats

### Test Parquet Files

```bash
cd /path/to/olake

# Check connection
./build.sh driver-s3 check --config $(pwd)/drivers/s3/examples/source-parquet.json

# Discover streams
./build.sh driver-s3 discover --config $(pwd)/drivers/s3/examples/source-parquet.json

# Expected: 3 streams (users, orders, products)
```

### Test CSV Files (with Gzip)

```bash
# Discover CSV files (including .csv.gz)
./build.sh driver-s3 discover --config $(pwd)/drivers/s3/examples/source-csv.json

# Expected: 3 streams
# Automatically detects and decompresses .csv.gz files
```

### Test JSON Files (JSONL + Array, with Gzip)

```bash
# Discover JSON files (JSONL, JSON arrays, gzipped)
./build.sh driver-s3 discover --config $(pwd)/drivers/s3/examples/source-json.json

# Expected: 3 streams
# Auto-detects format: JSONL vs JSON array
# Auto-decompresses .json.gz and .jsonl.gz files
```

---

## Configuration Files

### Source Configurations

| File | Format | Compression | Path Prefix |
|------|--------|-------------|-------------|
| `source-parquet.json` | Parquet | Snappy (native) | `parquet/` |
| `source-csv.json` | CSV | Gzip (auto-detect) | `csv/` |
| `source-json.json` | JSON/JSONL | Gzip (auto-detect) | `json/` |

### Example: source-csv.json

```json
{
  "bucket_name": "source-data",
  "region": "us-east-1",
  "path_prefix": "csv",
  "access_key_id": "admin",
  "secret_access_key": "password",
  "endpoint": "http://localhost:9000",
  "file_format": "csv",
  "compression": "gzip",
  "has_header": true,
  "delimiter": ",",
  "stream_grouping_enabled": true,
  "stream_grouping_level": 1
}
```

**Note**: `compression: "gzip"` is informational. The driver auto-detects compression by file extension (`.gz`).

---

## Stream Configuration

After discovery, configure sync behavior in `streams.json`.

### Generate from Discovery

```bash
# Discover and save catalog
./build.sh driver-s3 discover \
  --config $(pwd)/drivers/s3/examples/source-parquet.json \
  > catalog.json

# Extract streams and manually configure sync_mode
# See streams.template.json for example
```

### Example: streams.json (excerpt)

```json
{
  "selected_streams": {
    "s3": [
      {"stream_name": "users", "normalization": false},
      {"stream_name": "orders", "normalization": false}
    ]
  },
  "streams": [
    {
      "stream": {
        "name": "users",
        "sync_mode": "incremental",
        "destination_database": "olake_iceberg",
        "destination_table": "users",
        ...
      }
    }
  ]
}
```

**Important**: Ensure `destination_database` matches `iceberg_db` in `destination.json` (default: `olake_iceberg`).

---

## Running Sync

### Discovery (Required First Step)

```bash
# Parquet
./build.sh driver-s3 discover --config $(pwd)/drivers/s3/examples/source-parquet.json

# CSV
./build.sh driver-s3 discover --config $(pwd)/drivers/s3/examples/source-csv.json

# JSON
./build.sh driver-s3 discover --config $(pwd)/drivers/s3/examples/source-json.json
```

### Full Sync (Backfill)

**Note**: Currently, full sync has a known issue with Parquet data reading. Discovery works perfectly.

```bash
./build.sh driver-s3 sync \
  --config $(pwd)/drivers/s3/examples/source-parquet.json \
  --catalog $(pwd)/drivers/s3/examples/streams.json \
  --destination $(pwd)/drivers/s3/examples/destination.json
```

### Incremental Sync

```bash
./build.sh driver-s3 sync \
  --config $(pwd)/drivers/s3/examples/source-parquet.json \
  --catalog $(pwd)/drivers/s3/examples/streams.json \
  --destination $(pwd)/drivers/s3/examples/destination.json \
  --state $(pwd)/drivers/s3/examples/state.json
```

The state file will be created/updated with:
- Last synced timestamp per stream
- Cached schema per stream

---

## VS Code Debugging

Debug configurations are available in `.vscode/launch.json`:

### Discovery Debugging
- **S3: Check Connection**
- **S3: Discover - Parquet**
- **S3: Discover - CSV**
- **S3: Discover - JSON**

### Sync Debugging
- **S3: Sync - Parquet (No State)**
- **S3: Sync - CSV (With State)**
- **S3: Sync - JSON (With State)**

**To debug**:
1. Open VS Code
2. Set breakpoints in `drivers/s3/internal/`
3. Press F5 or go to Run → Start Debugging
4. Select configuration

---

## Key Features

### 1. Compression Auto-Detection ✅

**Extension-based detection** (not config-based):
- `.gz` files → Automatically decompressed with gzip
- Plain files → Read directly
- Mixed compression in same stream → Supported

**Works with**:
- CSV: `.csv` + `.csv.gz`
- JSON: `.json` + `.json.gz` + `.jsonl` + `.jsonl.gz`
- Parquet: Native Snappy compression (handled by library)

### 2. Intelligent JSON Format Detection ✅

**Auto-detects format** by analyzing file structure:
- `[...]` → JSON Array
- `{...}\n{...}` → JSONL (line-delimited)
- `{...}` → Single JSON object

**No configuration needed** - works automatically.

### 3. Stream Grouping ✅

**Folder-based logical streams**:

With `stream_grouping_level: 1`:
```
s3://bucket/parquet/users/2024-01-01/file1.parquet
s3://bucket/parquet/users/2024-01-02/file2.parquet
                    └─────┘
                    Stream name: "users"
```

All files in `users/` folder → One logical stream called "users"

### 4. Schema Caching ✅

**Avoids redundant schema inference**:
- Schema inferred from first file in stream
- Cached in state with LastModified timestamp
- Re-inferred only if files change

---

## Troubleshooting

### MinIO not accessible

```bash
# Check if containers are running
docker ps | grep olake

# Restart if needed
cd drivers/s3/examples
docker compose down
docker compose up -d
```

### Files not uploaded

```bash
# Verify test data exists
ls -la test_data/

# Check MinIO
docker exec olake-s3-minio mc ls local/source-data/ --recursive
```

### Discovery works but sync fails

**Known issue**: There's a current issue with Parquet data reading during sync. Discovery (schema inference) works correctly. This is being investigated.

### Gzipped files not decompressing

Check file extension:
```bash
# Must end with .gz
file test_data/csv/users/2024-01-02/user_data.csv.gz
# Should show: gzip compressed data
```

---

## File Structure

```
drivers/s3/examples/
├── docker-compose.yml          # MinIO + PostgreSQL setup
├── destination.json            # Iceberg destination config
├── source-parquet.json         # Parquet source config
├── source-csv.json             # CSV source config (with gzip)
├── source-json.json            # JSON source config (with gzip)
├── streams.json                # Stream configuration (generated)
├── streams.template.json       # Template for streams config
├── generate_test_data.py       # Generate test data (all formats)
├── upload_to_minio.sh          # Upload data to MinIO
├── quickstart.sh               # Automated setup script
├── setup-test-data.sh          # Helper script
├── README.md                   # This file
├── TESTING_GUIDE.md            # Detailed testing guide
├── SETUP_SUMMARY.md            # Quick reference
└── test_data/                  # Generated test files
    ├── parquet/
    ├── csv/
    ├── json/
    └── mixed/
```

---

## Access Points

### MinIO Console
- **URL**: http://localhost:9001
- **Username**: admin
- **Password**: password

### PostgreSQL (Iceberg Catalog)
- **Host**: localhost:5432
- **Database**: iceberg
- **Username**: iceberg
- **Password**: password

### MinIO S3 API
- **Endpoint**: http://localhost:9000
- **Access Key**: admin
- **Secret Key**: password

---

## Cleanup

```bash
# Stop services
cd drivers/s3/examples
docker compose down

# Remove volumes (clears all data)
docker compose down -v

# Remove generated test data
rm -rf test_data/
```

---

## Additional Documentation

- **TESTING_GUIDE.md** - Comprehensive testing scenarios and validation
- **SETUP_SUMMARY.md** - Quick reference for all configurations
- [OLake Documentation](https://olake.io/docs/) - Official documentation

---

## Support

For issues or questions:
- GitHub Issues: https://github.com/datazip-inc/olake/issues
- Documentation: https://olake.io/docs/
- Community: Join OLake Slack

---

**Last Updated**: November 6, 2025
