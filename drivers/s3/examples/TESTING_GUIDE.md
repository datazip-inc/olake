# S3 Driver Testing Guide - CSV, JSON, and Parquet with Gzip

This guide covers comprehensive testing of the S3 driver with multiple file formats and compression options.

## 📋 Test Coverage

### File Formats Tested
- ✅ **Parquet** (Snappy compression)
- ✅ **CSV** (Plain + Gzip compressed)
- ✅ **JSON** (Line-delimited JSONL + Array format, Plain + Gzip)

### Compression Options
- ✅ **None** (uncompressed files)
- ✅ **Gzip** (.gz extension)
- ✅ **Snappy** (Parquet default)

### Features Tested
- ✅ Folder-based stream grouping
- ✅ Schema inference per format
- ✅ Schema caching across syncs
- ✅ Incremental sync (LastModified-based)
- ✅ Full refresh/backfill
- ✅ State management
- ✅ Error handling (missing files)

## 🚀 Quick Test Setup

### 1. Generate All Test Data

```bash
cd /Users/ankit.singhal/Developer/personal/olake/drivers/s3/examples
python3 generate_test_data.py
```

**Output Structure:**
```
test_data/
├── parquet/          # 5 Parquet files (Snappy)
│   ├── users/        
│   │   ├── 2024-01-01/user_data.parquet
│   │   └── 2024-01-02/user_data.parquet
│   ├── orders/
│   │   ├── 2024-01-01/order_data.parquet
│   │   └── 2024-01-02/order_data.parquet
│   └── products/
│       └── 2024-01-01/product_data.parquet
│
├── csv/              # 5 CSV files (Plain + Gzip)
│   ├── users/
│   │   ├── 2024-01-01/user_data.csv
│   │   └── 2024-01-02/user_data.csv.gz
│   ├── orders/
│   │   ├── 2024-01-01/order_data.csv
│   │   └── 2024-01-02/order_data.csv.gz
│   └── products/
│       └── 2024-01-01/product_data.csv.gz
│
├── json/             # 5 JSON files (JSONL + Array, Plain + Gzip)
│   ├── users/
│   │   ├── 2024-01-01/user_data.jsonl
│   │   └── 2024-01-02/user_data.jsonl.gz
│   ├── orders/
│   │   ├── 2024-01-01/order_data.json
│   │   └── 2024-01-02/order_data.json.gz
│   └── products/
│       └── 2024-01-01/product_data.jsonl.gz
│
└── mixed/            # 3 files with different formats
    └── transactions/
        ├── data.parquet
        ├── data.csv.gz
        └── data.jsonl
```

### 2. Start MinIO and Upload Data

```bash
# Start services
docker compose up -d

# Upload all test files
./upload_to_minio.sh
```

### 3. Verify Upload in MinIO Console

- URL: http://localhost:9001
- Username: `admin`
- Password: `password`

Navigate to `source-data` bucket and verify folders: `parquet/`, `csv/`, `json/`, `mixed/`

## 🧪 Testing Scenarios

### Test 1: Parquet Files (Snappy Compression)

**Configuration:** `source-parquet.json`

```bash
# Discover
./build.sh driver-s3 discover \
  --config $(pwd)/drivers/s3/examples/source-parquet.json

# Sync
./build.sh driver-s3 sync \
  --config $(pwd)/drivers/s3/examples/source-parquet.json \
  --catalog $(pwd)/drivers/s3/examples/streams.json \
  --destination $(pwd)/drivers/s3/examples/destination.json
```

**Expected:**
- 3 streams: `users`, `orders`, `products`
- Total records: 600 (220 + 330 + 50)
- Compression: Automatically handled by Parquet reader

### Test 2: CSV Files (Plain + Gzip)

**Configuration:** `source-csv.json`

```json
{
  "file_format": "csv",
  "compression": "gzip",
  "has_header": true,
  "delimiter": ","
}
```

```bash
# Discover
./build.sh driver-s3 discover \
  --config $(pwd)/drivers/s3/examples/source-csv.json

# Sync
./build.sh driver-s3 sync \
  --config $(pwd)/drivers/s3/examples/source-csv.json \
  --catalog $(pwd)/drivers/s3/examples/streams-csv.json \
  --destination $(pwd)/drivers/s3/examples/destination.json
```

**Expected:**
- 3 streams: `users`, `orders`, `products`
- Total records: 470 (170 + 260 + 40)
- Files with `.csv.gz` automatically decompressed
- Plain `.csv` files read directly

**Verify:**
```bash
# Check logs for gzip decompression
# Should see: "Reading gzipped CSV file: users/2024-01-02/user_data.csv.gz"
```

### Test 3: JSON Files (JSONL + Array, Plain + Gzip)

**Configuration:** `source-json.json`

```json
{
  "file_format": "json",
  "compression": "gzip",
  "json_line_delimited": true
}
```

```bash
# Discover
./build.sh driver-s3 discover \
  --config $(pwd)/drivers/s3/examples/source-json.json

# Sync
./build.sh driver-s3 sync \
  --config $(pwd)/drivers/s3/examples/source-json.json \
  --catalog $(pwd)/drivers/s3/examples/streams-json.json \
  --destination $(pwd)/drivers/s3/examples/destination.json
```

**Expected:**
- 3 streams: `users`, `orders`, `products`
- Total records: 375 (135 + 210 + 30)
- JSONL format: One JSON object per line
- JSON Array format: Array of objects
- Gzipped files automatically decompressed

**Verify:**
```bash
# Check sample JSONL file
zcat drivers/s3/examples/test_data/json/users/2024-01-02/user_data.jsonl.gz | head -n 2

# Output should be:
# {"user_id":1,"username":"user_1",...}
# {"user_id":2,"username":"user_2",...}
```

### Test 4: Incremental Sync with Gzipped Files

```bash
# First sync (full load)
./build.sh driver-s3 sync \
  --config $(pwd)/drivers/s3/examples/source-csv.json \
  --catalog $(pwd)/drivers/s3/examples/streams-csv.json \
  --destination $(pwd)/drivers/s3/examples/destination.json

# Generate new gzipped CSV file with later timestamp
python3 -c "
from generate_test_data import *
save_csv(generate_users_data('2024-01-03', 50), 'test_data/csv/users/2024-01-03/user_data.csv.gz', compressed=True)
"

# Upload new file
docker exec olake-s3-setup mc cp test_data/csv/users/2024-01-03/user_data.csv.gz myminio/source-data/csv/users/2024-01-03/

# Incremental sync (should only process new file)
./build.sh driver-s3 sync \
  --config $(pwd)/drivers/s3/examples/source-csv.json \
  --catalog $(pwd)/drivers/s3/examples/streams-csv.json \
  --destination $(pwd)/drivers/s3/examples/destination.json \
  --state $(pwd)/drivers/s3/examples/state-csv.json
```

**Expected:**
- Only processes `2024-01-03/user_data.csv.gz` (50 records)
- State updated with new `last_modified` timestamp
- Logs show: "Syncing 1 modified files for stream users"

### Test 5: Schema Caching with Different Formats

```bash
# Run initial sync (schema inferred)
./build.sh driver-s3 sync \
  --config $(pwd)/drivers/s3/examples/source-json.json \
  --catalog $(pwd)/drivers/s3/examples/streams-json.json \
  --destination $(pwd)/drivers/s3/examples/destination.json

# Check state for cached schema
cat drivers/s3/examples/state-json.json | jq '.streams[0].state.schema_cache_users'

# Run second sync (should use cached schema)
./build.sh driver-s3 sync \
  --config $(pwd)/drivers/s3/examples/source-json.json \
  --catalog $(pwd)/drivers/s3/examples/streams-json.json \
  --destination $(pwd)/drivers/s3/examples/destination.json \
  --state $(pwd)/drivers/s3/examples/state-json.json
```

**Expected:**
- First run: Logs show "Inferring schema from file: users/2024-01-01/user_data.jsonl"
- Second run: Logs show "Using cached schema for stream users"
- No redundant S3 reads for schema inference

## 🐛 VS Code Debugging

### Debug Configurations Available

Open `.vscode/launch.json` and use these configurations:

1. **S3: Discover - Parquet**
   - Tests Parquet file discovery
   - Breakpoint: `GetStreamNames()` in `s3.go`

2. **S3: Discover - CSV**
   - Tests CSV file discovery (with gzip)
   - Breakpoint: `extractStreamName()` in `s3.go`

3. **S3: Discover - JSON**
   - Tests JSON/JSONL discovery
   - Breakpoint: `ProduceSchema()` in `s3.go`

4. **S3: Sync - CSV (No State)**
   - Full load of CSV files
   - Breakpoint: `ChunkIterator()` in `backfill.go`

5. **S3: Sync - JSON (With State)**
   - Incremental sync of JSON files
   - Breakpoint: `StreamIncrementalChanges()` in `incremental.go`

### How to Debug

1. Open VS Code
2. Open any S3 driver file (e.g., `drivers/s3/internal/s3.go`)
3. Set breakpoints:
   - `GetStreamNames()` - File discovery
   - `ProduceSchema()` - Schema inference
   - `ChunkIterator()` - Data reading
   - `StreamIncrementalChanges()` - Incremental logic
4. Press `F5` or go to Run → Start Debugging
5. Select desired configuration from dropdown
6. Step through code with `F10` (step over), `F11` (step into)

### Key Breakpoints for Testing

```go
// File: drivers/s3/internal/s3.go

// Line ~150: File discovery
func (s *S3) GetStreamNames(ctx context.Context) ([]string, error) {
    // Set breakpoint here to inspect discovered files
    for _, obj := range result.Contents {
        // Inspect obj.Key, obj.LastModified, obj.Size
    }
}

// Line ~250: Schema inference
func (s *S3) ProduceSchema(ctx context.Context, streamName string, stream *types.Stream) error {
    // Set breakpoint to see schema caching logic
    if !shouldReinfer {
        // Cached schema path
    } else {
        // Schema inference path
    }
}
```

```go
// File: drivers/s3/internal/incremental.go

// Line ~80: Incremental filtering
func (s *S3) StreamIncrementalChanges(...) error {
    for _, file := range files {
        if file.LastModified > lastSynced {
            // Set breakpoint to verify timestamp comparison
            filesToSync = append(filesToSync, file)
        }
    }
}
```

## ✅ Validation Checklist

### Format-Specific Tests

#### Parquet
- [ ] Discovers all .parquet files
- [ ] Reads Snappy-compressed Parquet
- [ ] Correctly infers schema (nested types, timestamps)
- [ ] Groups files by folder

#### CSV
- [ ] Reads plain .csv files
- [ ] Automatically decompresses .csv.gz files
- [ ] Respects `has_header` setting
- [ ] Handles custom delimiters
- [ ] Infers column types correctly

#### JSON
- [ ] Reads JSONL (line-delimited) format
- [ ] Reads JSON array format
- [ ] Automatically decompresses .json.gz and .jsonl.gz
- [ ] Handles nested JSON objects
- [ ] Converts timestamps to proper types

### Compression Tests
- [ ] Gzip (.gz) files automatically decompressed
- [ ] Non-compressed files read directly
- [ ] Mixed compression in same stream works
- [ ] Compression detection by file extension

### Integration Tests
- [ ] Full load syncs all files
- [ ] Incremental sync filters by LastModified
- [ ] State persists cursor values
- [ ] Schema caching reduces S3 calls
- [ ] Error handling for missing files

## 🔧 Troubleshooting

### Issue: Gzipped files not read correctly

**Symptoms:** Error "invalid file format" or "unexpected EOF"

**Solution:**
```bash
# Verify file is valid gzip
file test_data/csv/users/2024-01-02/user_data.csv.gz
# Should show: gzip compressed data

# Test decompression
zcat test_data/csv/users/2024-01-02/user_data.csv.gz | head

# Check file in MinIO
docker exec olake-s3-setup mc cat myminio/source-data/csv/users/2024-01-02/user_data.csv.gz | zcat | head
```

### Issue: Schema inference fails for JSON

**Symptoms:** Error "failed to infer schema"

**Solution:**
```bash
# Validate JSON format
cat test_data/json/users/2024-01-01/user_data.jsonl | jq empty
# Should return no errors

# For JSONL, verify one object per line
head -n 1 test_data/json/users/2024-01-01/user_data.jsonl | jq .
```

### Issue: CSV delimiter not recognized

**Symptoms:** All columns in single field

**Solution:**
Check `source-csv.json`:
```json
{
  "delimiter": ",",  // Ensure matches actual file
  "has_header": true
}
```

### Issue: Incremental sync processes all files

**Symptoms:** All files processed even with state

**Solution:**
```bash
# Check state file
cat drivers/s3/examples/state-csv.json | jq .

# Verify LastModified timestamps
docker exec olake-s3-setup mc stat myminio/source-data/csv/users/2024-01-02/user_data.csv.gz

# Touch file to update timestamp (for testing)
docker exec olake-s3-setup mc cp myminio/source-data/csv/users/2024-01-02/user_data.csv.gz myminio/source-data/csv/users/2024-01-02/user_data.csv.gz
```

## 📊 Performance Benchmarks

### File Reading Performance

Test with 100K records per file:

```bash
# Generate large files
python3 -c "
from generate_test_data import *
save_parquet(generate_users_data('2024-01-01', 100000), 'test_data/large/users.parquet')
save_csv(generate_users_data('2024-01-01', 100000), 'test_data/large/users.csv.gz', compressed=True)
save_json(generate_users_data('2024-01-01', 100000), 'test_data/large/users.jsonl.gz', compressed=True, line_delimited=True)
"

# Time each format
time ./build.sh driver-s3 sync --config source-parquet.json ...
time ./build.sh driver-s3 sync --config source-csv.json ...
time ./build.sh driver-s3 sync --config source-json.json ...
```

**Expected Results:**
- Parquet: Fastest (columnar format, efficient compression)
- CSV (gzip): Medium (row-based, good compression)
- JSON (gzip): Slower (verbose format, needs parsing)

## 📚 References

- [S3 Driver Implementation Summary](../IMPLEMENTATION_SUMMARY.md)
- [OLake Documentation](https://olake.io/docs/)
- [Parquet Format Spec](https://parquet.apache.org/docs/)
- [JSON Lines Format](https://jsonlines.org/)

## 🎯 Test Summary

**Total Test Files:** 18
- Parquet: 5 files
- CSV: 5 files (3 gzipped)
- JSON: 5 files (3 gzipped)
- Mixed: 3 files

**Expected Streams:** 10
- parquet/users, orders, products
- csv/users, orders, products
- json/users, orders, products
- mixed/transactions

**Compression Coverage:**
- ✅ Snappy (Parquet)
- ✅ Gzip (CSV, JSON)
- ✅ None (Plain files)

**All formats tested with:**
- Full load ✅
- Incremental sync ✅
- Schema caching ✅
- VS Code debugging ✅

