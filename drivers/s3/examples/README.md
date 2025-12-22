# S3 Driver Testing Guide

Complete testing documentation for the S3 driver with **Go + Shell** (no Python dependencies).

## 🚀 Quick Start

**Test everything in 2 minutes with a single command!**

```bash
cd /Users/ankit.singhal/Developer/personal/olake/drivers/s3/examples

# One command to rule them all!
./run_tests.sh
```

That's it! This will:
1. ✅ Start Docker containers (MinIO + PostgreSQL)
2. ✅ Generate test data (Go - no Python!)
3. ✅ Upload to MinIO
4. ✅ Run S3 driver sync (Parquet, CSV, JSON)
5. ✅ Verify data in Iceberg

### Prerequisites

- Docker installed and running
- Go 1.19+ installed
- Ports 9000, 9001, 5432 available

### Test Results

After running, you'll see:

```
=== Record Counts ===
parquet_users:    10,000 records
parquet_orders:   50,000 records
parquet_products:  5,000 records
csv_users:        10,000 records
csv_orders:       50,000 records
csv_products:      5,000 records
json_users:       10,000 records
json_orders:      50,000 records
json_products:     5,000 records

✅ All Tests Passed!
```

## 📊 Testing Options

| Test Type | Command | Duration | What It Tests |
|-----------|---------|----------|---------------|
| **Unit Tests** | `go test ./drivers/s3/internal/... -v` | ~2s | Core logic, 154+ tests |
| **Quick Integration** | `./run_tests.sh` | ~2min | Small files, all formats |
| **Medium Integration** | `./run_tests.sh --size medium` | ~10min | ~2.6M records |
| **Large Integration** | `./run_tests.sh --size large` | ~45min | ~15.5M records, 10GB |
| **Chunk Testing** | `./test_chunking.sh` | ~5min | 2GB chunk boundaries |

---

## 📦 1. Unit Tests (Fast)

Test core logic without Docker:

```bash
cd /Users/ankit.singhal/Developer/personal/olake

# All S3 driver tests
go test ./drivers/s3/internal/... -v

# Specific test
go test ./drivers/s3/internal/... -v -run TestGroupFilesIntoChunks
go test ./drivers/s3/internal/... -v -run TestIncrementalSyncFiltering

# With coverage
go test ./drivers/s3/internal/... -cover
```

**Coverage:** 154+ test cases covering:
- Chunking logic (2GB boundaries)
- Cursor-based filtering (incremental sync)
- Configuration validation
- Parser configs (CSV/JSON/Parquet)
- Type inference (AND logic)
- Edge cases
- Backfill vs incremental flow separation

---

## 🚀 2. Integration Tests

End-to-end testing with Docker for all file formats and sizes.

### Small Files (~2 minutes)

```bash
cd drivers/s3/examples
./run_tests.sh
```

**Generates:**
- 10K users, 50K orders, 5K products
- ~65K total records
- ~50MB data
- Tests all formats: Parquet, CSV, JSON

**Verifies:**
- Data in MinIO
- Sync to Iceberg
- All compressions work
- Cursor field present

### Medium Files (~10 minutes)

```bash
./run_tests.sh --size medium
```

**Generates:**
- 1M users, 1.5M orders, 100K products
- ~2.6M total records
- ~2GB data

**Stress tests:**
- Larger datasets
- Memory efficiency
- Chunk grouping

### Large Files (~45 minutes)

```bash
./run_tests.sh --size large
```

**Generates:**
- 5M users, 10M orders, 500K products
- ~15.5M total records
- ~10GB data

**Validates:**
- Large-scale performance
- >2GB file handling
- Streaming mode
- Memory stability

### Test Data Sizes

| Size | Users | Orders | Products | Total Records | Disk Size |
|------|-------|--------|----------|---------------|-----------|
| **small** | 10K | 50K | 5K | ~65K | ~50MB |
| **medium** | 1M | 1.5M | 100K | ~2.6M | ~2GB |
| **large** | 5M | 10M | 500K | ~15.5M | ~10GB |

### What Gets Tested

#### File Formats
- ✅ **Parquet** (Snappy compression)
- ✅ **CSV** (plain + gzip)
- ✅ **JSON** (JSONL format, plain + gzip)

#### Features
- ✅ Multi-format support (Parquet, CSV, JSON)
- ✅ Compression handling (gzip, snappy)
- ✅ Chunking logic (2GB chunks, using constants.EffectiveParquetSize)
- ✅ Schema inference (CSV/JSON/Parquet with AND logic)
- ✅ Incremental sync with `_last_modified_time` cursor field
- ✅ Stream grouping at level 1 (first folder)
- ✅ Optimized file metadata lookups
- ✅ Data integrity in Iceberg

---

## 🧩 3. Chunk Boundary Testing

Test 2GB chunking logic with files of various sizes to verify correct grouping behavior.

### 🎯 What This Tests

The S3 driver groups files into 2GB chunks for parallel processing. This test verifies:

- ✅ **Small files** (<100MB) group together into single 2GB chunks
- ✅ **Medium files** (500MB-1.5GB) group intelligently without exceeding 2GB
- ✅ **Large files** (>2GB) are processed as individual chunks
- ✅ **Edge cases** near the 2GB boundary

### Lite Mode (Recommended for Local Testing) 💡

```bash
cd /Users/ankit.singhal/Developer/personal/olake/drivers/s3/examples
./test_chunking.sh
```

**Fast & Lightweight:** Generates 8 files (1-35MB, ~120MB total) in seconds!

### Full Mode (Production Testing) ⚠️

```bash
./test_chunking.sh --full
```

**Resource Intensive:** Generates 8 files (10MB-3GB, ~7.3GB total). Takes several minutes.

### What The Test Does:
1. Generate test files (lite or full mode)
2. Upload to MinIO
3. Run S3 driver sync
4. Verify chunking behavior in logs
5. Confirm data in Iceberg

### Test Files Generated

#### Lite Mode (Default)

| File Name | Size | Purpose |
|-----------|------|---------|
| `small_1mb.parquet` | ~1MB | Fast grouping test |
| `small_5mb.parquet` | ~5MB | Fast grouping test |
| `small_10mb.parquet` | ~10MB | Fast grouping test |
| `medium_15mb.parquet` | ~15MB | Edge case grouping |
| `medium_20mb.parquet` | ~20MB | Edge case grouping |
| `medium_25mb.parquet` | ~25MB | Edge case grouping |
| `large_30mb.parquet` | ~30MB | Larger file test |
| `large_35mb.parquet` | ~35MB | Larger file test |

**Total:** ~120MB | **Generation Time:** <10 seconds

#### Full Mode (--full flag)

| File Name | Size | Purpose |
|-----------|------|---------|
| `small_10mb.parquet` | 10MB | Should group with others |
| `small_50mb.parquet` | 50MB | Should group with others |
| `small_100mb.parquet` | 100MB | Should group with others |
| `medium_500mb.parquet` | 500MB | Edge case grouping |
| `medium_1gb.parquet` | 1GB | Edge case grouping |
| `medium_1500mb.parquet` | 1.5GB | Edge case grouping |
| `large_2500mb.parquet` | 2.5GB | **Individual chunk** (>2GB) |
| `large_3gb.parquet` | 3GB | **Individual chunk** (>2GB) |

**Total:** ~7.3GB | **Generation Time:** Several minutes

---

## 🐛 VS Code Debugging

### Debug Configurations Available

The workspace includes **11 S3-specific debug configurations**:

#### Discovery Debugging
1. **S3: Check Connection** - Test S3 connectivity
2. **S3: Discover - Parquet** - Debug Parquet file discovery
3. **S3: Discover - CSV** - Debug CSV (+ gzip) discovery
4. **S3: Discover - JSON** - Debug JSON/JSONL (+ gzip) discovery

#### Sync Debugging (Full Load)
5. **S3: Sync - Parquet (No State)** - Full backfill of Parquet files
6. **S3: Sync - CSV (No State)** - Full backfill of CSV files
7. **S3: Sync - JSON (No State)** - Full backfill of JSON files

#### Sync Debugging (Incremental)
8. **S3: Sync - Parquet (With State)** - Incremental Parquet sync
9. **S3: Sync - CSV (With State)** - Incremental CSV sync (tests gzip)
10. **S3: Sync - JSON (With State)** - Incremental JSON sync (tests gzip)

#### Utilities
11. **S3: Spec** - View driver specification

### How to Use

1. Open VS Code in the Olake workspace
2. Go to **Run and Debug** (⇧⌘D or Ctrl+Shift+D)
3. Select configuration from dropdown
4. Set breakpoints in S3 driver code
5. Press **F5** to start debugging

### Recommended Breakpoints

- `drivers/s3/internal/s3.go:150` - File discovery
- `drivers/s3/internal/s3.go:250` - Schema inference
- `drivers/s3/internal/backfill.go` - Backfill logic and chunking
- `drivers/s3/internal/incremental.go` - Incremental filtering
- `pkg/parser/csv.go` - CSV parsing with schema inference
- `pkg/parser/json.go` - JSON parsing (JSONL/Array/Object)
- `pkg/parser/parquet.go` - Parquet parsing with streaming

---

## 🛠️ Manual Testing

### Generate Test Data Only

```bash
# Small files (default)
./generate_testdata.sh

# Medium files
./generate_testdata.sh --size medium

# Large files
./generate_testdata.sh --size large

# Specific format only
./generate_testdata.sh --format parquet
./generate_testdata.sh --format csv
./generate_testdata.sh --format json
```

### Generate Chunk Test Files

```bash
# Lite mode (fast, lightweight)
./generate-testdata -chunk-test -lite -format=parquet

# Full mode (production sizes)
./generate-testdata -chunk-test -format=parquet

# All formats (lite mode)
./generate-testdata -chunk-test -lite -format=all

# Generate specific size
./generate-testdata -target-mb=500 -format=parquet
./generate-testdata -target-mb=2500 -format=parquet
```

### Run Specific Tests

```bash
# Start services
docker compose up -d

# Generate data
./generate_testdata.sh --size small

# Upload to MinIO
./upload_to_minio.sh

# Test Parquet only
cd ../../..
./build.sh driver-s3 sync \
  --config drivers/s3/examples/source-parquet.json \
  --catalog drivers/s3/examples/streams-parquet.json \
  --destination drivers/s3/examples/destination.json

# Test CSV only
./build.sh driver-s3 sync \
  --config drivers/s3/examples/source-csv.json \
  --catalog drivers/s3/examples/streams-csv.json \
  --destination drivers/s3/examples/destination.json

# Test JSON only
./build.sh driver-s3 sync \
  --config drivers/s3/examples/source-json.json \
  --catalog drivers/s3/examples/streams-json.json \
  --destination drivers/s3/examples/destination.json
```

---

## 🔍 Verification & Monitoring

### Verify Data in Iceberg

```bash
# Connect to PostgreSQL
docker exec -it olake-iceberg-catalog psql -U iceberg -d iceberg

# List tables
\dt

# Check record counts
SELECT COUNT(*) FROM parquet_users;
SELECT COUNT(*) FROM parquet_orders;
SELECT COUNT(*) FROM parquet_products;

SELECT COUNT(*) FROM csv_users;
SELECT COUNT(*) FROM csv_orders;

SELECT COUNT(*) FROM json_users;
SELECT COUNT(*) FROM json_orders;

# Check cursor field
SELECT _last_modified_time, COUNT(*) 
FROM parquet_users 
GROUP BY _last_modified_time;

# Sample data
SELECT * FROM parquet_users LIMIT 5;
SELECT status, COUNT(*) FROM parquet_orders GROUP BY status;
```

### MinIO Console

```bash
open http://localhost:9001
# Username: admin
# Password: password
```

### View Uploaded Files

```bash
# List all files
docker exec olake-s3-minio mc ls myminio/source-data/ --recursive

# Get file info
docker exec olake-s3-minio mc stat myminio/source-data/parquet/users/data.parquet

# Download file
docker exec olake-s3-minio mc cp myminio/source-data/parquet/users/data.parquet /tmp/
```

### Check Logs

```bash
# MinIO logs
docker logs olake-s3-minio

# PostgreSQL logs
docker logs olake-iceberg-catalog

# Follow logs
docker logs -f olake-s3-minio

# Run sync and capture logs
./build.sh driver-s3 sync \
  --config config-chunk-test.json \
  --catalog streams-chunk-test.json \
  --destination destination.json 2>&1 | tee chunk_test.log

# Look for chunking patterns
grep -i "chunk" chunk_test.log
grep "Processing.*files" chunk_test.log
```

---

## 🐛 Troubleshooting

### Docker Not Running

```bash
# Check Docker status
docker info

# Start Docker Desktop (macOS)
open -a Docker

# Restart services
docker compose down
docker compose up -d
```

### Test Data Generation Fails

```bash
# Check Go version
go version  # Should be 1.19+

# Rebuild generator
cd testdata
go build -o ../generate-testdata main.go
cd ..

# Test generator
./generate-testdata -size=small -format=parquet
```

### MinIO Upload Fails

```bash
# Check MinIO is running
docker ps | grep minio

# Check buckets exist
docker exec olake-s3-minio mc ls myminio/

# Create bucket if missing
docker exec olake-s3-minio mc mb myminio/source-data --ignore-existing
```

### Iceberg Tables Not Created

```bash
# Check PostgreSQL
docker exec olake-iceberg-catalog pg_isready -U iceberg

# List databases
docker exec olake-iceberg-catalog psql -U iceberg -l

# Check for errors in sync logs
./build.sh driver-s3 sync --config source-parquet.json ... 2>&1 | tee sync.log
```

### Docker Port Conflicts

If you see:
```
Bind for 0.0.0.0:9000 failed: port is already allocated
```

**Solution:** The test script now auto-detects running containers and retries with network prune if needed.

### Out of Disk Space

```bash
# Check usage
df -h

# Cleanup
rm -rf test_data test_data_*
docker compose down -v
docker system prune -a
```

### Memory Issues (Large Tests)

```bash
# Monitor memory
docker stats

# Use medium instead
./run_tests.sh --size medium

# Or custom smaller size
./generate-testdata -target-mb=500
```

---

## 📁 Project Structure

```
drivers/s3/
├── internal/
│   ├── s3.go                    # Main driver (discovery, setup)
│   ├── backfill.go              # Backfill logic (chunking, file processing)
│   ├── incremental.go           # Incremental sync logic
│   ├── config.go                # Configuration validation
│   ├── types.go                 # Type definitions
│   ├── s3_test.go               # Integration tests
│   ├── edge_cases_test.go       # Edge case tests
│   └── config_test.go           # Config tests
│
├── pkg/parser/                  # Reusable parser package (moved from drivers/parser)
│   ├── parser.go                # Parser interface
│   ├── csv.go                   # CSV parser with schema inference
│   ├── json.go                  # JSON parser (JSONL/Array/Object)
│   └── parquet.go               # Parquet parser with streaming
│
└── examples/
    ├── testdata/
    │   └── main.go              # Go test data generator
    ├── generate_testdata.sh    # Generator wrapper
    ├── run_tests.sh             # Test suite runner
    ├── test_chunking.sh         # Chunk boundary tests
    ├── upload_to_minio.sh      # MinIO upload script
    ├── docker-compose.yml       # Docker services
    └── README.md                # This file
```

---

## 🔧 Technology Stack

- **Go 1.19+** - Test data generation, unit tests
- **Shell (Bash)** - Test orchestration
- **Docker** - MinIO, PostgreSQL/Iceberg
- **MinIO** - S3-compatible storage
- **PostgreSQL** - Iceberg catalog
- **Parquet-go** - Parquet file handling

---

## 📈 Test Execution Times

| Test Type | Records | Data Size | Time | Memory |
|-----------|---------|-----------|------|--------|
| Unit tests | N/A | N/A | ~2s | <100MB |
| Small integration | ~65K | ~50MB | ~2min | ~500MB |
| Medium integration | ~2.6M | ~2GB | ~10min | ~2GB |
| Large integration | ~15.5M | ~10GB | ~45min | ~4GB |
| Chunk boundary (lite) | ~1M | ~120MB | ~1min | ~500MB |
| Chunk boundary (full) | ~50M | ~7GB | ~5min | ~2GB |

---

## ✅ What Gets Tested

### Functional
✅ Multi-format support (Parquet, CSV, JSON)  
✅ Compression (gzip, snappy, none)  
✅ Stream grouping at level 1 (first folder)  
✅ Schema inference with AND logic  
✅ Incremental sync with cursor  
✅ State management  
✅ Chunk-based parallel processing  
✅ 2GB chunk boundaries  

### Data Quality
✅ Record counts match expectations  
✅ No NULL primary keys  
✅ Timestamps parsed correctly  
✅ Data types correct  
✅ Cursor field present (`_last_modified_time`)  
✅ Compression roundtrips  

### Performance
✅ Small files group efficiently  
✅ Large files (>2GB) stream properly  
✅ Memory stays within bounds  
✅ Parallel processing works  

---

## 🎯 Recommended Test Sequence

### For Development

```bash
# 1. Unit tests (fast feedback)
go test ./drivers/s3/internal/... -v

# 2. Quick integration (verify end-to-end)
cd drivers/s3/examples
./run_tests.sh

# 3. Chunk testing (verify boundary logic)
./test_chunking.sh
```

### Before PR/Release

```bash
# 1. All unit tests with coverage
go test ./drivers/s3/internal/... -cover

# 2. Medium integration test
cd drivers/s3/examples
./run_tests.sh --size medium

# 3. Chunk boundary testing
./test_chunking.sh

# 4. Large scale (optional, if major changes)
./run_tests.sh --size large
```

---

## 🎉 Success Criteria

After running all tests, you should see:

- ✅ **Unit tests:** 154+ passing
- ✅ **Small integration:** ~65K records synced
- ✅ **Chunk testing:** 8 files, correct grouping
- ✅ **All formats:** Parquet, CSV, JSON working
- ✅ **All compressions:** Gzip, Snappy, None working
- ✅ **Iceberg tables:** Created with correct schemas
- ✅ **Cursor field:** `_last_modified_time` present
- ✅ **No errors:** Clean logs, no panics

---

## 🧹 Cleanup

```bash
# Stop containers
docker compose down

# Remove volumes (deletes all data)
docker compose down -v

# Remove generated test data
rm -rf test_data test_data_*

# Remove generated binary
rm -f generate-testdata
```

---

## 🎉 Summary

### What We've Built

✅ **Pure Go + Shell testing** (no Python!)  
✅ **One-command testing** for all scenarios  
✅ **154+ comprehensive unit tests**  
✅ **Multiple integration test sizes**  
✅ **Chunk boundary verification with Lite Mode**  
✅ **Large dataset support** (15.5M records)  
✅ **VS Code debugging** with 11 configurations  

### Quick Commands Reference

```bash
# Unit tests
go test ./drivers/s3/internal/... -v

# Quick integration
cd drivers/s3/examples && ./run_tests.sh

# Large dataset
./run_tests.sh --size large

# Chunk testing (lite mode - fast!)
./test_chunking.sh

# Chunk testing (full mode - comprehensive)
./test_chunking.sh --full

# Custom size
./generate-testdata -target-mb=1000 -format=parquet

# Cleanup
docker compose down -v && rm -rf test_data*
```

---

**Ready to test?** Just run: `./run_tests.sh` 🚀
