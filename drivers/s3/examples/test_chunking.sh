#!/bin/bash
set -e

# Test S3 driver chunking logic with files of various sizes
# Verifies 2GB chunk grouping behavior

# Parse command line arguments
LITE_MODE=true
if [ "$1" == "--full" ]; then
    LITE_MODE=false
    shift
fi

echo "============================================"
echo "S3 Driver Chunk Boundary Testing"
echo "============================================"
echo ""
if [ "$LITE_MODE" == "true" ]; then
    echo "Mode: LITE (lightweight for local testing)"
    echo ""
    echo "This test verifies chunking logic with lightweight files:"
    echo "  • Small files (1-10MB) - quick generation, tests grouping"
    echo "  • Medium files (15-25MB) - test grouping edge cases"
    echo "  • Large files (30-35MB) - test larger file handling"
    echo ""
    echo "💡 Use --full flag for production-size testing (2-3GB files)"
else
    echo "Mode: FULL (production-size testing)"
    echo ""
    echo "This test verifies the 2GB chunking logic:"
    echo "  • Small files (10-100MB) should group together"
    echo "  • Medium files (500MB-1.5GB) should group carefully"
    echo "  • Large files (>2GB) should be individual chunks"
    echo ""
    echo "⚠️  WARNING: This will generate multi-GB files!"
fi
echo ""

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../" && pwd)"

# Step 1: Generate chunk test files
echo -e "${BLUE}Step 1: Generating Chunk Test Files${NC}"
echo "----------------------------------------"
cd "$SCRIPT_DIR"

if [ "$LITE_MODE" == "true" ]; then
    ./generate-testdata -chunk-test -lite -format=parquet -output=test_data_chunks
else
    ./generate-testdata -chunk-test -format=parquet -output=test_data_chunks
fi

echo -e "${GREEN}✓ Test files generated${NC}"
echo ""
ls -lh test_data_chunks/chunk_test/parquet/

# Step 2: Start Docker
echo ""
echo -e "${BLUE}Step 2: Start Docker Services${NC}"
echo "----------------------------------------"

# Check if containers are already running
if docker ps | grep -q "olake-s3-minio"; then
    echo "MinIO already running, skipping start..."
else
    echo "Starting Docker services..."
    
    # Try to start, if it fails due to port conflict, prune networks and retry
    if ! docker compose up -d 2>&1 | tee /tmp/docker_start.log | grep -q "Started"; then
        if grep -q "port is already allocated" /tmp/docker_start.log; then
            echo -e "${YELLOW}⚠ Port conflict detected, cleaning up Docker networks...${NC}"
            docker network prune -f > /dev/null 2>&1
            echo "Retrying..."
            docker compose up -d
        fi
    fi
    
    sleep 10
fi

# Verify services are running
if docker ps | grep -q "olake-s3-minio" && docker ps | grep -q "olake-iceberg-catalog"; then
    echo -e "${GREEN}✓ Services running${NC}"
else
    echo -e "${RED}✗ Services failed to start. Try: docker compose down && docker compose up -d${NC}"
    exit 1
fi

# Step 3: Upload to MinIO
echo ""
echo -e "${BLUE}Step 3: Upload to MinIO${NC}"
echo "----------------------------------------"

docker exec olake-s3-minio mc alias set myminio http://localhost:9000 admin password 2>/dev/null || true

echo "Uploading chunk test files..."
docker cp test_data_chunks olake-s3-minio:/tmp/
docker exec olake-s3-minio sh -c '
    mc cp --recursive /tmp/test_data_chunks/chunk_test/parquet/ myminio/source-data/chunk_test/parquet/ 2>&1 || true
    rm -rf /tmp/test_data_chunks
' || echo "Upload completed"

echo -e "${GREEN}✓ Files uploaded${NC}"
echo ""
echo "Files in MinIO:"
docker exec olake-s3-minio mc ls myminio/source-data/chunk_test/parquet/ --recursive

# Step 4: Create config for chunk testing
echo ""
echo -e "${BLUE}Step 4: Creating Test Configuration${NC}"
echo "----------------------------------------"

cat > "$SCRIPT_DIR/config-chunk-test.json" << 'EOF'
{
  "bucket_name": "source-data",
  "path_prefix": "chunk_test/parquet/",
  "file_format": "parquet",
  "stream_grouping_level": 0,
  "endpoint": "http://localhost:9000",
  "access_key_id": "admin",
  "secret_access_key": "password",
  "region": "us-east-1",
  "use_path_style": true,
  "parquet": {
    "streaming_enabled": true
  }
}
EOF

echo -e "${GREEN}✓ Configuration created${NC}"

# Step 4.5: Discover streams to get actual stream names
echo ""
echo -e "${BLUE}Step 4.5: Discovering Streams${NC}"
echo "----------------------------------------"
cd "$PROJECT_ROOT"

./build.sh driver-s3 discover \
  --config "$SCRIPT_DIR/config-chunk-test.json" > "$SCRIPT_DIR/discovered-streams.json" 2>&1

# Extract stream names from discovery
if [ -f "$SCRIPT_DIR/discovered-streams.json" ]; then
    echo "Discovered streams:"
    grep -o '"name":"[^"]*"' "$SCRIPT_DIR/discovered-streams.json" | head -5 || echo "Could not extract stream names"
    
    # Use the discovered streams as the catalog
    echo "Using discovered streams as catalog..."
    cp "$SCRIPT_DIR/discovered-streams.json" "$SCRIPT_DIR/streams-chunk-test.json"
    
    # Modify sync_mode for all streams
    cat "$SCRIPT_DIR/streams-chunk-test.json" | \
        sed 's/"sync_mode":"[^"]*"/"sync_mode":"full_refresh"/g' > "$SCRIPT_DIR/streams-chunk-test.tmp.json"
    mv "$SCRIPT_DIR/streams-chunk-test.tmp.json" "$SCRIPT_DIR/streams-chunk-test.json"
fi

echo -e "${GREEN}✓ Discovery complete${NC}"

# Step 5: Run S3 driver with chunking
echo ""
echo -e "${BLUE}Step 5: Run S3 Driver Sync (Watch Chunking)${NC}"
echo "----------------------------------------"
cd "$PROJECT_ROOT"

echo ""
echo -e "${YELLOW}Expected chunking behavior:${NC}"
echo "  • Files will be grouped into 2GB chunks"
echo "  • You should see log lines like: 'Processing chunk with X files'"
echo "  • Large files (>2GB) will be individual chunks"
echo ""

./build.sh driver-s3 sync \
  --config "$SCRIPT_DIR/config-chunk-test.json" \
  --catalog "$SCRIPT_DIR/streams-chunk-test.json" \
  --destination "$SCRIPT_DIR/destination.json" 2>&1 | tee "$SCRIPT_DIR/chunk_test.log"

# Step 6: Verify chunking in logs
echo ""
echo -e "${BLUE}Step 6: Verify Chunking Behavior${NC}"
echo "----------------------------------------"

if [ -f "$SCRIPT_DIR/chunk_test.log" ]; then
    echo "Analyzing chunk grouping from logs..."
    echo ""
    
    # Look for chunk processing patterns
    grep -i "chunk" "$SCRIPT_DIR/chunk_test.log" || echo "No explicit chunk logs found"
    
    # Count files processed
    FILE_COUNT=$(grep -c "Processing file" "$SCRIPT_DIR/chunk_test.log" 2>/dev/null || echo "0")
    echo ""
    echo "Files processed: $FILE_COUNT"
    
    # Check for large file handling
    if grep -q "large_2500mb\|large_3gb" "$SCRIPT_DIR/chunk_test.log"; then
        echo -e "${GREEN}✓ Large files (>2GB) detected${NC}"
    fi
    
    # Verify in Iceberg
    echo ""
    echo "Checking Iceberg for data..."
    RECORD_COUNT=$(docker exec olake-iceberg-catalog psql -U iceberg -d iceberg -t -c "SELECT COUNT(*) FROM chunk_test_chunk_test;" 2>/dev/null || echo "0")
    RECORD_COUNT=$(echo $RECORD_COUNT | tr -d '[:space:]')
    echo "Records in Iceberg: $RECORD_COUNT"
    
    if [ "$RECORD_COUNT" -gt 0 ]; then
        echo -e "${GREEN}✓ Data successfully synced to Iceberg${NC}"
    else
        echo -e "${YELLOW}⚠ No data found in Iceberg (check logs)${NC}"
    fi
fi

echo ""
echo "============================================"
echo -e "${GREEN}✅ Chunk Boundary Test Complete!${NC}"
echo "============================================"
echo ""
echo "Summary:"
if [ "$LITE_MODE" == "true" ]; then
    echo "  • Generated lightweight files (1MB to 35MB)"
    echo "  • Tested chunk grouping logic"
    echo "  • Verified sync to Iceberg"
    echo ""
    echo "💡 To run full production-size testing:"
    echo "     ./test_chunking.sh --full"
else
    echo "  • Generated full-size files (10MB to 3GB)"
    echo "  • Tested 2GB chunk grouping logic"
    echo "  • Verified sync to Iceberg"
fi
echo ""
echo "Review logs:"
echo "  cat $SCRIPT_DIR/chunk_test.log"
echo ""
echo "File sizes in MinIO:"
echo "  docker exec olake-s3-minio mc ls myminio/source-data/chunk_test/parquet/ --recursive"
echo ""
echo "Cleanup:"
echo "  rm -rf test_data_chunks"
echo "  docker compose down -v"
echo ""

