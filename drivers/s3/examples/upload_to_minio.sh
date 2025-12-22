#!/bin/bash

# Upload generated test data to MinIO
# This script uploads the Parquet, CSV, and JSON files to the source-data bucket

set -e

echo "=== Uploading Test Data to MinIO ==="

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Check if test_data directory exists
if [ ! -d "test_data" ]; then
    echo -e "${RED}Error: test_data directory not found!${NC}"
    echo "Please run generate_test_data.py first:"
    echo "  python3 generate_test_data.py"
    exit 1
fi

# Check if MinIO is running
if ! docker ps | grep -q olake-s3-minio; then
    echo -e "${RED}Error: MinIO container is not running!${NC}"
    echo "Please start MinIO first:"
    echo "  docker compose up -d"
    exit 1
fi

echo -e "${BLUE}Uploading files to MinIO using docker cp + mc...${NC}"
echo ""

# Copy test data into MinIO container
echo -e "${BLUE}Copying test data into MinIO container...${NC}"
docker cp test_data olake-s3-minio:/tmp/
echo -e "${GREEN}✓ Files copied to container${NC}"

# Install mc (MinIO Client) if needed and upload files
echo -e "${BLUE}Uploading files to S3 buckets...${NC}"

docker exec olake-s3-minio sh -c '
set -e

# Set up mc alias
mc alias set local http://localhost:9000 admin password 2>/dev/null || true

# Upload Parquet files
echo "📦 Uploading Parquet files..."
mc cp --recursive /tmp/test_data/parquet/ local/source-data/parquet/ 2>&1 || true

# Upload CSV files  
echo "📄 Uploading CSV files..."
mc cp --recursive /tmp/test_data/csv/ local/source-data/csv/ 2>&1 || true

# Upload JSON files
echo "🔤 Uploading JSON files..."
mc cp --recursive /tmp/test_data/json/ local/source-data/json/ 2>&1 || true

# Upload Mixed format files
echo "🔀 Uploading Mixed format files..."
mc cp --recursive /tmp/test_data/mixed/ local/source-data/mixed/ 2>&1 || true

# Cleanup
rm -rf /tmp/test_data
'

echo -e "${GREEN}✓ All files uploaded${NC}"

echo ""
echo -e "${GREEN}=== Upload Complete ===${NC}"
echo ""
echo "Files uploaded to s3://source-data/ with structure:"
echo "  ├── parquet/ (Parquet files)"
echo "  ├── csv/     (CSV + gzipped CSV)"
echo "  ├── json/    (JSON + JSONL + gzipped)"
echo "  └── mixed/   (Multiple formats)"
echo ""
echo "Verify in MinIO Console: http://localhost:9001"
echo "  Username: admin"
echo "  Password: password"
echo ""
echo -e "${BLUE}Next steps - Test different formats:${NC}"
echo ""
echo "1. Test Parquet:"
echo "   ./build.sh driver-s3 discover --config \$(pwd)/drivers/s3/examples/source-parquet.json"
echo ""
echo "2. Test CSV:"
echo "   ./build.sh driver-s3 discover --config \$(pwd)/drivers/s3/examples/source-csv.json"
echo ""
echo "3. Test JSON:"
echo "   ./build.sh driver-s3 discover --config \$(pwd)/drivers/s3/examples/source-json.json"
echo ""
echo "4. Run sync (example for Parquet):"
echo "   ./build.sh driver-s3 sync \\"
echo "     --config \$(pwd)/drivers/s3/examples/source-parquet.json \\"
echo "     --catalog \$(pwd)/drivers/s3/examples/streams.json \\"
echo "     --destination \$(pwd)/drivers/s3/examples/destination.json"

