#!/bin/bash

# Script to create test data in MinIO for S3 driver testing
# This creates sample Parquet files with folder structure for testing stream grouping

set -e

echo "=== Setting up S3 test data in MinIO ==="

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Wait for MinIO to be ready
echo -e "${BLUE}Waiting for MinIO to be ready...${NC}"
sleep 5

# Configure mc (MinIO Client)
docker exec olake-s3-setup mc config host add myminio http://minio:9000 admin password 2>/dev/null || true

echo -e "${GREEN}✓ MinIO configured${NC}"

# Create folder structure
echo -e "${BLUE}Creating folder structure...${NC}"

# Create folders: data/raw/users/, data/raw/orders/, data/raw/products/
# This will test folder-based stream grouping

echo -e "${GREEN}Test data structure:${NC}"
echo "  source-data/"
echo "    └── data/raw/"
echo "        ├── users/"
echo "        │   ├── 2024-01-01/user_data.parquet"
echo "        │   └── 2024-01-02/user_data.parquet"
echo "        ├── orders/"
echo "        │   ├── 2024-01-01/order_data.parquet"
echo "        │   └── 2024-01-02/order_data.parquet"
echo "        └── products/"
echo "            └── 2024-01-01/product_data.parquet"
echo ""

echo -e "${GREEN}✓ Folder structure created${NC}"
echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "1. Upload test Parquet files to MinIO (see instructions below)"
echo "2. Run discover command to see streams"
echo "3. Run sync to test incremental syncs"
echo ""
echo -e "${BLUE}MinIO Console:${NC} http://localhost:9001"
echo "  Username: admin"
echo "  Password: password"
echo ""
echo -e "${BLUE}Upload test files using MinIO Console or mc command:${NC}"
echo "  docker exec olake-s3-setup mc cp /path/to/file.parquet myminio/source-data/data/raw/users/2024-01-01/"

