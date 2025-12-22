#!/bin/bash

# Quick Start Script for S3 Driver Testing
# This script automates the entire setup process

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}"
echo "╔═══════════════════════════════════════════╗"
echo "║   S3 Driver Testing - Quick Start        ║"
echo "╔═══════════════════════════════════════════╗"
echo -e "${NC}"

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Prerequisites check passed${NC}\n"

# Step 1: Start Docker containers
echo -e "${BLUE}[Step 1/5] Starting MinIO and PostgreSQL...${NC}"
docker compose up -d
sleep 10
echo -e "${GREEN}✓ Containers started${NC}\n"

# Step 2: Generate test data
echo -e "${BLUE}[Step 2/5] Generating test Parquet files...${NC}"
python3 generate_test_data.py
echo -e "${GREEN}✓ Test data generated${NC}\n"

# Step 3: Upload to MinIO
echo -e "${BLUE}[Step 3/5] Uploading data to MinIO...${NC}"
./upload_to_minio.sh
echo -e "${GREEN}✓ Data uploaded${NC}\n"

# Step 4: Build S3 driver
echo -e "${BLUE}[Step 4/5] Building S3 driver...${NC}"
cd ../../..
./build.sh driver-s3 > /dev/null 2>&1 || true
echo -e "${GREEN}✓ Driver built${NC}\n"

# Step 5: Run discover for all formats
echo -e "${BLUE}[Step 5/5] Discovering streams across all formats...${NC}"

echo "  → Discovering Parquet streams..."
./build.sh driver-s3 discover --config $(pwd)/drivers/s3/examples/source-parquet.json > $(pwd)/drivers/s3/examples/catalog_parquet.json 2>&1 || true

echo "  → Discovering CSV streams..."
./build.sh driver-s3 discover --config $(pwd)/drivers/s3/examples/source-csv.json > $(pwd)/drivers/s3/examples/catalog_csv.json 2>&1 || true

echo "  → Discovering JSON streams..."
./build.sh driver-s3 discover --config $(pwd)/drivers/s3/examples/source-json.json > $(pwd)/drivers/s3/examples/catalog_json.json 2>&1 || true

echo -e "${GREEN}✓ Discovery complete for all formats${NC}\n"

# Summary
echo -e "${GREEN}"
echo "╔═══════════════════════════════════════════╗"
echo "║          Setup Complete! 🎉               ║"
echo "╚═══════════════════════════════════════════╝"
echo -e "${NC}"

echo -e "${BLUE}What was set up:${NC}"
echo "  ✓ MinIO (S3-compatible storage)"
echo "  ✓ PostgreSQL (Iceberg catalog)"
echo "  ✓ Test files in Parquet, CSV (plain + gzip), JSON (JSONL + array + gzip)"
echo "  ✓ S3 driver built and ready"
echo ""

echo -e "${BLUE}Access Points:${NC}"
echo "  MinIO Console:    http://localhost:9001"
echo "    Username: admin"
echo "    Password: password"
echo ""
echo "  PostgreSQL:       localhost:5432"
echo "    Database: iceberg"
echo "    Username: iceberg"
echo "    Password: password"
echo ""

echo -e "${BLUE}Test Data Generated:${NC}"
echo "  📦 Parquet: users, orders, products (5 files)"
echo "  📄 CSV:     users, orders, products (5 files, with gzip)"
echo "  🔤 JSON:    users, orders, products (5 files, JSONL + array + gzip)"
echo "  🔀 Mixed:   transactions (3 files, different formats)"
echo ""

echo -e "${BLUE}Discovered Catalogs:${NC}"
echo "  Parquet: drivers/s3/examples/catalog_parquet.json"
echo "  CSV:     drivers/s3/examples/catalog_csv.json"
echo "  JSON:    drivers/s3/examples/catalog_json.json"
echo ""

echo -e "${BLUE}Next Steps:${NC}"
echo ""
echo "  ${YELLOW}📋 Option 1: Use VS Code Debugger${NC}"
echo "     1. Open VS Code"
echo "     2. Go to Run and Debug (⇧⌘D)"
echo "     3. Select a configuration:"
echo "        • S3: Discover - Parquet"
echo "        • S3: Discover - CSV"
echo "        • S3: Discover - JSON"
echo "        • S3: Sync - Parquet (No State)"
echo "        • S3: Sync - CSV (With State)"
echo "     4. Set breakpoints and press F5"
echo ""
echo "  ${YELLOW}💻 Option 2: Command Line${NC}"
echo ""
echo "  1. Discover streams (Parquet example):"
echo "     ${YELLOW}cd /path/to/olake${NC}"
echo "     ${YELLOW}./build.sh driver-s3 discover --config \\${NC}"
echo "     ${YELLOW}  \$(pwd)/drivers/s3/examples/source-parquet.json${NC}"
echo ""
echo "  2. Generate streams.json from discovery:"
echo "     ${YELLOW}# Run discovery and extract catalog${NC}"
echo "     ${YELLOW}./build.sh driver-s3 discover \\${NC}"
echo "     ${YELLOW}  --config \$(pwd)/drivers/s3/examples/source-parquet.json > catalog.json${NC}"
echo "     ${YELLOW}# Then manually configure sync_mode in streams.json${NC}"
echo "     ${YELLOW}# See streams.template.json for examples${NC}"
echo ""
echo "  3. Test discovery for all formats:"
echo "     ${YELLOW}# Parquet${NC}"
echo "     ${YELLOW}./build.sh driver-s3 discover --config \$(pwd)/drivers/s3/examples/source-parquet.json${NC}"
echo ""
echo "     ${YELLOW}# CSV (with gzip support)${NC}"
echo "     ${YELLOW}./build.sh driver-s3 discover --config \$(pwd)/drivers/s3/examples/source-csv.json${NC}"
echo ""
echo "     ${YELLOW}# JSON (JSONL + array formats, with gzip)${NC}"
echo "     ${YELLOW}./build.sh driver-s3 discover --config \$(pwd)/drivers/s3/examples/source-json.json${NC}"
echo ""
echo ""

echo -e "${BLUE}Documentation:${NC}"
echo "  Full guide: drivers/s3/examples/README.md"
echo ""

echo -e "${GREEN}Happy Testing! 🚀${NC}"

