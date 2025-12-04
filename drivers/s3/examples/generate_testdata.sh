#!/bin/bash
set -e

# Generate test data using Go generator
# No Python dependencies required!

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${BLUE}=== S3 Driver Test Data Generator ===${NC}"
echo ""

# Parse arguments
SIZE="small"
FORMAT="all"
OUTPUT_DIR="test_data"

while [[ $# -gt 0 ]]; do
    case $1 in
        --size)
            SIZE="$2"
            shift 2
            ;;
        --format)
            FORMAT="$2"
            shift 2
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --size SIZE      Data size: small, medium, or large (default: small)"
            echo "                   - small:  10K users, 50K orders, 5K products"
            echo "                   - medium: 1M users, 1.5M orders, 100K products"
            echo "                   - large:  5M users, 10M orders, 500K products"
            echo "  --format FORMAT  Format: parquet, csv, json, or all (default: all)"
            echo "  --output DIR     Output directory (default: test_data)"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                           # Generate small files, all formats"
            echo "  $0 --size medium            # Generate medium files"
            echo "  $0 --format parquet          # Generate only Parquet files"
            echo "  $0 --size large --format csv # Generate large CSV files"
            exit 0
            ;;
        *)
            echo -e "${YELLOW}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}Configuration:${NC}"
echo "  Size:   $SIZE"
echo "  Format: $FORMAT"
echo "  Output: $OUTPUT_DIR"
echo ""

# Build the Go generator
echo -e "${BLUE}Building Go test data generator...${NC}"
cd testdata
go build -o ../generate-testdata main.go
cd ..
echo -e "${GREEN}✓ Generator built${NC}"
echo ""

# Run the generator
echo -e "${BLUE}Generating test data...${NC}"
./generate-testdata -size="$SIZE" -format="$FORMAT" -output="$OUTPUT_DIR"

echo ""
echo -e "${GREEN}✅ Test data generated successfully!${NC}"
echo ""
echo -e "${BLUE}Generated data in: $OUTPUT_DIR${NC}"
ls -lh "$OUTPUT_DIR"/*/ 2>/dev/null | head -20 || true

echo ""
echo -e "${BLUE}Next steps:${NC}"
echo "  1. Start Docker:  docker compose up -d"
echo "  2. Upload data:   ./upload_to_minio.sh"
echo "  3. Run tests:     ./run_tests.sh"

