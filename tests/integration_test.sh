#!/bin/bash
set -e

echo "Running integration test for config migration changes..."

# Create a temporary directory for test files
TEMP_DIR=$(mktemp -d)
echo "Using temp directory: $TEMP_DIR"

# Clean up on exit
trap "rm -rf $TEMP_DIR" EXIT

# Create test files

# 1. Old config.json (with default_mode)
cat > "$TEMP_DIR/config.json" << EOF
{
  "host": "localhost",
  "port": 5432,
  "database": "postgres",
  "username": "postgres",
  "password": "postgres",
  "default_mode": "full_refresh",
  "ssl": {
    "mode": "disable"
  },
  "reader_batch_size": 10000,
  "max_threads": 10
}
EOF

# 2. New source.json (with sync_settings)
cat > "$TEMP_DIR/source.json" << EOF
{
  "host": "localhost",
  "port": 5432,
  "database": "postgres",
  "username": "postgres",
  "password": "postgres",
  "sync_settings": {
    "mode": "full_refresh"
  },
  "ssl": {
    "mode": "disable"
  },
  "reader_batch_size": 10000,
  "max_threads": 10
}
EOF

# 3. Catalog with default_mode
cat > "$TEMP_DIR/catalog.json" << EOF
{
  "streams": [
    {
      "stream": {
        "name": "test_table",
        "namespace": "public"
      }
    }
  ],
  "default_mode": "full_refresh"
}
EOF

# 4. Empty state and writer config for testing
cat > "$TEMP_DIR/state.json" << EOF
{}
EOF

cat > "$TEMP_DIR/writer.json" << EOF
{
  "type": "jsonl",
  "destination_path": "${TEMP_DIR}/output"
}
EOF

echo "Test files created. You can manually run these commands:"
echo "1. Test with old config.json:"
echo "   ./build.sh driver-postgres sync --config $TEMP_DIR/config.json --catalog $TEMP_DIR/catalog.json --destination $TEMP_DIR/writer.json --state $TEMP_DIR/state.json"
echo ""
echo "2. Test with new source.json:"
echo "   ./build.sh driver-postgres sync --config $TEMP_DIR/source.json --catalog $TEMP_DIR/catalog.json --destination $TEMP_DIR/writer.json --state $TEMP_DIR/state.json"
echo ""
echo "3. Test with --source flag:"
echo "   ./build.sh driver-postgres sync --source $TEMP_DIR/source.json --catalog $TEMP_DIR/catalog.json --destination $TEMP_DIR/writer.json --state $TEMP_DIR/state.json"
echo ""

echo "Integration test files prepared at $TEMP_DIR"
echo "You'll need to run these commands manually to verify since they require a working PostgreSQL database." 