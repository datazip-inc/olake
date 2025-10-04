#!/bin/bash
set -e

echo "================================================"
echo "Initializing Spark with Iceberg REST Catalog"
echo "================================================"

# Environment variables
export SPARK_HOME=/usr/local/spark
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/spark/conf
export SPARK_CONF_DIR=/opt/spark/conf

# Copy configuration files if they exist
if [ -d "/opt/spark/conf-mounted" ]; then
    echo "✓ Copying Spark configuration files..."
    cp -r /opt/spark/conf-mounted/* /opt/spark/conf/ 2>/dev/null || true
fi

# Create necessary directories
mkdir -p /tmp/spark-events
mkdir -p /tmp/spark-warehouse

# Set proper permissions
chmod -R 755 /tmp/spark-events
chmod -R 755 /tmp/spark-warehouse

echo "✓ Spark configuration initialized successfully"
echo "✓ Configuration directory: $SPARK_CONF_DIR"
echo "✓ Hadoop configuration directory: $HADOOP_CONF_DIR"
echo "================================================"

# Install required Python packages
echo "Installing Python dependencies..."
pip install --quiet --no-cache-dir \
    pyiceberg \
    pandas \
    matplotlib \
    seaborn

echo "✓ Python dependencies installed"
echo "================================================"

# Verify Iceberg REST catalog connectivity
echo "Verifying Iceberg REST catalog..."
CATALOG_URL="http://rest:8181/v1/config"
TIMEOUT=60
ELAPSED=0

while [ $ELAPSED -lt $TIMEOUT ]; do
    if curl -s -f "$CATALOG_URL" > /dev/null 2>&1; then
        echo "✓ Iceberg REST catalog is accessible at http://rest:8181"
        break
    fi
    echo "  Waiting for Iceberg REST catalog... ($ELAPSED/$TIMEOUT seconds)"
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

if [ $ELAPSED -ge $TIMEOUT ]; then
    echo "⚠ Warning: Could not verify Iceberg REST catalog"
fi

echo "================================================"
echo "Spark initialization complete!"
echo "================================================"

# Start Jupyter
exec "$@"
