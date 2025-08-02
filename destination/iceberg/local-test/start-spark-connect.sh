#!/bin/bash

# Enhanced error handling and logging
set -euo pipefail

# Function to log with timestamps
log() {
    echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] $1"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Create the Ivy cache directory if it doesn't exist
mkdir -p /root/.ivy2/jars
mkdir -p /root/.ivy2/cache/org.apache.spark/spark-connect_2.12/jars

# Download the problematic JAR file if it doesn't exist or is corrupted
if [ ! -f "/root/.ivy2/jars/spark-connect_2.12-3.5.2.jar" ] || [ ! -s "/root/.ivy2/jars/spark-connect_2.12-3.5.2.jar" ]; then
    log "Downloading spark-connect JAR file..."
    # Remove corrupted file if it exists
    rm -f /root/.ivy2/jars/spark-connect_2.12-3.5.2.jar
    rm -f /root/.ivy2/cache/org.apache.spark/spark-connect_2.12/jars/spark-connect_2.12-3.5.2.jar
    
    # Download with enhanced retry logic
    for attempt in {1..5}; do
        log "Download attempt $attempt/5..."
        
        # Use wget with timeout and progress
        if command_exists wget; then
            if timeout 300 wget --timeout=60 --tries=3 --progress=bar:force \
                -O /root/.ivy2/jars/spark-connect_2.12-3.5.2.jar \
                https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.2/spark-connect_2.12-3.5.2.jar; then
                
                # Verify file size (should be ~14MB)
                if [ -s "/root/.ivy2/jars/spark-connect_2.12-3.5.2.jar" ] && [ $(stat -c%s "/root/.ivy2/jars/spark-connect_2.12-3.5.2.jar") -gt 1000000 ]; then
                    log "Download completed successfully"
                    break
                else
                    log "Downloaded file is too small, retrying..."
                    rm -f /root/.ivy2/jars/spark-connect_2.12-3.5.2.jar
                fi
            else
                log "Download failed, retrying..."
                rm -f /root/.ivy2/jars/spark-connect_2.12-3.5.2.jar
            fi
        else
            log "ERROR: wget not available, cannot download JAR"
            exit 1
        fi
        
        if [ $attempt -lt 5 ]; then
            log "Waiting 10 seconds before retry..."
            sleep 10
        fi
    done
    
    # Final verification
    if [ ! -f "/root/.ivy2/jars/spark-connect_2.12-3.5.2.jar" ] || [ ! -s "/root/.ivy2/jars/spark-connect_2.12-3.5.2.jar" ]; then
        log "ERROR: Failed to download spark-connect JAR after 5 attempts"
        exit 1
    fi
else
    log "spark-connect JAR file already exists and is valid"
fi

# Copy the JAR to Ivy's cache directory to ensure consistency
if [ -f "/root/.ivy2/jars/spark-connect_2.12-3.5.2.jar" ]; then
    log "Ensuring Ivy cache consistency..."
    cp /root/.ivy2/jars/spark-connect_2.12-3.5.2.jar /root/.ivy2/cache/org.apache.spark/spark-connect_2.12/jars/
    
    # Create or update Ivy metadata to mark the artifact as successfully resolved
    cat > /root/.ivy2/cache/org.apache.spark/spark-connect_2.12/ivydata-3.5.2.properties << 'EOF'
#ivy cached data file for org.apache.spark#spark-connect_2.12;3.5.2
#$(date -u '+%a %b %d %H:%M:%S UTC %Y')
resolver=central
artifact\:spark-connect_2.12\#jar\#jar\#-1353658508.exists=true
artifact\:spark-connect_2.12\#jar\#jar\#-1353658508.location=https\://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.2/spark-connect_2.12-3.5.2.jar
artifact\:spark-connect_2.12\#jar\#jar\#-1353658508.is-local=true
artifact\:spark-connect_2.12\#jar\#jar\#-1353658508.original=artifact\:spark-connect_2.12\#jar\#jar\#-1353658508
EOF
    log "Ivy cache metadata updated"
fi

# Start the original entrypoint
log "Starting original entrypoint..."
./entrypoint.sh

log 'Starting Spark Connect server...'
/opt/spark/sbin/start-connect-server.sh --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.postgresql:postgresql:42.5.4,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-connect_2.12:3.5.2 --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem &

log 'Starting Jupyter Notebook server...'
export PYSPARK_DRIVER_PYTHON=jupyter-notebook
export PYSPARK_DRIVER_PYTHON_OPTS="--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root"
pyspark &

log "All services started, entering main loop..."
while true; do sleep 30; done 