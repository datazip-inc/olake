#!/bin/bash
set -e

set -a
source .env
set +a

BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)
TEMP_DIR="/tmp/olake-$BRANCH_NAME"
CONFIG_DIR="$TEMP_DIR/config"
DRIVER_NAME="postgres"
DOCKER_IMAGE="source-$DRIVER_NAME"

mkdir -p "$CONFIG_DIR"

echo "$SOURCE_JSON2" > "$CONFIG_DIR/source.json"
echo "$DESTINATION_JSON_GLUE" > "$CONFIG_DIR/destination.json"
cp ./performance/benchmark_stats.json "$CONFIG_DIR/benchmark_stats.json"
cp ./performance/benchmark_stats_cdc.json "$CONFIG_DIR/benchmark_stats_cdc.json"


if [ ! -s "$CONFIG_DIR/source.json" ]; then
  echo "source.json not created or empty."
  exit 1
fi
if [ ! -s "$CONFIG_DIR/destination.json" ]; then
  echo "destination.json not created or empty."
  exit 1
fi
if [ ! -s "$CONFIG_DIR/benchmark_stats.json" ]; then
  echo "benchmark_stats.json not created or empty."
  exit 1
fi

mvn -f writers/iceberg/debezium-server-iceberg-sink/pom.xml clean package -DskipTests > /dev/null || { echo "Maven build failed"; exit 1; }

echo "Building Docker image..."
docker build -t "$DOCKER_IMAGE" --build-arg DRIVER_NAME="$DRIVER_NAME" . > /dev/null || { echo "Docker build failed"; exit 1; }
sleep 30

echo "Starting Iceberg"
docker compose -f ./writers/iceberg/local-test/docker-compose.yml up -d
sleep 30

echo "Running discover..."
ls -lh "$CONFIG_DIR"
docker run --network local-test_iceberg_net -v "$CONFIG_DIR:/mnt/config" "$DOCKER_IMAGE" discover --config /mnt/config/source.json > /dev/null || { echo "Discover failed"; exit 1; }

if [ ! -f "$CONFIG_DIR/streams.json" ]; then
  echo "streams.json not found. Discovery failed."
  exit 1
fi

jq '
{
  selected_streams: {
    public: (
      .selected_streams.public
      | map(select(.stream_name == "spatial_ref_sys"))
    )
  },
  streams: (
    .streams
    | map(select(.stream.name == "spatial_ref_sys")
      | .stream.sync_mode = "full_refresh"
      | {stream: .stream}
    )
  )
}
' "$CONFIG_DIR/streams.json" > "$CONFIG_DIR/streams.json.tmp" && mv "$CONFIG_DIR/streams.json.tmp" "$CONFIG_DIR/streams.json" || { echo "Error updating streams.json"; exit 1; }

echo "Updated streams.json content:"
cat "$CONFIG_DIR/streams.json"

echo "Performance Test: Full Refresh"

echo "Running sync..."
docker run --network local-test_iceberg_net -v "$CONFIG_DIR:/mnt/config" "$DOCKER_IMAGE" sync --config /mnt/config/source.json --catalog /mnt/config/streams.json --destination /mnt/config/destination.json 

if [ ! -f "$CONFIG_DIR/stats.json" ]; then
  echo "stats.json not found. Sync failed."
  exit 1
fi

ACTUAL_RPS=$(jq -r '.Speed' "$CONFIG_DIR/stats.json" | sed 's/ rps//')
BENCHMARK_RPS=$(jq -r '.Speed' "$CONFIG_DIR/benchmark_stats.json" | sed 's/ rps//')

if (( $(echo "$ACTUAL_RPS >= 0.9 * $BENCHMARK_RPS" | bc -l) )); then
  echo "RPS check passed. Actual: $ACTUAL_RPS, benchmark: $BASELINE_RPS"
else
  echo "RPS check failed. Actual: $ACTUAL_RPS, benchmark: $BENCHMARK_RPS"
  exit 1
fi

echo "Performance Test: CDC"

echo "Running discover..."
ls -lh "$CONFIG_DIR"
docker run --network local-test_iceberg_net  -v "$CONFIG_DIR:/mnt/config" "$DOCKER_IMAGE" discover --config /mnt/config/source.json > /dev/null || { echo "Discover failed"; exit 1; }

if [ ! -f "$CONFIG_DIR/streams.json" ]; then
  echo "streams.json not found. Discovery failed."
  exit 1
fi

jq '
{
  selected_streams: {
    public: (
      .selected_streams.public
      | map(select(.stream_name == "spatial_ref_sys_cdc"))
    )
  },
  streams: (
    .streams
    | map(select(.stream.name == "spatial_ref_sys_cdc")
      | .stream.sync_mode = "cdc"
      | {stream: .stream}
    )
  )
}
' "$CONFIG_DIR/streams.json" > "$CONFIG_DIR/streams.json.tmp" && mv "$CONFIG_DIR/streams.json.tmp" "$CONFIG_DIR/streams.json" || { echo "Error updating streams.json"; exit 1; }

echo "Updated streams.json content:"
cat "$CONFIG_DIR/streams.json"
cat "$CONFIG_DIR/destination.json"

echo "Running sync without state..."
docker run --network local-test_iceberg_net -v "$CONFIG_DIR:/mnt/config" "$DOCKER_IMAGE" sync --config /mnt/config/source.json --catalog /mnt/config/streams.json --destination /mnt/config/destination.json 

echo "Running Postgres"



echo "Running sync with state..."
docker run --network local-test_iceberg_net -v "$CONFIG_DIR:/mnt/config" "$DOCKER_IMAGE" sync --config /mnt/config/source.json --catalog /mnt/config/streams.json --destination /mnt/config/destination.json --state /mnt/config/state.json

echo "Running Postgres"


echo "Stopping Iceberg"
docker compose -f ./writers/iceberg/local-test/docker-compose.yml down

if [ ! -f "$CONFIG_DIR/stats.json" ]; then
  echo "stats.json not found. Sync failed."
  exit 1
fi

ACTUAL_RPS=$(jq -r '.Speed' "$CONFIG_DIR/stats.json" | sed 's/ rps//')
BENCHMARK_RPS=$(jq -r '.Speed' "$CONFIG_DIR/benchmark_stats_cdc.json" | sed 's/ rps//')

if (( $(echo "$ACTUAL_RPS >= 0.9 * $BENCHMARK_RPS" | bc -l) )); then
  echo "RPS check passed. Actual: $ACTUAL_RPS, benchmark: $BASELINE_RPS"
else
  echo "RPS check failed. Actual: $ACTUAL_RPS, benchmark: $BENCHMARK_RPS"
  exit 1
fi
