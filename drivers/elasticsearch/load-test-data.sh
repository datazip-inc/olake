#!/bin/bash
set -e
ES_HOST="${1:-http://localhost:9200}"
INDEX="${2:-test_logs}"
echo "Loading test data to $ES_HOST/$INDEX..."
until curl -s "$ES_HOST/_cluster/health" > /dev/null 2>&1; do sleep 1; done
curl -s -X PUT "$ES_HOST/$INDEX" -H "Content-Type: application/json" -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}' > /dev/null || true
for i in {1..5}; do
  curl -s -X POST "$ES_HOST/$INDEX/_doc" -H "Content-Type: application/json" -d "{\"doc_id\":$i,\"message\":\"Document $i\",\"timestamp\":\"2025-01-13T10:0${i}:00Z\"}" > /dev/null
done
curl -s -X POST "$ES_HOST/$INDEX/_refresh" > /dev/null
COUNT=$(curl -s "$ES_HOST/$INDEX/_count" | grep -o '"count":[0-9]*' | cut -d: -f2)
echo "âœ“ Loaded $COUNT documents"
