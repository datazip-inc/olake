#!/bin/sh
set -eu

BASE_URL="http://amoro:1630"
HDR_SOURCE="X-Request-Source: Web"

COOKIE_JAR="/tmp/amoro-cookies.txt"

while true; do
  echo "Waiting for Amoro to be reachable..."
  until curl -sf "${BASE_URL}/" >/dev/null 2>&1; do
    sleep 1
  done

  echo "Logging into Amoro..."
  curl -sf -c "${COOKIE_JAR}" \
    -X POST "${BASE_URL}/api/ams/v1/login" \
    -H "Content-Type: application/json" \
    -H "${HDR_SOURCE}" \
    -d '{"user":"admin","password":"admin"}' >/dev/null

  # Wait until the local container is registered (it comes from config.yaml)
  echo "Waiting for localContainer to be registered..."
  until curl -sf -b "${COOKIE_JAR}" -H "${HDR_SOURCE}" "${BASE_URL}/api/ams/v1/optimize/containers/get" | grep -q 'localContainer'; do
    sleep 1
  done

  echo "Ensuring optimizer group 'local' exists with memory=1024..."
  # PUT is idempotent (create-or-update semantics in this UI API)
  curl -sf -b "${COOKIE_JAR}" \
    -X PUT "${BASE_URL}/api/ams/v1/optimize/resourceGroups" \
    -H "Content-Type: application/json" \
    -H "${HDR_SOURCE}" \
    -d '{"name":"local","container":"localContainer","properties":{"memory":"1024"}}' >/dev/null

  echo "Ensuring at least one RUNNING optimizer exists..."
  opt_json="$(curl -sf -b "${COOKIE_JAR}" -H "${HDR_SOURCE}" "${BASE_URL}/api/ams/v1/optimize/optimizerGroups/all/optimizers?page=1&pageSize=50")"
  if echo "$opt_json" | grep -q '"groupName":"local"'; then
    if echo "$opt_json" | grep -q '"groupName":"local".*"jobStatus":"RUNNING"'; then
      echo "Optimizer is RUNNING."
    else
      echo "Optimizer record exists but not RUNNING; scaling out (parallelism=1)..."
      curl -sf -b "${COOKIE_JAR}" \
        -X POST "${BASE_URL}/api/ams/v1/optimize/optimizerGroups/local/optimizers" \
        -H "Content-Type: application/json" \
        -H "${HDR_SOURCE}" \
        -d '{"parallelism":1}' >/dev/null || true
    fi
  else
    echo "No optimizer found for group 'local'; scaling out (parallelism=1)..."
    curl -sf -b "${COOKIE_JAR}" \
      -X POST "${BASE_URL}/api/ams/v1/optimize/optimizerGroups/local/optimizers" \
      -H "Content-Type: application/json" \
      -H "${HDR_SOURCE}" \
      -d '{"parallelism":1}' >/dev/null || true
  fi

  # Re-check periodically so restarts of `amoro` automatically restore the optimizer.
  sleep 15
done


