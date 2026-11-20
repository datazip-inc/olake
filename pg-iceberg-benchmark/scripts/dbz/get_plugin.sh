#!/bin/bash
# Downloads the Iceberg Kafka Connect sink runtime into dbz/plugins (needed once per host).
set -e; cd "$(dirname "${BASH_SOURCE[0]}")"; V=0.6.19
[ -d "plugins/iceberg-kafka-connect-runtime-$V" ] && { echo "already present"; exit 0; }
mkdir -p plugins
curl -sfL -o plugins/rt.zip "https://github.com/databricks/iceberg-kafka-connect/releases/download/v$V/iceberg-kafka-connect-runtime-$V.zip"
python3 -m zipfile -e plugins/rt.zip plugins/ && mv plugins/rt.zip "plugins/runtime-$V.zip.done"
ls plugins
