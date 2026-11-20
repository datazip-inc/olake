#!/bin/bash
# Benchmark driver: 4 full loads (sequential), one CDC workload, 4 CDC catch-ups (sequential).
# usage: run_all.sh <tag> [cdc_transactions] [rows_per_transaction]
#   <tag> names the run; it must not have been used before against this source and Glue database.
# Run inside tmux. Stops at the first failed step and prints CHAIN_ABORTED.
cd "$(dirname "${BASH_SOURCE[0]}")" && source ./common.sh || exit 1
TAG="${1:?usage: run_all.sh <tag> [cdc_transactions] [rows_per_transaction]}"; TXNS="${2:-100}"; PER="${3:-100000}"
export POLL_TIMEOUT="${POLL_TIMEOUT:-14400}"
SRC="$SRC_TABLE"; FULL=$(src_count "$SRC"); CDC=$((FULL + TXNS*PER))
echo "tag=$TAG source=$SRC full_rows=$FULL rows_after_cdc=$CDC"; echo "$TAG $FULL $CDC" > "$R/$TAG.targets"
step() { echo "=== $(date -u +%FT%TZ) $*"; "$@"; rc=$?; echo "=== rc=$rc $*"; [ $rc -eq 0 ] || { echo CHAIN_ABORTED; exit 1; }; }
step ./olake/run.sh full "$SRC" "$TAG" "$FULL"
step ./dbz/run.sh kc full "$SRC" "$TAG" "$FULL"
(cd dbz && docker compose stop)
step ./flink/run.sh full "$SRC" "$TAG" "$FULL"
(cd flink && docker compose stop)
step ./dbz/run.sh spark full "$SRC" "$TAG" "$FULL"
(cd dbz && docker compose stop)
mark cdc_load.start; step ./sql/02_cdc_load.sh "$SRC" "$TXNS" "$PER"; mark cdc_load.end
step ./olake/run.sh cdc "$SRC" "$TAG" "$CDC"
step ./dbz/run.sh kc cdc "$SRC" "$TAG" "$CDC"
step ./dbz/run.sh spark cdc "$SRC" "$TAG" "$CDC"
(cd dbz && docker compose stop)
step ./flink/run.sh cdc "$SRC" "$TAG" "$CDC"
(cd flink && docker compose stop)
for i in "$OLAKE_IMAGE" "$DBZ_CONNECT_IMAGE" apache/kafka:3.9.0 bench-flink apache/spark:3.5.3; do echo "$i $(docker image inspect "$i" --format '{{.Id}} {{range .RepoDigests}}{{.}} {{end}}' 2>/dev/null)"; done > "$R/versions_$TAG.txt"
echo "PARALLELISM=$PARALLELISM MEM_GB=$MEM_GB COMMIT_INTERVAL_S=$COMMIT_INTERVAL_S FLINK_IMAGE=$FLINK_IMAGE" >> "$R/versions_$TAG.txt"
python3 tools/summarize.py "$R" "$TAG" "$FULL" "$CDC" | tee "$R/summary_$TAG.json"
echo ALL_DONE
