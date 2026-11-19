#!/bin/bash
# Update/delete test: all four pipelines in upsert mode on a new small table, then cross-engine reads.
# usage: upsert_all.sh <tag> [rows]      (tag must be unused; default 1,000,000 rows; about 25 minutes)
# Run inside tmux. Not a speed benchmark: it checks correctness and which delete files each pipeline writes.
cd "$(dirname "${BASH_SOURCE[0]}")" && export WRITE_MODE=upsert && source ./common.sh || exit 1
TAG="${1:?usage: upsert_all.sh <tag> [rows]}"; N="${2:-1000000}"; SRC="bench_upsert_$TAG"
export POLL_TIMEOUT="${POLL_TIMEOUT:-1800}"
# rows committed as data after the workload: original + inserts (N/10) + updated copies (N/5)
AFTER=$((N + N/10 + N/5))
step() { echo "=== $(date -u +%FT%TZ) $*"; "$@"; rc=$?; echo "=== rc=$rc $*"; [ $rc -eq 0 ] || { echo CHAIN_ABORTED; exit 1; }; }
step ./sql/03_upsert_setup.sh "$SRC" "$N"
step ./olake/run.sh full "$SRC" "$TAG" "$N"
step ./dbz/run.sh kc full "$SRC" "$TAG" "$N"
(cd dbz && docker compose stop)
step ./flink/run.sh full "$SRC" "$TAG" "$N"
(cd flink && docker compose stop)
step ./dbz/run.sh spark full "$SRC" "$TAG" "$N"
(cd dbz && docker compose stop)
step ./sql/04_upsert_load.sh "$SRC" "$N" | tee "$R/upsert_$TAG.source.txt"
step ./olake/run.sh cdc "$SRC" "$TAG" "$AFTER"
step ./dbz/run.sh kc cdc "$SRC" "$TAG" "$AFTER"
step ./dbz/run.sh spark cdc "$SRC" "$TAG" "$AFTER"
(cd dbz && docker compose stop)
step ./flink/run.sh cdc "$SRC" "$TAG" "$AFTER"
(cd flink && docker compose stop)
echo "=== source after workload (count | distinct keys | sum(c_acctbal) | updated rows)"; tail -1 "$R/upsert_$TAG.source.txt"
./interop/run.sh "$TAG"
echo ALL_DONE
