#!/bin/bash
# Dry run of all four pipelines on the small smoke table: full load, 500 new rows, CDC resume.
# usage: smoke_all.sh <tag>     (tag must be unused; takes about 10 minutes)
cd "$(dirname "${BASH_SOURCE[0]}")" && source ./common.sh || exit 1
TAG="${1:?usage: smoke_all.sh <tag>}"; export POLL_TIMEOUT="${POLL_TIMEOUT:-600}"
SRC="$SMOKE_TABLE"; FULL=$(src_count "$SRC"); CDC=$((FULL + 500))
step() { echo "=== $(date -u +%FT%TZ) $*"; "$@"; rc=$?; echo "=== rc=$rc $*"; [ $rc -eq 0 ] || { echo CHAIN_ABORTED; exit 1; }; }
step ./olake/run.sh full "$SRC" "$TAG" "$FULL"
step ./dbz/run.sh kc full "$SRC" "$TAG" "$FULL"
(cd dbz && docker compose stop)
step ./flink/run.sh full "$SRC" "$TAG" "$FULL"
(cd flink && docker compose stop)
step ./dbz/run.sh spark full "$SRC" "$TAG" "$FULL"
step ./sql/02_cdc_load.sh "$SRC" 1 500
step ./olake/run.sh cdc "$SRC" "$TAG" "$CDC"
step ./dbz/run.sh kc cdc "$SRC" "$TAG" "$CDC"
step ./dbz/run.sh spark cdc "$SRC" "$TAG" "$CDC"
(cd dbz && docker compose stop)
step ./flink/run.sh cdc "$SRC" "$TAG" "$CDC"
(cd flink && docker compose stop)
echo ALL_DONE
