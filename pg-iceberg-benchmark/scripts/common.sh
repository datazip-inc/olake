# Helpers shared by pipeline scripts. Usage: source common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh" || return 1
# WRITE_MODE=append (default) or upsert (updates and deletes applied; pipelines write delete files)
export WRITE_MODE="${WRITE_MODE:-append}"
R=$BENCH/results; L=$BENCH/logs; mkdir -p $R $L
ts() { date +%s.%N | cut -c1-14; }
mark() { echo "$(ts)" > "$R/$1"; echo "[$(date -u +%T)] mark $1"; }   # mark <name>.<event>
# start_poller <name> <glue_table> <target_rows> [timeout_s]
start_poller() {
  docker rm -f "poll_$1" >/dev/null 2>&1
  docker run -d --name "poll_$1" --network host $AWS_ENV -e AWS_DEFAULT_REGION=$AWS_REGION bench-poller "$GLUE_DB.$2" "$3" "${POLL_TIMEOUT:-43200}" "${POLL_INTERVAL:-5}" >/dev/null
}
# wait_poller <name>: blocks until target reached, saves CSV
wait_poller() {
  local rc; rc=$(docker wait "poll_$1"); docker logs "poll_$1" > "$R/$1.rows.csv" 2>&1
  echo "[$(date -u +%T)] poller $1 exit=$rc last=$(tail -1 "$R/$1.rows.csv")"; return "$rc"
}
# start_stats <name>: sample CPU/memory of all containers every 10s
start_stats() {
  ( while true; do docker stats --no-stream --format "$(date +%s),{{.Name}},{{.CPUPerc}},{{.MemUsage}}" | grep -v poll_ ; sleep 10; done ) > "$R/$1.stats.csv" 2>/dev/null &
  echo $! > "$R/$1.stats.pid"
}
stop_stats() { kill "$(cat "$R/$1.stats.pid")" 2>/dev/null; }
src_count() { psqlc -At -c "select count(*) from public.$1"; }
# trino_props <dir>: writes the Trino Iceberg catalog file for the Glue region in bench.env
trino_props() { mkdir -p "$1"; printf 'connector.name=iceberg\niceberg.catalog.type=glue\nhive.metastore.glue.region=%s\nfs.native-s3.enabled=true\ns3.region=%s\n' "$AWS_REGION" "$AWS_REGION" > "$1/iceberg.properties"; }
