# Shared settings for the Postgres -> Iceberg (Glue) benchmark. Source this file.
# All site-specific values come from bench.env next to this file (written during setup; variables are listed in SKILL.md, "Setup on the host").
# The source database password is read from .pgpass_bench next to this file.
BENCH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; export BENCH
if [ ! -f "$BENCH/bench.env" ]; then echo "missing $BENCH/bench.env (write it first: see SKILL.md, Setup on the host)" >&2; return 1 2>/dev/null || exit 1; fi
if [ ! -f "$BENCH/.pgpass_bench" ]; then echo "missing $BENCH/.pgpass_bench (source database password, one line)" >&2; return 1 2>/dev/null || exit 1; fi
set -a; . "$BENCH/bench.env"; set +a
: "${PGHOST:?set in bench.env}" "${PGDATABASE:?set in bench.env}" "${PGUSER:?set in bench.env}"
: "${AWS_REGION:?set in bench.env}" "${WAREHOUSE:?set in bench.env}" "${GLUE_DB:?set in bench.env}"
: "${BASE_TABLE:?set in bench.env}" "${SRC_TABLE:?set in bench.env}" "${SMOKE_TABLE:?set in bench.env}" "${PUBLICATION:?set in bench.env}"
export PGPORT="${PGPORT:-5432}" PGSSLMODE="${PGSSLMODE:-require}" AWS_DEFAULT_REGION="$AWS_REGION"
# One value for each setting, used by all four pipelines (set in bench.env to override).
export PARALLELISM="${PARALLELISM:-16}" MEM_GB="${MEM_GB:-32}" COMMIT_INTERVAL_S="${COMMIT_INTERVAL_S:-30}"
export OLAKE_IMAGE="${OLAKE_IMAGE:-olakego/source-postgres:v1.2.0}" DBZ_CONNECT_IMAGE="${DBZ_CONNECT_IMAGE:-quay.io/debezium/connect:3.0.8.Final}" FLINK_IMAGE="${FLINK_IMAGE:-flink:1.19.3}"
export ICEBERG_SINK_PLUGIN_DIR="${ICEBERG_SINK_PLUGIN_DIR:-$BENCH/dbz/plugins/iceberg-kafka-connect-runtime-0.6.19}"
# Optional AWS keys. Only for hosts without an instance role: put AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
# (and AWS_SESSION_TOKEN if temporary) in .aws_bench next to this file. Unset means instance role is used.
if [ -f "$BENCH/.aws_bench" ]; then set -a; . "$BENCH/.aws_bench"; set +a; fi
AWS_ENV="-e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN"; export AWS_ENV   # docker passes each variable only if it is set
PGPASSWORD="$(cat "$BENCH/.pgpass_bench")"; export PGPASSWORD
psqlc() { docker run --rm -i --network host -e PGHOST -e PGPORT -e PGDATABASE -e PGUSER -e PGSSLMODE -e PGPASSWORD postgres:16 psql -X -v ON_ERROR_STOP=1 "$@"; }
