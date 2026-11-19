#!/bin/bash
# usage: sf_run.sh <file in snowflake/>
#   <name>.sql  runs each statement with sf.py
#   <name>.py   runs that script (for example sf_horizon_setup.py)
# Credentials and settings come from .snowflake_bench in the scripts folder (git-ignored).
BENCH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
F="${1:?usage: sf_run.sh <file in snowflake/>}"
[ -f "$BENCH/.snowflake_bench" ] || { echo "missing $BENCH/.snowflake_bench" >&2; exit 1; }
[ -f "$BENCH/snowflake/$F" ] || { echo "missing $BENCH/snowflake/$F" >&2; exit 1; }
set -a; . "$BENCH/.snowflake_bench"; set +a
docker image inspect bench-sf >/dev/null 2>&1 || printf 'FROM python:3.12-slim\nRUN pip install --no-cache-dir -q snowflake-connector-python\n' | docker build -q -t bench-sf - >/dev/null
case "$F" in *.py) CMD="/s/$F" ;; *) CMD="/s/sf.py /s/$F" ;; esac
docker run --rm --network host -e SNOWFLAKE_ACCOUNT -e SNOWFLAKE_USER -e SNOWFLAKE_PASSWORD -e SNOWFLAKE_ROLE -e SNOWFLAKE_WAREHOUSE \
  -e SNOWFLAKE_ALLOWED_IP -e SNOWFLAKE_HORIZON_DB -e SNOWFLAKE_HORIZON_ROLE -e SNOWFLAKE_HORIZON_USER \
  -v "$BENCH/snowflake:/s" bench-sf python -u $CMD 2>&1 | grep -v -i warning
