#!/bin/bash
# usage: sf_run.sh <sql file in snowflake/>   runs it with credentials from .snowflake_bench
cd ~/saksham/benchmark && set -a && . ./.snowflake_bench && set +a
docker image inspect bench-sf >/dev/null 2>&1 || printf 'FROM python:3.12-slim\nRUN pip install --no-cache-dir -q snowflake-connector-python\n' | docker build -q -t bench-sf - >/dev/null
docker run --rm --network host -e SNOWFLAKE_ACCOUNT -e SNOWFLAKE_USER -e SNOWFLAKE_PASSWORD -e SNOWFLAKE_ROLE -e SNOWFLAKE_WAREHOUSE -v $PWD/snowflake:/s bench-sf python -u /s/sf.py /s/$1 2>&1 | grep -v -i warning
