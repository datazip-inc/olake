#!/bin/bash
# OLake -> Snowflake Horizon correctness test, once per delete format.
# usage: horizon_test.sh <tag> pos|dv [rows]     (tag must be unused; default 1,000,000 rows)
cd "$(dirname "${BASH_SOURCE[0]}")" && export WRITE_MODE=upsert && source ./common.sh || exit 1
TAG="${1:?usage: horizon_test.sh <tag> pos|dv [rows]}"; export OLAKE_UPDATE_TYPE="${2:?pos|dv}"; N="${3:-1000000}"; SRC="bench_upsert_$TAG"; OUT=$R/horizon_$TAG.txt
say() { echo "$*" | tee -a $OUT; }
sfq() { echo "$1" > snowflake/q_$TAG.sql; ./snowflake/sf_run.sh q_$TAG.sql | grep -E "^ +\(|ERR" | cut -c1-400; }
TBL="OLAKE_BENCH_HZ.\"bench_hz\".\"olake_$TAG\""
QA="select count(*), count(distinct \"c_custkey\"), sum(\"c_acctbal\"), count_if(\"c_comment\"='updated') from $TBL where \"_op_type\" <> 'd'"
QB="select count(*), count_if(\"_op_type\"='d') from $TBL"
say "--- run $(date -u +%FT%TZ) tag=$TAG update_type=$OLAKE_UPDATE_TYPE rows=$N"
./sql/03_upsert_setup.sh "$SRC" "$N" || exit 1
say "full: $(./olake/run_horizon.sh full "$SRC" "$TAG" | tr '\n' ' ')"
say "commit report full: $(grep -o -E 'added(DataFiles|Records|PositionalDeleteFiles|PositionalDeletes|EqualityDeleteFiles|EqualityDeletes|DVs)=[0-9]+' $L/olake_${TAG}_full.log | sort | uniq -c | tr '\n' ' ')"
say "source after full (rows | distinct | sum | updated): $(psqlc -At -F ' | ' -c "select count(*), count(distinct c_custkey), sum(c_acctbal), count(*) filter (where c_comment='updated') from public.\"$SRC\"")"
say "snowflake after full (rows, distinct, sum, updated; deleted rows excluded): $(sfq "$QA")"
say "source workload: $(./sql/04_upsert_load.sh "$SRC" "$N" | tail -1)"
say "cdc: $(./olake/run_horizon.sh cdc "$SRC" "$TAG" | tr '\n' ' ')"
say "commit report cdc: $(grep -o -E '(added|total)(DataFiles|Records|PositionalDeleteFiles|PositionalDeletes|EqualityDeleteFiles|EqualityDeletes|DVs|PositionDeletes|DeleteFiles)=[0-9]+' $L/olake_${TAG}_cdc.log | sort | uniq -c | tr '\n' ' ')"
say "snowflake after cdc (rows, distinct, sum, updated; deleted rows excluded): $(sfq "$QA")"
say "snowflake after cdc (all rows, tombstone rows): $(sfq "$QB")"
say "snowflake table info: $(sfq "show iceberg tables like 'olake_$TAG' in schema OLAKE_BENCH_HZ.\"bench_hz\"")"
say "snowflake column types: $(sfq "select listagg(column_name || ':' || data_type || coalesce('(' || numeric_precision || ',' || numeric_scale || ')', ''), ', ') within group (order by ordinal_position) from OLAKE_BENCH_HZ.information_schema.columns where table_schema='bench_hz' and table_name='olake_$TAG'")"
say "snowflake metadata: $(sfq "select SYSTEM\$GET_ICEBERG_TABLE_INFORMATION('OLAKE_BENCH_HZ.\"bench_hz\".\"olake_$TAG\"')")"
echo HORIZON_DONE
