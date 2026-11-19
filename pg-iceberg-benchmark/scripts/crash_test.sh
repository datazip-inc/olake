#!/bin/bash
# Crash test: SIGKILL each pipeline's writer while it is writing a CDC backlog, restart it, then compare
# the Iceberg table with the source. Checks loss and duplicates after an unplanned stop.
# usage: crash_test.sh <tag> [rows] ["pipelines"]   (needs a finished upsert_all.sh run with the same tag)
#   rows default 2,000,000; pipelines default "olake dbz_kc dbz_spark flink"
# The kill lands at a fixed delay, not at a chosen point inside a commit. Two kills per pipeline.
cd "$(dirname "${BASH_SOURCE[0]}")" && export WRITE_MODE=upsert SCHEMA_EVOLVE=1 && source ./common.sh || exit 1
TAG="${1:?usage: crash_test.sh <tag> [rows] [pipelines]}"; ROWS="${2:-2000000}"; PIPES="${3:-olake dbz_kc dbz_spark flink}"; SRC="bench_upsert_$TAG"; OUT=$R/crash_$TAG.txt
has() { case " $PIPES " in *" $1 "*) return 0;; *) return 1;; esac; }
echo "--- run $(date -u +%FT%TZ) pipelines: $PIPES" >> $OUT
export OLAKE_UPDATE_TYPE="${OLAKE_UPDATE_TYPE:-pos}"
say() { echo "$*" | tee -a $OUT; }
fast() { docker rm -f fastpoll >/dev/null 2>&1; docker run -d --name fastpoll --network host $AWS_ENV -e AWS_DEFAULT_REGION=$AWS_REGION bench-poller "$GLUE_DB.$1" 99999999999 7200 1 >/dev/null; sleep 5; }
lastpoll() { docker logs fastpoll 2>&1 | grep -E '^[0-9.]+,[0-9]+$' | tail -1; }
rowsnow() { local lp; lp=$(lastpoll); echo "${lp#*,}"; }
# wait until the table grew past <base rows>; give up after <seconds>
grew() { local t0; t0=$(date +%s); while [ "$(rowsnow)" -le "$1" ]; do [ $(( $(date +%s) - t0 )) -gt "$2" ] && return 1; sleep 1; done; }
# recovery is complete when the table grew past <rows at the last kill> and then did not change for 120 s.
# If it never grows, wait the full 15 minutes: a restarted pipeline can need several minutes before its first commit.
settle() { local t0 lp age; t0=$(date +%s); while :; do lp=$(lastpoll); age=$(python3 -c "import time;print(int(time.time()-float('${lp%,*}')))")
    [ "${lp#*,}" -gt "$1" ] && [ "$age" -gt 120 ] && return 0
    [ $(( $(date +%s) - t0 )) -gt 900 ] && { say "  no recovery within 15 minutes"; return 1; }; sleep 5; done; }

# workload: new rows only, 20 transactions
KEY=$(psqlc -At -c "select greatest(max(c_custkey), 700000000) from public.\"$SRC\""); PER=$((ROWS/20))
for i in $(seq 0 19); do echo "insert into public.\"$SRC\" (c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment) select $KEY+$((i*PER))+g, 'crash', 'crash', 1, '0', 1.00, 'CRASH', 'crash test' from generate_series(1,$PER) g;"; done | psqlc -q
say "workload: $ROWS rows inserted into $SRC"

if has olake; then
say "## olake: SIGKILL the sync container twice, then run to completion"
fast olake_$TAG; b=$(rowsnow)
for k in 6 14; do
  POLL_TIMEOUT=5 ./olake/run.sh cdc "$SRC" "$TAG" +$ROWS > $L/crash_${TAG}_olake_$k.log 2>&1 &
  sleep $k; docker kill -s KILL olake_bench >/dev/null 2>&1 && say "olake: killed ${k}s after start, table rows $(rowsnow) (before test $b)" || say "olake: container already gone at ${k}s"
  wait
done
./olake/run.sh cdc "$SRC" "$TAG" +0 > $L/crash_${TAG}_olake_final.log 2>&1; sleep 10; say "olake: rows after recovery $(rowsnow)"
fi

if has dbz_kc; then
say "## dbz_kc: SIGKILL Kafka Connect (Debezium source and Iceberg sink) twice while it writes"
fast dbz_kc_$TAG; b=$(rowsnow)
./dbz/run.sh kc cdc "$SRC" "$TAG" +100000000 > $L/crash_${TAG}_kc.log 2>&1 &
grew $b ${KC_FIRST_KILL_WAIT:-300} && sleep 3; docker kill -s KILL bench-dbz-connect-1 >/dev/null; say "dbz_kc: killed Kafka Connect, table rows $(rowsnow) (before test $b)"; sleep 3; (cd dbz && docker compose up -d connect >/dev/null 2>&1)
sleep 50; docker kill -s KILL bench-dbz-connect-1 >/dev/null; k=$(rowsnow); say "dbz_kc: killed Kafka Connect again, table rows $k"; sleep 3; (cd dbz && docker compose up -d connect >/dev/null 2>&1)
settle $k; say "dbz_kc: rows after recovery $(rowsnow)"; docker stop -t 1 "poll_dbz_kc_${TAG}_cdc" >/dev/null 2>&1; wait
fi

if has dbz_spark; then
say "## dbz_spark: SIGKILL the Spark job twice while it writes"
fast dbz_spark_$TAG; b=$(rowsnow)
./dbz/run.sh spark cdc "$SRC" "$TAG" +100000000 > $L/crash_${TAG}_spark.log 2>&1 &
sleep 24; docker kill -s KILL spark_bench >/dev/null; say "dbz_spark: killed Spark 24 s after start, table rows $(rowsnow) (before test $b)"; sleep 3; docker start spark_bench >/dev/null
sleep 26; docker kill -s KILL spark_bench >/dev/null; k=$(rowsnow); say "dbz_spark: killed Spark again, table rows $k"; sleep 3; docker start spark_bench >/dev/null
settle $k; say "dbz_spark: rows after recovery $(rowsnow)"; docker stop -t 1 "poll_dbz_spark_${TAG}_cdc" >/dev/null 2>&1; wait
fi
(cd dbz && docker compose stop >/dev/null 2>&1)

if has flink; then
say "## flink: SIGKILL TaskManager, then SIGKILL JobManager and TaskManager together"
fast flink_$TAG; b=$(rowsnow)
./flink/run.sh cdc "$SRC" "$TAG" +100000000 > $L/crash_${TAG}_flink1.log 2>&1 &
sleep 45; docker kill -s KILL bench-flink-taskmanager-1 >/dev/null; say "flink: killed TaskManager, table rows $(rowsnow) (before test $b)"; sleep 3; docker start bench-flink-taskmanager-1 >/dev/null
sleep 60
JOB=$(curl -s localhost:8081/jobs/overview | python3 -c "import sys,json;j=[x['jid'] for x in json.load(sys.stdin)['jobs'] if x['state'] not in ('CANCELED','FAILED','FINISHED')];print(j[0] if j else '')")
say "flink: job $JOB state before second kill: $(curl -s localhost:8081/jobs/$JOB | python3 -c "import sys,json;print(json.load(sys.stdin).get('state'))" 2>/dev/null), table rows $(rowsnow)"
docker kill -s KILL bench-flink-jobmanager-1 bench-flink-taskmanager-1 >/dev/null; say "flink: killed JobManager and TaskManager"
docker stop -t 1 "poll_flink_${TAG}_cdc" >/dev/null 2>&1; wait
[ -n "$JOB" ] && echo "$JOB" > flink/work/$TAG.jobid
./flink/run.sh cdc "$SRC" "$TAG" +100000000 > $L/crash_${TAG}_flink2.log 2>&1 &     # resubmits from the last complete checkpoint
settle $b; say "flink: rows after recovery $(rowsnow)"; docker stop -t 1 "poll_flink_${TAG}_cdc" >/dev/null 2>&1; wait
(cd flink && docker compose stop >/dev/null 2>&1); docker rm -f fastpoll >/dev/null 2>&1
fi
docker rm -f fastpoll >/dev/null 2>&1

say "## verification with Trino: rows | distinct keys | sum(c_acctbal)"
say "source | $(psqlc -At -F ' | ' -c "select count(*), count(distinct c_custkey), sum(c_acctbal) from public.\"$SRC\"")"
docker rm -f trino_interop >/dev/null 2>&1
docker run -d --name trino_interop --network host $AWS_ENV -v $BENCH/interop/trino/iceberg.properties:/etc/trino/catalog/iceberg.properties trinodb/trino:476 >/dev/null
for i in $(seq 1 60); do docker exec trino_interop trino --execute "select 1" >/dev/null 2>&1 && break; sleep 3; done
q() { docker exec trino_interop trino --execute "$1" 2>&1 | grep -v -E "WARNING|jline" | tail -1 | tr -d '"' | sed 's/,/ | /g'; }
has olake && say "olake (deleted rows excluded) | $(q "select count(*), count(distinct c_custkey), cast(sum(c_acctbal) as decimal(20,2)) from iceberg.$GLUE_DB.olake_$TAG where _op_type <> 'd'")"
for t in dbz_kc flink dbz_spark; do has $t && say "$t | $(q "select count(*), count(distinct c_custkey), cast(sum(c_acctbal) as decimal(20,2)) from iceberg.$GLUE_DB.${t}_$TAG")"; done
docker stop trino_interop >/dev/null
echo CRASH_DONE
