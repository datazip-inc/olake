#!/bin/bash
# Live CDC latency: time from a committed source insert until the rows are committed in the Iceberg table.
# usage: latency.sh <tag> [rounds]    (needs a finished upsert_all.sh run with the same tag; default 5 rounds of 1000 rows)
# Streaming pipelines stay running for all rounds. OLake has no continuous mode here, so each round starts one sync run.
cd "$(dirname "${BASH_SOURCE[0]}")" && export WRITE_MODE=upsert SCHEMA_EVOLVE=1 && source ./common.sh || exit 1
TAG="${1:?usage: latency.sh <tag> [rounds]}"; ROUNDS="${2:-5}"; SRC="bench_upsert_$TAG"; B=1000
export POLL_TIMEOUT=$((ROUNDS*200+300))
KEYBASE=$(psqlc -At -c "select greatest(max(c_custkey), 300000000) from public.\"$SRC\"")
ins() { # insert B new rows, print commit time
  psqlc -q -c "insert into public.\"$SRC\" (c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment) select $KEYBASE+g, 'lat', 'lat', 1, '0', 1.00, 'LAT', 'latency' from generate_series(1,$B) g" && ts
}
fast() { docker rm -f fastpoll >/dev/null 2>&1; docker run -d --name fastpoll --network host $AWS_ENV -e AWS_DEFAULT_REGION=$AWS_REGION bench-poller "$GLUE_DB.$1" 99999999999 $POLL_TIMEOUT 1 >/dev/null; sleep 4; }
# first poll time at which the table held at least <rows>; empty if not yet
reached() { docker logs fastpoll 2>&1 | python3 -c "
import sys
p=[l.strip().split(',') for l in sys.stdin if l.count(',')==1 and l.strip().split(',')[1].isdigit()]
print(next((t for t,n in p if int(n)>=int(sys.argv[1])), ''))" "$1"; }
lastpoll() { docker logs fastpoll 2>&1 | grep -E '^[0-9.]+,[0-9]+$' | tail -1; }    # "time,rows" of last change
# wait until the table has not changed for 75 s: backlog from earlier steps is fully committed
drain() { sleep 45; while :; do lp=$(lastpoll); [ -n "$lp" ] && [ "$(python3 -c "import time;print(int(time.time()-float('${lp%,*}')>75))")" = 1 ] && break; sleep 5; done; }
streaming() { # <pipeline name> <table> <start command...>
  local NAME=$1 TBL=$2; shift 2; local OUT=$R/latency_${TAG}_$NAME.csv; : > $OUT
  fast $TBL
  "$@" "+100000000" > $L/latency_${TAG}_$NAME.run.log 2>&1 &         # resumes pipeline; it stays up until its poller is stopped below
  drain; local base; base=$(lastpoll); base=${base#*,}; echo "$NAME drained, base rows $base"
  for r in $(seq 1 $ROUNDS); do
    t0=$(ins); KEYBASE=$((KEYBASE+B)); t1=""
    for i in $(seq 1 180); do t1=$(reached $((base + r*B))); [ -n "$t1" ] && break; sleep 1; done
    echo "$r,$t0,$t1" >> $OUT; echo "$NAME round $r: $(python3 -c "print(round(float('${t1:-nan}')-float('$t0'),1))") s"
    sleep 7                                                          # do not align inserts with the commit timer
  done
  docker stop -t 1 "poll_${NAME}_${TAG}_cdc" >/dev/null 2>&1           # lets the run script park the pipeline
  wait; docker rm -f fastpoll >/dev/null 2>&1
}
olake_rounds() {
  local OUT=$R/latency_${TAG}_olake.csv; : > $OUT
  ./olake/run.sh cdc "$SRC" "$TAG" +0 > $L/latency_${TAG}_olake.run.log 2>&1    # drain anything pending first
  for r in $(seq 1 $ROUNDS); do
    fast olake_$TAG; local base; base=$(lastpoll); base=${base#*,}
    t0=$(ins); KEYBASE=$((KEYBASE+B))
    ./olake/run.sh cdc "$SRC" "$TAG" +$B > $L/latency_${TAG}_olake.run.log 2>&1
    t1=$(reached $((base + B))); echo "$r,$t0,$t1" >> $OUT; echo "olake round $r: $(python3 -c "print(round(float('${t1:-nan}')-float('$t0'),1))") s"
  done; docker rm -f fastpoll >/dev/null 2>&1
}
olake_rounds
streaming dbz_kc dbz_kc_$TAG ./dbz/run.sh kc cdc "$SRC" "$TAG"
streaming dbz_spark dbz_spark_$TAG ./dbz/run.sh spark cdc "$SRC" "$TAG"
(cd dbz && docker compose stop)
streaming flink flink_$TAG ./flink/run.sh cdc "$SRC" "$TAG"
(cd flink && docker compose stop)
python3 - "$R" "$TAG" <<'PY' | tee "$R/latency_$TAG.summary.txt"
import sys, statistics as st
R, TAG = sys.argv[1:3]
for p in ["olake", "dbz_kc", "flink", "dbz_spark"]:
    v = []
    for l in open(f"{R}/latency_{TAG}_{p}.csv"):
        a = l.strip().split(",")
        if len(a) == 3 and a[2]: v.append(float(a[2]) - float(a[1]))
    print(p, "rounds", len(v), "min", round(min(v), 1) if v else None, "median", round(st.median(v), 1) if v else None, "max", round(max(v), 1) if v else None)
PY
echo LATENCY_DONE
