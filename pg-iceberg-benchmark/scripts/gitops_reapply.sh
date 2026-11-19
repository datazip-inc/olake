#!/bin/bash
# GitOps check: every pipeline is defined by files in this folder. This applies each definition a second time
# while the pipeline exists and checks whether that is harmless (no duplicate rows, no second pipeline).
# usage: gitops_reapply.sh <tag>      (tag must be unused; uses the small smoke table; about 15 minutes)
cd "$(dirname "${BASH_SOURCE[0]}")" && export WRITE_MODE=append && source ./common.sh || exit 1
TAG="${1:?usage: gitops_reapply.sh <tag>}"; SRC="$SMOKE_TABLE"; export POLL_TIMEOUT=420
OUT=$R/gitops_$TAG.txt; : > $OUT
rows() { docker run --rm --network host $AWS_ENV -e AWS_DEFAULT_REGION=$AWS_REGION bench-poller "$GLUE_DB.$1" +0 60 1 | tail -1 | cut -d, -f2; }
KEY=$(psqlc -At -c "select greatest(max(c_custkey), 500000000) from public.\"$SRC\"")
add100() { psqlc -q -c "insert into public.\"$SRC\" (c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment) select $KEY+g, 'g', 'g', 1, '0', 1.00, 'G', 'gitops' from generate_series(1,100) g"; KEY=$((KEY+100)); }
say() { echo "$*" | tee -a $OUT; }

say "## olake: run the same sync command again with nothing new"
./olake/run.sh full "$SRC" "$TAG" "$(src_count $SRC)" >/dev/null 2>&1
./olake/run.sh cdc "$SRC" "$TAG" +0 >/dev/null 2>&1; ./olake/run.sh cdc "$SRC" "$TAG" +0 >/dev/null 2>&1
say "olake source_rows=$(src_count $SRC) table_rows=$(rows olake_$TAG) last_run: $(grep -o 'Total records read: [0-9]*' $L/olake_${TAG}_cdc.log | tail -1)"

say "## dbz_kc: PUT the same connector configs twice while running, then 100 new rows"
./dbz/run.sh kc full "$SRC" "$TAG" "$(src_count $SRC)" >/dev/null 2>&1
add100
./dbz/run.sh kc cdc "$SRC" "$TAG" +100 > $L/gitops_${TAG}_kc.log 2>&1 &
sleep 25; P=dbzkc$TAG
for i in 1 2; do for c in src sink; do say "PUT $P-$c -> $(curl -s -o /dev/null -w '%{http_code}' -X PUT -H 'Content-Type: application/json' localhost:8083/connectors/$P-$c/config -d @dbz/$P-$c.json)"; done; done
say "connectors: $(curl -s localhost:8083/connectors | tr -d '\n')"
wait; say "dbz_kc source_rows=$(src_count $SRC) table_rows=$(rows dbz_kc_$TAG)"

say "## dbz_spark: start a second identical Spark job on the same checkpoint, then 100 new rows"
./dbz/run.sh spark full "$SRC" "$TAG" "$(src_count $SRC)" >/dev/null 2>&1
add100
./dbz/run.sh spark cdc "$SRC" "$TAG" +100 > $L/gitops_${TAG}_spark.log 2>&1 &
sleep 20
docker rm -f spark_bench2 >/dev/null 2>&1
docker run -d --name spark_bench2 --user 0 --network host $AWS_ENV -v $BENCH/spark:/work apache/spark:3.5.3 $(docker inspect spark_bench --format '{{range .Config.Cmd}}{{.}} {{end}}') >/dev/null
sleep 90
say "spark_bench2 state: $(docker inspect spark_bench2 --format '{{.State.Status}} exit={{.State.ExitCode}}') error: $(docker logs spark_bench2 2>&1 | grep -m1 -E 'Exception|ERROR' | cut -c1-220)"
wait; docker logs spark_bench2 > $L/gitops_${TAG}_spark2.log 2>&1; docker rm -f spark_bench2 >/dev/null 2>&1
say "dbz_spark source_rows=$(src_count $SRC) table_rows=$(rows dbz_spark_$TAG)"
(cd dbz && docker compose stop >/dev/null 2>&1)

say "## flink: submit the same SQL file twice"
./flink/run.sh full "$SRC" "$TAG" "$(src_count $SRC)" >/dev/null 2>&1      # first apply; job is cancelled at the end
(cd flink && docker compose up -d >/dev/null 2>&1); for i in $(seq 1 60); do curl -sf localhost:8081/overview >/dev/null && break; sleep 2; done
for i in 1 2; do docker compose -f flink/docker-compose.yml exec -T jobmanager ./bin/sql-client.sh -f /work/$TAG.sql > $L/gitops_${TAG}_flink_submit$i.log 2>&1; say "flink submit $i: $(grep -oE 'Job ID: [0-9a-f]+|ERROR.*' $L/gitops_${TAG}_flink_submit$i.log | tail -1 | cut -c1-200)"; sleep 5; done
sleep 80
say "flink jobs: $(curl -s localhost:8081/jobs/overview | python3 -c "import sys,json;print([(j['name'],j['state']) for j in json.load(sys.stdin)['jobs'] if j['state'] not in ('CANCELED',)])")"
say "flink source_rows=$(src_count $SRC) table_rows=$(rows flink_$TAG)"
for j in $(curl -s localhost:8081/jobs/overview | python3 -c "import sys,json;print(' '.join(j['jid'] for j in json.load(sys.stdin)['jobs'] if j['state'] in ('RUNNING','RESTARTING','FAILING')))"); do curl -s -X PATCH "localhost:8081/jobs/$j?mode=cancel" >/dev/null; done; sleep 5
(cd flink && docker compose stop >/dev/null 2>&1)
echo GITOPS_DONE
