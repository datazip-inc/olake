#!/bin/bash
# Update/delete workload on the upsert test table only (a table this benchmark created).
# usage: 04_upsert_load.sh <table> [rows_in_table]
# Order matters: inserts, then deletes, then updates. Updates are last so "all updates arrived" implies deletes arrived.
#   inserts: rows/10 new keys (100,000,000 + 1..rows/10)
#   deletes: last rows/10 original keys
#   updates: first rows/5 original keys (c_acctbal + 1, c_comment = 'updated')
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/env.sh" || exit 1
T="${1:?usage: 04_upsert_load.sh <table> [rows_in_table]}"; N="${2:-1000000}"; TEN=$((N/10)); FIFTH=$((N/5))
psqlc -q <<SQL
INSERT INTO public."$T" SELECT 100000000+c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment FROM $BASE_TABLE WHERE c_custkey BETWEEN 1 AND $TEN;
DELETE FROM public."$T" WHERE c_custkey BETWEEN $((N-TEN+1)) AND $N;
UPDATE public."$T" SET c_acctbal = c_acctbal + 1, c_comment = 'updated' WHERE c_custkey BETWEEN 1 AND $FIFTH;
SQL
psqlc -At -F ' | ' -c "select count(*), count(distinct c_custkey), sum(c_acctbal), count(*) filter (where c_comment='updated') from public.\"$T\""
