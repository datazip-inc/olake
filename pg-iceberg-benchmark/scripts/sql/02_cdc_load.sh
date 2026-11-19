#!/bin/bash
# Generates the CDC workload: NEW rows only, copied from BASE_TABLE with keys shifted by 100,000,000.
# usage: 02_cdc_load.sh [source_table] [transactions] [rows_per_transaction]   (default: SRC_TABLE 100 100000 = 10M rows)
# Insert only. Existing rows and existing tables are not modified.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/env.sh" || exit 1
T="${1:-$SRC_TABLE}"; TXNS="${2:-100}"; PER="${3:-100000}"
for i in $(seq 0 $((TXNS-1))); do
  lo=$((i*PER+1)); hi=$(((i+1)*PER))
  echo "insert into public.\"$T\" select 100000000+c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment from $BASE_TABLE where c_custkey between $lo and $hi;"
done | psqlc -q
psqlc -At -c "select count(*) from public.\"$T\""
