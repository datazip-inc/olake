#!/bin/bash
# Creates a NEW source table for the update/delete test and adds it to the benchmark publication.
# usage: 03_upsert_setup.sh <new_table> [rows]     (default 1,000,000 rows, keys 1..rows copied from BASE_TABLE)
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/env.sh" || exit 1
T="${1:?usage: 03_upsert_setup.sh <new_table> [rows]}"; N="${2:-1000000}"
psqlc -q <<SQL
CREATE TABLE public."$T" (LIKE $BASE_TABLE INCLUDING DEFAULTS);
INSERT INTO public."$T" SELECT * FROM $BASE_TABLE WHERE c_custkey <= $N;
ALTER TABLE public."$T" ADD PRIMARY KEY (c_custkey);
ALTER PUBLICATION "$PUBLICATION" ADD TABLE public."$T";
SQL
psqlc -At -c "select count(*) from public.\"$T\""
