#!/bin/bash
# Creates the benchmark source table (copy of BASE_TABLE), a 2000-row smoke table and a publication.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/env.sh" || exit 1
psqlc -v base="$BASE_TABLE" -v src="$SRC_TABLE" -v smoke="$SMOKE_TABLE" -v pub="$PUBLICATION" -f - < "$BENCH/sql/01_create_source.sql"
