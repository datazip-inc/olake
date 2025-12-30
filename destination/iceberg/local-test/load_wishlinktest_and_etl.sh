#!/usr/bin/env bash
set -euo pipefail

# ========= Postgres connection (as provided) =========
export PGHOST="dz-stag-postgres-private.postgres.database.azure.com"
export PGPORT="5432"
export PGUSER="postgres"
export PGPASSWORD="eVqz5HAbiotM3LV"
export PGDATABASE="postgres"

# Azure Postgres typically requires SSL
export PGSSLMODE="${PGSSLMODE:-require}"

TABLE="${TABLE:-public.wishlinktest}"
SLEEP_SECS="${SLEEP_SECS:-5}"

if ! command -v psql >/dev/null 2>&1; then
  echo "psql not found. Install PostgreSQL client tools first." >&2
  exit 1
fi

rand_hex() {
  # 16 hex chars
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 8
  else
    # fallback
    dd if=/dev/urandom bs=8 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n'
  fi
}

rand_bool() {
  if (( RANDOM % 2 == 0 )); then echo "true"; else echo "false"; fi
}

rand_int() {
  local max="${1:-100000}"
  echo $(( (RANDOM % max) + 1 ))
}

echo "Target: ${PGUSER}@${PGHOST}:${PGPORT}/${PGDATABASE} sslmode=${PGSSLMODE} table=${TABLE}"
echo "Loop: insert + random update + OLake sync every ${SLEEP_SECS}s"
echo "Press Ctrl+C to stop."

while true; do
  now_iso="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  click_uuid="$(rand_hex)"
  ga="$(rand_hex)"
  short_url_id="$(rand_hex)"
  wishlink_id="$(rand_hex)"
  product_id="$(rand_int 500000)"
  post_id="$(rand_int 500000)"
  banner_id="$(rand_int 500000)"
  user_id="$(rand_int 500000)"
  is_short_url="$(rand_bool)"
  source="script"

  # INSERT
  psql -v ON_ERROR_STOP=1 -q <<SQL
INSERT INTO ${TABLE} (
  click_time, ga, short_url_id, click_uuid, product_id, additional_info,
  post_id, banner_id, user_id, is_short_url, "source", insta_user_id, wishlink_id
)
VALUES (
  '${now_iso}'::timestamptz,
  '${ga}',
  '${short_url_id}',
  '${click_uuid}',
  ${product_id},
  jsonb_build_object(
    'run_ts', '${now_iso}',
    'rand', '${click_uuid}',
    'note', 'insert'
  ),
  ${post_id},
  ${banner_id},
  ${user_id},
  ${is_short_url},
  '${source}',
  '${user_id}',
  '${wishlink_id}'
);
SQL

  # UPDATE one random existing row (if any)
  new_ga="$(rand_hex)"
  psql -v ON_ERROR_STOP=1 -q <<SQL
WITH picked AS (
  SELECT id
  FROM ${TABLE}
  ORDER BY random()
  LIMIT 1
)
UPDATE ${TABLE} t
SET
  click_time = now(),
  ga = '${new_ga}',
  additional_info = COALESCE(t.additional_info, '{}'::jsonb)
                   || jsonb_build_object('updated_at', now(), 'note', 'update', 'new_ga', '${new_ga}')
FROM picked
WHERE t.id = picked.id;
SQL

  # Run the OLake ETL sync (as requested)
  (
    cd /Users/shubham/Desktop/datazip/work/code/olake
    ./build.sh driver-postgres sync \
      --config /Users/shubham/Desktop/datazip/work/code/olake/drivers/postgres/examples/config.json \
      --catalog /Users/shubham/Desktop/datazip/work/code/olake/drivers/postgres/examples/streams.json \
      --destination /Users/shubham/Desktop/datazip/work/code/olake/drivers/postgres/examples/writer.json \
      --state /Users/shubham/Desktop/datazip/work/code/olake/drivers/postgres/examples/state.json
  )

  sleep "${SLEEP_SECS}"
done


