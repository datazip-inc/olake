#!/bin/sh
# runs the 10 smt.md examples; each writes ex/out/NN.parquet and ex/out/NN.txt
B=./flatten-poc
run() { n=$1; shift; echo "== $n"; $B "$@" -out ex/out/$n.parquet > ex/out/$n.txt 2>&1; grep -E "ERROR|error:" ex/out/$n.txt; }

run 01 -source orders.jsonl -query "select _id.\"\$oid\" as order_id, flatten(customer, 2), createdAt.\"\$date\"::timestamptz as created_at, list_transform(items, lambda x: {'sku': x.sku, 'qty': x.qty, 'price': x.price::decimal(18,2)}) as items, list_sum(list_transform(items, lambda x: x.qty * x.price::decimal(18,2))) as order_total, status = 'paid' as is_paid from batch"
run 02 -source ex/clicks.jsonl -query "select event, epoch_ms(ts::bigint) as event_ts, md5(user.id) as user_hash, flatten(props, 1) from batch where event <> 'heartbeat'"
run 03 -source ex/products.jsonl -query "select id, name, flatten(attributes, 2), attributes.dims.w * attributes.dims.h as area from batch"
run 04 -source ex/stripe.jsonl -query "select id as event_id, split_part(type, '.', 1) as object_type, data.object.id as intent_id, (try_cast(data.object.amount as bigint) / 100.0)::decimal(18,2) as amount, upper(data.object.currency) as currency, data.object.metadata as metadata, data.object.metadata->>'\$.order' as order_ref from batch"
run 05 -source ex/depths.jsonl -depth 1 -query "select flatten(user, 2), flatten(meta.deep, 5), flatten(meta.other), tags from batch"
run 06 -source ex/clicks.jsonl -batch-size 1 -query "select event, flatten(props, 1) from batch"
run 07 -source ex/conflict.jsonl -batch-size 1 -query "select id as id_raw, try_cast(id as bigint) as id_num, try_cast(id as bigint) is null as id_bad from batch"
run 08 -source ex/pii.jsonl -query "select coalesce(nullif(trim(name), ''), 'unknown') as name, regexp_replace(email, '^(.).*(@.*)\$', '\\1***\\2') as email_masked, right(ssn, 4) as ssn_last4, note from batch"
run 09 -source orders.jsonl -query "select * from batch"
run 10 -source orders.jsonl -query "select _id.\"\$oid\" as order_id, flatten(customer, 1), status, _olake_raw::JSON::VARIANT as doc from batch"
