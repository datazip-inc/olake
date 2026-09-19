# SMT, JSON flattening and Variant for OLake — design notes + runnable examples

Status: design exploration, 2026-09-16 → 2026-09-18. Everything below was run, not reasoned about.
Code: `smt-poc/`. The implementation spec for the team is `smt.md`; this file is the evidence and the journey.
Related Graphiti episode (group `olake`): *Iceberg variant interop matrix: DuckDB vs Spark/Iceberg-Java*.

---

## 0. TL;DR — decisions

| Topic                    | Decision                                                                                                                                                                                  | Why                                                                                                                                                                                                 |
|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Engine                   | DuckDB as an in-process transform over Arrow record batches (`go-duckdb`, CGO), not as the Iceberg writer                                                                                 | DuckDB-written variant files are unreadable by Spark (§2.1); duckdb-iceberg's writer is immature; keeps the Java writer + positional-delete work on `feat/interoperability`                         |
| Three roadmap features   | One SQL box. SMT = the query. Flattening = `flatten(path, depth)` inside it. Variant = `::JSON::VARIANT` inside it                                                                        | No second config surface; preview = run the same query on a sample                                                                                                                                  |
| Schema                   | No upfront schema from the user. Per batch: infer structure → merge into a never-shrinking running structure → generate SQL                                                               | New keys become columns in the batch they appear in, feeding the existing schema-evolution path                                                                                                     |
| Depth                    | A structure edit before `json_transform`, never a SQL rewrite of leaves                                                                                                                   | `json_transform` needs a literal structure; cutting the structure is the only way to stop descending                                                                                                |
| Naming                   | Flat: path joined with `_` through `utils.Reformat`; nested mode (`select *`) keeps struct columns                                                                                        | Struct columns cost nothing in Parquet (§2.4); flattening is for reach + keys                                                                                                                       |
| Variant                  | Declared in the SQL window as `expr::VARIANT`; built in Go with arrow-go 18.8 (unshredded or shredded), written by pqarrow, registered by Iceberg-Java — never by DuckDB's Parquet writer | Go-written variant (both layouts) reads in Spark and DuckDB (§2.5–2.7); DuckDB-written shredded variant breaks Spark (§2.1). Readers need Iceberg ≥ 1.10.2                                          |
| Flattening still needed? | Yes                                                                                                                                                                                       | Iceberg v2 reach (Trino/Athena/Flink/BI); partition/sort/identifier fields must be top-level; stats/pruning for unshredded variant. Variant removes the capture problem, not the projection problem |

---

## 1. What exists in the repo today

| Where                                                   | What                                                                                                   |
|---------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `utils/typeutils/flatten.go`                            | depth-1 flattener: nested values hit the `default:` branch and become `json.Marshal` strings           |
| `destination/iceberg/iceberg.go:83`, `parquet.go:202`   | `normalization: false` → the whole record is one stringified column                                    |
| `types/catalog.go:54`, `:334`                           | `StreamMetadata.Normalization` and its change detection                                                |
| `destination/iceberg/iceberg.go`                        | type promotion ladder `isValidTransition` / `getCommonAncestorType` — reuse, do not build a second one |
| `go.mod`                                                | `arrow-go v18.2.0` — no variant type                                                                   |
| `destination/iceberg/olake-iceberg-java-writer/pom.xml` | Iceberg 1.10.2 — `VariantType` available server-side                                                   |

"Flattening" therefore = extending existing normalization from depth 1 to depth N, not a new concept.

---

## 2. Experimental findings (all verified on this machine)

### 2.1 Variant interop matrix

Stack: `destination/iceberg/local-test` (MinIO) + Lakekeeper 0.13.5 + DuckDB 1.5.5 + Spark 4.0.0 with `iceberg-spark-runtime-4.0_2.13:1.10.0`.

| Writer                    | Physical layout                                      | DuckDB reads               | Spark 4.0 + Iceberg 1.10 reads                                                |
|---------------------------|------------------------------------------------------|----------------------------|-------------------------------------------------------------------------------|
| DuckDB 1.5.5              | shredded (`typed_value` per observed key, automatic) | yes                        | **no** — `IllegalArgumentException: Cannot find field name in metadata: user` |
| Spark / Iceberg-Java 1.10 | unshredded (`metadata` + `value` only)               | yes, incl. `doc.user.name` | yes                                                                           |

- DuckDB always shreds on Parquet write; `variant_minimum_shredding_size=-1` and `force_variant_shredding` had no effect on the COPY / Iceberg write path in 1.5.5.
- DuckDB omits shredded field names from the variant metadata dictionary; Spark's reader requires them. Reverse direction is clean, so DuckDB's writer is the odd one out.
- Conclusion: unshredded variant is the interoperable form today. Do not let DuckDB own the Parquet write for variant columns.

### 2.2 Tooling gaps hit on the way

| Component                                                     | Gap                                                                                                                                                                                                                                            |
|---------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Lakekeeper 0.11.1 (pinned in `local-test/docker-compose.yml`) | rejects any variant schema: `data did not match any variant of untagged enum SchemaEnum`. 0.13.5 accepts, reports `allowed-format-versions [1,2,3]`                                                                                            |
| duckdb-iceberg                                                | `CREATE TABLE` is always format-version 2, no SQL to request v3. Workaround: POST REST `create-table` with `"properties":{"format-version":"3"}`, then DuckDB `INSERT` works                                                                   |
| duckdb-iceberg                                                | no `CREATE OR REPLACE`; CTAS (staged create) 404s against Lakekeeper; explicit `CREATE TABLE` + `INSERT` works; warehouse needs `sts-enabled: true` or DuckDB uses an empty internal secret → MinIO 403                                        |
| `apache/iceberg-rest-fixture:1.10.0`                          | attaches read-only in DuckDB (no credential vending)                                                                                                                                                                                           |
| `arrow-go`                                                    | v18.8.0 (2026-09-04) has variant: `arrow/extensions/variant.go`, `parquet/variant/`, `arrow/compute/variant_get.go`. Repo is on v18.2.0. No v19 exists                                                                                         |
| `go-duckdb` v2.4.3                                            | bundles DuckDB 1.4.1 — `VARIANT` exists but `COPY … TO parquet` with VARIANT is `Not implemented`; Arrow export of VARIANT fails (`duckdb_query_arrow_schema`). Pins `arrow-go v18.4.1`. Needs build tag `duckdb_arrow` for `NewArrowFromConn` |

### 2.3 DuckDB primitives that make this cheap

| Function                           | Role                                                                | Verified behaviour                                                                        |
|------------------------------------|---------------------------------------------------------------------|-------------------------------------------------------------------------------------------|
| `json_group_structure(j)`          | discover shape + types across rows, unioning keys                   | type conflict → `"JSON"`; all-null → `"NULL"`; `{}` and `[]` → `"JSON"` (ambiguous, §4.3) |
| `json_transform(j, '<structure>')` | JSON → typed STRUCT in one parse; casts inline; missing keys → NULL | structure must be a literal constant: `Binder Error: JSON structure must be a constant!`  |
| `SELECT o.*` on a struct           | exposes struct fields as top-level columns                          | user writes `customer.email`, `status` without a prefix                                   |
| `json_tree` + `PIVOT`              | fully dynamic flatten, no schema                                    | works, but every column is `json`-typed and rows explode per leaf — rejected              |
| `unnest(struct, max_depth := N)`   | depth-limited struct flatten                                        | leaf-only names: `user.id` and `order.id` both become `id` — rejected                     |
| `variant_to_parquet_variant()`     | variant → `struct<metadata blob, value blob>`                       | boundary representation if Go must carry variant without arrow-go ≥ 18.8                  |

### 2.4 Struct columns vs flat columns — query performance

5M rows, same data, DuckDB-written Parquet:

| Layout                  | Leaf columns | Column chunks | Size     | Filter on 5M rows |
|-------------------------|--------------|---------------|----------|-------------------|
| nested `user.addr.city` | 5            | 205           | 68.63 MB | ~8 ms             |
| flat `user_addr_city`   | 5            | 205           | 68.63 MB | ~3–4 ms           |

Byte-identical files. Per-leaf min/max stats exist for nested fields (`user, addr, city → c0 … c999`), so Iceberg manifest pruning is the same. Projection and predicate pushdown reach into the struct (`Projections: user.addr.city, user.score`, `Filters: user.score>500`). The CPU gap is definition-level decoding, a constant factor.

Not identical: engines without nested pruning (older Presto, Redshift Spectrum, many BI connectors) read the whole struct; partition/sort/identifier fields and OLake's own equality-delete keys must be top-level.

### 2.5 Arrow VARIANT → Parquet (arrow-go only) → Iceberg v3 — works

Question: with Arrow as the interchange format and DuckDB only running the query, can Go write the variant Parquet itself and register it in Iceberg, without DuckDB touching Arrow → Parquet? Yes. Now built into `smt-poc/main.go` (`::VARIANT` → Go-built Arrow variant, `-out` via pqarrow); registration in `smt-poc/iceberg/Register.java`.

| Step          | Tool                                                                                                                     | Result                                                                                                 |
|---------------|--------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| query         | go-duckdb 2.4.3 (DuckDB 1.4.1) → Arrow                                                                                   | returns the document as JSON text (`doc_json`); DuckDB cannot export VARIANT to Arrow, so Go builds it |
| build variant | arrow-go 18.8: `variant.ParseJSON` → `extensions.NewVariantBuilder(NewDefaultVariantType())`                             | `extension<arrow.parquet.variant>`, unshredded                                                         |
| write Parquet | `pqarrow.NewFileWriter` + `PARQUET:field_id` metadata                                                                    | `doc` = group `{metadata BYTE_ARRAY, value BYTE_ARRAY}`, `logical=Variant`, field_id 4                 |
| register      | Iceberg-Java 1.10.2 via RESTCatalog (Lakekeeper 0.13.5): `ParquetUtil.fileMetrics` + `newAppend().appendFile().commit()` | metrics ok (bounds for cols 1–3, none for variant), snapshot committed to a format-version 3 table     |
| read          | Spark 4.0 + Iceberg 1.10                                                                                                 | `variant_get(doc,'$.customer.addr.city','string')` → `pune`, `schema_of_variant` full                  |
| read          | DuckDB 1.5.5 iceberg extension                                                                                           | `doc.customer.addr.city` → `pune`, `doc.items[1].sku` → `a`, `variant_typeof` → `OBJECT(...)`          |

This is the interoperable variant path for OLake: the Go/Arrow side owns the file, Iceberg-Java registers it (the arrow writer's `REGISTER_AND_COMMIT` shape). Contrast §2.1: DuckDB-written variant files are auto-shredded and Spark cannot read them.

Gotchas on the way: Lakekeeper's catalog config overrides `s3.endpoint` to the in-network hostname (register from inside the network); vended STS credentials are scoped to the table location, so a data file outside `<table location>/` gets 403 — copy it under the table location first; Spark `add_files` from an `s3a://` source needs hadoop-aws + awssdk transfer-manager that `iceberg-aws-bundle` does not ship — the Java API path avoids it.

### 2.6 Shredded variant from Go — also works, in both engines

Same pipeline with `-shred` (auto schema from the running structure; the original experiment used a hand-written one): `extensions.NewShreddedVariantType(struct{customer{id int64, email string, addr{city string}}, status string})`. `VariantBuilder.Append` splits each value itself: typed paths → `typed_value`, everything else (`_id`, `createdAt`, `customer.addr.geo`, `items`) → residual `value`; the full name dictionary stays in `metadata`.

| Reader                                   | Shredded path (`customer.addr.city`, `customer.id`, `status`)                                                               | Residual path (`customer.addr.geo.lat`, `items[0].sku`) |
|------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------|
| Spark 4.0 + Iceberg 1.10 (`variant_get`) | `pune` / `7` / `paid`                                                                                                       | `18.5` / `a`                                            |
| DuckDB 1.5.5 iceberg extension           | `pune`                                                                                                                      | `18.5` / `a`                                            |
| Iceberg-Java 1.10.2 metrics              | bounds for field 4 (the variant) collected from the typed columns — pruning on variant sub-fields only exists when shredded |                                                         |

Why this works where DuckDB's shredded file didn't: arrow-go keeps every field name in the variant `metadata` dictionary; DuckDB's writer drops the shredded ones and Spark refuses the file.

Options for OLake, in preference order:

| Option                    | Where shredding happens                                                                                                                                              | Status                                |
|---------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| A. Go, arrow-go 18.8      | `NewShreddedVariantType` from a schema OLake derives (running JSON structure + optional user hint), one code path feeding both the parquet and arrow-iceberg writers | verified end-to-end                   |
| B. Java writer            | Iceberg 1.10.2 ships `VariantShreddingFunction` + `ParquetVariantWriters.ShreddedVariantWriter`; the legacy gRPC path would pass a shredding schema per table        | present in the fat jar, not exercised |
| C. Unshredded             | default; always interoperable; no sub-field stats                                                                                                                    | verified (§2.5)                       |
| D. DuckDB writes the file | breaks Spark                                                                                                                                                         | rejected (§2.1)                       |

Note: arrow-go has no "shred an existing unshredded array" kernel (only `UnshredVariant`); shredding is decided when the array is built from `variant.Value`, which is where OLake would decide it anyway. Shredding schema is per file, so it can evolve batch to batch without a table change.

### 2.7 Mixing shredded and unshredded files in one table — OK, with reader caveats

Hourly ingestions will not shred the same paths every time. Shredding is a per-file property; the Iceberg schema only says `doc: variant`. Test: appended the unshredded file into `demo.arrow_variant_shred` next to the shredded one (2 files, 4 rows).

| Check                                                                          | Result                                                                        |
|--------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| Spark 4.0 + Iceberg 1.10.2, `SELECT … variant_get` over both files             | 4 rows, correct values from both layouts                                      |
| `WHERE variant_get(doc,'$.customer.addr.city','string') = 'pune'` with `_file` | 2 rows, one from each file                                                    |
| DuckDB 1.5.5 iceberg extension                                                 | 4 rows, `count(*) FILTER (city='pune')` = 2                                   |
| manifest `lower_bounds`                                                        | shredded file: bounds for field 4 (from `typed_value`); unshredded file: none |

So: correct always; pruning on a variant sub-path only skips the files that happened to shred it, the others get scanned. Performance varies per file, never results. Keeping a shredding decision sticky per stream (persist the last shredding schema with the running structure; only add paths, drop only on type break) keeps that variance small.

Spark reader bugs hit on the way — independent of who wrote the file (reproduced on Spark's own variant files). Spark logs were not kept in the repo; rerun with the commands in §8:

| Iceberg Spark runtime | Symptom                                                                                                                                                                                                                                            | Status                                                                                                                              |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| 1.10.0                | any row filter touching the variant column returns **0 rows** (`doc IS NOT NULL` → 0, `doc IS NULL` → 0, `variant_get(...) = x` → 0) while `SELECT variant_get(...)` works; projections without `doc` NPE (`Type.asVariantType() … iType is null`) | fixed in 1.10.2                                                                                                                     |
| 1.10.2                | projections **without** the variant column (`SELECT order_id FROM t`, `count(*) WHERE order_id = …`) fail: vectorized reader → `UnsupportedOperationException: Not implemented for variant`                                                        | workaround `spark.sql.iceberg.vectorization.enabled=false` (or table property `read.parquet.vectorization.enabled=false`), verified |

For OLake: require Iceberg ≥ 1.10.2 on the Spark reader side for variant tables and document the vectorization flag until the vectorized reader handles unprojected variant groups.

---

## 3. Architecture

```
driver.Read → []RawRecord / Arrow batch
            │
            ▼
   ┌─────────────────────────── transform stage (go-duckdb, in-process) ──────────────────────────┐
   │ 1. SELECT json_group_structure(doc)              → batch structure                            │
   │ 2. merge(running, batch)                        → running structure (monotonic)              │
   │ 3. parse flatten(path, depth) terms in user SQL → depth map                                  │
   │ 4. cut(running, depths)                         → objects past their limit become "JSON"     │
   │ 5. rewrite each flatten() term                  → one projection per leaf of the cut tree    │
   │ 6. WITH __inner AS (SELECT b.*, json_transform(doc,'<cut>') AS __o FROM batch b),            │
   │         batch   AS (SELECT i.* EXCLUDE (__o, doc), doc AS _olake_raw, __o.* FROM __inner i)  │
   │    <user query>                                 → Arrow record batch / COPY … TO parquet     │
   └───────────────────────────────────────────────────────────────────────────────────────────────┘
            │
            ▼
   FlattenAndCleanData → existing writers (parquet / legacy iceberg gRPC / arrow iceberg), unchanged
```

| Property       | Detail                                                                                                                                                                                                              |
|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Position       | before `FlattenAndCleanData`; writers see plain typed columns (or structs in nested mode), so all three destinations get it with zero writer changes                                                                |
| System columns | `_olake_id`, `_op_type`, `_cdc_timestamp` attached after the query — a `SELECT` cannot drop them                                                                                                                    |
| Plan cache     | generated SQL cached by hash; unchanged structure ⇒ "SQL unchanged → cached plan"; only shape changes re-plan                                                                                                       |
| State          | running structure must be persisted per stream (`state.json`) or the column set resets on restart                                                                                                                   |
| Cost           | `go-duckdb` is CGO across 8 driver modules × linux/amd64+arm64 (db2 amd64-only) in a plain `golang:1.25.13-bookworm` builder; ~40–60 MB per binary. DataFusion is the same FFI class and has no variant type either |

---

## 4. Semantics

### 4.1 `flatten(path, depth)`

| Form                           | Meaning                                                                                                                                                                                                                                                                                                                                                     |
|--------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `flatten(customer, 2)`         | every leaf up to 2 key-levels below `customer` becomes a column; objects at level 3 become JSON text columns (`customer_addr_geo`)                                                                                                                                                                                                                          |
| `flatten(*)` / `flatten(*, N)` | the root at default depth / N                                                                                                                                                                                                                                                                                                                               |
| `flatten(_olake_raw, N)`       | same as `flatten(*, N)`; a relational row's JSON column is an ordinary key (`flatten(attributes, 2)`)                                                                                                                                                                                                                                                       |
| per-path override              | restarts the count at that subtree; ancestors of a deeper term stay open, their other children obey the inherited limit (`flatten(meta.deep, 5)` with default 1 does not flatten `meta.other`)                                                                                                                                                              |
| overlapping terms              | each term walks only its own depth; an object left open by a deeper term but past this term's depth is emitted as JSON text (`to_json`); the same column requested twice is emitted once. `flatten(customer, 1), flatten(customer.addr.geo, 1)` → `customer_addr` (JSON), `customer_email`, `customer_id`, `customer_addr_geo_lat`, `customer_addr_geo_lng` |
| unflattened keys               | still addressable by name (`customer.email`, `status`, `data.object.metadata->>'$.order'`) because `__o.*` is exposed                                                                                                                                                                                                                                       |
| `_olake_raw`                   | the raw document, for `::JSON::VARIANT`, overflow, audit. `select *` includes it; `select * exclude (_olake_raw)` drops it                                                                                                                                                                                                                                  |
| arrays                         | never exploded — row count must not change (`_olake_id`, PK dedup, equality deletes, positional-delete index). `list<T>` / `list<struct>` land as Iceberg lists; reshape with `list_transform`, aggregate with `list_sum`                                                                                                                                   |
| changing a depth               | schema-affecting (string ↔ struct); include the query hash in catalog change detection (`types/catalog.go:334`)                                                                                                                                                                                                                                             |

### 4.2 Naming

| Rule                                                                                                    | Example                                                        |
|---------------------------------------------------------------------------------------------------------|----------------------------------------------------------------|
| flat alias = path joined with `_`, then `utils.Reformat` (lowercase, every non-letter/digit rune → `_`) | `createdAt.$date` → `createdat__date`, `_id.$oid` → `_id__oid` |
| collisions refused at check time, never silently renamed                                                | `a.b` vs `a_b`, `Id` vs `id`, `a b` vs `a-b`                   |
| nested mode (`select *`) has no naming problem                                                          | struct field names are verbatim                                |
| policy knobs, one function (`reformat`)                                                                 | collapse `__`, keep case, `$` handling                         |

### 4.3 Types

| Situation                                                                       | Behaviour                                                                                                                                                                                                                                                               |
|---------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| numeric widening                                                                | `UBIGINT → BIGINT → HUGEINT → DOUBLE`, monotonic                                                                                                                                                                                                                        |
| any other conflict (`BOOLEAN` vs number, number vs `VARCHAR`, object vs scalar) | `JSON` — raw text, JSON-encoded (a widened string column holds `"abc"` with quotes)                                                                                                                                                                                     |
| all-null in a batch (`"NULL"`)                                                  | wildcard: `NULL ∪ X = X`                                                                                                                                                                                                                                                |
| DuckDB says `"JSON"` where the running structure has an object/array            | ambiguous — also means `{}` / `[]`. One `json_type()` count against the batch for that path; if every non-null value is `OBJECT`/`ARRAY`, the known shape is kept. Without this one heartbeat with `props: {}` poisoned `props` for the rest of the stream (example 06) |
| big ints                                                                        | DuckDB collapses `UBIGINT + DOUBLE` to `DOUBLE` (`2^64-1` → `1.8446744073709552e+19`). OLake's ladder should promote to `DECIMAL(38,x)` / string — override the inferred leaf before `cut`                                                                              |
| typed-looking strings (`"12.50"`, ISO timestamps, `"007"`, `"true"`)            | stay `VARCHAR`; casts belong in the query                                                                                                                                                                                                                               |
| struct → scalar on the same key                                                 | the only path where a previously emitted column disappears; in Iceberg it is `struct → string`, an incompatible type change — fail loudly or route the scalar to overflow (decide before shipping)                                                                      |

### 4.4 Rules the user learns

| Want                           | Write                                                 |
|--------------------------------|-------------------------------------------------------|
| nested object typed to depth N | `flatten(path, N)`                                    |
| leave a subtree as JSON        | don't flatten past it, or `to_json(path)`             |
| rename                         | `expr as name`                                        |
| change type                    | `::type` / `try_cast(x as type)`                      |
| drop a column                  | don't select it                                       |
| drop rows                      | `where …`                                             |
| mask / hash / default          | `regexp_replace` / `md5` / `coalesce(nullif(…))`      |
| arrays                         | `list_transform`, `list_sum`, `x[1]` — never `unnest` |
| whole document as variant (v3) | `_olake_raw::JSON::VARIANT as doc`                    |
| nothing                        | `select * from batch`                                 |

### 4.5 Variant in the same SQL window

`expr::VARIANT` in the user's query is the type declaration — no separate config. It is a **marker**, not something DuckDB materializes, because go-duckdb's DuckDB cannot export VARIANT to Arrow (§2.2) and DuckDB's own Parquet variant breaks Spark (§2.1). Two passes over the same SQL:

| Pass                     | What                                                                                                                                                          | Result                                                                                                    |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| 1. `DESCRIBE <user SQL>` | binds the original query, `::VARIANT` included (works on DuckDB 1.4.1)                                                                                        | output types; columns typed `VARIANT` are flagged; this is the Iceberg schema source, not the Arrow batch |
| 2. rewrite + execute     | `expr::VARIANT` → `expr::JSON` (VARCHAR input) or `to_json(expr)` (STRUCT/JSON input), decided from the DESCRIBE types; same AST rewrite stage as `flatten()` | Arrow batch with those columns as JSON text                                                               |
| 3. Go, once per row      | `variant.ParseJSON` → `extensions.VariantBuilder` (default or shredded type)                                                                                  | Arrow batch with `doc: extension<arrow.parquet.variant>`                                                  |
| 4. writer                | arrow-iceberg writer: Arrow → Parquet via pqarrow, unchanged                                                                                                  | Parquet `VARIANT` logical type; Iceberg-Java registers `variant`                                          |

The user writes plain `payload::VARIANT`; the VARCHAR-vs-STRUCT cast difference (`::VARIANT` on a string gives a variant *string*, §5 ex. 10) is absorbed by the rewrite.

**Parse placement.** The batch the writer receives already *is* variant; the writer does no parsing and its Arrow → Parquet path is unchanged. The document arrives as text, so exactly one JSON → variant encode is unavoidable; today it sits in Go after DuckDB.

| Where the parse happens                                                      | Status                                                                                                                                          |
|------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| Go, after DuckDB (current)                                                   | verified end-to-end; shredding decided here                                                                                                     |
| DuckDB emitting `arrow.parquet.variant` to Arrow                             | needs DuckDB ≥ 1.5 in go-duckdb (2.4.3 bundles 1.4.1); unverified whether its Arrow export emits the canonical type, and it would be unshredded |
| Go before DuckDB (variant built from source bytes, fed into DuckDB as Arrow) | unverified whether DuckDB accepts the extension type as input                                                                                   |

The one real inefficiency: a query using both `flatten(payload, 2)` and `payload::VARIANT` parses the document twice (DuckDB for `json_transform`, Go for the variant). Acceptable for v1 (JSON parse is a few hundred MB/s per core, well below Parquet encode + upload); later either let DuckDB emit variant, or produce the flattened columns from the variant in Go with arrow-go's `variant_get` kernels and skip `json_transform` for those columns.

### 4.6 Shredding policy — which paths get `typed_value`

No system publishes a presence threshold: Parquet spec / Iceberg-Java / Spark / arrow-go take a schema from the caller; DuckDB shreds every observed key; ClickHouse caps by count (`max_dynamic_paths` = 1024, `max_dynamic_types` = 32); Snowflake's rule is undocumented. Presence % is the wrong knob; use, in this order:

| Filter            | Rule                                                                                                                    | Why                                                                                                                               |
|-------------------|-------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------|
| 1. type stability | shred a path only if ≥ ~99 % of its non-null values in the batch share one primitive type (numeric widening = one type) | a row whose value doesn't match the typed type falls back to `value` per spec; an unstable column silently stops helping pruning  |
| 2. column cap     | at most ~200–300 shredded leaves per file, ranked by presence descending                                                | the failure mode is unbounded paths (ID-keyed objects), not sparsity; Iceberg collects stats for the first 100 columns by default |
| 3. presence floor | ≥ 5–10 % as a tie-breaker only                                                                                          | a sparse shredded column is nearly free (null definition levels RLE to bytes)                                                     |
| 4. stickiness     | persist the shredding schema per stream with the running structure; only add paths, drop only on type break             | keeps pruning behaviour stable across hourly files (§2.7)                                                                         |

Inputs already exist: the running JSON structure (`JSON` leaf = unstable) and per-key presence from `json_group_structure`. A wrong guess costs one file's pruning — shredding is per file, readers merge `typed_value` and `value` per row (§2.7).

---

## 5. Ten runnable examples

Produced by `smt-poc/ex/run.sh` → `ex/out/NN.txt` (log) and `ex/out/NN.parquet` (OLake-style column names). The binary prints each batch as a DuckDB-style box table (header, type row, rows); `-v` adds the inferred structures and generated SQL. The italic row under each header below is the output type. `∅` = NULL.

### 01 — MongoDB orders: flatten + casts + list aggregation

Input `orders.jsonl`, column `doc`:

```json
{"_id":{"$oid":"66a1b2"},"customer":{"id":7,"email":"Ada@X.COM","addr":{"city":"pune","geo":{"lat":18.5,"lng":73.8}}},
 "items":[{"sku":"a","qty":2,"price":"12.50"},{"sku":"b","qty":1,"price":"3.25"}],"status":"paid","createdAt":{"$date":"2024-05-01T10:00:00Z"}}
{"_id":{"$oid":"66a1b3"},"customer":{"id":8,"email":"bob@y.org","addr":{"city":"goa"}},"items":[],"status":"open","createdAt":{"$date":"2024-05-02T11:00:00Z"}}
```

Query:

```sql
select
  _id."$oid"                                  as order_id,
  flatten(customer, 2),
  createdAt."$date"::timestamptz              as created_at,
  list_transform(items, lambda x: {'sku': x.sku, 'qty': x.qty, 'price': x.price::decimal(18,2)}) as items,
  list_sum(list_transform(items, lambda x: x.qty * x.price::decimal(18,2)))                     as order_total,
  status = 'paid'                             as is_paid
from batch
```

Generated structure (depth cut applied) and the expansion of `flatten(customer, 2)`:

```
{"_id":{"$oid":"VARCHAR"},"createdAt":{"$date":"VARCHAR"},
 "customer":{"addr":{"city":"VARCHAR","geo":"JSON"},"email":"VARCHAR","id":"UBIGINT"},
 "items":[{"price":"VARCHAR","qty":"UBIGINT","sku":"VARCHAR"}],"status":"VARCHAR"}
```
```sql
"customer"."addr"."city" AS customer_addr_city,
"customer"."addr"."geo"  AS customer_addr_geo,
"customer"."email"       AS customer_email,
"customer"."id"          AS customer_id
```

Output:

| order_id  | customer_addr_city | customer_addr_geo         | customer_email | customer_id | created_at           | items                                         | order_total     | is_paid   |
|-----------|--------------------|---------------------------|----------------|-------------|----------------------|-----------------------------------------------|-----------------|-----------|
| *varchar* | *varchar*          | *json*                    | *varchar*      | *ubigint*   | *timestamptz*        | *list<struct(sku, qty, price decimal(18,2))>* | *decimal(38,2)* | *boolean* |
| 66a1b2    | pune               | `{"lat":18.5,"lng":73.8}` | Ada@X.COM      | 7           | 2024-05-01T10:00:00Z | `[{a,2,12.50},{b,1,3.25}]`                    | 28.25           | true      |
| 66a1b3    | goa                | ∅                         | bob@y.org      | 8           | 2024-05-02T11:00:00Z | `[]`                                          | ∅               | false     |

Parquet `01.parquet`:

| Column            | Parquet type                                   | Logical type                     |
|-------------------|------------------------------------------------|----------------------------------|
| customer_addr_geo | BYTE_ARRAY                                     | JsonType()                       |
| created_at        | INT64                                          | TimestampType(isAdjustedToUTC=1) |
| items             | list / element / {sku, qty INT64, price INT64} | DecimalType(18,2) on price       |
| order_total       | FIXED_LEN_BYTE_ARRAY                           | DecimalType(38,2)                |

Note: the first draft also had `lower(customer.email) as customer_email`, which collides with the `customer_email` produced by `flatten(customer, 2)` — refused at check.

### 02 — Kafka clickstream: row filter, PII drop, hash, partial flatten

Input `ex/clicks.jsonl`:

```json
{"event":"click","ts":1712345678901,"user":{"id":"u1","ua":"Mozilla/5.0"},"props":{"page":"/x","ref":"google","ab":{"exp1":"B"}}}
{"event":"heartbeat","ts":1712345678902,"user":{"id":"u1","ua":"Mozilla/5.0"},"props":{}}
{"event":"purchase","ts":1712345678903,"user":{"id":"u2","ua":"curl/8"},"props":{"page":"/checkout","amount":19.99}}
```

```sql
select event, epoch_ms(ts::bigint) as event_ts, md5(user.id) as user_hash, flatten(props, 1)
from batch
where event <> 'heartbeat'
```

| event     | event_ts                 | user_hash | props_ab       | props_amount | props_page | props_ref |
|-----------|--------------------------|-----------|----------------|--------------|------------|-----------|
| *varchar* | *timestamp*              | *varchar* | *json*         | *double*     | *varchar*  | *varchar* |
| click     | 2024-04-05T19:34:38.901Z | e4774cdd… | `{"exp1":"B"}` | ∅            | /x         | google    |
| purchase  | 2024-04-05T19:34:38.903Z | 270c1b08… | ∅              | 19.99        | /checkout  | ∅         |

`user.ua` never lands (not selected). `props.amount` exists only in row 3 and is a column anyway.

### 03 — Postgres row with a JSONB column

Input `ex/products.jsonl` — the row is the document; the JSONB column is a nested key:

```json
{"id":42,"name":"widget","attributes":{"color":"red","dims":{"w":10,"h":4},"tags":["a","b"]}}
{"id":43,"name":"gadget","attributes":{"color":"blue","dims":{"w":3,"h":3,"d":1},"tags":[]}}
```

```sql
select id, name, flatten(attributes, 2), attributes.dims.w * attributes.dims.h as area from batch
```

| id        | name      | attributes_color | attributes_dims_d | attributes_dims_h | attributes_dims_w | attributes_tags | area      |
|-----------|-----------|------------------|-------------------|-------------------|-------------------|-----------------|-----------|
| *ubigint* | *varchar* | *varchar*        | *ubigint*         | *ubigint*         | *ubigint*         | *varchar[]*     | *ubigint* |
| 42        | widget    | red              | ∅                 | 4                 | 10                | `["a","b"]`     | 40        |
| 43        | gadget    | blue             | 1                 | 3                 | 3                 | `[]`            | 9         |

Relational columns pass through as ordinary keys; `attributes.dims.d` (row 2 only) is a column anyway.

### 04 — Stripe-style deep webhook: default depth + JSON path on a cut leaf

Input `ex/stripe.jsonl`:

```json
{"id":"evt_1","type":"payment_intent.succeeded","data":{"object":{"id":"pi_1","amount":1999,"currency":"usd","customer":"cus_9",
 "metadata":{"order":"o-77","tier":"gold"},"charges":{"data":[{"id":"ch_1","outcome":{"risk":"normal"}}]}}}}
{"id":"evt_2","type":"charge.refunded","data":{"object":{"id":"ch_2","amount":500,"currency":"eur","metadata":{}}}}
```

```sql
select
  id                                          as event_id,
  split_part(type, '.', 1)                    as object_type,
  data.object.id                              as intent_id,
  (data.object.amount / 100.0)::decimal(18,2) as amount,
  upper(data.object.currency)                 as currency,
  data.object.metadata                        as metadata,        -- level 3 → JSON text
  data.object.metadata->>'$.order'            as order_ref        -- still queryable
from batch
```

| event_id  | object_type    | intent_id | amount          | currency  | metadata                         | order_ref |
|-----------|----------------|-----------|-----------------|-----------|----------------------------------|-----------|
| *varchar* | *varchar*      | *varchar* | *decimal(18,2)* | *varchar* | *json*                           | *varchar* |
| evt_1     | payment_intent | pi_1      | 19.99           | USD       | `{"order":"o-77","tier":"gold"}` | o-77      |
| evt_2     | charge         | ch_2      | 5.00            | EUR       | `{}`                             | ∅         |

No `flatten()` at all — default depth 3 cut `metadata` / `charges`; dot access did the rest.

The shipped `ex/stripe.jsonl` has a third row with `"amount":"500"` (string), which widens `amount` to `JSON`; `ex/run.sh` therefore uses `try_cast(data.object.amount as bigint) / 100.0` — the §4.3 rule in action (that row yields `amount = NULL`).

### 05 — Per-path depth overrides

Input `ex/depths.jsonl`:

```json
{"user":{"id":7,"addr":{"city":"pune","geo":{"lat":1.5,"lng":73.8}}},"meta":{"deep":{"x":{"y":{"z":1}}},"other":{"k":1}},"tags":["a","b"]}
{"user":{"id":8,"addr":{"city":"goa"}},"meta":{"deep":{"x":{"y":{"z":2,"w":true}}}},"tags":[]}
```

```sql
-- run with -depth 1
select flatten(user, 2), flatten(meta.deep, 5), flatten(meta.other), tags from batch
```

| user_addr_city | user_addr_geo            | user_id   | meta_deep_x_y_w | meta_deep_x_y_z | meta_other_k | tags        |
|----------------|--------------------------|-----------|-----------------|-----------------|--------------|-------------|
| *varchar*      | *json*                   | *ubigint* | *boolean*       | *ubigint*       | *ubigint*    | *varchar[]* |
| pune           | `{"lat":1.5,"lng":73.8}` | 7         | ∅               | 1               | 1            | `["a","b"]` |
| goa            | ∅                        | 8         | true            | 2               | ∅            | `[]`        |

`meta` stays open because deeper terms reference it; `meta.deep` goes 5 down; `user` stops at 2 (`geo` → JSON).

### 06 — Schema evolution across batches

`ex/clicks.jsonl` with `-batch-size 1`:

```sql
select event, flatten(props, 1) from batch
```

| Batch | Row       | Output columns                                                    | Note                                           |
|-------|-----------|-------------------------------------------------------------------|------------------------------------------------|
| 1     | click     | event, props_ab, props_page, props_ref                            |                                                |
| 2     | heartbeat | event, props_ab, props_page, props_ref                            | `props: {}` kept the known shape (§4.3), all ∅ |
| 3     | purchase  | event, props_ab, props_page, props_ref, **props_amount** (double) | `schema evolution: +[props_amount]`            |

Three Parquet files (`06.parquet`, `06_b2.parquet`, `06_b3.parquet`) — one per batch; schema grows, never shrinks.

### 07 — Type conflict + recovery

`ex/conflict.jsonl` = `{"id":1}`, `{"id":"abc"}`, `{"id":"77"}` with `-batch-size 1`:

```sql
select id as id_raw, try_cast(id as bigint) as id_num, try_cast(id as bigint) is null as id_bad from batch
```

| Batch | id_raw  | type of id_raw                             | id_num | id_bad |
|-------|---------|--------------------------------------------|--------|--------|
| 1     | 1       | ubigint                                    | 1      | false  |
| 2     | `"abc"` | json (widened UBIGINT → JSON, column kept) | ∅      | true   |
| 3     | `"77"`  | json                                       | 77     | false  |

The user decides what bad values mean; OLake never guesses.

### 08 — Null hygiene + masking + column drop

Input `ex/pii.jsonl`:

```json
{"name":"  ","email":"ada@example.com","ssn":"123-45-6789","note":null}
{"name":"Bob","email":"b@y.org","ssn":"987-65-4321","note":"vip"}
```

```sql
select
  coalesce(nullif(trim(name), ''), 'unknown')        as name,
  regexp_replace(email, '^(.).*(@.*)$', '\1***\2')   as email_masked,
  right(ssn, 4)                                      as ssn_last4,
  note
from batch
```

| name      | email_masked     | ssn_last4 | note      |
|-----------|------------------|-----------|-----------|
| *varchar* | *varchar*        | *varchar* | *varchar* |
| unknown   | a***@example.com | 6789      | ∅         |
| Bob       | b***@y.org       | 4321      | vip       |

`note` was null in row 1 → `NULL` wildcard → `VARCHAR` from row 2.

### 09 — Nested mode: `select * from batch` (orders)

| Column     | Type                                                                   |
|------------|------------------------------------------------------------------------|
| _olake_raw | varchar                                                                |
| _id        | struct("$oid" varchar)                                                 |
| createdat  | struct("$date" varchar)                                                |
| customer   | struct(addr struct(city varchar, geo json), email varchar, id ubigint) |
| items      | struct(price varchar, qty ubigint, sku varchar)[]                      |
| status     | varchar                                                                |

Parquet `09.parquet` is a genuine nested layout — `customer` group → `addr` group → `city`, `geo (JsonType)`; `items` → `list / element / {price, qty, sku}`. This is Iceberg v2 `struct` / `list`; readers with nested pruning pay nothing extra (§2.4). `select * exclude (_olake_raw)` drops the raw copy.

### 10 — Variant column (Iceberg v3) next to flattened keys

```sql
select _id."$oid" as order_id, flatten(customer, 1), status, _olake_raw::JSON::VARIANT as doc from batch
```

The binary treats `::VARIANT` as a marker (§4.5): DuckDB returns JSON text, Go builds the Arrow variant column and pqarrow writes `10.parquet` with the Parquet `VARIANT` logical type (unshredded; add `-shred` for `typed_value`). DuckDB 1.5.5 reads it back:

| order_id | doc.customer.addr.city | variant_typeof(doc)                             |
|----------|------------------------|-------------------------------------------------|
| 66a1b2   | pune                   | OBJECT(_id, createdAt, customer, items, status) |
| 66a1b3   | goa                    | OBJECT(_id, createdAt, customer, items, status) |

Historical note — the first version of this example used DuckDB's own `COPY … TO parquet` (CLI 1.5.5, `::JSON::VARIANT`): that file is **shredded by DuckDB** and Spark cannot read it (§2.1); go-duckdb's 1.4.1 cannot write it at all. That is why the Go path exists.

Gotcha kept from that version: in raw DuckDB SQL `x::VARIANT` on a VARCHAR gives a variant *string*; `::JSON::VARIANT` gives an object. The binary's rewrite (`::VARIANT` → `::JSON`, then Go) hides this.

---

## 6. Edge-case matrix

`smt-poc -cases`: 26 cases, 24 pass, 2 refused by design.

| #   | Case                                                    | Outcome                                                         |
|-----|---------------------------------------------------------|-----------------------------------------------------------------|
| 01  | flat scalars                                            | `UBIGINT + DOUBLE → DOUBLE`                                     |
| 02  | default depth 2                                         | `a_b` JSON text, `a_x` typed                                    |
| 03  | per-path overrides                                      | ancestors of deeper terms stay open                             |
| 04  | sparse keys                                             | union of keys, missing → null                                   |
| 05  | all-null then typed                                     | `NULL` wildcard, no JSON poisoning                              |
| 06  | int → string                                            | `→ JSON`, column kept, value `"abc"` quoted                     |
| 07  | int → float                                             | `→ DOUBLE`                                                      |
| 08  | `-1`, `2^64-1`, `1e30`                                  | DuckDB → DOUBLE, precision loss — needs decimal/string in OLake |
| 09  | bool → number                                           | `→ JSON`, never `true → 1`                                      |
| 10  | struct → scalar                                         | `a_b` disappears, `a` appears — policy decision pending         |
| 11  | arrays                                                  | native lists; `[]`-only → `list<JSON>` until elements arrive    |
| 12  | array of objects                                        | `list<struct>`, element keys unioned                            |
| 13  | mixed array                                             | `list<JSON>`                                                    |
| 14  | nested arrays                                           | `list<list<>>`                                                  |
| 15  | `a.b`, `a b`, `a-b`, `$oid`, `名前`, `123`, `""`, `UPPER` | refused: mangled names collide                                  |
| 16  | `a.b` vs `a_b`, `Id` vs `id`                            | refused                                                         |
| 17  | duplicate key `{"a":1,"a":2}`                           | first wins                                                      |
| 18  | typed-looking strings                                   | all VARCHAR                                                     |
| 19  | 8 levels, cut 6                                         | `l1_l2_l3_l4_l5_l6` = JSON text of the rest                     |
| 20  | root = string / number / array                          | raw passthrough                                                 |
| 21  | `{}`, `{"e":{}}`                                        | JSON leaf; `{}` no longer poisons a known shape                 |
| 22  | Mongo `$oid` / `$date` / `$numberLong`                  | ordinary struct fields; `_id__oid`                              |
| 23  | key appears in batch 2, absent in 3                     | column persists as null, plan cached                            |
| 24  | 150 keys                                                | fine; max-columns is a policy guard                             |
| 25  | escapes, unicode, emoji                                 | untouched                                                       |
| 26  | flatten + derived + passthrough list                    | as example 01                                                   |

---

## 7. Open decisions before productionizing

| #   | Decision                                                          | Options                                                                                                                                                                                      |     |     |     |     |     |
|-----|-------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----|-----|-----|-----|-----|
| 1   | struct → scalar retype (case 10)                                  | fail at writer via `isValidTransition`, or overflow                                                                                                                                          |     |     |     |     |     |
| 2   | big-int widening (case 08)                                        | decimal / string, not double                                                                                                                                                                 |     |     |     |     |     |
| 3   | naming policy                                                     | collapse `__`, keep case, `$` handling — one function (`reformat`)                                                                                                                           |     |     |     |     |     |
| 4   | relational column vs JSON key clash (example 03)                  | refuse at check                                                                                                                                                                              |     |     |     |     |     |
| 5   | running structure persistence                                     | `state.json` per stream                                                                                                                                                                      |     |     |     |     |     |
| 6   | `flatten()` parsing                                               | regex → AST rewrite via `json_serialize_sql`; `flatten(` inside a string literal must not fire                                                                                               |     |     |     |     |     |
| 7   | unknown-key policy when a key first appears mid-stream            | evolve (current), ignore, or overflow column `_olake_unflattened` (string at v2 / variant at v3)                                                                                             |     |     |     |     |     |
| 8   | preview                                                           | same pipeline on N sampled rows at `discover`; ship `select * from batch` as the default query                                                                                               |     |     |     |     |     |
| 9   | go-duckdb / DuckDB version                                        | 1.4.1 bundled today; DuckDB never writes variant (Go does, §4.5); ≥ 1.5 only matters if we want DuckDB to emit variant to Arrow and drop the Go parse                                        |     |     |     |     |     |
| 10  | CGO build                                                         | across drivers / arches in the Dockerfile builder stage                                                                                                                                      |     |     |     |     |     |
| 11  | reader requirements for variant tables                            | Spark: Iceberg ≥ 1.10.2 (1.10.0 returns 0 rows on any variant filter); `spark.sql.iceberg.vectorization.enabled=false` until the vectorized reader handles unprojected variant groups (§2.7) |     |     |     |     |     |
| 12  | shredding policy                                                  | §4.6: type stability → column cap → presence floor, sticky per stream                                                                                                                        |     |     |     |     |     |
| 13  | double parse when `flatten()` and `::VARIANT` hit the same column | accept for v1; revisit with DuckDB ≥ 1.5 or arrow-go `variant_get` (§4.5)                                                                                                                    |     |     |     |     |     |

---

## 8. How to run

```sh
cd smt-poc
GOWORK=off go build -tags duckdb_arrow -o flatten-poc .
./flatten-poc -cases                                   # 26 edge cases
./flatten-poc -cases -only "10 "                       # one
./ex/run.sh                                            # the 10 examples → ex/out/NN.{txt,parquet}
./flatten-poc -source orders.jsonl -depth 3 -batch-size 1 -out ex/out/x.parquet \
  -query "select _id.\"\$oid\" as order_id, flatten(customer, 2), status from batch"
```

| Flag          | Meaning                                                                    |
|---------------|----------------------------------------------------------------------------|
| `-source`     | JSONL, one document per line, read as raw text                             |
| `-depth`      | default for `flatten()` without an explicit depth                          |
| `-query`      | the transform; `flatten(path[, depth])`, `FROM batch`                      |
| `-batch-size` | rows per batch (0 = all)                                                   |
| `-preview`    | rows printed per batch                                                     |
| `-out`        | Parquet path with OLake-style names; `_bN` suffix per batch                |
| `-v`          | also print inferred structures, depth cut, generated SQL, shredding schema |
| `-shred`      | shred `::VARIANT` columns (typed_value for stable primitive paths)         |

Variant (§2.5–2.7): `./flatten-poc -source orders.jsonl -out /tmp/v.parquet [-shred] -query "select _id.\"\$oid\" as order_id, status, _olake_raw::VARIANT as doc from batch"`; register with `iceberg/Register.java` (header has the docker command; the file must sit under the table location); read back with `iceberg/duck_read*.sql` (DuckDB in the docker network) or Spark 4.0 + `iceberg-spark-runtime-4.0_2.13:1.10.2` with `spark.sql.iceberg.vectorization.enabled=false`.
