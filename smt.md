# SMT + JSON flattening + Variant — implementation spec

One feature: a per-stream **SQL transform** run by DuckDB over Arrow record batches, sitting between the source driver and the destination writers. Flattening and variant are expressed inside that SQL. This doc says what to build. Evidence, experiments and the reasoning are in `smt-research.md`; runnable code in `smt-poc/`.

## 1. Decisions (settled)

| Topic          | Decision                                                                                                                                                     |
|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Engine         | DuckDB in-process via `go-duckdb` (CGO, build tag `duckdb_arrow`), **transform only**. It never writes Parquet or Iceberg                                    |
| User surface   | One SQL box per stream. `flatten(path, depth)` and `expr::VARIANT` are the only OLake-specific tokens; everything else is plain DuckDB SQL                   |
| Schema         | Never declared by the user. Inferred per batch, merged into a running structure that only grows / widens                                                     |
| Depth          | Applied by editing the inferred structure before `json_transform` — never by enumerating leaves in SQL                                                       |
| Naming         | Flat columns = path joined with `_` through `utils.Reformat`. Collisions fail at `check`                                                                     |
| Variant        | Built in Go with `arrow-go ≥ v18.8.0` (`extensions.VariantType`), written by `pqarrow`, registered by Iceberg-Java. Shredded or not — OLake decides per file |
| Writers        | Unchanged. They receive plain typed columns (plus a variant extension column when requested). Iceberg `struct`/`list` for nested mode                        |
| Iceberg format | v2 for everything except variant (v3). Nothing else in this feature needs v3                                                                                 |

## 2. What the user writes

```sql
-- Mongo orders
select
  _id."$oid"                                   as order_id,
  flatten(customer, 2),                        -- customer_id, customer_email, customer_addr_city, customer_addr_geo (JSON text)
  createdAt."$date"::timestamptz               as created_at,
  list_sum(list_transform(items, lambda x: x.qty * x.price::decimal(18,2))) as order_total,
  status = 'paid'                              as is_paid,
  _olake_raw::VARIANT                          as doc          -- whole document as Iceberg variant
from batch
where status <> 'test'
```

Rules the UI/docs teach:

| Want                                              | Write                                                                                                                        |
|---------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| nested object as typed columns, N levels          | `flatten(path, N)`; `flatten(*)` = the whole record; a relational row's JSON column is just a key (`flatten(attributes, 2)`) |
| leave a subtree as JSON text                      | don't flatten past it (objects past the depth become JSON text columns)                                                      |
| keep nested Iceberg structs instead of flat names | just reference the key: `select customer, status from batch`                                                                 |
| variant                                           | `expr::VARIANT`                                                                                                              |
| rename / cast / drop column / drop rows / mask    | `as`, `::type`, don't select it, `where`, `regexp_replace`/`md5`/`coalesce`                                                  |
| arrays                                            | stay `list<…>`; `list_transform`/`list_sum`; **never `unnest`** (row count must not change)                                  |
| default                                           | `select * from batch`                                                                                                        |

Depth counts key levels below the path. `flatten(user, 2)` → `user.id`, `user.addr.city` columns; `user.addr.geo` (level 3) → JSON text column `user_addr_geo`. Per-path terms override the default; ancestors of a deeper term stay open, their other children obey the default.

## 3. Pipeline (per batch)

```
driver.Read → batch (doc = the record as raw JSON text; a relational row is one document, its JSON column a nested key)
  1. SELECT json_group_structure(doc)                  → batch structure
  2. merge(running, batch)                             → running structure (never shrinks)
  3. parse flatten() / ::VARIANT terms from user SQL   → depth map, variant column set
  4. cut(running, depths)                              → objects past their limit become "JSON"
  5. rewrite SQL:
       flatten(p, N)   → one projection per leaf of the cut structure under p, alias = reformat(path)
       expr::VARIANT   → expr::JSON (VARCHAR) or to_json(expr) (STRUCT/JSON), remember the column
       wrap:  WITH __inner AS (SELECT b.*, json_transform(doc,'<cut>') AS __o FROM batch b),
                   batch   AS (SELECT i.* EXCLUDE (__o, doc), doc AS _olake_raw, __o.* FROM __inner i)
              <user SQL>
  6. execute → Arrow record batch
  7. Go: for each variant column, JSON text → extensions.VariantBuilder (shredded per §5) → replace column
  8. attach _olake_id / _op_type / _cdc_timestamp → FlattenAndCleanData → writers (unchanged)
```

Cache the generated SQL by hash of (running structure, user SQL); re-plan only when it changes.

## 4. Rules that must hold

**Structure merge**

| Situation                                             | Rule                                                                                                                                                                                                                                     |
|-------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| new key                                               | added                                                                                                                                                                                                                                    |
| `"NULL"` (all-null in batch)                          | wildcard: `NULL ∪ X = X`                                                                                                                                                                                                                 |
| numeric conflict                                      | widen with the existing ladder in `destination/iceberg/iceberg.go` (`isValidTransition`, `getCommonAncestorType`) — do not write a second one. Do **not** let int + float collapse to DOUBLE (loses `2^64-1`); promote to decimal/string |
| any other conflict (bool vs number, number vs string) | `JSON` = raw text, JSON-encoded (`"abc"` keeps its quotes)                                                                                                                                                                               |
| batch says `"JSON"` where running has object/array    | ambiguous: DuckDB also says `"JSON"` for `{}` / `[]`. Run `SELECT count(*) FROM batch WHERE json_type(doc, '$.path') NOT IN ('OBJECT'/'ARRAY')`; if 0, keep the known shape                                                              |
| object → scalar on the same key                       | incompatible type change (`struct → string`). Fail loudly at the writer; never silently retype                                                                                                                                           |
| persistence                                           | running structure and shredding schema live in `state.json` per stream; otherwise the column set resets on restart                                                                                                                       |

**Naming / collisions** — `reformat` = lowercase, every non-letter/digit rune → `_`, path joined with `_`. Detect duplicate aliases (`a.b` vs `a_b`, `Id` vs `id`, `a b` vs `a-b`) → error at `check`, not mid-sync.

**System columns** — attached after the query. The user's `SELECT` cannot drop `_olake_id`, `_op_type`, `_cdc_timestamp`. Identifier / partition / sort fields must be flat top-level columns (equality deletes need them).

**Schema source** — the Iceberg schema comes from `DESCRIBE <user SQL>` (pass 1, with `::VARIANT` intact), not from the Arrow batch — otherwise variant columns look like strings to schema evolution. Changing a depth or a cast is a schema change: include the query hash in `types/catalog.go` change detection.

**Parsing `flatten()` / `::VARIANT`** — AST rewrite (`json_serialize_sql` → walk → `json_deserialize_sql`), not regex: `flatten(` inside a string literal must not fire.

## 5. Variant

| Step     | How                                                                                                                                                                                                                                    |
|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| build    | `variant.ParseJSON(text)` → `extensions.NewVariantBuilder(mem, vt).Append(v)`; `vt` = `NewDefaultVariantType()` (unshredded) or `NewShreddedVariantType(arrow.StructOf(...))` (shredded — the builder splits typed vs residual itself) |
| write    | `pqarrow` emits Parquet `VARIANT` logical type; set `PARQUET:field_id` metadata on every top-level field                                                                                                                               |
| register | Iceberg-Java (already 1.10.2 in the writer): table format-version 3, column type `variant`; `ParquetUtil.fileMetrics` works on these files, including bounds from shredded leaves                                                      |
| never    | let DuckDB write a variant Parquet file — its shredded layout is unreadable by Spark                                                                                                                                                   |

**Shredding schema per file** (recompute each batch, persist per stream):

1. candidate path = present in the running structure with a stable primitive type (≥ ~99 % of non-null values one type; numeric widening counts as one)
2. rank by presence, cap at ~200–300 leaves per file
3. presence floor 5–10 % only as tie-breaker
4. sticky: only add paths over time; drop a path only when its type breaks

Mixing shredded and unshredded files in one table is valid; pruning on a sub-path only skips files that shredded it.

## 6. Versions and reader requirements

| Component                       | Requirement                                                                                                                                                                                        |
|---------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `arrow-go`                      | ≥ v18.8.0 (variant type + shredding + pqarrow support). Repo is on v18.2.0                                                                                                                         |
| `go-duckdb`                     | v2.4.3 bundles DuckDB 1.4.1 — fine for the transform; cannot export VARIANT to Arrow (that is why Go builds it)                                                                                    |
| Iceberg-Java (writer)           | 1.10.2 — already in `pom.xml`                                                                                                                                                                      |
| Spark readers of variant tables | Iceberg ≥ 1.10.2 (1.10.0 returns 0 rows for any filter touching a variant column); `spark.sql.iceberg.vectorization.enabled=false` until the vectorized reader handles unprojected variant columns |
| Catalogs                        | Lakekeeper ≥ 0.13 for variant schemas (0.11.1 in `local-test` rejects them)                                                                                                                        |
| Build                           | CGO for all 8 driver binaries × linux/amd64 + arm64; ~40–60 MB per binary                                                                                                                          |

## 7. Acceptance behaviour (from the POC's 26 cases)

| Input                                           | Expected                                                                           |
|-------------------------------------------------|------------------------------------------------------------------------------------|
| key appears in batch 3 only                     | column added in batch 3, null before/after when absent, plan cached when unchanged |
| all-null key, typed later                       | typed, never poisoned to JSON                                                      |
| `{}` / `[]` in some rows                        | known shape kept                                                                   |
| int then string                                 | column kept, type JSON, values keep JSON encoding                                  |
| bool then number                                | JSON, never `true → 1`                                                             |
| arrays / arrays of objects / mixed / nested     | `list<T>` / `list<struct>` / `list<JSON>` / `list<list<>>`; never exploded         |
| `flatten(a, 6)` on an 8-level doc               | 6 typed levels, level-7 subtree as one JSON text column                            |
| root is a scalar/array                          | raw passthrough                                                                    |
| `a.b` vs `a_b`, `Id` vs `id`                    | refused at check                                                                   |
| duplicate key in one object                     | first wins (DuckDB parser)                                                         |
| typed-looking strings `"12.50"`, ISO timestamps | stay strings; casts are the user's job                                             |
| 150-key document                                | works; enforce a max-columns guard                                                 |

## 8. Open decisions (need an owner)

| #   | Decision                                                                                                            |
|-----|---------------------------------------------------------------------------------------------------------------------|
| 1   | struct → scalar retype: fail at writer, or route to an overflow column                                              |
| 2   | naming policy: collapse `__`, keep case, `$` handling                                                               |
| 3   | unknown-key policy: evolve (default), ignore, or overflow column `_olake_unflattened` (string at v2, variant at v3) |
| 4   | preview: run the same pipeline on N sampled rows at `discover`; default query `select * from batch`                 |
| 5   | double parse when `flatten()` and `::VARIANT` hit the same column — accept for v1                                   |

## 9. Reference code

`smt-poc/` (see its `README.md`) — `main.go`: structure inference / merge / depth cut / `flatten()` and `::VARIANT` rewrite, Arrow output, Go-built VARIANT columns (`-shred` for shredding), Parquet via pqarrow; `-cases` (26 edge cases); `ex/run.sh` (10 examples); `iceberg/Register.java` (register a Parquet file into an Iceberg v3 table through the REST catalog) + DuckDB read scripts. Build: `GOWORK=off go build -tags duckdb_arrow`.
