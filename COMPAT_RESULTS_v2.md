# OLake backward-compat sweep v2 — versioned input configs

Run date: 2026-08-06. Candidate: this branch (`compat-port`, 28364ef), built as `olake/source-<driver>:local`. Baselines: every `olakego/source-<driver>` release tag pushed within one year (v0.1.9, ~12 months ago → v0.9.0, ~2 weeks ago).

**What changed since [COMPAT_RESULTS.md](COMPAT_RESULTS.md).** That sweep wrote a *current* `streams.json` to both sides, so any config key introduced after the baseline read as a behaviour change: the older binary ignored the key and synced something the candidate filtered away. This run derives an **input generation** from the baseline's release tag and writes that shape to *both* sides, so the binary is the only thing that varies. A surviving diff now means the candidate changed its mind about an input the baseline also understood — the actual backward-compatibility contract.

Every version below carries how long ago it shipped, rounded, as of **2026-08-06**; exact release dates for all 58 tags are in the appendix.

Status: all runs complete — **postgres 58/58**, **mysql 26/26**, **mongodb 58/58**, and **mysql-utf8 33/33**: the same suite against a pure-ASCII fixture, which lifts mysql's UTF-8 floor and reaches 33 versions the main mysql sweep could never run.

**kafka joined on 2026-08-13 and is still running.** It sweeps `./compat-baselines.sh`'s 8 state-version baselines rather than all 58 tags, and has already produced three kafka-only findings (K1, K2, K3) and one harness bug (H1). **oracle started on the same list on 2026-08-13**: it first hit a second harness artifact (H2), fixed below, and then produced **P3** — a parquet writer finding at v0.3.11. Ages in the kafka section are rounded as of **2026-08-13**.

Logs: `olake-data/compat-results-v2/<driver>/<version>/compat.log`, roll-up per driver in `summary.tsv`. The v1 logs are preserved untouched at `olake-data/compat-results/`. Kafka's runs are terminal-only so far — nothing under `compat-results-v2/kafka/` yet.

## Headline: F1 and F2 were config artifacts, and they are gone

The v1 sweep's two most widespread findings do not reproduce. postgres v0.4.0 → v0.9.0 (~5 months ago → ~2 weeks ago) now **passes on all 25 versions**; in v1, v0.4.0 → v0.5.2 failed F1 and v0.2.6 → v0.3.18 failed F1 + F2.

Neither was a regression. `filter_config` was introduced by the *same commit* that applies filters on CDC ([#756](https://github.com/datazip-inc/olake/pull/756), v0.6.0), so no pre-v0.6.0 pipeline could have had one — and a real one carrying the legacy `filter` string takes `GetFilter`'s `isLegacy` branch, where `FilterRecords` returns records unchanged. `selected_columns` ([#840](https://github.com/datazip-inc/olake/pull/840), v0.4.0) is the same shape of mistake. Feeding both to binaries that predate them measured the harness, not the product.

## Finding legend

| ID | Change | Landed in | When | Baselines that differ from HEAD |
| --- | --- | --- | --- | --- |
| **M1** | MySQL `SET` columns serialise as the string form, not the numeric bitmask (CDC path) | v0.7.2 ([#940](https://github.com/datazip-inc/olake/pull/940)) | ~3 months ago | **everything below v0.7.2** — v0.7.1 → v0.2.5, 36 versions |
| **M2** | MySQL `ENUM` columns (`grade`, `priority`, `status`) serialise differently | v0.3.9 (PR pending) | ~7 months ago | **everything below v0.3.9** — v0.3.8 → v0.2.5, 13 versions |
| **M3** | MySQL `DECIMAL`/`NUMERIC` round-trip through float32, losing precision | v0.3.7 (PR pending) | ~7 months ago | **everything below v0.3.7** — v0.3.6 → v0.2.5, 11 versions |
| **G1** | MongoDB BSON regex serialises with Go field names, not lowercase keys | v0.3.14 ([#657](https://github.com/datazip-inc/olake/pull/657)) | ~7 months ago | **everything below v0.3.14** — v0.3.13 → v0.2.0, 21 versions |
| **F4** | Postgres CDC plugin switched wal2json → pgoutput | v0.2.6 ([#533](https://github.com/datazip-inc/olake/pull/533)) | ~10 months ago | **everything below v0.2.6** — cannot run at all, see floors |
| **P1** | Iceberg **Arrow** writer's files carry an integer width the reader cannot cast | v0.3.6 ([#531](https://github.com/datazip-inc/olake/pull/531)) | ~7 months ago | **v0.3.6 → v0.3.15 only** — resolved by v0.3.17; passes above and below |
| **P2** | `data` (`StringifiedData`) added unconditionally to the schema | v0.3.16 ([#810](https://github.com/datazip-inc/olake/pull/810)) | ~6 months ago | **v0.3.16 only** — fixed by [#826](https://github.com/datazip-inc/olake/pull/826) in v0.3.17 |
| **P3** | Parquet flush panics on a **widened** column: `EvolveSchema` closed the open files instead of recreating them | v0.3.16 ([#602](https://github.com/datazip-inc/olake/pull/602)) | ~6 months ago | **v0.3.15 and below** — parquet group only; only oracle reproduces it |
| **K1** | Kafka JSON numbers reach the destination as **strings** — the flattener had no `json.Number` case | v0.3.16 ([#796](https://github.com/datazip-inc/olake/pull/796)) | ~6 months ago | **v0.3.12 → v0.3.15** — the baseline's own sync FATALs; below v0.3.12 is unrunnable for a different reason, see floors |
| **K2** | Iceberg **Arrow** identity partition transform panics on `json.Number` | v0.6.5 ([#904](https://github.com/datazip-inc/olake/pull/904)) | ~4 months ago | **v0.3.16 → v0.6.4** in code ([#713](https://github.com/datazip-inc/olake/pull/713)); gated from v0.3.17 since P1 covers below. Arrow group only — legacy Iceberg and parquet compare fine |
| **K3** | Kafka + Iceberg + a structured filter writes **nothing at all**, and the sync still exits 0 | v0.6.5 ([#904](https://github.com/datazip-inc/olake/pull/904)) | ~4 months ago | **v0.6.0 → v0.6.4** — iceberg groups only; parquet compares clean on the same run |
| **H1** | *Harness.* Test brokers auto-created a topic 70 ms after the harness deleted it, so teardown never settled | fixed in this branch (compose) | — | any concurrent kafka run — not a product finding |
| **H2** | *Harness.* The legacy `filter` string carried an ISO-8601 timestamp to oracle/db2, which no release can execute | fixed in this branch (input generation) | — | oracle and db2 on every pre-v0.6.0 generation — an artifact, like F1/F2/F5 |

M1, M2, M3, G1 and F4 are **thresholds**: a behaviour changed once and every older baseline disagrees with HEAD, so there is nothing to fix — the current behaviour is the intended one. P1, P2, K1 and K2 are **bounded regressions**: they were introduced and then resolved, so versions on *both* sides pass and only the window between them fails.

M1 and G1 carry over from v1. P1, P2, M2 and M3 are new — v1 never saw them because F1/F2 aborted those comparisons on row count first, and because mysql's UTF-8 floor hid everything below v0.4.0.

**P1 and P2 reproduce on all three drivers at identical version boundaries**, which is the strongest signal in this run: they live in the shared Iceberg writer and schema layer, not in any driver.

**K1, K2 and K3 are kafka's alone, and they are the same story three times.** The driver decodes message numbers with `decoder.UseNumber()` ([#735](https://github.com/datazip-inc/olake/pull/735), v0.3.12), and shared code downstream assumed a concrete Go numeric type: the flattener until v0.3.16 (K1), the arrow writer's identity partition transform until v0.6.5 (K2), and `Compare` — which the iceberg writer's filter runs on raw values — until v0.6.5 (K3). Both later fixes rode along in [#904](https://github.com/datazip-inc/olake/pull/904), the PR that first gave kafka an integration test at all, which is why none of the three windows was noticed while it was shipping. K3 is the one that should worry a user: it loses every row without failing.

## Results by driver

### postgres — 58 versions, complete

| Baselines | Released | Result | Signature |
| --- | --- | --- | --- |
| v0.3.17 → v0.9.0 (33) | ~6 months → ~2 weeks ago | **PASS** | Fully backward-compatible, across three state-version boundaries |
| v0.3.16 (1) | ~6 months ago | FAIL | **P2** — destination schema differs by a `data` column |
| v0.3.6 → v0.3.15 (10) | ~7 → ~6 months ago | FAIL | **P1** — Arrow reader `ClassCastException`; legacy Iceberg variants pass |
| v0.2.6 → v0.3.5 (15) | ~10 → ~8 months ago | **PASS** | Fully backward-compatible |
| v0.1.9 → v0.2.5 (9) | ~12 → ~10 months ago | not comparable | **F4** — wal2json-era driver refuses the harness's pgoutput slot |

The shape matters: v0.2.6 → v0.3.5 passes and v0.3.17 → v0.9.0 passes, with a **failing hole** between them. A floor would fail everything below a point; this does not, so P1 is a genuine change that landed at v0.3.6 and stopped at v0.3.17.

v1 reported v0.2.6 → v0.3.5 as "F1 + F2 failures". With era-correct inputs every one of those 15 passes, so postgres is demonstrably backward-compatible as far back as v0.2.6 (~10 months).

### mysql — 26 versions, complete

| Baselines | Released | Result | Signature |
| --- | --- | --- | --- |
| v0.7.2 → v0.9.0 (11) | ~3 months → ~2 weeks ago | **PASS** | Fully backward-compatible |
| v0.4.0 → v0.7.1 (14) | ~5 → ~3 months ago | FAIL | **M1** — `permissions` is the only differing column in all 14 |
| v0.3.18 (1) | ~5 months ago | not comparable | UTF-8 floor — lifted by the mysql-utf8 run below |

**mysql now exercises its filter for the first time.** v1's `TestMySQLCompat` set `FilterConfig = ""` to stop the row-count mismatch from masking the UNSIGNED gate — which meant mysql had *zero* filter coverage across all 25 tested versions. With generations that workaround is unnecessary, so it was removed, and the filter is live on every mysql run here. It produced no new findings, which is itself the result.

**v0.4.0 (~5 months ago) remains the run this suite exists for**: the newest release still on state version 3, so it exercises the UNSIGNED widening gate ([#846](https://github.com/datazip-inc/olake/pull/846)). It logs `pipeline pinned at state version 3 against a build at 6` and differs *only* by `permissions`.

### mongodb — 58 versions, complete

| Baselines | Released | Result | Signature |
| --- | --- | --- | --- |
| v0.3.17 → v0.9.0 (35) | ~6 months → ~2 weeks ago | **PASS** | Fully backward-compatible |
| v0.3.16 (1) | ~6 months ago | FAIL | **P2** — same `data` column as postgres |
| v0.3.14 → v0.3.15 (2) | ~7 → ~6 months ago | FAIL | **P1** — same Arrow `ClassCastException` as postgres |
| v0.3.9 → v0.3.13 (5) | ~7 months ago | FAIL | **P1 + G1** |
| v0.2.0 → v0.3.8 (19) | ~11 → ~7 months ago | FAIL | **G1** — `id_regex` only |
| v0.1.9 → v0.1.11 (3) | ~12 → ~11 months ago | not comparable | `error occurred while reading records` |

**G1 survived the generation fix, and that is the point.** `id_regex` differs from v0.3.13 all the way down — an ungated value change, so unlike F1 and F2 it *should* survive, and it does. It is the control that shows the input-generation work suppressed artifacts rather than suppressing everything.

### kafka — 8 state-version baselines, in progress (started 2026-08-13)

Kafka joined after the other four runs and sweeps a different list: `./compat-baselines.sh` emits the newest release on each state version, so the table is 8 rows rather than 58. The driver itself first ships in **v0.3.0** (2025-11-11, ~9 months ago), so that is where its map starts.

| Baseline | Released | Result | Signature |
| --- | --- | --- | --- |
| v0.9.1 | ~2 days ago | not run yet | |
| v0.9.0 | ~3 weeks ago | not run yet | |
| v0.6.5 | ~4 months ago | not run yet | first baseline where all three writer groups can run |
| v0.6.1 | ~5 months ago | FAIL | **K3** — both iceberg runs wrote zero rows while reporting success; parquet compared identical (1 row, 8 columns). Arrow was already gated off by **K2** |
| v0.4.0 | ~6 months ago | FAIL | **K2** — arrow flush panics on `json.Number`; legacy ran first and its result was not kept, parquet was skipped once a run had failed |
| v0.3.16 | ~6 months ago | inconclusive | legacy syncs passed on both sides, then the run died in teardown on **H1**; parquet skipped by **P2**, arrow by **P1** |
| v0.3.15 | ~7 months ago | FAIL | **K1** — `expected type: long, detected type: string`, both sides; now below the floor, so it skips |
| v0.3.11 | ~7 months ago | not comparable | pre-[#735](https://github.com/datazip-inc/olake/pull/735) number typing, see floors |

Read as a map from the oldest kafka release forward, that is:

| Range | What can be compared | Blocked by |
| --- | --- | --- |
| v0.3.0 → v0.3.11 (~9 → ~7 months ago) | nothing | pre-#735: the driver plain `json.Unmarshal`s message values, so every JSON number types as double against today's integer catalog and the baseline's own sync FATALs |
| v0.3.12 → v0.3.15 (~7 months ago) | nothing | **K1** |
| v0.3.16 (~6 months ago) | legacy Iceberg only | **P2** (parquet), **P1** (arrow) |
| v0.3.17 → v0.5.2 (~6 → ~5 months ago) | legacy Iceberg + parquet | **K2** (arrow) |
| v0.6.0 → v0.6.4 (~4 months ago) | parquet only | **K2** (arrow) + **K3** (both iceberg writers) |
| v0.6.5 → v0.9.1 (~4 months → ~2 days ago) | all three writer groups | — |

Every failure kafka has produced so far is **baseline-side**, and none is a regression in the candidate. K1 and K2 kill the baseline's own first sync — which both the reference and the upgrade run execute — before the candidate writes anything. K3 is quieter: every sync succeeds and the baseline simply writes an empty table, so the failure surfaces one step later, in the comparison.

## Findings in detail

### M1 — MySQL `SET` columns: numeric bitmask → string form (v0.7.2, ~3 months ago, PR [#940](https://github.com/datazip-inc/olake/pull/940))

Unchanged from v1, and now confirmed with the filter enabled rather than cleared. `fix(mysql): fix CDC charset corruption for utf16/ucs2/latin1 columns` ([#940](https://github.com/datazip-inc/olake/pull/940), ea5ca92) also altered how `SET` columns serialise on the binlog CDC path.

A MySQL `SET` is stored as a bitmask — `read = 1`, `write = 2`, `execute = 4` — and all three fixture rows check out:

| Row | Source value | Reference | Upgrade (HEAD) |
| --- | --- | --- | --- |
| `id = 6`, op `c` | `'read,write'` | `3` (1+2) | `read,write` |
| `id = 999`, op `c` | `'execute'` | `4` | `execute` |
| `id = 1`, op `d` | `'read,write,execute'` | `7` (1+2+4) | `read,write,execute` |

```text
column `permissions` differs in 3 row(s)
reference: [6 c … text_val tinytext_val varchar_val 3          123.45 …]
upgrade:   [6 c … text_val tinytext_val varchar_val read,write 123.45 …]
```

Identical in all 14 failing versions — `permissions` is the *only* differing column in every one, so every other column, including all the unsigned ones the state gate covers, matches exactly.

### P1 — Arrow reader cannot read a table written by two integer widths ([#531](https://github.com/datazip-inc/olake/pull/531), first released v0.3.6, ~7 months ago)

The new finding, and the more serious one. In the upgrade run the baseline writes the initial load and the candidate writes every `--state` sync **into the same Iceberg table**. For baselines v0.3.6 → v0.3.15 those two binaries disagree about an integer column's width, so the table ends up holding files of both widths, and Spark's vectorized Arrow reader fails outright:

```text
java.lang.ClassCastException: class org.apache.iceberg.shaded.org.apache.arrow.vector.BigIntVector
  cannot be cast to class org.apache.iceberg.shaded.org.apache.arrow.vector.IntVector
    at org.apache.iceberg.arrow.vectorized.VectorizedArrowReader.allocateVectorBasedOnOriginalType
    at org.apache.iceberg.arrow.vectorized.VectorizedArrowReader.allocateFieldVector
```

The signature is precise about scope:

- **Arrow variants only** — `compare/ice_arrow_cdc` and `compare/ice_arrow_inc` fail; `ice_legacy_cdc` and `ice_legacy_inc` compare cleanly on the same data. The legacy writer path tolerates the mixed widths that the vectorized reader will not.
- **Both directions** — it fails on CDC and incremental alike, so it is not tied to one sync mode.
- Reported by the harness as `failed to collect the row diff` at `compatibility.go:665`, because the exception surfaces inside `rowsOnlyIn`'s `df.Collect`.

The user-facing consequence is worse than a value diff: an existing Iceberg table becomes **unreadable by the Arrow reader** after upgrading, rather than merely holding different values. Reading it needs the legacy reader or a rewrite.

**The Arrow writer landed here.** `feat: arrow writer` ([#531](https://github.com/datazip-inc/olake/pull/531), 9b30d7a) adds `destination/iceberg/arrow-writer/writer.go`, `OlakeArrowIngester.java` and `ArrowIngestServiceGrpc.java`, and splits the existing path out into `legacy-writer/writer.go`; it first ships in v0.3.6. That is exactly the boundary the sweep found, and it explains why only the arrow variants fail while the legacy ones pass on the same data.

One detail sharpens the diagnosis: **the schema comparison passes** for this whole band. Both tables declare the same Iceberg schema — so the mismatch is not in the declared types but between the declared type and the *physical* width the Arrow writer put in the files. A reader that trusts the schema then tries to cast a `BigIntVector` into an `IntVector` and fails.

Unlike the threshold findings, this one **stops**: v0.3.5 and below pass, v0.3.17 and above pass, and only the window between fails. That makes it a bounded regression rather than a behaviour change users still live with. The range `v0.3.16..v0.3.17` that resolves it is only seven commits, and none is obviously responsible:

```text
6fe92e6  fix: parse timezone offset correctly (#827)
173c9b5  fix: primary key order mismatch during chunk processing (MSSQL, DB2) (#829)
39f469d  Merge pull request #826 (fix/parquet-extra-column)   <- the P2 fix
7457596  feat: add support for custom endpoint in glue catalog (#824)
1a2dfe7  fix: kafka avro schema normalized as per avro conventions (#825)
cd8549b  docs: update README with benchmarks (#821)
fde8bed  Merge pull request #791 (remove lakekeeper-latest)
```

`39f469d` is the only one touching `types/type_schema.go`, but its diff is confined to renaming `onlyOlakeColumns` to `defaultColumns` and moving `StringifiedData` — nothing about integer width. The arrow-writer sources are untouched across the range. So the resolution is real but unexplained by the diff, which is worth a second look: it may be incidental rather than deliberate, and an incidental fix can regress. The affected column is likewise not isolated; `col_int` and `col_integer` are the candidates, since postgres `INT` is the type whose Iceberg mapping moves between `int` and `bigint`.

### P2 — `data` column added unconditionally to the schema ([#810](https://github.com/datazip-inc/olake/pull/810), first released v0.3.16, ~6 months ago; fixed by [#826](https://github.com/datazip-inc/olake/pull/826))

v0.3.16 alone fails differently — no `ClassCastException`, a schema mismatch instead:

```text
destination schema differs between the reference and upgrade runs.
- (string) (len=4) "data": (string) (len=6) "string",
```

The `-` is testify's marker for "expected only", so the **reference** side has a `data` column the upgrade side does not. `constants.StringifiedData = "data"` (`constants/constants.go:22`), and the git history explains the one-release window exactly:

| Release | `TypeSchema.ToParquet` default columns |
| --- | --- |
| v0.3.15 | `groupNode := parquet.Group{}` — no defaults, no `data` |
| **v0.3.16** ([#810](https://github.com/datazip-inc/olake/pull/810)) | `data` added to the group **unconditionally**, "for backward compatibility for olake columns" |
| v0.3.17 ([#826](https://github.com/datazip-inc/olake/pull/826)) | moved behind `if defaultColumns { groupNode[constants.StringifiedData] = … }` |

So v0.3.16 is the only release that emits `data` on every stream, which is why the band is exactly one version wide between two passing neighbours.

### P3 — parquet flush panics on a widened column ([#602](https://github.com/datazip-inc/olake/pull/602), fixed v0.3.16, ~6 months ago)

oracle v0.3.11, on the incremental `evolve-schema` step, which widens the column and adds another:

```sql
ALTER TABLE … MODIFY (col_int NUMBER(19,0));   -- Int32 -> Int64
ALTER TABLE … ADD (includedColumn NUMBER(9,0));
```

```text
schema evolution detected
created new partition file[…2026-08-13_10-50-50_….parquet]
FATAL … failed to flush data while closing:
       panic recovered in flush: cannot create parquet value of type INT32 from go value of type int64
```

The run separates the sides cleanly, because the upgrade run hands every stateful sync to the candidate:

| Run | Who runs `Incremental - update` | Result |
| --- | --- | --- |
| `reference-pq` | baseline v0.3.11 | **FAIL** — the panic above |
| `upgrade-pq` | candidate | **PASS** |
| both `ice_legacy` | baseline / candidate | **PASS** — same DDL, iceberg is unaffected |

`EvolveSchema` is the difference, and the boundary is exact:

| Release | `EvolveSchema` body |
| --- | --- |
| ≤ v0.3.15 | `return p.schema.Clone(), p.closePqFiles()` — closes the open files and leaves the next write to make new ones |
| **v0.3.16** ([#602](https://github.com/datazip-inc/olake/pull/602), f7e34a75) | loops `p.partitionedFiles` and calls `createNewPartitionFile(path)` for each |

Closing alone is not enough: the widened value still meets a file whose parquet schema says `INT32`, and parquet-go panics at flush. Every JDBC suite widens an int to a bigint in its `evolve-schema` (postgres `ALTER COLUMN col_int TYPE BIGINT`, mysql `MODIFY COLUMN id_int BIGINT`, mssql `ALTER COLUMN col_int BIGINT`), yet `cannot create parquet value` appears in **none** of the stored postgres, mysql, mysql-utf8 or mongodb logs across all 58 baselines — so only oracle reproduces it, and the gate is scoped to oracle:

```go
// tests/testutils/compatibility.go, the pq group
if driver == string(constants.Oracle) {
        pq.minBaseline = "v0.3.16"
}
```

With P2 already skipping v0.3.16 for parquet, oracle's parquet comparison effectively starts at v0.3.17. The prediction the rest of the sweep should confirm: v0.3.15 fails identically, v0.3.16 and up do not.

### K1 — Kafka numbers reach the destination as strings ([#735](https://github.com/datazip-inc/olake/pull/735) v0.3.12 → [#796](https://github.com/datazip-inc/olake/pull/796) v0.3.16, ~7 → ~6 months ago)

Observed on v0.3.15. Both sides FATAL on the first sync, identically:

```text
FATAL error occurred while reading records: ... failed to detect schema:
      failed to validate schema for field[int_value] (detected two different types in batch),
      expected type: long, detected type: string
```

The two halves of the cause shipped four releases apart:

| Release | Change | Effect |
| --- | --- | --- |
| **v0.3.12** ([#735](https://github.com/datazip-inc/olake/pull/735), 79fc439) | kafka reads message values through `decoder.UseNumber()` | every JSON number is now a `json.Number` |
| v0.3.12 – v0.3.15 | `FlattenerImpl.flatten`'s type switch lists no `json.Number` | the value falls to `default:` → `json.Marshal` → stored as a **Go string** |
| **v0.3.16** ([#796](https://github.com/datazip-inc/olake/pull/796), ac6fdde) | `json.Number` added to that type switch | fixed |

Discover is unaffected — `TypeFromValue(json.Number)` returned `Int64` from v0.3.12 on — so the Iceberg table is created with `int_value: optional long` and then the flush presents a string for the same field. In that window every numeric field of every Kafka JSON message is stringified before it reaches any writer, so parquet is no better off than Iceberg; there is nothing at those versions worth comparing. Hence a driver floor rather than a per-writer gate:

```go
// tests/kafka/kafka_test.go
cfg.CompatMinBaseline = "v0.3.16"
```

Below v0.3.12 the run is unrunnable for the opposite reason — pre-#735 the driver `json.Unmarshal`s plainly, so every number types as double, and `long` → `double` is not a valid promotion against today's catalog.

### K2 — Arrow identity partition transform panics on `json.Number` ([#904](https://github.com/datazip-inc/olake/pull/904), fixed v0.6.5, ~4 months ago)

Observed on v0.4.0, again on both sides of the first sync, and again only in the arrow writer group:

```text
FATAL error occurred while reading records: ... failed to flush data while closing:
      panic recovered in flush: interface conversion: interface {} is json.Number, not int64
```

The kafka suite partitions on `/{int_value,identity}` (`Adding partition field: int_value with transform: identity` in the sync log), so every flush routes through `identityTransform`, which asserted the Go type outright:

```go
// v0.4.0 destination/iceberg/arrow-writer/transforms.go:72
case "long":
        v := val.(int64)     // json.Number from the kafka driver -> panic
```

That form entered in `fix(arrow): iceberg writer improvements` ([#713](https://github.com/datazip-inc/olake/pull/713), e0f0850, v0.3.16) — v0.3.6's `identityTransform` had only `fmt.Sprintf("%v", val)` in its default branch and could not panic. `test: kafka integration test` ([#904](https://github.com/datazip-inc/olake/pull/904), 43cb8dc) rewrote every assertion in the file to `typeutils.ReformatInt64`/`ReformatInt32`/`ReformatBool`/…, which accept `json.Number`, and first shipped in **v0.6.5**. So 14 releases carried it — v0.3.16 → v0.6.4 — and the PR that fixed it is the one that gave kafka an integration test at all. The compat gate starts a release later, at v0.3.17, only because P1 already blocks the arrow group below that.

Other drivers hand the transform real ints, which is why postgres, mysql and mongodb all pass v0.3.17 → v0.9.0. So this is a **writer-group** boundary for one driver, not a floor — `ice_legacy` and `pq` still run across the window:

```go
// tests/testutils/compatibility.go, inside the arrow group
if driver == string(constants.Kafka) {
        g.minBaseline = "v0.6.5"
}
```

The cost is real and worth stating: kafka's arrow coverage now starts at v0.6.5 (~4 months of baselines), because older binaries genuinely cannot write a partitioned arrow table from Kafka.

### K3 — Kafka + Iceberg + a filter silently writes nothing ([#756](https://github.com/datazip-inc/olake/pull/756) v0.6.0 → [#904](https://github.com/datazip-inc/olake/pull/904) v0.6.5, ~4 months ago)

The most serious kafka finding, and the only one that does not announce itself. On v0.6.1 **every sync succeeded** — `Sync successful for kafka driver` on all four runs, both writer groups — and the parquet comparison passed on 1 row over 8 columns. The iceberg comparison then failed on the harness's own vacuity guard:

```text
Error: "0" is not greater than "0"
Messages: the reference run produced no rows in
          olake_iceberg.kafka_topics_ref_ice_legacy_cdc.kafka_json_test_table_olake_ref_ice_legacy
```

The tables outlive the run, so the state is checkable directly. The reference table has **no snapshots at all** — nothing was ever committed — while the upgrade table has exactly one, written by the candidate:

```text
ref  count(*) = 0    snapshots: (none)
upg  count(*) = 1    snapshots: 2026-08-13 09:46:16  append  added=1  total=1
```

That single append is the candidate's stateful sync; the baseline's own sync on the same table committed nothing either. So the baseline wrote zero rows to Iceberg while writing parquet correctly, in the same run, from the same messages.

The cause is the third instance of the `json.Number` story, and the two destinations diverge on one line:

| | v0.6.1 path | Result |
| --- | --- | --- |
| parquet | `typeutils.ReformatRecord(p.schema, …)` **then** `FilterRecords` | values are `float64`/`int64` by then; `float_value < 100` compares numerically |
| iceberg | flatten, detect schema, `FilterRecords` on the **raw** values | values are still `json.Number` |

`FilterRecords` → `evaluate` → `Compare`, and v0.6.1's `Compare` has no `json.Number` case: a named string type matches none of its typed branches and falls to `strings.Compare(fmt.Sprintf("%v", a), …)`. So `float_value < 100` becomes `strings.Compare("99.99", "100")`, which is **1** — `"9" > "1"` — and the condition is false. Under the suite's `And`, every record fails and `FilterRecords` returns an empty slice. The writer has nothing to flush (`no writer to complete`, `No files to commit for thread` on the Java side), the sync exits 0, and the table stays empty.

Both boundaries are verifiable in the tree:

| Release | Change | Effect |
| --- | --- | --- |
| **v0.6.0** ([#756](https://github.com/datazip-inc/olake/pull/756)) | `options.ApplyFilter` block added to the iceberg writer's `FlattenAndCleanData` (absent at v0.5.2) | destination-side filtering starts running on raw values |
| **v0.6.5** ([#904](https://github.com/datazip-inc/olake/pull/904)) | `json.Number` case added to `utils/typeutils/compare.go` | comparison becomes numeric; fixed |

It only bites when a **structured** `filter_config` is present, which is exactly what the `filter-config` input generation writes for baselines at or above v0.6.0. Below that the harness writes the legacy `filter` string, `GetFilter` takes its `isLegacy` branch, and `FilterRecords` returns records untouched — so the window is precisely v0.6.0 → v0.6.4, and it is kafka's alone because no other driver hands the filter a `json.Number`.

Gated per writer group, like P1 and P2, so parquet keeps its coverage of the window:

```go
// tests/testutils/compatibility.go, both iceberg groups
if driver == string(constants.Kafka) {
        g.skipBaselines = append(g.skipBaselines, "v0.6.0", "v0.6.1", "v0.6.2", "v0.6.3", "v0.6.4")
}
```

**This one deserves a product issue rather than only a harness gate.** Five shipped releases, ~4 months old, in which a Kafka → Iceberg pipeline with any structured filter loses every row and reports success. The fix exists (v0.6.5) but shipped inside a PR titled `test: kafka integration test`, so nothing in the release notes tells an affected user to upgrade — or that the rows they think they synced were never written.

### H1 — the harness: deleted topics came back 70 ms later

Not a product finding, but it failed a run that had otherwise passed, so it is recorded here. On v0.3.16 both sides' syncs succeeded and the run then died in teardown:

```text
Error: TOPIC_ALREADY_EXISTS: Topic with this name already exists.
Messages: deletion of topic(s) [kafka_json_test_table_olake_upg_ice_legacy] did not complete
[timing] kafka query "drop": 1m42.318s
```

The broker log shows the delete working and being undone:

```text
09:17:27,372  Deletion of topic ..._upg_ice_legacy successfully completed
09:17:27,442  New topics: [Set(..._upg_ice_legacy)]
```

The survivor had `PartitionCount: 1` — `num.partitions`, i.e. auto-created, not the harness's 5-partition create. Kafka discover enumerates the **whole broker** and produces a schema for every topic, so the sibling run's still-live driver kept touching the topic the other run had just deleted, and `confluentinc/cp-kafka` defaults `auto.create.topics.enable=true`. `ensureTopicDeletion` probes with `ValidateCreateTopics`, so it correctly saw `TOPIC_ALREADY_EXISTS` for its whole ~102 s ladder. Two controls in the same logs: at 09:18:05 the sibling topic was deleted and stayed deleted once no driver was running, and in the v0.3.15 run both deletions settled instantly because both syncs had already FATAL'd.

Fixed by disabling auto-creation on both test brokers (`drivers/kafka/docker-compose.yml`); every topic the suites use is created explicitly, Schema Registry creates `_schemas` through the admin client, and `__consumer_offsets` is coordinator-managed. Deletions settled in ~220 ms on the next run. One residual: with auto-creation off, a sibling discover that enumerates a topic *during* another run's drop now gets `UNKNOWN_TOPIC_OR_PARTITION` instead of silently resurrecting it — if that surfaces, the fix is to drop topics at group teardown rather than per run.

### H2 — the harness: a legacy filter no Oracle release could run

oracle's first baseline (v0.3.11) failed on both sides before any comparison, in the backfill query itself:

```text
DEBUG Starting backfill ... with filter: (…) AND ("COL_DOUBLE_PRECISION" < 239834.89
      and "COL_TIMESTAMP" >= '2022-07-01T15:30:00.000+00:00')
FATAL error occurred while reading records: … ORA-01843: An invalid month was specified.
       error occur at position: 261
```

Oracle will not implicitly convert an ISO-8601 literal — its `NLS_TIMESTAMP_FORMAT` expects `DD-MON-RR` — so the pushed-down filter dies on the timestamp condition. The important part is where the conversion lives in the product:

| Path | Value rendering |
| --- | --- |
| structured `filter_config` (v0.6.0+) | `TO_TIMESTAMP('…','YYYY-MM-DD HH24:MI:SS.FF')` for oracle, `TIMESTAMP('…')` for db2 |
| legacy `filter` string | the literal, bare — `jdbc.SQLFilter`'s `isLegacy` branch returns before the ISO handling |

That legacy branch is unchanged at HEAD, and v0.3.11 contains no `TO_TIMESTAMP` at all, so **the candidate would fail on this input exactly as the baseline does**. No Oracle pipeline could ever have run a legacy filter with an ISO timestamp; feeding one measures the harness, not the product — the same family as F1, F2 and F5.

Fixed in the input generation: for the drivers whose engine needs the explicit conversion — oracle and db2, precisely the two the structured path special-cases — the legacy generations drop timestamp conditions when rendering the `filter` string, and each run logs that it did. The oracle and db2 suites keep the numeric half of their filter (`COL_DOUBLE_PRECISION < 239834.89`); postgres, mysql and mssql are untouched, so their recorded results stay comparable. db2 has the identical condition in its suite, so the same fix pre-empts the same failure there.

### mysql below v0.4.0 — lifting the UTF-8 floor

The main mysql sweep stops at v0.4.0 because `ExtraExcludedColumns` hides the non-UTF-8 columns via the *catalog*, which a pre-column-selection binary ignores; both sides then sync bytes the Iceberg gRPC marshal rejects, and the driver retries on a doubling backoff that looks like a hang. Running the same suite against an ASCII-only fixture removes the cause at source instead:

| Column | Why it blocked the sweep | Change |
| --- | --- | --- |
| `grade` | `ENUM('naïve','café','résumé')` latin1 — `ï` is byte `0xEF`, invalid UTF-8 alone | ASCII members `naive`/`cafe`/`resume` |
| `name_ucs2` | the **charset** emits UTF-16BE, so even ASCII content is invalid UTF-8 | → `utf8mb4` |
| `name_utf16le` | same | → `utf8mb4` |
| `name_latin1` | nothing — ASCII in latin1 is byte-identical to UTF-8 | unchanged |

Changing values alone would not have worked for the two UTF-16 columns; the column charset had to change. With that done the floor lifts and 33 previously unreachable versions run. Two caveats on reading these numbers: the exclusion list is inert below v0.4.0 (the `legacy-filter` generation deletes `selected_columns` entirely, which is correct modelling but also nullifies `ExtraExcludedColumns`), so `tags` appears alongside `permissions`; and the fixture is not identical to the main sweep's, so these results are comparable to each other but not directly to the v0.4.0+ band.

What it found, by band:

| Baselines | Columns that differ | Reading |
| --- | --- | --- |
| v0.3.17 → v0.3.18 | `permissions`, `tags` | **M1 only** — the SET finding extends two versions below the old floor |
| v0.3.16 | `permissions`, `tags` + schema | **P2** — mysql confirms it too |
| v0.3.9 → v0.3.15 | `permissions`, `tags` + `ClassCastException` | **P1** — mysql confirms it too |
| v0.3.7 → v0.3.8 | + `grade`, `priority`, `status` | **M2** — ENUM |
| v0.1.9 → v0.3.6 | + `amount_decimal_9_2`, `price_decimal`, `price_numeric` | **M3** — DECIMAL/NUMERIC |

### M2 — MySQL `ENUM` columns serialise differently (landed v0.3.9, ~7 months ago)

`grade`, `priority` and `status` differ on every baseline at v0.3.8 and below, and match from v0.3.9 up. All three are `ENUM`, and no non-ENUM column differs at that boundary, so the change is specific to how ENUM values are rendered rather than to charset or width.

### M3 — MySQL `DECIMAL`/`NUMERIC` lose precision (landed v0.3.7, ~7 months ago)

`amount_decimal_9_2`, `price_decimal` and `price_numeric` differ at v0.3.6 and below. The values are the tell:

```text
reference (v0.3.6): 50.119998931884766      <- float32 round-trip of 50.12
reference (v0.3.6): 5.3301975e+06           <- float32 round-trip of 5330197.27
```

Those are exactly what a `DECIMAL` looks like after a trip through 32-bit float. The older binaries widen decimals to float32 and lose precision; from v0.3.7 the value survives intact. This is the most user-visible of the new findings: silent precision loss in monetary columns. It is a threshold, so v0.3.7 onwards is correct and only pipelines still on ≤ v0.3.6 carry the damaged values — but those values are already written, and upgrading does not repair them.

### F4 — postgres CDC plugin switched wal2json → pgoutput (v0.2.6, ~10 months ago, PR [#533](https://github.com/datazip-inc/olake/pull/533))

Unchanged, and now the honest floor for postgres ≤ v0.2.5 (~10 months ago). Dropping `--destination-database-prefix` for pre-v0.2.0 baselines removed v1's F5 artifact and revealed F4 underneath it:

```text
FATAL failed to check existence of replication slot postgres_test_table_olake_upg:
      plugin not supported[pgoutput]: driver only supports wal2json
```

In v1 those three versions (v0.1.9 → v0.1.11) recorded as "F5 — flag does not exist yet; sync exits 1 silently". That was the harness handing them a flag introduced in v0.2.0. With the flag withheld they run and report their real limitation. **F5 is not a finding in this sweep** — it was an artifact, like F1 and F2.

## Harness floors — versions that cannot be compared

- **mysql ≤ v0.3.18 (~5 months ago).** Unchanged from v1 in effect, but the mechanism moved: the `legacy-filter` generation removes `selected_columns` entirely, which is correct modelling for a pre-v0.4.0 binary and also removes the mechanism `ExtraExcludedColumns` uses to hide the non-UTF-8 columns. Both sides then sync them, the Iceberg gRPC marshal fails on invalid UTF-8, and the driver retries on a doubling backoff. v0.3.18 was run once to record it; the 32 below were not.
- **postgres ≤ v0.2.5 (~10 months ago)** — F4: the wal2json-era driver cannot attach to the harness's pgoutput slot, and `postgres:15` ships no wal2json extension. Liftable by seeding a wal2json-capable image, the way the mysql-utf8 run lifted mysql's floor.
- **kafka ≤ v0.3.15 (~7 months ago)** — K1 below v0.3.16 and pre-#735 number typing below v0.3.12; the driver has shipped since v0.3.0, so this floor costs 16 releases (v0.3.0 → v0.3.15). Not liftable by fixture or config: the baseline mis-serialises every number before any writer sees it. Recorded as `CompatMinBaseline` in `TestKafkaCompat`, and skipped with that reason per baseline.
- **kafka arrow ≤ v0.6.4 (~4 months ago)** — K2, gated per writer group rather than per driver, so `ice_legacy` and `pq` keep the full range.
- **oracle parquet ≤ v0.3.15 (~7 months ago)** — P3: the baseline panics on the incremental evolve step, so the run never reaches a comparison. Iceberg keeps the full range.
- **kafka iceberg on v0.6.0 → v0.6.4 (~4 months ago)** — K3: the baseline writes an empty table and exits 0, so the comparison would be vacuous. Skipped for both iceberg groups; parquet still runs and passes there.

## Reproducing

```sh
# one version
COMPATIBILITY_BASELINE=v0.3.10 make test.compatibility.postgres

# pin the candidate so the harness does not rebuild it per run
OLAKE_DRIVER_IMAGE=olake/source-postgres:local COMPATIBILITY_BASELINE=v0.3.10 make test.compatibility.postgres

# force an input generation instead of deriving it from the baseline tag
OLAKE_COMPATIBILITY_INPUT_GENERATION=current        # today's shape: filter_config + selected_columns
OLAKE_COMPATIBILITY_INPUT_GENERATION=legacy-filter  # pre-v0.4.0 shape

# harness knobs added for this sweep
OLAKE_TEST_CONTAINER_MEMORY=1536m   # 0/off disables the cap
OLAKE_TEST_WRITER_HEAP=-Xmx512m     # off leaves the JVM's own sizing alone
OLAKE_COMPATIBILITY_EXCLUDE_COLUMNS=a,b    # columns a baseline cannot sync (appended for mysql)
```

## Suggested follow-ups

1. **Attribute M2 and M3.** Both are new, both are mysql, and M3 — decimals round-tripping through float32 — is the most user-visible thing either sweep has found. Boundaries are v0.3.9 (ENUM) and v0.3.7 (DECIMAL); neither PR is identified yet.
2. **Attribute and triage P1.** An existing Iceberg table becoming unreadable by the Arrow reader after upgrade is the most serious thing either sweep has found. Isolate the column, bisect v0.3.5 → v0.3.6, and decide whether the legacy reader tolerating it is sufficient mitigation.
3. **P2 is fully attributed** — one version wide, and the `data` column name points at the destination-check probe rather than the stream.
4. **The ASCII fixture is strictly more informative** (33 versions) and decide whether the ASCII fixture should replace the exclusion list in the committed suite — it is strictly more informative.
5. **Reuse one writer JVM per sync** instead of one per phase. It is the shared root of the OOM cascade and the p50 = 9.1 s sync time, and it would roughly halve the sweep's wall clock.
6. **Finish the kafka sweep** — v0.3.16 needs a re-run now that H1 is fixed, and v0.6.1 → v0.9.1 have not run at all. Capture the logs under `compat-results-v2/kafka/` the way the other four runs did.
7. **Decide whether K2 deserves a non-partitioned arrow variant.** The floor exists because the suite always partitions on `int_value`; an unpartitioned arrow run would say whether the rest of the arrow path was compatible across v0.3.17 → v0.6.4, or whether the floor is the whole truth.
8. **File K3 as a product issue.** It is the only silent data-loss finding either sweep has produced: v0.6.0 → v0.6.4, Kafka → Iceberg with a structured filter, zero rows written and exit 0. Worth checking whether any other destination filters raw values the same way, and whether a "filtered everything" batch should warn.
9. **All three kafka findings predate kafka's own integration test** ([#904](https://github.com/datazip-inc/olake/pull/904)). Worth asking which other drivers or destinations carry the same shape of blind spot — a decode choice in one component meeting a type assertion in another.

## Appendix — release timeline

All four runs. Ages are rounded as of 2026-08-06; `n/c` = not comparable; `—` marks versions a run did not cover — mysql stops at its UTF-8 floor, which the mysql-utf8 column covers instead. Kafka is deliberately absent: it samples 8 state-version baselines rather than every tag, so its timeline lives in its own section above.

| Version | Released | Age | postgres | mysql | mysql-utf8 | mongodb |
| --- | --- | --- | --- | --- | --- | --- |
| v0.9.0 | 2026-07-24 | ~2 weeks | **PASS** | **PASS** | — | **PASS** |
| v0.8.2 | 2026-07-10 | ~4 weeks | **PASS** | **PASS** | — | **PASS** |
| v0.8.1 | 2026-07-08 | ~4 weeks | **PASS** | **PASS** | — | **PASS** |
| v0.8.0 | 2026-07-04 | ~5 weeks | **PASS** | **PASS** | — | **PASS** |
| v0.7.8 | 2026-06-29 | ~5 weeks | **PASS** | **PASS** | — | **PASS** |
| v0.7.7 | 2026-06-19 | ~7 weeks | **PASS** | **PASS** | — | **PASS** |
| v0.7.6 | 2026-06-13 | ~8 weeks | **PASS** | **PASS** | — | **PASS** |
| v0.7.5 | 2026-06-05 | ~2 months | **PASS** | **PASS** | — | **PASS** |
| v0.7.4 | 2026-05-29 | ~2 months | **PASS** | **PASS** | — | **PASS** |
| v0.7.3 | 2026-05-16 | ~3 months | **PASS** | **PASS** | — | **PASS** |
| v0.7.2 | 2026-05-08 | ~3 months | **PASS** | **PASS** | — | **PASS** |
| v0.7.1 | 2026-05-05 | ~3 months | **PASS** | M1 | — | **PASS** |
| v0.7.0 | 2026-04-29 | ~3 months | **PASS** | M1 | — | **PASS** |
| v0.6.5 | 2026-04-21 | ~4 months | **PASS** | M1 | — | **PASS** |
| v0.6.4 | 2026-04-08 | ~4 months | **PASS** | M1 | — | **PASS** |
| v0.6.3 | 2026-04-07 | ~4 months | **PASS** | M1 | — | **PASS** |
| v0.6.2 | 2026-04-01 | ~4 months | **PASS** | M1 | — | **PASS** |
| v0.6.1 | 2026-03-27 | ~4 months | **PASS** | M1 | — | **PASS** |
| v0.6.0 | 2026-03-24 | ~4 months | **PASS** | M1 | — | **PASS** |
| v0.5.2 | 2026-03-20 | ~5 months | **PASS** | M1 | — | **PASS** |
| v0.5.1 | 2026-03-17 | ~5 months | **PASS** | M1 | — | **PASS** |
| v0.5.0 | 2026-03-06 | ~5 months | **PASS** | M1 | — | **PASS** |
| v0.4.2 | 2026-03-05 | ~5 months | **PASS** | M1 | — | **PASS** |
| v0.4.1 | 2026-03-01 | ~5 months | **PASS** | M1 | — | **PASS** |
| v0.4.0 | 2026-02-21 | ~5 months | **PASS** | M1 | — | **PASS** |
| v0.3.18 | 2026-02-20 | ~5 months | **PASS** | FAIL | M1 | **PASS** |
| v0.3.17 | 2026-02-14 | ~6 months | **PASS** | — | M1 | **PASS** |
| v0.3.16 | 2026-02-08 | ~6 months | P2 | — | P2+M1 | P2 |
| v0.3.15 | 2026-01-21 | ~6 months | P1 | — | FAIL | P1 |
| v0.3.14 | 2026-01-16 | ~7 months | P1 | — | P1+M1 | P1 |
| v0.3.13 | 2026-01-12 | ~7 months | P1 | — | P1+M1 | P1+G1 |
| v0.3.12 | 2026-01-08 | ~7 months | P1 | — | P1+M1 | P1+G1 |
| v0.3.11 | 2026-01-03 | ~7 months | P1 | — | P1+M1 | P1+G1 |
| v0.3.10 | 2026-01-01 | ~7 months | P1 | — | P1+M1 | P1+G1 |
| v0.3.9 | 2025-12-29 | ~7 months | P1 | — | P1+M1 | P1+G1 |
| v0.3.8 | 2025-12-28 | ~7 months | P1 | — | P1+M1+M2 | FAIL |
| v0.3.7 | 2025-12-27 | ~7 months | P1 | — | P1+M1+M2 | FAIL |
| v0.3.6 | 2025-12-22 | ~7 months | P1 | — | P1+M1+M3+M2 | FAIL |
| v0.3.5 | 2025-12-19 | ~8 months | **PASS** | — | M1+M3+M2 | G1 |
| v0.3.4 | 2025-11-28 | ~8 months | **PASS** | — | M1+M3+M2 | G1 |
| v0.3.3 | 2025-11-17 | ~9 months | **PASS** | — | FAIL | G1 |
| v0.3.2 | 2025-11-17 | ~9 months | **PASS** | — | FAIL | G1 |
| v0.3.1 | 2025-11-12 | ~9 months | **PASS** | — | M1+M3+M2 | G1 |
| v0.3.0 | 2025-11-11 | ~9 months | **PASS** | — | M1+M3+M2 | G1 |
| v0.2.10 | 2025-11-06 | ~9 months | **PASS** | — | M1+M3+M2 | FAIL |
| v0.2.9 | 2025-10-30 | ~9 months | **PASS** | — | M1+M3+M2 | G1 |
| v0.2.8 | 2025-10-14 | ~10 months | **PASS** | — | M1+M3+M2 | G1 |
| v0.2.7 | 2025-10-10 | ~10 months | **PASS** | — | M1+M3+M2 | G1 |
| v0.2.6 | 2025-10-07 | ~10 months | **PASS** | — | M1+M3+M2 | G1 |
| v0.2.5 | 2025-09-27 | ~10 months | n/c F4 | — | M1+M3+M2 | G1 |
| v0.2.4 | 2025-09-20 | ~11 months | n/c F4 | — | M1+M3+M2 | G1 |
| v0.2.3 | 2025-09-17 | ~11 months | n/c F4 | — | M1+M3+M2 | G1 |
| v0.2.2 | 2025-09-16 | ~11 months | n/c F4 | — | M1+M3+M2 | G1 |
| v0.2.1 | 2025-09-11 | ~11 months | n/c F4 | — | M1+M3+M2 | G1 |
| v0.2.0 | 2025-09-10 | ~11 months | n/c F4 | — | M1+M3+M2 | G1 |
| v0.1.11 | 2025-08-27 | ~11 months | n/c F4 | — | FAIL | FAIL |
| v0.1.10 | 2025-08-20 | ~12 months | n/c F4 | — | FAIL | FAIL |
| v0.1.9 | 2025-08-15 | ~12 months | n/c F4 | — | FAIL | FAIL |

Read the timeline as: everything **~6 months old or newer** is clean on postgres and mongodb, and **~3 months or newer** on mysql. Below that the failures are not a broad sweep but four narrow, well-bounded bands — M1 from v0.7.1 down on mysql, P1/P2 in the v0.3.6 → v0.3.16 hole on all three drivers, G1 from v0.3.13 down on mongodb, and M2/M3 below v0.3.9 on mysql.