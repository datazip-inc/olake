# OLake backward-compat sweep v2 — versioned input configs

Run date: 2026-08-06. Candidate: this branch (`compat-port`, 28364ef), built as `olake/source-<driver>:local`. Baselines: every `olakego/source-<driver>` release tag pushed within one year (v0.1.9, ~12 months ago → v0.9.0, ~2 weeks ago).

**What changed since [COMPAT_RESULTS.md](COMPAT_RESULTS.md).** That sweep wrote a *current* `streams.json` to both sides, so any config key introduced after the baseline read as a behaviour change: the older binary ignored the key and synced something the candidate filtered away. This run derives an **input generation** from the baseline's release tag and writes that shape to *both* sides, so the binary is the only thing that varies. A surviving diff now means the candidate changed its mind about an input the baseline also understood — the actual backward-compatibility contract.

Every version below carries how long ago it shipped, rounded, as of **2026-08-06**; exact release dates for all 58 tags are in the appendix.

Status: all runs complete — **postgres 58/58**, **mysql 26/26**, **mongodb 58/58**, and **mysql-utf8 33/33**: the same suite against a pure-ASCII fixture, which lifts mysql's UTF-8 floor and reaches 33 versions the main mysql sweep could never run.

Logs: `olake-data/compat-results-v2/<driver>/<version>/compat.log`, roll-up per driver in `summary.tsv`. The v1 logs are preserved untouched at `olake-data/compat-results/`.

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

The last two are a different shape from the rest. M1, M2, M3, G1 and F4 are **thresholds**: a behaviour changed once and every older baseline disagrees with HEAD, so there is nothing to fix — the current behaviour is the intended one. P1 and P2 are **bounded regressions**: they were introduced and then resolved, so versions on *both* sides pass and only the window between them fails.

M1 and G1 carry over from v1. P1, P2, M2 and M3 are new — v1 never saw them because F1/F2 aborted those comparisons on row count first, and because mysql's UTF-8 floor hid everything below v0.4.0.

**P1 and P2 reproduce on all three drivers at identical version boundaries**, which is the strongest signal in this run: they live in the shared Iceberg writer and schema layer, not in any driver.

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
- Reported by the harness as `failed to collect the row diff` at `compat.go:665`, because the exception surfaces inside `rowsOnlyIn`'s `df.Collect`.

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

## Reproducing

```sh
# one version
COMPAT_BASELINE=v0.3.10 make test.compat.postgres

# pin the candidate so the harness does not rebuild it per run
OLAKE_DRIVER_IMAGE=olake/source-postgres:local COMPAT_BASELINE=v0.3.10 make test.compat.postgres

# force an input generation instead of deriving it from the baseline tag
OLAKE_COMPAT_INPUT_GENERATION=current        # today's shape: filter_config + selected_columns
OLAKE_COMPAT_INPUT_GENERATION=legacy-filter  # pre-v0.4.0 shape

# harness knobs added for this sweep
OLAKE_TEST_CONTAINER_MEMORY=1536m   # 0/off disables the cap
OLAKE_TEST_WRITER_HEAP=-Xmx512m     # off leaves the JVM's own sizing alone
OLAKE_COMPAT_EXCLUDE_COLUMNS=a,b    # columns a baseline cannot sync (appended for mysql)
```

## Suggested follow-ups

1. **Attribute M2 and M3.** Both are new, both are mysql, and M3 — decimals round-tripping through float32 — is the most user-visible thing either sweep has found. Boundaries are v0.3.9 (ENUM) and v0.3.7 (DECIMAL); neither PR is identified yet.
2. **Attribute and triage P1.** An existing Iceberg table becoming unreadable by the Arrow reader after upgrade is the most serious thing either sweep has found. Isolate the column, bisect v0.3.5 → v0.3.6, and decide whether the legacy reader tolerating it is sufficient mitigation.
3. **P2 is fully attributed** — one version wide, and the `data` column name points at the destination-check probe rather than the stream.
4. **The ASCII fixture is strictly more informative** (33 versions) and decide whether the ASCII fixture should replace the exclusion list in the committed suite — it is strictly more informative.
5. **Reuse one writer JVM per sync** instead of one per phase. It is the shared root of the OOM cascade and the p50 = 9.1 s sync time, and it would roughly halve the sweep's wall clock.

## Appendix — release timeline

All four runs. Ages are rounded as of 2026-08-06; `n/c` = not comparable; `—` marks versions a run did not cover — mysql stops at its UTF-8 floor, which the mysql-utf8 column covers instead.

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