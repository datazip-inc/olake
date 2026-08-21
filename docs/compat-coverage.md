# Backward-compat coverage: baselines and exclusions

What the compat suite actually covers, and every place it deliberately asserts less than full
equality. The design of the suite itself lives in [backward-compatibility.md](backward-compatibility.md);
this file is the data: which released versions run, and which columns/writers/inputs are excluded
or relaxed against which baselines — each with the PR or finding that justifies it.

---

## 1. Baselines covered

The sweep (`compat-baselines.sh`, used when `COMPATIBILITY_BASELINE` is empty) runs **one baseline per
state version**, read from the manifest `compat-baselines.txt` — **release tags only**, because
the column rules, writer-group gates and input generations all date a baseline by parsing its
tag, and the image is pullable. New entries are **auto-generated**: when the first release at a
new state version is tagged, the `compat-baselines-release` workflow reads that tag's own version
(via its `make print.state-version` in a worktree — location-independent) and appends the row; a
release jumping several versions carries every skipped one, and the sweep runs shared baselines
once. Between a bump and its release the newest version simply has no sweep baseline — the Base
Compat Check gates every PR in that window, and `assertBaselineManifestCurrent` notes the gap on
every run (and fails on an unparseable manifest). Manual edits are for exceptional repair only.
Today's entries were seeded by hand (newest release still on each version, kept as already
validated):

A baseline pinned at state version N makes the candidate run version-N semantics, so it exercises
every gate introduced *after* N (the gate list is `constants/state_version.go`):

| Baseline | State version | Newest gate its state exercises |
| --- | :-: | --- |
| v0.3.11 | 0 | v1: strict timestamp parsing (no more silent epoch fallback); plus every later gate. Newest pre-versioning release — absent `version` reads as 0 |
| v0.3.15 | 1 | v2: mysql binlog CDC timezone alignment |
| v0.3.16 | 2 | v3: mysql offset-format timezone parsing |
| v0.4.0 | 3 | v4: mysql UNSIGNED int/integer/bigint → Int64 |
| v0.6.1 | 4 | v5: mongodb nested DateTime → UTC time.Time |
| v0.6.5 | 5 | v6: []uint8 support in ReformatInt64 |
| v0.9.0 | 6 | v7: parquet INT96 → timestamp, unsigned 32-bit → Int64 |
| v0.9.2 | 7 | none — same version as HEAD; pins destination continuity only |

A single run defaults to `latest` (destination continuity, no version gate); `OLAKE_COMPATIBILITY_BASELINE`
(or the per-driver `OLAKE_COMPATIBILITY_BASELINE_<DRIVER>`) accepts a tag, an image ref, or a commit sha
(built from a worktree). `COMPAT_SWEEP_FLOOR=<state version>` trims the sweep to entries at or
above it.

### Per-driver floors (`CompatMinBaseline`)

Baselines below the floor are skipped with the reason; the floor is the driver's own limitation,
not a finding.

| Driver | Floor | Sweep baselines covered | Why the floor |
| --- | --- | :-: | --- |
| postgres | v0.2.6 | 8/8 | F4: ≤ v0.2.5 speak only wal2json (#533 switched to pgoutput); the postgres:15 stack ships no wal2json |
| mysql | v0.2.0 | 8/8 | v0.1.9–v0.1.11 fail unattributed even on the ASCII fixture |
| mongodb | v0.2.0 | 8/8 | v0.1.9–v0.1.11 die with "error occurred while reading records", unattributed |
| oracle | — | 8/8 | images exist for every sweep baseline |
| mssql | v0.3.15 | 7/8 | first release carrying the driver |
| kafka | v0.3.16 | 6/8 | #735 (v0.3.12) added `decoder.UseNumber()` but the flattener only learned `json.Number` in #796 (v0.3.16); between them the baseline's own sync FATALs, before that every number types double |
| db2 | v0.3.14 | 7/8 | first release carrying the driver |
| s3 | v0.3.12 | 7/8 | first release carrying the driver |

## 2. Writer-group gates

Each baseline runs up to three writer groups (parquet, iceberg-legacy, iceberg-arrow) × the
driver's scenarios (CDC and/or incremental). A group with a known bounded regression is left out
for that baseline — the other groups keep their coverage.

| Group | Gate | Applies to | Why |
| --- | --- | --- | --- |
| ice_arrow | min v0.3.17 | all drivers | P1: #531 (v0.3.6) → v0.3.16 arrow integer widths disagree with today's; below v0.3.6 the baseline has no arrow writer at all |
| ice_arrow | min v0.6.5 | kafka | `identityTransform` asserted `.(int64)` on `json.Number` until #904; the baseline's own first partitioned flush panics |
| ice_legacy, ice_arrow | skip v0.6.0–v0.6.4 | kafka | K3: #756 filters RAW values in the writer and `Compare` had no `json.Number` case until #904 — `float_value < 100` string-compares, every record drops, sync exits 0 empty. Parquet reformats before filtering and keeps this window |
| pq | skip v0.3.16 (exactly) | all drivers | P2: v0.3.16 alone adds the `data` column unconditionally to the parquet schema (#810, fixed by #826 in v0.3.17) |
| pq | min v0.3.16 | s3, db2 | no-CDC drivers' parquet destinations gained `_cdc_timestamp` only with #810; below it the reference is legitimately a column short |
| pq | min v0.3.16 | oracle | P3: before #602 `EvolveSchema` only closed open files, so a widened NUMBER still met an INT32 file schema and the flush panicked |

## 3. Input-generation seeding (what the baseline is fed)

Both runs get the `streams.json` shape (and CLI flags) **the baseline shipped with**, not today's —
a key the old binary never knew must not read as a behavior change. `OLAKE_COMPATIBILITY_INPUT_GENERATION`
pins a shape explicitly.

| Generation | From | What it adds |
| --- | --- | --- |
| pre-namespace | — | none; `--destination-database-prefix` is withheld (cobra exits 1 silently on unknown flags) |
| legacy-filter | v0.2.0 (#461) | the prefix flag; filtering only as the legacy `filter` string |
| selected-columns | v0.4.0 (#840) | `selected_columns` (older binaries ignore it and sync every column) |
| filter-config | v0.6.0 (#756) | structured `filter_config` |

**Legacy-filter exclusion**: in the legacy string a timestamp can only be a bare ISO-8601 literal,
which **oracle** (ORA-01843) and **db2** refuse on *every* release including the candidate — so the
timestamp condition is dropped from the filter for those two drivers in pre-v0.6.0 generations.
Both sides run the remaining conditions.

## 4. Column rules (assertion and seeding exclusions)

Three policies, applied per baseline (`compatibility_columns.go`); no rule = full type+value assertion.
Undatable baselines (latest, image ref, sha) count as newest, so only `TypeOnly` fires.

- **ExcludeBelow R** — dropped from seed data *and* catalog below release R. Hard fails only: the
  baseline cannot carry the column at any price.
- **AssertValueFrom R** — type-only below R, full value assertion from R. For values whose form
  legitimately changed at R (the old form was the bug).
- **TypeOnly** — never value-compared, any baseline.

| Driver | Column(s) | Policy | Why |
| --- | --- | --- | --- |
| mysql | `name_ucs2`, `name_utf16le`, `grade` | ExcludeBelow v0.7.2 | #940: older baselines hand raw utf16/ucs2/latin1 bytes to the writer as invalid UTF-8; gRPC marshal fails, retry backoff looks like a hang |
| mysql | `permissions`, `tags` | AssertValueFrom v0.7.2 | M1: SET columns emitted the numeric bitmask on the binlog path before #940 |
| mysql | `priority`, `status` | AssertValueFrom v0.3.9 | M2: ENUM serialization changed |
| mysql | `price_decimal`, `amount_decimal_9_2`, `price_numeric` | AssertValueFrom v0.3.7 | M3: DECIMAL/NUMERIC round-tripped through float32 |
| mongodb | `id_regex` | AssertValueFrom v0.3.14 | G1: BSON regex serialized with Go field names, not lowercase keys, until #657 |
| mongodb | `_id`, `_olake_id` | volatile (ExtraVolatileColumns) | server-generated ObjectID differs per run, and `_olake_id` hashes it |
| db2 | `col_decfloat` | AssertValueFrom v0.7.6 | #936: ReformatValue rendered a float into a String column with `%d` → `%!d(float64=…)` |
| s3 (all formats) | `_last_modified_time`, `_olake_id` | TypeOnly | the cursor IS the object's upload stamp and each run uploads its own copy; `_olake_id` hashes the record carrying it |
| s3 JSON | `mixed_col` | AssertValueFrom v0.7.6 | #936, same `%d`-on-float window |
| s3 CSV | `new_col` (evolve column) | ExcludeBelow v0.9.1 | #1020: an unknown CSV header failed the whole sync before the parser learned to infer it |
| s3 Parquet | `map_col`, `struct_col`, `list_col`, `int96_col` | ExcludeBelow v0.9.1 | #1020: the *baseline's own discover* panics calling `Kind()` on a group node |
| s3 Parquet | `ts_col`, `ts_ms_col`, `ts_ns_col`, `ts_far_col`, `uuid_col` | AssertValueFrom v0.9.1 | #1020 also fixed values: sub-second truncation (`time.Unix(sec, 0)`), the year-1816 nano overflow, base64 UUIDs |
| postgres, mssql, oracle, kafka | — | none | compare clean on every reachable baseline |

`ExcludeBelow` requires the driver fixture to honor `SeedExcludedColumns`
(`SupportsSeedExclusion`; mysql and s3 do) — a rule naming a column the fixture cannot leave out
fails loudly rather than silently seeding it.

## 5. Always-volatile columns (built-in, every driver)

Type-compared, never value-compared, because they cannot match across two independent runs:
`_olake_timestamp` plus every column the driver declares in `DefaultCDCColumnsSchema` (wall-clock
stamps and source-log coordinates — `_cdc_timestamp`, LSN/offset columns). A `_cdc_lsn` that
changed *type* still fails. `_olake_id` and `_op_type` are explicitly kept value-compared
(`GetKeysHash` is deterministic over the source PK; mongodb opts `_olake_id` back out, above).

## 6. Sweep affordances and remaining relaxations

- `OLAKE_COMPATIBILITY_EXCLUDE_COLUMNS=a,b` — appends catalog-level exclusions (both sides) to any run,
  for probing a baseline without editing the driver's rules.
- `OLAKE_COMPATIBILITY_REQUIRE_BASELINE` — an unavailable baseline fails instead of skipping (CI release
  gates; a silent skip reads as green while testing nothing).
- kafka runs its writer groups **serially** (`CompatSerialGroups`): discover enumerates the whole
  broker, so concurrent groups would scan and race-delete each other's topics.
- The parquet destination compares only the **last** scenario case's files: successive syncs write
  the same column with different types and Spark refuses the mixed directory
  (`CANNOT_MERGE_SCHEMAS`) — F2, parquet has no schema evolution.
- s3 incremental uploads (`insert_1`/`update_1`/`evolve_1`) are re-uploaded until they land in a
  strictly later second than everything under the prefix (`putFilePastCursor`): the driver's cursor
  is second-precision with a strict `>`, so a same-second file is silently skipped — by every
  release, so the ambiguity would compare two wrongs, or one wrong against timing luck.
- mssql CDC scenarios wait for the async capture agent (`wait-cdc-catchup`) after seeding, before
  the full load: pre-#843 (v0.5.1) binaries capture their initial LSN without waiting, and a
  lagging agent makes the first stateful sync replay the seed rows as CDC inserts — a race, so it
  is removed from the input rather than version-gated.
- Comparisons read parquet with Spark's vectorized reader **disabled**: it mis-decodes
  `DELTA_LENGTH_BYTE_ARRAY` string columns holding nulls (values after a null read back as `""`) in
  files every other reader agrees are correct.

## 7. Deliberately out of scope

- **Discover output** — both runs seed from the same frozen `test_streams.json`; discover is
  ungated by design (A4).
- **The reverse direction** — a new state file fed to an old image is not a supported operation.
- **Gates older than the baseline under test** — a sweep entry only proves the versions it spans.
