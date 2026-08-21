# Backward Compatibility

## The contract

> A sync run's data semantics are pinned by the `version` field of its state file. Upgrading the OLake binary must not change the records or the column types an existing pipeline produces. Pipelines with no state file get the latest semantics. A state version, once released, is supported forever.

`state.LoadedVersion()` is the only mechanism that enforces the first half of that sentence. It does not cover everything a user hits when they upgrade — §2 enumerates what it covers and what it does not, and §3 says when any of it actually matters.

---

## 1. How versioning works today

Two constants and one global:

| Symbol | Location | Meaning |
| --- | --- | --- |
| `state.LatestVersion()` | `constants/state-versions.json` (go:embed-ed by `constants/init.go`) | the version a *new* pipeline is created at (currently `7`) |
| `LatestStateVersion` | `tests/testutils/constants/constants.go:35` | the test harness's deliberately separate copy — a black-box suite sharing the product's symbol could not detect a contract break |
| `state.LoadedVersion()` | `constants/state_version.go` | the version the *current process* is running as |

`state.LoadedVersion()` is assigned in exactly one place:

```go
// protocol/sync.go:72-80
if statePath != "" {
    utils.UnmarshalFile(statePath, state, false)   // version comes from the file
} else {
    state.Version = constants.LatestStateVersion
}
constants.SetLoadedStateVersion(state.Version)
```

Three consequences that surprise people:

1. **Only `sync` sets it.** `discover`, `check`, `spec` and `clear-destination` run at the package default (`LatestStateVersion`). Anything version-dependent in the discover path is therefore *not* pinned to the pipeline's version — see A4.

2. **The version is sticky forever.** Nothing upgrades a state file. A pipeline created at v3 runs v3 semantics for the rest of its life, so every gate must be kept alive indefinitely. Deleting a gate is itself a breaking change.

3. **Version 0 and "no version" are the same value.** ``Version int `json:"version,omitempty"` `` (`types/state.go:67`) means a `0` is never written out, and an absent field reads back as `0` — which is a *meaningful* legacy mode, not a null. See C1.

### The gates

The gate sites span three Go modules — root (`github.com/datazip-inc/olake`), `drivers/mysql` and `drivers/mongodb` — which is why the unit tests for them cannot live in one place.

| Behaviour | Gate | Below the gate | At or above |
| --- | --- | --- | --- |
| Unparseable timestamp string, not from a DB timestamp column | `utils/typeutils/reformat.go:274` | v0: returns epoch (`1970-01-01`) | v≥1: returns an error, value stays a string |
| `[]uint8` → `int64` | `utils/typeutils/reformat.go:326` | v≤5: error | v≥6: parsed as int/uint |
| Binlog `TimestampStringLocation` | `pkg/binlog/binlog.go:47` | v≤1: UTC/Local depending on context | v≥2: the connection's timezone |
| MySQL timezone offset (`+05:30` form) | `drivers/mysql/internal/mysql.go:387` | v≤2: `LoadLocation` fails → UTC | v≥3: `time.FixedZone` |
| MySQL `UNSIGNED` reinterpretation | `drivers/mysql/internal/mysql.go:274` | v≤3: strips the leading `unsigned` → Int32 | v≥4: widens the raw bits → Int64 |
| Mongo BSON `DateTime` at any depth | `drivers/mongodb/internal/mon.go:60` | v≤4: stock decoder | v≥5: `time.Time` in UTC |
| Parquet `INT96` + unsigned 32-bit width | `pkg/parser/parquet.go` (`parquetTypeStateVersion`, 5 read sites) | v≤6: INT96 emitted as its raw string, uint32 read as Int32 and wraps negative | v≥7: INT96 → timestamp, uint32 → Int64 |

### Version history → release tags

Needed when picking a baseline image to test an upgrade against. Pinned per version in `compat-baselines.txt`.

| State version | Introduced by | First release | Newest release still on it |
| --- | --- | --- | --- |
| 0 | — (pre-versioning) | — | v0.3.11 |
| 1 | #646 time parsing of string values | v0.3.12 | v0.3.15 |
| 2 | #815 align CDC and full refresh TIMESTAMP | v0.3.16 | v0.3.16 |
| 3 | #827 parse timezone offset correctly | v0.3.17 | v0.4.0 |
| 4 | #846 unsigned integer overflow | v0.4.1 | v0.6.1 |
| 5 | #900 mongo out-of-bound dates for nested objects | v0.6.2 | v0.6.5 |
| 6 | #797 `[]uint8` in `ReformatInt64` | v0.7.0 | v0.9.0 |
| 7 | parquet INT96 + unsigned 32-bit mapping (OL-2946) | unreleased | — (current) |

The "newest release still on it" column is what the compat sweep fans out to on every merge to master — but the table is documentation, not the source. `compat-baselines.sh` reads `compat-baselines.txt` — release tags only, one per state version — whose entry for a new state version is **auto-committed when the first release carrying it is tagged** (compat-baselines-release workflow). See docs/compat-coverage.md.

Released images are `olakego/source-<driver>:<tag>` (`release-tool.sh:62`, with `type=source` at `release-tool.sh:140`).

---

## 2. Where backward compatibility can break

Seven surfaces. For each: what the change is, the code that governs it, what the user sees, and whether the state version covers it.

### A. Value semantics — covered by a version gate

| | Change | Governed by | User sees |
| --- | --- | --- | --- |
| **A1** | A source type maps to a different OLake type (e.g. `unsigned int` Int32 → Int64) | `drivers/<d>/internal/datatype_conversion.go` + the gate in the driver's `dataTypeConverter` | New rows arrive with a different type than the existing column; destination must promote (see F1) |
| **A2** | A parse or format rule changes (timestamp fallback, timezone resolution, UTC vs local) | `utils/typeutils/reformat.go`, `pkg/binlog/binlog.go:47`, `drivers/mysql/internal/mysql.go:387` | Timestamps shift by a fixed offset, or a column that used to hold `1970-01-01` now holds the raw string |
| **A3** | An encoding is newly handled or newly rejected (`[]uint8` → int64) | `utils/typeutils/reformat.go:326` | A column that used to fail the sync now populates — or the reverse |

**A4 — Gate asymmetry (live hazard).** The *value* path is gated but the *schema* path that feeds it is not. `drivers/mysql/internal/datatype_conversion.go` is read by three callers:

- `dataTypeConverter` (`mysql.go:299`) — gated at `mysql.go:274`;
- `ProduceSchema` (`mysql.go:224`) — **ungated**, this is what `discover` writes into `streams.json`;
- `isNumericAndEvenDistributed` (`backfill.go:436`) — **ungated**, this is what decides chunk boundaries.

So editing the map changes discover output and chunk planning for *every* pipeline regardless of its state version, even though the values keep their old shape. Any change to a type map must be reasoned about on all three paths, not just the gated one.

### B. Value semantics — covered by nothing

This is the largest silent-break class. These paths have no version gate at all, so an edit rewrites every existing pipeline's data with no escape hatch:

- `ReformatDate`'s year clamping — year `< 1` → epoch, year `> 9999` → shifted (`reformat.go:219-224`)
- `ReformatValue`'s `String` coercions for ints, floats, bools, `[]byte` (`reformat.go:76-91`)
- `ReformatFloat32` / `ReformatFloat64`, `ReformatInt32`, `ReformatBool`
- `ReformatGeoType` (WKB → WKT), `ReformatTimeValue` (`15:04:05` formatting)
- The `DateTimeFormats` layout list (`reformat.go:25-40`) — adding a layout makes previously-unparseable strings parse, which changes their stored value

**Rule:** a change to any of these needs either a version bump with a new gate, or an explicit "this cannot change any existing output" justification in the PR.

### C. State-file compatibility

**C1 — An empty `{}` state file means version 0, i.e. legacy semantics.** Because `Version` is `omitempty`, `{}` and `{"version": 0}` are indistinguishable, and an orchestrator that seeds an empty state file for a *brand new* pipeline silently opts it into v0 behaviour: epoch fallback for unparseable dates, MySQL unsigned → Int32, Mongo dates through the stock decoder. This has already bitten the integration harness, which now seeds `{"version": N}` explicitly (`tests/testutils/test_utils.go:626`).

**C2 — Versions are never upgraded**, so gates accumulate. Removing a gate breaks every pipeline still on that version. Treat the gate list in §1 as append-only.

**C3 — `clear-destination` can drop the version.** When `--state` is not passed, `protocol/clear.go:33` builds a bare `types.State{Type: StreamType}` (version 0); `ClearState` returns `&types.State{}` if the driver has no state (`drivers/abstract/abstract.go:154`); and `newState.LogState()` (`clear.go:70`) writes that to the default state path (`protocol/root.go:59`) with `version` omitted. The next sync then reads version 0.

**C4 — The state payload's shape is a compat surface.** Renaming a JSON key or changing a field's type either errors out or silently zero-values, and both mean a full re-backfill or a CDC position advanced past unread changes:

- `global.state`, per driver: `MySQLGlobalState` (`drivers/mysql/internal/mysql.go:43`), `waljs.WALState` for Postgres, the Mongo resume token, Kafka offsets. MySQL hard-fails when `server_id` is missing (`drivers/mysql/internal/cdc.go:81`); Postgres fails to parse an empty LSN.
- `streams[].state`: cursor keys, and the `chunks` array with its custom unmarshal into `*Set[Chunk]` (`types/state.go:349-366`). A chunks decode failure silently means "no chunks", i.e. re-backfill.
- `global.streams`: the set that records which streams finished backfill (`HasCompletedBackfill`, `types/state.go:108`).

**C5 — Cursor encoding.** `Cursor()` splits the configured cursor field on `:` into primary/secondary (`types/stream_configured.go:188`). Changing that encoding invalidates every stored cursor.

**C6 — Sync-mode migration.** The incremental → CDC path reuses a stored cursor as the recovery cursor so the switch does not need a full load (`types/state.go:32-36`). Changing how cursors are stored breaks that migration specifically.

### D. Catalog (`streams.json`) compatibility

**D1 — Legacy string filters.** `GetFilter` (`types/stream_configured.go:213`) accepts either the structured `filter_config` or the legacy string `filter` parsed by a regex. Old catalogs in the field contain the string form; the regex must keep accepting everything it used to.

**D2 — `filter_config` is honoured only when `normalization` is on** (`types/stream_configured.go:215`). Turning normalization off silently falls back to the legacy string filter.

**D3 — Silent stream drops.** A stream in an old catalog that fails validation is skipped with a `Warn`, not an error — the sync exits 0 having synced fewer streams than the user asked for (`protocol/sync.go:217`, `:224`, `:231`, `:236`, `:241`, `:247`, `:252`). Any change to discover output or to the validation rules is a candidate for this, and it is invisible in an exit code.

**D4 — Constraint changes**, e.g. the "more than 2 conditions is unsupported" rejection (`protocol/sync.go:236`). Tightening a constraint turns a working catalog into a skipped stream.

### E. Config compatibility

**E1 — Driver `Config` structs** (`drivers/<d>/internal/config.go`): a new required field, a renamed JSON key, or a changed default breaks configs already stored by the UI. Note `UpdateMethod interface{}` is polymorphic and decided by key presence (`utils.IsOfType`, `utils/utils.go:183`) — adding a variant changes how existing configs are classified.

**E2 — The destination envelope** `{type, writer}` (`types/adapter.go:11`) and the per-destination config underneath it (Iceberg catalog type, `use_arrow_writes`, S3/REST/Glue variants).

**E3 — Encrypted configs.** `UnmarshalFile` decrypts when `--encryption-key` is set (`utils/utils.go:159-181`). A change to the encryption scheme makes stored configs unreadable.

### F. Destination state — covered by nothing

The state version pins what OLake *produces*. It says nothing about the table that already exists in the destination, which was created by an older binary.

**F1 — Type promotion is narrow.** `promotionTransitions` (`destination/iceberg/iceberg.go:52`) allows exactly two transitions:

```go
"int":   {"long": true},
"float": {"double": true},
```

So an Int32 → Int64 change promotes cleanly, but `long → string`, `timestamp → string`, `string → int` and every reverse direction do not: `EvolveSchema` (`iceberg.go:388`) will not evolve, and the write path either rejects the batch outright (`isValidTransition`, `iceberg.go:263`) or falls through to parsing the value. A type-mapping change that is *not* a legal promotion breaks every existing table even if the state version pins the value semantics — because a *new* pipeline writing to an *old* table hits it too.

**F2 — Parquet has no schema evolution.** `EvolveSchema` only rolls the file (`destination/parquet/parquet.go:440`), so a table ends up as a directory of mixed-schema files and the break surfaces in the reader, not in OLake.

**F3 — 2PC metadata.** The `olake_2pc` table property holds a `types.MetadataState` JSON blob written by one binary and read by the next (`destination/iceberg/iceberg.go:125`). Its shape is a compat surface exactly like the state file's.

**F4 — Naming rules.** `--destination-database-prefix`, `GetDestinationDatabase` / `GetDestinationTable`, and column-name normalisation decide where data lands. A change here does not error — it starts writing to a *new* table and orphans the old one.

**F5 — Partition spec.** `parsePartitionRegex` (`destination/iceberg/iceberg.go:466`) is applied at table creation. An existing table keeps the spec it was created with.

**F6 — Full refresh drops the table, so the destination half of the contract does not apply.** Every stream that is not CDC or incremental is classified as a full-load stream (`protocol/sync.go:259-273`) and its destination table is dropped at the start of *every* sync run, not just the first (`protocol/sync.go:113-125`): Iceberg sends `DROP_TABLE` (`iceberg.go:541`), Parquet deletes the files (`parquet.go:541`). The table is then re-created in `Setup` from `stream.Schema().ToIceberg(...)` (`iceberg.go:97`) — i.e. from `streams.json` — and evolved from there by the record-derived types. Three consequences:

- **F1 and F2 do not apply to a full-refresh stream.** There is no old table to promote against, so `long → string`, `timestamp → string` and every reverse direction land cleanly, and Parquet cannot accumulate mixed-schema files. One full refresh is the supported way to move a table across a transition that is not a legal promotion — at the cost of a reload and a window with no table, since the drop happens before the writer pool is even built.

- **The state version still applies.** `state.LoadedVersion()` is assigned from the state file in `PersistentPreRunE` (`protocol/sync.go:72-80`) with no sync-mode branch, and the full-refresh clearing runs later in `RunE` and only wipes per-stream entries — `ClearState` returns the same state object with `version` intact (`drivers/abstract/abstract.go:152-178`). So a full refresh re-reads the whole source and re-writes it under the *old* semantics: a brand-new table holding old-shaped values. Re-syncing is not how a user picks up a gated fix; the state file has to go (see 3.2).

- **The catalog and the values can disagree.** `discover` is ungated (A4), so after an upgrade `streams.json` carries the new type map while `--state` still pins the old value semantics. The table is created from the new catalog type and the records arrive in the old shape. A numeric mismatch is silent: an `int` record into a `long` column needs no promotion, so `differentSchema` never fires and the write succeeds (`iceberg.go:413-431`). A cross-family mismatch has no promotion entry either, so no evolution happens and the write path falls back to parsing the value — which fails if it is not parsable (`iceberg.go:409-412`).

### G. CLI and process surface

The UI and orchestrator drive OLake as a subprocess, so these are contracts too:

- Flag names (`--config`, `--catalog`/`--streams`, `--destination`, `--state`, `--destination-database-prefix`, `--encryption-key`) — `protocol/root.go:136-149`.
- The implicit default paths: with no `--state`, state is written next to `--config` (`protocol/root.go:55-64`). The integration harness depends on this exact behaviour.
- Exit codes, and the JSON shape of `streams.json`, `state.json`, `stats.json`.

---

## 3. When the contract applies

The state version exists to stop old and new output from **mixing inside one table**. Where mixing cannot happen, nothing in §2.A/B/F is a break and a type or value change can land unconditionally. Two questions decide which case you are in.

### 3.1 Do rows written by the old binary survive the upgrade?

| Pipeline shape | Old rows survive | Changing a column's type | Changing a value's shape |
| --- | --- | --- | --- |
| `full_refresh` | **No** — per-stream state cleared and table dropped at the start of every run (F6) | **Free.** The table is recreated from scratch; F1's promotion table and F2 do not apply | **Free.** The whole table is re-emitted under one semantics — no half-old column |
| `incremental` / `cdc` with an existing state file | Yes | Legal Iceberg promotion only (F1), otherwise the table breaks | **Gate it.** Otherwise one column holds two meanings and nothing records which row is which |
| No state file, but the table already exists (a re-created job pointed at an old table) | Yes — the table does | F1 applies even though the semantics are the latest | Not pinned; this pipeline gets the latest semantics by design (I2) |
| No state file, no table | No | Free | Free |

Row 3 is the one people miss: "new pipeline" means new *state*, not a new table. Destination continuity (I6) is decided by the table, not by the version.

### 3.2 Which layer decides the value?

- **A gated value path** (the six sites in §1) — an existing pipeline keeps its old behaviour automatically and new ones get the fix. Note the corollary from F6: a full-refresh pipeline sitting on an old state file will *never* see the fix, however many times it re-syncs. If the fix matters to those users, the change needs a state reset in the release notes, not just a gate.
- **An ungated value path** (§2.B) — every pipeline changes at once, mid-stream, at any version. Bump and gate, or justify in the PR that no existing output can change.
- **The catalog** — `ProduceSchema` → `streams.json`, and chunk planning. Never pinned by a version (A4); it changes for everyone the next time `discover` runs. A type-map edit is therefore *at minimum* a catalog change even when the value path is gated.
- **The destination table** — pinned by nothing (§2.F). Governed entirely by whether the table survives, i.e. by 3.1.

### 3.3 So when is compat work actually required?

**None needed** when all of these hold:

- every affected stream runs `full_refresh`, or the change only lands in tables that do not exist yet; and
- the state-file and config shapes are untouched (C4, E1); and
- discover output and stream validation are not narrowed (D3) — a stream that stops validating is a silent partial sync, not an error.

**A version gate is enough** when an existing incremental or CDC pipeline would produce a different value *and* the resulting type transition is either nothing at all or a legal promotion (`int → long`, `float → double`).

**A gate is not enough** when the type transition is not a legal promotion. The gate protects existing pipelines' values, but the table is shared: a *new* pipeline writing the new type into that old table hits F1 regardless of any state version. That needs a migration story — in practice, one forced full refresh (F6) or a new destination table.

**Never free, at any sync mode:** the state-file shape (C4), the config shape (E1), the CLI surface (G), and destination naming (F4, F5). These are read by the orchestrator or by the next process no matter what any table holds, so 3.1 does not excuse them.

---

## 4. The invariants a test suite must pin

| | Invariant |
| --- | --- |
| **I1** | **Sticky semantics** — new binary + a state file at version N produces the same records and column types as the newest released binary at version N. |
| **I2** | **New pipelines get the fix** — new binary with no `--state` uses the latest semantics. |
| **I3** | **The version is inert** — a sync never rewrites the `version` it read. |
| **I4** | **Every gate is pinned on both sides** — for each version N, an explicit expectation at N-1 and at N. |
| **I5** | **Resumability** — state written by an older binary resumes on the new one without re-backfilling. |
| **I6** | **Destination continuity** — the new binary appends to a table an older binary created, with no schema break. |

I2, I3 and I4 are checkable without infrastructure. I1 is only half-checkable there: unit gates pin each gated *value*, but "the same records and column types as the released binary" is a claim about two binaries and can only be proved by Layer 3. I5 and I6 need a real old binary and a real destination.

---

## 5. Test suite design

Four layers. Each is listed with what it catches and — more importantly — what it cannot.

### Layer 1 — Unit matrix over every gate

- **Catches:** a gate deleted, inverted, or moved to the wrong boundary; a version bump with no gate.
- **Cannot catch:** anything about the destination, the state file, or how the two binaries interact.

One test file per module that owns a gate, since they span three modules and `make test.unit` runs each module separately:

| File | Module | Gates |
| --- | --- | --- |
| `utils/typeutils/reformat_version_test.go` | root | `reformat.go:274`, `reformat.go:326` |
| `pkg/binlog/binlog_version_test.go` | root | `binlog.go:47` |
| `drivers/mysql/internal/mysql_version_test.go` | `drivers/mysql` | `mysql.go:274`, `mysql.go:387` |
| `drivers/mongodb/internal/mon_version_test.go` | `drivers/mongodb` | `mon.go:60` |

The shape that matters: assert an **explicit expectation for every version from 0 to `LatestStateVersion`**, not just the two versions either side of the boundary. A boundary-only table silently inherits its expectation when the constant is bumped; a dense table forces whoever bumps it to write down what the new version does.

```go
// one row per version — a missing entry is a test failure, not a default
want := map[int]result{
    0: {value: epoch},
    1: {err: "failed to parse datetime"},
    // ... through LatestStateVersion
}
for v := 0; v <= state.LatestVersion(); v++ {
    exp, ok := want[v]
    require.Truef(t, ok, "state version %d has no expectation — add one when bumping LatestStateVersion", v)
    withStateVersion(t, v)
    // ... exercise and assert
}
```

`withStateVersion` is one shared helper that sets `state.SetLoadedVersion` and restores it via `t.Cleanup`; the idiom is already inlined twice in `utils/typeutils/reformat_test.go` (`:1270-1276` and `:1583-1589`) and should be extracted rather than copied again.

Plus a **meta-test** (`constants/state_version_test.go`) that fails a bump made without the accompanying work:

- every version `0…LatestStateVersion` has an entry in the version-history comment block;
- the highest documented version equals `LatestStateVersion`;
- the harness's copy at `tests/testutils/constants/constants.go` agrees with the product's — the two are duplicated on purpose and nothing else checks them against each other;
- for every version N ≥ 1, at least one `state.LoadedVersion()` comparison exists somewhere in the tree that distinguishes N-1 from N.

The last check reads Go source outside its own module. That works — module boundaries constrain *imports*, not `os`/`filepath` reads — but it means the test walks up to the repo root by relative path. Guard it with an explicit "repo root not found → fail" rather than a skip.

### Layer 2 — State-file contract tests

- **Catches:** a renamed or retyped state key (C4); a chunk-decoding regression; the C1 aliasing changing by accident.
- **Cannot catch:** value semantics, destination schema.

No Docker, no database — frozen JSON fixtures and an unmarshal:

- `types/state_compat_test.go` with fixtures under `types/testdata/state/`: one realistic state file per released shape (global + streams + cursors + chunks). Assert that `version` survives, that `chunks` decodes into `*Set[Chunk]`, that cursor keys survive a marshal → unmarshal round trip, and — explicitly — that `{}` yields version 0, so C1 stays a documented decision rather than an accident.
- `drivers/<d>/internal/state_compat_test.go`: unmarshal a frozen `global.state` blob into `MySQLGlobalState`, `waljs.WALState`, and the Mongo/Kafka equivalents, asserting every field. These fixtures are the alarm that goes off when someone renames a key.

### Layer 3 — Real old-image → new-image upgrade suite (implemented: `tests/testutils/compatibility.go`)

- **Catches:** I1, I3, I5, I6 — the breaks that only appear when a binary meets a state file and a table it did not create.
- **Cannot catch:** gates for versions older than the oldest baseline in the sweep; anything on s3, the one source driver with no local stack. Every other driver carries a `Test<Driver>Compat` entry point, each declaring its own `CompatMinBaseline` floor where old releases cannot run.

Rather than per-version expectations — which rot — `RunBackwardCompat` runs one driver's six scenarios (iceberg legacy/arrow/parquet × CDC/incremental) twice and asserts the two destinations are indistinguishable. Both sides of all three writer groups run in parallel — six isolated pipelines, each its own suite with its own source table, working dir, catalog, state file and (postgres) replication slot; CDC and incremental stay serial inside a group because they share its table:

    reference : every sync on the BASELINE image
    upgrade   : the stateless initial load on the baseline, every --state sync after it on the CANDIDATE

The reference run **is** the expectation. The comparison (`compareRelations`) checks row counts, the destination schema as a map (I6 — this is what catches a type-mapping change), per-`_op_type` counts, and full-row `EXCEPT ALL` diffs in both directions; `assertStateVersionUnchanged` pins I3. Wall-clock stamps and log coordinates are compared by type but not value (`volatileColumns`).

**Inputs are era-correct** (`compatibility_input.go`). Both sides get the `streams.json` shape and CLI flags the *baseline* shipped with — `selected_columns` (v0.4.0), `filter_config` vs the legacy `filter` string (v0.6.0), `--destination-database-prefix` (v0.2.0). A key the baseline never understood would otherwise read as a behaviour change. `OLAKE_COMPATIBILITY_INPUT_GENERATION` pins a shape by hand.

**Per-column assertion policy** (`compatibility_columns.go`). Known, documented findings are encoded as `CompatColumnRule` entries on each driver's compat config, resolved against the baseline's release tag:

| Policy | Meaning | Declared by |
| --- | --- | --- |
| full (default) | type and value both asserted | no rule |
| type-only below a release | the value legitimately changed at that release (the old form was the bug); older baselines assert the type through the schema comparison only | `AssertValueFrom` (or `TypeOnly` to never value-compare) |
| seed-excluded below a release | the baseline cannot sync the column at all (hard fail); it is left out of the seed DDL/DML and the catalog on both sides | `ExcludeBelow` — the driver must set `SupportsSeedExclusion` and wire an `ExecuteQuery` that honours `SeedExcludedColumns` (mysql: `ExecuteQueryExcluding`) |

An undatable baseline (`latest`, an image ref, a commit sha) is treated as newest: only `TypeOnly` rules apply, everything else is fully asserted. Every applied policy is logged, so a run's assertion surface is explicit in its output. The current rule sets encode the findings of the 58-version sweep (COMPAT_RESULTS_v2.md): mysql's charset hard-fails and SET/ENUM/DECIMAL thresholds, mongodb's BSON-regex threshold.

**Baselines.** `COMPATIBILITY_BASELINE` (make) / `OLAKE_COMPATIBILITY_BASELINE` (env, `_<DRIVER>` suffix per driver) accepts a release tag, a full image ref, or a commit sha — the sha path builds the baseline from a detached worktree. Default: the newest release. The sweep list `COMPAT_SWEEP_BASELINES` is **derived, not maintained**: `compat-baselines.sh` reads `constants/state_version.go` at every release tag and emits the newest release still on each state version, oldest first — explicit tags rather than the floating `latest`, so every entry is datable and a run is reproducible after the next release. There is no default floor: the ideal sweep reaches back to the first release. What a *driver* cannot run is declared as `CompatMinBaseline` in its compat config and skips with its reason (postgres: v0.2.6, the wal2json era, F4; mysql/mongodb: v0.2.0, unattributed v0.1.x failures) — each floor a documented, ideally liftable limitation. `COMPAT_SWEEP_FLOOR` trims the list for a shallower one-off sweep.

**Writer-level gates.** A bounded regression in one writer must not cost a baseline the other writers' coverage, so `compatGroup` carries version gates the way columns carry `CompatColumnRule`s: the **arrow** group runs only against baselines ≥ v0.3.17 (P1 — from #531 in v0.3.6 through v0.3.16 its integer width disagrees with today's, and a table holding both is unreadable; below v0.3.6 the baseline has no arrow writer to test), and the **parquet** group skips exactly v0.3.16 (P2 — #810's unconditional `data` column in the parquet file schema, fixed by #826). A gated group logs why it did not run; the legacy iceberg group runs everywhere, which is what lets the sweep reach the state-version 0/1/2 baselines (v0.3.11, v0.3.15, v0.3.16).

**CI cadence** — three triggers, two workflows:

- **Merge to master** (`compat-tests.yml`): the full sweep, every source driver × `COMPAT_SWEEP_BASELINES`, one job each. `OLAKE_COMPATIBILITY_REQUIRE_BASELINE` is set, so an unresolvable baseline is a red run, never a silent skip.
- **PR into staging** (`compat-staging.yml`): baseline = the PR's **base commit**, built from a worktree; candidate = the merge result. Two adjacent builds, so full assertions on the current input shape — any diff is a behaviour change the PR introduces.
- **Dispatch** (`compat-tests.yml`): one baseline against chosen drivers, for bisecting or probing an old tag.

The reverse direction — a state file written by the new binary fed to an old image — is deliberately out of scope. It is not a supported operation.

### Layer 4 — Process guard

- **Catches:** the human step — a bump that lands without a baseline or a fixture.

A checklist item in `.github/pull_request_template.md` for changes that touch `LatestStateVersion`. The Layer-1 meta-test enforces the mechanical parts (history entry, gate present, constant agreement); the checklist covers what a test cannot see.

---

## 6. Runbook — "I am changing…"

### …a source type mapping (e.g. `datatype_conversion.go`)

1. Classify it with §3.3 first — if every affected stream is `full_refresh` and nothing else in §2 is touched, steps 3-5 are optional and a gate would only stop those users from getting the fix (F6).
2. Check all three call sites of the map — the gated converter, `ProduceSchema`, and chunk planning (A4).
3. Check the transition is a legal Iceberg promotion (F1). If it is not, the change breaks existing tables for *new* pipelines too, and needs a migration story, not just a state version.
4. Bump `LatestStateVersion`, add the history entry, gate the value path.
5. Add the Layer-1 rows and a Layer-3 baseline for the previous version.

### …a parse or format rule (`utils/typeutils/reformat.go`)

1. If it is one of the ungated helpers in §2.B, it changes every existing pipeline — bump and gate it, or justify why no output can change.
2. Add the Layer-1 rows.

### …the state file's shape

1. Add a frozen fixture of the *old* shape to Layer 2 before making the change; make it pass by reading both shapes.
2. Confirm the failure mode: does the old shape error, or silently zero-value into a re-backfill? (C4)

### …a config field

1. Keep it additive and optional, with the old default preserved (E1). A renamed key needs to keep accepting the old name.

### …discover output or stream validation

1. Confirm an old `streams.json` still validates — a failure here is a `Warn` and a silently smaller sync, not an error (D3).

### …destination naming or partitioning

1. Confirm existing tables are still targeted; a change writes to a new table instead of failing (F4, F5).

### …nothing, but a user asks how to pick up a fix they are not getting

1. Their pipeline is pinned by its state file (§1). A full refresh will not help on its own — it rebuilds the table but re-writes old-shaped values (F6). Deleting the state file is the only reset, and it costs a full re-backfill.

---

## 7. Known gaps

Documented, not fixed here:

- **C1** — `{}` state file means version 0 (legacy), and `omitempty` makes it indistinguishable from an explicit `0`. A fresh pipeline seeded with an empty state file gets legacy semantics.
- **C3** — `clear-destination` without `--state` writes a state file with no version, downgrading the pipeline to version 0.
- **A4** — type maps are read by ungated paths (`ProduceSchema`, chunk planning) as well as the gated value path.
- **B** — most of `utils/typeutils/reformat.go` has no gate at all.
- **F2** — the Parquet destination has no schema evolution; the break surfaces in the reader.
- **F6** — a full-refresh pipeline still runs at its state file's version, so a gated fix never reaches it: re-syncing rebuilds the table but re-writes old-shaped values. Deleting the state file is the only way out, and it is not something the UI exposes as "pick up the fix".
- **F6 / A4** — after an upgrade, a re-run `discover` puts the new type map in `streams.json` while `--state` still pins the old value semantics, so the recreated table's declared type and its incoming values can come from different versions. Numeric mismatches pass silently; cross-family ones fail at write time rather than at schema evolution.
