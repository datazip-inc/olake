# Compatibility rules

The compatibility tests run every scenario twice: once staying on an old
release, once upgrading to the new build. The two results must match exactly.

A few differences are known and expected, though. An old release may not
have had a driver yet, may have had a bug that a later release fixed, or a
column may hold a different value on every run. `compatibility_rules.json`
lists those exceptions, each with a note saying why. Everything the file does
not mention is compared strictly.

For how the tests themselves work, see the [README](README.md).

## Two kinds of entries

- A **gate** answers "should this old release be tested here at all?"
- A **rule** answers "how should this column be compared?"

## Gates

A gate skips old releases. It has three fields:

| Field | Meaning |
| --- | --- |
| `min_baseline` | skip every release older than this one |
| `skip_baselines` | skip exactly these releases |
| `note` | why; printed when a release is skipped |

### Example: a driver that did not exist yet

mssql shipped in v0.3.15, so older releases have no mssql image to test:

```json
"mssql": {
  "min_baseline": "v0.3.15",
  "note": "first release carrying the driver"
}
```

### Example: a writer broken in one release

v0.3.16's parquet writer added a `data` column to every table, fixed in
v0.3.17. So v0.3.16 is skipped for parquet, while the iceberg writers are
still tested against it:

```json
"parquet": {
  "skip_baselines": ["v0.3.16"],
  "note": "P2: v0.3.16 adds the `data` column unconditionally"
}
```

### Where a gate can go

| Put it in | Matching releases are skipped for |
| --- | --- |
| `drivers.<driver>` | that driver |
| `drivers.<driver>.formats.<format>` | one data format, e.g. s3's xml |
| `destinations.parquet` | the parquet writer, for every driver |
| `destinations.iceberg.modes.<mode>` | the iceberg `arrow` or `legacy` writer |
| `drivers.<driver>.destinations.parquet` | the parquet writer, for one driver |

A driver's own destination gate can only narrow the shared one: the later
`min_baseline` wins, and the skip lists add up.

Nothing older than the oldest release in `constants/state-versions.json`
(v0.3.11) is ever tested, so a gate below it changes nothing.

## Rules

A rule changes how some columns are compared. It is written in two steps.

**1. Pick the columns**, with exactly one of:

| Field | Picks |
| --- | --- |
| `column` | one column, by name |
| `data_types` | every column of these types |

`data_types` match the types the driver's test gives its columns. For mysql,
that is a column's base type (`set`), its unsigned form (`unsigned bigint`)
and its charset (`latin1`).

**2. Say how to compare them**, with at least one of:

| Field | Use it when |
| --- | --- |
| `type_only: true` | the value is different on every run |
| `assert_value_from: "vX"` | releases before vX wrote a wrong value |
| `exclude_below: "vX"` | releases before vX cannot handle the column |

Then add a `note`: what went wrong, and where it was fixed.

### `type_only`: values that can never match

The time olake wrote a row is different on every run, so only the column's
type is compared, against every release:

```json
{
  "column": "_olake_timestamp",
  "type_only": true,
  "note": "olake's write stamp: wall-clock, never value-comparable"
}
```

CDC log positions (`_cdc_lsn`) and ids the database generates (mongodb's
`_id`) are handled the same way.

### `assert_value_from`: an old value bug

Before v0.7.2, mysql's binlog path wrote SET columns as a number. So against
releases older than v0.7.2, SET columns are compared by type only; from
v0.7.2 on, their values are compared too:

```json
{
  "data_types": ["set"],
  "assert_value_from": "v0.7.2",
  "note": "M1: SET columns emitted the numeric bitmask before the fix"
}
```

### `exclude_below`: an old release cannot cope

Releases before v0.7.2 cannot write ucs2 or utf16le text at all: the sync
fails or hangs. So when testing them, those columns are left out of the test
data entirely, on both sides:

```json
{
  "data_types": ["ucs2", "utf16le"],
  "exclude_below": "v0.7.2",
  "note": "non-UTF-8 charset bytes reach the writer as invalid UTF-8"
}
```

Prefer `assert_value_from` when the old release only writes a wrong value:
`exclude_below` stops testing the column altogether.

### Where a rule can go

| Put it in | It covers |
| --- | --- |
| `destinations.rules` | columns every writer adds, for every driver |
| `drivers.<driver>.rules` | the driver's own source columns |
| `drivers.<driver>.destination_rules` | columns the driver adds (`_cdc_*`) |
| `drivers.<driver>.formats.<format>.rules` | one data format's columns |
| `destinations.iceberg.modes.<mode>.rules` | one iceberg writer, see below |

Under an iceberg writer mode, `data_types` are the column types iceberg
reports, such as `timestamp`, and only `assert_value_from` applies:

```json
"legacy": {
  "rules": [
    {
      "data_types": ["timestamp"],
      "assert_value_from": "v0.11.1",
      "note": "#1233: the legacy writer truncated timestamps to ms"
    }
  ]
}
```

## Worked example: mysql against v0.4.1

| Entry | Effect |
| --- | --- |
| no mysql gate; v0.4.1 is newer than v0.3.11 | the release is tested |
| arrow `min_baseline: v0.3.17` | arrow runs: v0.4.1 is newer |
| parquet `skip_baselines: [v0.3.16]` | parquet runs: v0.4.1 is not listed |
| ucs2, utf16le, latin1, unsigned bigint | left out of the test data |
| set, unsigned mediumint | compared by type only |
| `timestamp` columns, legacy writer | compared by type only |
| `_cdc_*` columns and `_olake_timestamp` | compared by type only |

Every other column is compared by type and by value.

## Baselines that are not releases

A commit is read as the newest release it contains, so a commit made after
v0.7.2 follows the rules exactly as v0.7.2 would. A commit with no release in
it, or an image reference, has no version to compare: gates and dated rules
never apply to it, while `type_only` still does.

## Mistakes fail loudly

Every driver's test binary loads the file when it starts, and stops on:

- a misspelled key, e.g. `json: unknown field "min_baselin"`
- an unknown driver, destination or iceberg mode
- a version that is not a release tag like `v0.7.2`
- a rule that picks no columns, picks by both name and type, or does not say
  how to compare

The compatibility tests also stop when they start, on:

- a `data_types` rule that matches no column in the driver's test
- a version at or below the oldest tested release (v0.3.11), since such a
  rule can never take effect: drop it, and keep the story in a `note` the
  way mysql's note about M2 and M3 does
- two rules that give the same column different versions

## Adding an entry

1. Find the release that fixed the problem: that is the version to use.
2. Pick the narrowest place: one data format before a whole driver, one
   driver before every driver.
3. Prefer a rule to a gate: a rule keeps the rest of that release tested.
4. Write a `note`: what broke, and where it was fixed.
5. Expect a review: the file belongs to `@datazip-inc/state-version-owners`,
   and the "State version approval" ruleset requires their approval.
