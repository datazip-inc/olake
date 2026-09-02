# Backward compatibility tests

## 1. What this suite asserts

A user upgrades their olake driver image and resumes from the state file an older build wrote. Nothing about that sync may change: same rows, same values, same destination schema.

The suite asserts this by running the same scenario twice in parallel and comparing the two destinations:

- **reference run** — every sync on the baseline build
- **upgrade run** — the stateless load on the baseline build, every sync after it on the candidate

Both sides start from identical data. Any difference between the destinations is a backward-incompatible change introduced by the candidate.

It therefore serves two purposes:

- **state-version gates keep working.** Code gated on `constants.LoadedStateVersion` exists so a resumed old state keeps old semantics. If a gate is removed or altered, the upgrade run diverges from the reference run and the suite fails.
- **a PR that needs a gate is caught before merge.** If a change alters what olake writes and is not gated, the two runs disagree — which is the signal that the change needs a state version.

## 2. `constants/state-versions.json`

The manifest of every state version and the release that introduced it.

```json
{
  "state_version": 2,
  "release_tag": "v0.3.16",
  "drivers": "mysql",
  "note": "consistent MySQL timezone handling: binlog CDC uses TimestampStringLocation to match the connection's timezone, so CDC timestamps agree with Full Refresh"
}
```

| field | meaning |
|---|---|
| `state_version` | the version a state file written by this release carries |
| `release_tag` | the release that introduced it — the image the sweep runs as the baseline |
| `drivers` | which drivers the version gates: a driver name, a comma-separated list, or `*` |
| `note` | what changed, quoted verbatim in the failure report |

### How the sweep uses it

With no baseline given, `test.compatibility` sweeps **every** entry in the manifest, oldest first, skipping the ones whose `drivers` does not include the driver under test. Each entry becomes one full reference-vs-upgrade comparison against that release's image.

So a driver at state version 7 is verified against v0.3.11, v0.3.15, v0.9.0 and every other applicable release — every upgrade path a user could actually take, not just the most recent one.

Passing `COMPATIBILITY_BASELINE=<tag|sha|image>` runs that single baseline instead. CI uses the PR's base commit for the per-PR check, and the full sweep separately.

> **Adding a state version in a PR: use a pre-release tag.**
> The sweep only reaches releases already in the manifest, so a PR introducing version N is verified against every version **before** N. The entry for N itself needs a `release_tag` that exists as an image — a pre-release tag cut from the PR. The next version's sweep then picks up N automatically, because it is by then a published release in the manifest.

### Ownership

`constants/state-versions.json` and `tests/testutils/compatibility/compatibility_rules.json` are listed in `.github/CODEOWNERS` under `@datazip-inc/state-version-owners`, so a new state version or a new exemption cannot be introduced without those owners reviewing it. The approval count is enforced by the ruleset in `.github/rulesets/state-version-approval.json`.

## 3. `compatibility_rules.json`

Some baselines genuinely cannot match today's build — an old release predates a driver, or has a known bug in a specific column type. Rather than teach the harness about each case, the exceptions are declared as data:

```json
{"data_types": ["set"], "assert_value_from": "v0.7.2", "note": "M1: SET columns emitted the numeric bitmask on the binlog path before the fix"}
```

Rules can raise a driver's minimum baseline, exclude a column from the seed below a version, compare a column by type only, or skip a destination. Each carries a `note` explaining why.

The point is that adding a driver or an exception is a JSON edit, not a harness change.

## 4. Harness flow

```
for each baseline in state-versions.json (applicable to this driver)
  for each group      (iceberg-arrow, iceberg-legacy, parquet)
    for each variant  (cdc, inc)

        reference run                     upgrade run
        ─────────────                     ───────────
        seed source                       seed source
        stateless load  @ baseline        stateless load  @ baseline
        sync (insert)   @ baseline        sync (insert)   @ candidate
        sync (update)   @ baseline        sync (update)   @ candidate
        sync (delete)   @ baseline        sync (delete)   @ candidate
                    ↓                                 ↓
                    └───────── compare ───────────────┘
```

Both sides run in parallel on their own source table and destination namespace, then the two destinations are compared: row counts, per-column values, and the destination schema. Columns that cannot match by construction — `_olake_timestamp`, CDC log coordinates, server-generated ids — are compared by type only, per `destination_rules`.

### Example: a failure

```
STATE VERSION 3 FAILED for mysql -- baseline v0.4.0 is the release that introduced it
what that state version changed: MySQL unsigned int/integer/bigint map to Int64; earlier
they mapped to Int32 and overflowed

3 scenarios failed:

  legacy/cdc
    column `id_bigint` differs in 3 row(s)
      reference: [123456789012345 ...]
      upgrade:   [123456789012346 ...]

reference run: every sync on olakego/source-mysql:v0.4.0
upgrade run:   the stateless load on olakego/source-mysql:v0.4.0, every sync after it on
               olakego/source-mysql:local
```

The report names the state version, quotes its `note`, and lists the diverging columns with both sides' values.

### Running it

```bash
make test.compatibility.postgres                              # full manifest sweep
make test.compatibility.postgres COMPATIBILITY_BASELINE=v0.6.5   # one baseline
make test.compatibility                                       # every driver
```
