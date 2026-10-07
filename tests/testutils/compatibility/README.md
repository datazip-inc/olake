# Backward compatibility tests

## 1. What this suite asserts

A user upgrades their olake driver image and resumes from the state file an
older build wrote. Nothing about that sync may change: same rows, same values,
same destination schema.

The suite asserts this by running the same scenario twice in parallel and
comparing the two destinations:

- **reference run** — every sync on the baseline build
- **upgrade run** — the stateless load on the baseline build; then `discover`
  on the candidate with the existing catalog, so the candidate's merge runs,
  and every sync after it on the candidate

Both sides start from identical data. Any difference between the destinations
is a backward-incompatible change introduced by the candidate.

It therefore serves two purposes:

- **state-version gates keep working.** Code gated on
  `constants.LoadedStateVersion` exists so a resumed old state keeps old
  semantics. If a gate is removed or altered, the upgrade run diverges from the
  reference run and the suite fails.
- **a PR that needs a gate is caught before merge.** If a change alters what
  olake writes and is not gated, the two runs disagree — which is the signal
  that the change needs a state version.

## 2. `constants/state-versions.json`

The manifest of every state version and the release that introduced it.

```json
{
  "state_version": 2,
  "release_tag": "v0.3.16",
  "drivers": "mysql",
  "note": "consistent MySQL timezone handling between CDC and Full Refresh"
}
```

| field | meaning |
| --- | --- |
| `state_version` | the version a state file written by this release carries |
| `release_tag` | the release that introduced it; its image is the baseline |
| `drivers` | the drivers it gates: one name, a comma-separated list, or `*` |
| `note` | what changed, quoted verbatim in the failure report |

### How the compatibility tests use it

With no baseline given, `test.compatibility` runs **all the older releases
with a state version update**: every entry in the manifest, oldest first,
skipping the ones whose `drivers` does not include the driver under test. Each
entry becomes one full reference-vs-upgrade comparison against that release's
image.

So with the manifest at state version 7, mysql is verified against v0.3.11,
v0.3.12, v0.3.16, v0.3.17, v0.4.1 and v0.7.0 — every older release whose state
version update touched it, not just the most recent one. v0.9.1, which
introduced version 7 itself, is not run: its state files carry the version the
candidate writes.

Passing `COMPATIBILITY_BASELINE=<tag|sha|image>` runs that single baseline
instead. CI uses the PR's base commit for the per-PR check, and runs all the
older releases with a state version update in the separate
`backward-compatibility-with-old-releases` job.

> **Adding a state version in a PR: use a pre-release tag.**
> The compatibility tests only reach releases already in the manifest, so a PR
> introducing version N is verified against every version **before** N. The
> entry for N itself needs a `release_tag` that exists as an image — a
> pre-release tag cut from the PR. The next version's run against all the older
> releases with a state version update then picks up N automatically, because
> it is by then a published release in the manifest.

### Ownership

`constants/state-versions.json` and
`tests/testutils/compatibility/compatibility_rules.json` are listed in
`.github/CODEOWNERS` under `@datazip-inc/state-version-owners`, so a new state
version or a new exemption cannot be introduced without those owners reviewing
it. The review is enforced by the repository ruleset "State version approval"
in the GitHub settings, which requires a code owner's approval on pull requests
into `staging` and `master`.

## 3. `compatibility_rules.json`

Some baselines genuinely cannot match today's build — an old release predates
a driver, or has a known bug in a specific column type. Rather than teach the
harness about each case, the exceptions are declared as data:

```json
{
  "data_types": ["set"],
  "assert_value_from": "v0.7.2",
  "note": "M1: SET columns emitted the numeric bitmask before the fix"
}
```

Rules can raise a driver's minimum baseline, exclude a column from the seed
below a version, compare a column by type only, or skip a destination. Each
carries a `note` explaining why.

The point is that adding a driver or an exception is a JSON edit, not a harness
change.

[RULES.md](RULES.md) explains every field and where each entry can go, with
examples and a worked run.

## 4. Harness flow

```text
for each baseline in state-versions.json (applicable to this driver)
  for each group       (iceberg-arrow, iceberg-legacy, parquet)
    for each sync mode (cdc, inc)

        reference run                     upgrade run
        ─────────────                     ───────────
        seed source                       seed source
        discover        @ baseline        discover        @ baseline
        stateless load  @ baseline        stateless load  @ baseline
                                          discover        @ candidate (merge)
        sync (insert)   @ baseline        sync (insert)   @ candidate
        sync (update)   @ baseline        sync (update)   @ candidate
        sync (delete)   @ baseline        sync (delete)   @ candidate
                    ↓                                 ↓
                    └───────── compare ───────────────┘
```

Each side's `streams.json` is what the baseline's `discover` writes for its
freshly seeded table: a user upgrades with the catalog their older build
discovered, so the candidate is tested on that catalog, never on the committed
`streams.template.json`.

When the upgrade side hands off to the candidate, it first re-runs `discover`
on the candidate with that catalog passed as `--catalog streams.json`, the
same merge OLake UI runs when a job is edited after an upgrade. The merge keeps
the old catalog's selected streams, sync mode, cursor field and destination,
and takes everything else, the column types included, from the candidate's own
discover; every candidate sync then runs on the merged catalog. The reference
side never re-discovers.

`discover` never reads a state file, so it applies every state-version gate at
the latest version. A gate that changes discovered types therefore shows up
here as a diff between the two runs, which is exactly the upgrade behaviour
this step exists to test.

Both sides run in parallel on their own source table and, through the
destination database prefix each side hands its discover, their own
destination namespace, then the two destinations are compared: row counts,
per-column values, and the destination schema. Columns that cannot match by
construction — `_olake_timestamp`, CDC log coordinates, server-generated ids —
are compared by type only, per `destination_rules`.

### Example: a failure

```text
STATE VERSION 4 FAILED for mysql -- baseline v0.4.1 is the release that
introduced it
what that state version changed: MySQL unsigned int/integer/bigint map to
Int64; earlier they mapped to Int32 and overflowed

3 scenarios failed:

  legacy/cdc
    column `id_bigint` differs in 3 row(s)
      reference: [123456789012345 ...]
      upgrade:   [123456789012346 ...]

reference run: every sync on olakego/source-mysql:v0.4.1
upgrade run:   the stateless load on olakego/source-mysql:v0.4.1, every sync
               after it on olakego/source-mysql:local
```

The report names the state version, quotes its `note`, and lists the diverging
columns with both sides' values.

### Running it

```bash
# all the older releases with a state version update
make test.compatibility.postgres

# one baseline
make test.compatibility.postgres COMPATIBILITY_BASELINE=v0.6.5

# every driver
make test.compatibility
```
