---
name: pg-iceberg-benchmark
description: Run or rerun the Postgres to Iceberg (AWS Glue) comparison of OLake, Debezium + Kafka Iceberg sink, Flink (CDC source + Iceberg sink, one job), and Debezium + Kafka + Spark, using the scripts in pg-iceberg-benchmark/ of the olake repo. Use when asked to compare, benchmark, re-benchmark, connection-test, or validate these pipelines against a Postgres source and a Glue destination.
---

# Postgres to Iceberg (AWS Glue) comparison: OLake, Debezium, Flink, Spark

The folder `pg-iceberg-benchmark/` at the root of the olake repo holds three documents and one code folder:

| Path | Purpose |
|---|---|
| `SKILL.md` | This file: how to run the benchmark and write the results. |
| `REPORT.md` | Results of the last run, including the ranking. Template for the next report. |
| `features.md` | Numbered feature list for the comparison. Grows on every run. |
| `scripts/` | Everything that runs: test drivers, one folder per pipeline, tools, compose files. `scripts/results/` holds raw results of the last run; it is git-ignored and stays local. |

All commands and script paths below are relative to `scripts/`. Run them from inside that folder. Nothing needs to be generated first: the scripts are ready to run once `bench.env` is filled in.

### Find and use `scripts/` yourself

The user may hand over only this file. Do not ask the user for the scripts and do not write new ones. Locate the existing folder in this order:

1. `scripts/` next to this `SKILL.md`.
2. `pg-iceberg-benchmark/scripts/` in the olake repository on this machine (search the working directory and its parents).
3. The same path in the public olake repository on GitHub (`datazip-inc/olake`); fetch only that folder.

Then use those scripts as they are: copy them to the benchmark host, fill `bench.env`, run the drivers.

If the folder cannot be found in any of the three places, stop and tell the user that `scripts/` is required and where it was looked for. Do not rebuild the scripts from this description unless the user asks for that, and if they do, say that rebuilt scripts are not identical to the originals and their numbers are not directly comparable with `REPORT.md`.

## Ask before starting

Nothing about the environment is fixed in this skill. Ask the user only for items below that they have not already given, in the conversation or in a JSON file (see "If the user gives credentials or settings as JSON"):

1. **Benchmark host:** which machine to run on, how to reach it (SSH alias), and whether it must be started first and how.
2. **Source Postgres:** host, port, database, user, password, SSL mode.
3. **Destination:** S3 warehouse path, AWS region, Glue database name to create or use.
4. **AWS access:** check first with `aws sts get-caller-identity` on the host. If there is no instance role, or it cannot reach Glue and S3, ask for an access key, secret key and, if temporary, session token.
5. **Base table:** existing table to copy from (scripts assume the TPC-H `customer` schema).
6. **Scope:** full load, CDC, or both; CDC workload size; which of the four pipelines.
7. **Names for new objects:** source table, smoke table, publication, run tag.
8. **Snowflake:** do not ask about it. If the user gave Snowflake credentials (in chat or as a `snowflake` block in the JSON), run "Snowflake test". If not, skip it.

### If the user gives credentials or settings as JSON

A user may paste or attach JSON instead of answering the questions one by one: an OLake config (a `source` block and a `destination` block, together or as two files), or any other JSON holding the same kind of values. Then:

1. Read every value the JSON provides. Do not ask for any of them again.
2. Compare what was provided with the required list below.
3. Ask only for required items that are still missing, all in one message.
4. For optional items that are missing, use the defaults and tell the user what was chosen.

Required, and where each value goes:

| Required item | JSON fields that provide it | Goes to |
|---|---|---|
| Source host | `source.host`, `host` | `PGHOST` in `bench.env` |
| Source port | `source.port`, `port` | `PGPORT` (default 5432) |
| Source database | `source.database`, `database` | `PGDATABASE` |
| Source user | `source.username`, `username`, `user` | `PGUSER` |
| Source password | `source.password`, `password` | file `.pgpass_bench`, never `bench.env` |
| SSL mode | `source.ssl.mode`, `sslmode` | `PGSSLMODE` (default `require`) |
| S3 warehouse path | `destination.writer.iceberg_s3_path` | `WAREHOUSE` |
| AWS region | `destination.writer.aws_region` | `AWS_REGION` |
| AWS access | `destination.writer.aws_access_key`, `aws_secret_key`, `aws_session_token` | file `.aws_bench`. If absent, check for an instance role on the host with `aws sts get-caller-identity`; ask only if that fails |
| Benchmark host and how to reach it | `benchmark_host.ssh` (SSH alias or `user@host`), `benchmark_host.wake_command` (optional: command that starts the machine if it is stopped), `benchmark_host.dir` (optional: folder on the host for the scripts, default `~/pg-iceberg-benchmark`) | SSH target for every command. Ask if `benchmark_host.ssh` is absent |
| Snowflake (optional) | `snowflake.account`, `snowflake.user`, `snowflake.password`, `snowflake.warehouse`, `snowflake.org_account`, `snowflake.region` | file `.snowflake_bench`. Block present: run "Snowflake test". Block absent: skip the test, do not ask |
| Base table to copy from | `base_table` | `BASE_TABLE` in `bench.env`. Ask if absent. Check it exists and has the TPC-H `customer` columns |

Field names vary between tools. Match by meaning, not by exact name, and say which field was used for which setting.

Example of a complete input, after which nothing needs to be asked:

```json
{
  "source": {"host": "...", "port": 5432, "database": "...", "username": "...", "password": "...", "ssl": {"mode": "require"}},
  "destination": {"type": "ICEBERG", "writer": {"catalog_type": "glue", "iceberg_s3_path": "s3://...", "aws_region": "..."}},
  "benchmark_host": {"ssh": "<ssh alias>", "wake_command": "<optional>", "dir": "<optional>"},
  "base_table": "tpch.customer",
  "snowflake": {"account": "<optional block>", "user": "...", "password": "...", "warehouse": "...", "org_account": "...", "region": "..."}
}
```

Using the benchmark host:

- If `wake_command` is given, check first whether the host answers on SSH; run the command only if it does not, then wait until SSH works.
- If `dir` already holds the scripts and a filled `bench.env`, reuse them and say so. Do not overwrite `bench.env` or the secret files without telling the user.
- Check on the host before running: Docker works, the source port is reachable, `aws sts get-caller-identity` succeeds or keys were given.

- `destination.writer.catalog_type` must be `glue`. The scripts support only Glue; stop and say so for any other value.
- Optional items and their defaults: `PARALLELISM` 16, `MEM_GB` 32, `COMMIT_INTERVAL_S` 30 (lower the first two when the host is smaller: `MEM_GB` must fit in the host's memory with about 10 GB to spare), Glue database `bench_<tag>`, source table `bench_customer_<tag>`, smoke table `bench_smoke_<tag>`, publication `bench_pub_<tag>`, tag `run1` (next free number if taken), scope full load plus CDC with 10,000,000 rows, Snowflake not included.
- The JSON holds secrets. Do not copy them into the repo, the report, or any command shown to the user.

## Rules

- **Equal settings for all four pipelines, everywhere.** `PARALLELISM`, `MEM_GB` and `COMMIT_INTERVAL_S` in `bench.env` are each applied to every pipeline, in every script: full load, CDC, upsert, schema change. Never tune one pipeline alone and never pass a per-pipeline override.
  - Before a run, print the values each pipeline will use and check they match. After a run, check `results/versions_<tag>.txt`.
  - A number goes into `REPORT.md` only if all four pipelines ran with the same three values. If one pipeline cannot run with them, change the value for all four and run all four again; do not report a mixed set.
  - `REPORT.md` has no "Pipeline settings" section; the three values and the image versions stay in `results/versions_<tag>.txt`. The report does not carry sentences about earlier unequal runs or about reruns that are still needed.
- **Pinned versions.** Image tags are fixed in `env.sh` (`OLAKE_IMAGE`, `DBZ_CONNECT_IMAGE`, `FLINK_IMAGE`) and in the compose files and run scripts. Never use `latest`. Before a run, tell the user which release each pipeline will run and that `features.md` is written from current documentation, which can describe newer releases. Change a version only when the user asks, then run `smoke_all.sh` before the benchmark, because other releases are untested with these scripts. The versions of a run are recorded in `results/versions_<tag>.txt`.
- **This skill runs syncs and reads the tables back.** It does not kill pipelines and does not test deployment tooling.
- Never delete or modify existing objects on the source database. Create new tables, publications and slots only.
- Ask before dropping anything on the source, including slots this benchmark created.
- Prefer the host's instance role for AWS access; containers run with host networking so they can reach it. If the host has no role with access to the Glue database and S3 path, ask the user for AWS keys and put them in `.aws_bench` on the host (see Setup). Never write keys into the repo or into `bench.env`.
- Run pipelines one at a time. They share CPU and the source.
- Keep secrets out of the repo. The database password goes only in `.pgpass_bench` and AWS keys only in `.aws_bench`, both on the benchmark host and git-ignored.

## Host requirements

- Docker with Compose, tmux, Python 3, AWS CLI, `curl`, `rsync` access from the laptop.
- AWS access to read and write the Glue database and the S3 warehouse path: an instance role, or keys supplied by the user.
- Network path to the source Postgres on its port.
- No Go, Java or psql needed; everything runs in containers (`psqlc` in `env.sh` is psql in a container).
- Long runs go in tmux. An SSH command issued from a laptop session is cut off after 10 minutes.

## Setup on the host

1. Copy the code folder: `rsync -az pg-iceberg-benchmark/scripts/ <host>:<dir>/` (git-ignored files are not in the repo, so nothing secret is copied).
2. On the host, in that folder: write `bench.env` yourself from the user's answers or JSON. The user never fills a template. Content:
   ```
   # source Postgres (needs wal_level=logical and a user with replication rights)
   PGHOST=<source host>
   PGPORT=5432
   PGDATABASE=<database>
   PGUSER=<user>
   PGSSLMODE=require
   # destination: Iceberg on AWS Glue
   AWS_REGION=<region>
   WAREHOUSE=s3://<bucket or bucket/prefix>
   GLUE_DB=bench_<tag>
   # source objects. BASE_TABLE must already exist (TPC-H customer columns); 01_create_source creates the other three
   BASE_TABLE=<schema.table>
   SRC_TABLE=bench_customer_<tag>
   SMOKE_TABLE=bench_smoke_<tag>
   PUBLICATION=bench_pub_<tag>
   ```
   Optional lines, applied equally to all four pipelines (defaults shown):
   - `PARALLELISM=16`: OLake `max_threads`, Debezium `snapshot.max.threads`, Kafka partitions and sink tasks, Flink parallelism, Spark `local[N]`.
   - `MEM_GB=32`: memory for the writer of each pipeline (OLake container limit, Kafka Connect heap, Flink TaskManager, Spark driver).
   - `COMMIT_INTERVAL_S=30`: Kafka sink commit interval, Flink checkpoint interval, Spark trigger interval.
   - `OLAKE_IMAGE=olakego/source-postgres:v1.2.0`, `DBZ_CONNECT_IMAGE=quay.io/debezium/connect:3.0.8.Final`, `FLINK_IMAGE=flink:1.19.3`: pin exact tags so a rerun uses the same software.
3. Write the password, one line, to `.pgpass_bench` and `chmod 600` it.
   Only if the host has no usable instance role: create `.aws_bench` with `AWS_ACCESS_KEY_ID=...`, `AWS_SECRET_ACCESS_KEY=...` and, for temporary keys, `AWS_SESSION_TOKEN=...`, then `chmod 600` it. Scripts pass these to every container; without the file the instance role is used.
4. `./dbz/get_plugin.sh` downloads the Iceberg Kafka Connect sink once.
5. `docker build -t bench-poller tools` builds the row-count poller.
6. Create the Glue database if it does not exist: `aws glue create-database --database-input Name=<glue_db>`.
7. `./sql/01_create_source.sh` creates the source copy table, the 2,000-row smoke table and the publication.

## Layout

| Path | Purpose |
|---|---|
| `bench.env` | Site settings, written during setup (step 2). Git-ignored, not in the repo. |
| `env.sh` | Loads `bench.env` and `.pgpass_bench`; sets the shared settings and image tags; defines `psqlc`. |
| `common.sh` | Helpers: `mark`, `start_poller`, `wait_poller`, `start_stats`, `stop_stats`, `src_count`, `trino_props`. |
| `sql/01_create_source.sh` | Creates source table, smoke table and publication. Safe to run again. |
| `sql/02_cdc_load.sh` | Inserts new rows as the CDC workload. Default 100 transactions of 100,000 rows. Keys start above the table's highest key, so it can run again on the same table. |
| `olake/run.sh` | `full\|cdc <source_table> <tag> <target_rows>` |
| `dbz/run.sh` | `kc\|spark full\|cdc <source_table> <tag> <target_rows>` |
| `flink/run.sh` | `full\|cdc <source_table> <tag> <target_rows>` |
| `spark/job.py` | Spark Structured Streaming job used by `dbz/run.sh spark`. |
| `smoke_all.sh <tag>` | Dry run of all four pipelines on the smoke table, full load then CDC. About 10 minutes. |
| `run_all.sh <tag>` | Full benchmark: 4 full loads, CDC workload, 4 CDC catch-ups, then summary. |
| `upsert_all.sh <tag> [rows]` | Update/delete test: all four pipelines in upsert mode on a new small table, then cross-engine reads. About 10 minutes. |
| `schema_change.sh <tag>` | Schema change test: adds a source column and checks which pipelines carry it to Iceberg. Needs `upsert_all.sh` run first with the same tag. |
| `sql/03_upsert_setup.sh`, `sql/04_upsert_load.sh` | Table and insert/delete/update workload used by `upsert_all.sh`. |
| `olake/set_update_type.sh <tag> eq\|pos\|dv` | Switches an existing OLake run to another delete mode; the next sync rewrites equality deletes. |
| `interop/maintain.sh <glue_table>...` | Separate Spark maintenance job (`rewrite_data_files`) that removes delete files. Used for Kafka sink and Flink tables. Output in `results/maintenance/`. |
| `tools/count.py` | Row-count poller. Defines when a run is "done": Glue table shows the target committed rows. A target of `+N` means N more rows than at start. |
| `tools/summarize.py` | `<results_dir> <tag> <full_rows> <rows_after_cdc>`; timings, CPU used per row, memory. |
| `features.md` | One numbered feature table for the comparison, including reliability (two-phase commit) and Parquet format features. Grows by appended rows on every run. |
| `tools/validate.py` | Compares Iceberg tables with source aggregates. |
| `interop/run.sh <tag>` | Iceberg interoperability test: reads every pipeline's table with PyIceberg, Spark, Trino and Flink. Output in `results/interop_<tag>/`. |
| `interop/meta.py` | Prints format version, identifier fields, column types and delete-file counts per table. |
| `horizon_test.sh <tag> pos\|dv [rows]` | Snowflake test: OLake writes into Snowflake's Horizon catalog, then Snowflake SQL is compared with the source. See "Snowflake test". |
| `olake/run_horizon.sh full\|cdc <source_table> <tag>` | OLake pipeline with the Horizon catalog as destination; used by `horizon_test.sh`. |
| `snowflake/sf_run.sh <file>`, `snowflake/sf.py`, `snowflake/sf_horizon_setup.py` | `sf_run.sh` runs a `.sql` or `.py` file from `snowflake/` with the credentials in `.snowflake_bench`; `sf_horizon_setup.py` is the one-time Horizon setup. |

`<tag>` names a run. It sets slot names (`bench_<tool>_<tag>`), Kafka topic prefixes and Glue table names (`<pipeline>_<tag>`).
A tag can be used once per source and Glue database: its slots, offsets, checkpoints and tables remain afterwards.

## Running

1. Check the slot budget: `psqlc -c "select count(*), current_setting('max_replication_slots') from pg_replication_slots"`. Each tag uses 4 slots.
2. Dry run first: `./smoke_all.sh <smoke_tag>`. It catches config errors in minutes. Do not skip it.
3. Full run in tmux:
   `tmux new-session -d -s bench "./run_all.sh <tag> > logs/run_all.driver.log 2>&1"` (create `logs/` first).
4. Watch: `tail -f logs/run_all.driver.log`. The driver stops at the first failed step and prints `CHAIN_ABORTED`; success ends with `ALL_DONE`.
5. Validate:
   `source ./env.sh; docker run --rm --network host $AWS_ENV -v $PWD/tools:/t --entrypoint python bench-poller -u /t/validate.py <glue_db> olake_<tag> dbz_kc_<tag> flink_<tag> dbz_spark_<tag>`
   and compare with `psqlc -c "select count(*), count(distinct c_custkey), sum(c_custkey::bigint), sum(c_acctbal) from public.<source_table>"`.
6. Interoperability: `./interop/run.sh <tag>` (run in tmux; about 5 minutes) and
   `docker run --rm --network host $AWS_ENV -v $PWD/interop:/t --entrypoint python bench-poller -u /t/meta.py <glue_db> olake_<tag> dbz_kc_<tag> flink_<tag> dbz_spark_<tag>`.
7. Feature tests, each in tmux, in this order with one new tag:
   `./upsert_all.sh <tag2>`, then `./schema_change.sh <tag2>`.
   They are correctness tests on a 1,000,000-row table, not speed benchmarks.
   Then the equality-delete checks: `OLAKE_UPDATE_TYPE=pos` with `./olake/set_update_type.sh <tag2> pos` and one more `./olake/run.sh cdc ... +N` sync; `./interop/maintain.sh dbz_kc_<tag2> flink_<tag2>`; then `./interop/run.sh <tag2>` again.
   Slot cost: 4 slots per tag, so the benchmark tag, the smoke tag and this tag need 12 free slots.
8. Copy `results/` from the host back into `scripts/results/` on the working machine, then update `REPORT.md` and `features.md` (both one level above `scripts/`). `results/` is git-ignored: raw results stay local and are not published. Publish them only if the user asks, and scan them first for names, hosts, account numbers and secrets.

## Write modes

- `WRITE_MODE=append` (default): every pipeline only inserts. Used by `run_all.sh` for speed numbers.
- `WRITE_MODE=upsert`: updates and deletes are applied. Set by `upsert_all.sh` and `schema_change.sh`.
  - OLake: `append_mode=false`. Delete format is `OLAKE_UPDATE_TYPE`: `eq` equality deletes (default), `pos` position deletes, `dv` deletion vectors. `pos` and `dv` use a row index stored in `olake/<tag>/index` (`OLAKE_INDEX_DB_DIR`).
  - Debezium + Kafka Iceberg sink: Debezium adds `__op` and rewrites deletes as rows; sink uses `iceberg.tables.cdc-field=__op` and `default-id-columns`. Writes equality deletes.
  - Flink: source `changelog-mode=upsert`, sink table with primary key. Writes equality deletes.
  - Debezium + Spark: `MERGE INTO` per micro-batch on a merge-on-read table. Writes position deletes.
- `SCHEMA_EVOLVE=1` adds `iceberg.tables.evolve-schema-enabled=true` to the Kafka Iceberg sink.
- Row counts from the poller include rows that delete files remove, so in upsert mode use `+N` targets and verify with a query engine.

## Reporting: one home for each thing

Every fact is written in exactly one place. Do not repeat a number or a feature in a second table.

Title of `REPORT.md`: `# Postgres to Iceberg (AWS Glue) comparison: OLake, Debezium, Flink, Spark`. Call the work a comparison in headings, not a benchmark.

| What | Where | Source |
|---|---|---|
| Features of each pipeline, full wording | `features.md`, the numbered table | Documentation, then code |
| Speed, run time, memory, CPU per row, services running | `REPORT.md`, "Ranking" | Measured |
| Reliability facts seen in the runs | `REPORT.md`, "Reliability observed" | Measured |
| Results of feature tests (updates, deletes, schema change, cross-engine reads) | `REPORT.md`, "Measured test results" and the test sections after it | Measured |
| Row counts and sums against source | `REPORT.md`, "Data validation" | Measured |

`REPORT.md` structure, in order: Result (two sentences and notes), Ranking, Reliability observed, Measured test results, one section per test (Snowflake only when it was run), Data validation, What was tested, Objects created, Raw data.

- **`REPORT.md` has no feature table and no resource table.** Feature comparison lives only in `features.md`; the report links to it. CPU and memory appear only in "Ranking".
- **"Measured test results" holds test outcomes only,** one row per test. Never add speed, memory, CPU, run time or services rows to it.
- **When a test result adds to or differs from the documentation,** write it under "Where the tests add to the documentation" in `REPORT.md`. Do not change the documentation-based cell in `features.md`.

- **Ranking covers measured metrics only:** speed (full load and CDC catch-up), peak memory (full load and CDC), and the measured parts of cost: run time on the same machine, CPU used per row (core-seconds per 1M rows), and services running between syncs. Print the numbers in the ranking table. Never label CPU per row as "cost" on its own, and never call a pipeline cheapest or most expensive overall: total cost needs a verified machine price, running hours and data volume. If the user supplies those, compute money per pipeline and show the inputs. Do not rank features, interoperability or anything from documentation; those belong in `features.md` as rows. Do not compute an overall winner unless the user gives weights.
- **Continuous mode is a feature row** (`features.md` row 26), not a ranking: state per pipeline whether it can run nonstop.
- **No redundant tables.** Each number appears in one table only. `REPORT.md` opens with the result sentences, then "Ranking". Do not add a per-pipeline timing table, a summary table, a resource table or a feature table to the report. Before adding any table, check that its numbers are not already shown.
- **No assumptions, anywhere.** Do not rank, score or describe anything from general belief. If a use case has no measurement and no documentation behind it (for example "stable, battle-tested"), leave it out and say it is not ranked. Report observed facts in a facts table instead of a ranking. Add a use case to the ranking only after adding a test or a documented source for it.
- **Speed, cost and memory figures** come from `tools/summarize.py`: `seconds`, `rows_per_sec`, `core_seconds_per_million_rows` (CPU used per row), `avg_mem_gb` and `peak_mem_gb`.
- **CPU used per row** is average cores times run time per 1M rows on the benchmark host. It excludes the source database, Kafka disk and S3. Do not turn it into dollars without a verified instance price.
- **Memory goes in the ranking table, ordered by measured peak, lowest first.** Add one row per phase (full load, CDC) and state the memory limit each pipeline had in the row label. Under the table, add the note: a JVM uses the heap it is given, so the figure of a JVM pipeline shows the limit more than the demand.
- **Do not assign separate ranks inside the noise.** When two neighbouring places differ by less than the noise stated in the report (about 15% for single runs), say so in the note under the table.
- **Files written and correctness rows** come from `tools/validate.py`.
- **Iceberg interoperability rows** mean "can other engines read the table this pipeline wrote", not "which catalogs the tool supports". They come from `interop/run.sh` and `interop/meta.py`. Report only engines actually run; list the rest as not tested. Do not claim catalog support that was not exercised.
- **Feature rows come from official documentation.** For feature comparison (the rows in `features.md`: Iceberg interoperability, upserts and deletes, schema evolution, GitOps, delivery guarantee, reliability and two-phase commit, parallel load, maintenance, monitoring, catalogs, sources, destinations, license, plus appended Parquet and Iceberg format features), trust what the docs state. No test is needed for a feature the docs state.
- **Search the documentation sources, do not skim rendered pages.** Page-by-page reading missed whole sections twice (OLake Fusion, OLake GitOps). Clone the docs repositories with `git clone --depth 1` into a scratch folder and search them with `grep`:
  - `datazip-inc/olake-docs` (`docs/`): list every top-level section first. It has `fusion/` (maintenance, compaction, cleanup), `install/gitops/`, `monitoring-and-observability/`, `api/`, `features/`, `understanding/`, `connectors/`, `writers/`, `getting-started/`, `benchmarks/`.
  - `apache/iceberg` (`docs/docs/`): `kafka-connect.md`, `flink-writes.md`, `flink-maintenance.md`, `spark-structured-streaming.md`, `spark-writes.md`, `spark-procedures.md`, `maintenance.md`, `configuration.md`.
  - `apache/flink-cdc` (`docs/content/docs/`): `connectors/flink-sources/postgres-cdc.md`, `connectors/pipeline-connectors/postgres.md` and `iceberg.md`, `core-concept/schema-evolution.md`, `examples-and-best-practices/postgres-to-iceberg.md`, `deployment/`.
  - `debezium/debezium` (`documentation/modules/ROOT/pages/`): `features.adoc`, `connectors/postgresql.adoc`, `transformations/`, `operations/` (operator, platform, server, kubernetes).
  - `apache/flink-kubernetes-operator` (`docs/content/docs/`).
- **Order of evidence: docs first, then code, then "does not exist".** If the docs are silent on a feature, search that project's code once (for example option names such as `cdc-field`, `upsert`, `compaction`). Write "does not exist" only when both docs and code have nothing. Do not mark cells with their source: no "(code)" or "(docs and code)" tags.
- **Phrase cells the way the docs do.** Example for interoperability: OLake supports it directly in the writer, Flink through a maintenance task, the Apache Kafka Iceberg sink does not have it.
- **Know the product splits before judging a feature missing:** OLake maintenance is OLake Fusion (separate docs section and repository `datazip-inc/olake-fusion`); OLake delete format is chosen by the job setting "Target Query Engines"; the Kafka Iceberg sink exists as the Apache build (no upsert) and an older unmaintained Tabular build (upsert); Flink has SQL mode and YAML pipeline mode with different features and guarantees; Debezium Server has its own Iceberg sink.
- **Record doc conflicts and unsearched sources** in `features.md`, "Notes on the documentation", instead of picking one silently.
- **Only speed, memory, CPU, run time, file counts and data correctness must be measured.** Features come from documentation and stay in `features.md`.
- Updates and deletes and schema change have their own scripts (see Running step 7). Mark those rows measured only after running them.
- Report delete handling per pipeline: which delete files it wrote, whether deleted rows are really gone, and which engines could read the table.
- Do not report features that no script exercised (other destinations, other source databases, other catalogs, engines not run). Leave them out instead of listing them as untested.
- `REPORT.md` has no "Limits of this run" and no "Problems found on the way" section. Keep the single-run noise sentence in "Result" and the notes under "Ranking".
- **GitOps and crash recovery are not tested.** GitOps is a documentation row in `features.md` (row 4); recovery is the documentation row 6. Do not write scripts for them and do not add measured rows for them to the report.

## Refreshing the feature list (every run)

`features.md` (next to this file, one level above `scripts/`) is one numbered table of features that only grows. Do this on every run, before writing the report:

1. **Fetch the Parquet format sources:** `git clone --depth 1 https://github.com/apache/parquet-format` into a scratch folder. Read `CHANGES.md` (version sections newer than the commit in the "Source state" table of `features.md`), `LogicalTypes.md`, `Encodings.md`, `Compression.md`, and any feature document in the repository root (for example `VariantShredding.md`, `Geospatial.md`, `BloomFilter.md`, `PageIndex.md`, `Encryption.md`, `AlpEncoding.md`).
2. **Append new features as new rows** at the bottom of the table, with the next number, the prefix `Parquet:` and today's date in the "Added" column. Never remove, reorder or renumber existing rows.
3. **Do the same for Iceberg:** read `format/spec.md` and the release notes in `apache/iceberg`, append rows with the prefix `Iceberg:`.
4. **Fill support per pipeline** for new rows and re-check existing rows: docs first, then code once, then "does not exist". Debezium, Flink and Spark pipelines write Parquet through the Iceberg library, so check Iceberg `configuration.md` (`write.parquet.*`) and the engine type tables (`flink.md`, `spark-getting-started.md`). For OLake check `olake-docs`, then the olake repository.
5. **Add a line to "Source state"** for each repository read: run date, commit, date.
6. **Do not copy feature rows into `REPORT.md`.** The report links to `features.md`.

Reliability is a permanent row (row 6): always report whether each pipeline has two-phase commit or an equivalent, where it keeps commit state, and what its docs say about recovery after failure. For OLake this is two-phase commit with state in the destination table (`olake_2pc`).

## Snowflake test (only when Snowflake credentials are given)

OLake writes an Iceberg table into Snowflake's Horizon catalog with deletion vectors, then Snowflake SQL is compared with the source. OLake only; 1,000,000 rows; a correctness test, not a speed benchmark. No AWS IAM role is needed.

1. Put Snowflake values in `.snowflake_bench` in the scripts folder on the host (`chmod 600`): `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_ORG_ACCOUNT`, `SNOWFLAKE_REGION` (Snowflake region name of the account, asked from the user) and `SNOWFLAKE_ALLOWED_IP` (public IP of the benchmark host). Take them from what the user gave. `SNOWFLAKE_ALLOWED_IP` is read from the host. Ask in one message only for values that are missing; none has a default.
2. One-time setup: `./snowflake/sf_run.sh sf_horizon_setup.py`. It writes the token to `snowflake/.pat` (git-ignored). Add that value to `.snowflake_bench` as `SNOWFLAKE_PAT`, then delete `snowflake/.pat`.
3. Run in tmux: `./horizon_test.sh <tag> dv`.
4. Pass: in `results/horizon_<tag>.txt` the Snowflake row count, distinct keys and `sum(c_acctbal)` equal the source, and the CDC log shows `addedDVs` above 0 with `totalEqualityDeletes` 0.
5. Report the result in `REPORT.md`, "Snowflake", in one or two sentences.

## Connection tests only (no sync)

- **OLake source** (after `source ./env.sh`): `docker run --rm --network host -v <dir>:/mnt/config $OLAKE_IMAGE check --config /mnt/config/source.json`
- **OLake destination:** same with `check --destination /mnt/config/destination.json`. It returns `SUCCEEDED` without writing a file, so it proves Glue access, not S3 write.
- **Debezium source:** `PUT /connector-plugins/io.debezium.connector.postgresql.PostgresConnector/config/validate` on Kafka Connect. Creates nothing on the source.
- **Flink:** no test mode. A JDBC catalog `SHOW TABLES` proves login; only a real job proves replication.
- **Kafka Iceberg sink and Flink Iceberg sink:** no test mode. Write a few rows to a throwaway table.
- **Source prerequisites to check with SQL:** `wal_level=logical`, user has replication rights, free replication slots, `pgoutput` available, tables have a primary key.

## Known traps

- **Host network.** Without it containers cannot reach instance-role credentials.
- **Temporary AWS keys expire.** With a session token, check its lifetime covers the whole run; a long-running pipeline fails mid-run when it lapses. Instance-role credentials renew on their own.
- **Key path is untested.** The finished run used an instance role only; `.aws_bench` wiring has not been exercised end to end.
- **OLake `initial_wait_time`.** Values below 120 are ignored and replaced by 10800 seconds.
- **OLake needs slot and publication to exist** before a CDC sync. `olake/run.sh` creates the slot.
- **OLake parallelism** is `max_threads` in the source config; `olake/run.sh` uses `PARALLELISM`.
- **Iceberg Kafka Connect sink** misses its first commit trigger after start and needs steady traffic to answer triggers. A single test record looks like a failure (`committed to 0 table(s)`); send several records over 30+ seconds.
- **Iceberg Kafka Connect sink build.** Apache Iceberg publishes no ready runtime zip. `dbz/get_plugin.sh` fetches `iceberg-kafka-connect-runtime-0.6.19` from the `databricks/iceberg-kafka-connect` GitHub releases, class `io.tabular.iceberg.connect.IcebergSinkConnector`.
- **`flink cancel` is broken** by the Hadoop uber jar (`NoSuchMethodError` in commons-cli). Cancel with `curl -X PATCH "localhost:8081/jobs/<id>?mode=cancel"`.
- **Flink resume** uses the last retained checkpoint as `execution.savepoint.path`. Wait one checkpoint interval after the last commit before cancelling.
- **Flink CDC slots.** Keep `scan.incremental.snapshot.backfill.skip=true`, or each of 16 readers opens its own temporary slot and a low `max_replication_slots` is exceeded. Only safe while the table is not changing during the snapshot.
- **Postgres plugin.** Use `pgoutput`. Managed Postgres often has no `wal2json`.
- **`pkill -f run.sh` over SSH** kills the SSH command itself, because the pattern is in its own command line.
- **Slot budget.** A full set of tests (benchmark, smoke and upsert tags) leaves 12 benchmark slots. Count slots before every run and get agreement to drop old ones when fewer than 4 are free.
- **Inactive slots hold WAL.** List slots and their lag before and after a run, and check `max_slot_wal_keep_size`.
- **Source disk space.** Copying the base table writes the table plus the same amount of WAL. Check free storage on the source first; the benchmark host's role may not be able to.
- **Quoting over SSH.** SQL with single quotes inside an `ssh '...'` command breaks silently. Write scripts locally, copy them with `rsync`, then run them.
- **Flink SQL files** take one statement per line; two `SET` statements on one line fail with `only single statement supported`.
- **Double columns drift across engines.** Debezium with `decimal.handling.mode=double` gives sums that differ in the last digits between engines. Use `precise` when exact decimals matter.
- **Athena** needs `athena:StartQueryExecution` on the host role; without it, use Trino as the closest stand-in.
- **OLake keeps deleted rows as tombstones.** A deleted source row stays in the Iceberg table with `_op_type = 'd'` and null source columns. Row counts match the source only with `WHERE _op_type <> 'd'`.
- **Equality deletes limit readers.** PyIceberg 0.8 refuses tables with equality deletes. OLake removes them itself: with `update_type = pos` the next sync rewrote all equality deletes as position deletes and PyIceberg could read the table. The Kafka Iceberg sink and Flink sink always write them; removing them takes a separate maintenance job (`interop/maintain.sh`), which must be repeated because the writers keep producing them.
- **PyIceberg version matters.** 0.8.1 returned extra rows on a table with position deletes written in the same commit as the data; 0.12.0 was correct. The poller image pins 0.8.1 for row counting only; use a current PyIceberg for interoperability reads.
- **Run maintenance only on parked pipelines.** `rewrite_data_files` was tested with writers stopped; running it against a live upsert writer was not tested.
- **Flink and Spark drop new source columns silently.** Their schema is fixed in the job; the pipeline keeps running and the new column never reaches Iceberg.
- **Flink updates on a table without `REPLICA IDENTITY FULL`:** the script uses `changelog-mode=upsert`, which worked. The default changelog mode was not tried on such a table.
- **Debezium 3 delete handling** uses `delete.tombstone.handling.mode=rewrite` (not the older `delete.handling.mode`).
- **Schema is fixed.** `flink/run.sh` and `spark/job.py` hard-code the TPC-H `customer` columns. Another base table needs those two files changed.

## Cleanup (ask first)

- **Source:** slots named `bench_*`, the source and smoke tables, and the publication from `bench.env`. Drop only inactive benchmark slots:
  `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 'bench\_%' AND NOT active;`
- **Glue / S3:** tables in the Glue database from `bench.env` and their files under `<warehouse>/<glue_db>.db/`.
- **Host:** `docker compose down -v` in `dbz/` and `flink/` removes Kafka data and Flink containers. This also removes Debezium offsets, so those tags cannot resume.
- **Snowflake (if used):** the database, role, service user, network rule, network policy and token created by `sf_horizon_setup.py`, and the `bench_upsert_<tag>` source table and `bench_olake_<tag>` slot of the Horizon tag.
- **Files holding secrets in plain text on the host:** `.aws_bench` (if used), `.snowflake_bench` and `snowflake/.pat` (if used), `.pgpass_bench`, `olake/<tag>/source.json`, `dbz/*-src.json`, `flink/work/*.sql`.
