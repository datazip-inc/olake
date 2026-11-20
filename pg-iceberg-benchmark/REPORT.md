# Postgres to Iceberg (AWS Glue) comparison: OLake, Debezium, Flink, Spark

Run date: 2026-11-19 (UTC). Benchmark host: one c7i.16xlarge (64 vCPU, 123 GiB) in the same region as source and destination.
Scripts and raw results are in `scripts/` next to this file (`pg-iceberg-benchmark/scripts/` in the olake repo). Full tool logs stay on the benchmark host. How to rerun: `SKILL.md`.

## Result

All four pipelines connected, loaded, resumed for CDC, and produced Iceberg tables that match the source exactly.
There was no authentication or permission failure in any pipeline.

Time is measured from tool start until the Iceberg table in Glue shows the full committed row count.

"Flink (CDC source + Iceberg sink, one job)" is a single Flink job that reads Postgres with the Flink CDC connector and writes Iceberg with the Iceberg Flink connector. There is no Kafka in that pipeline; the two connectors are separate add-on jars joined by one SQL statement.

Flink was fastest on full load; OLake was fastest on CDC catch-up.
These are single runs, so treat differences under about 15% as noise.

### Ranking

Rank 1 is best. Rankings cover only what was measured: speed and the measured parts of cost. Memory rows are ordered by measured peak, lowest first; see the memory note below the table before comparing them. Features are not ranked; they are compared in `features.md`.

| Ranked on | 1st | 2nd | 3rd | 4th | Numbers |
|---|---|---|---|---|---|
| **Speed, full load** | Flink | OLake | Debezium + Kafka + Spark | Debezium + Kafka + Iceberg sink | 459k, 350k, 127k, 101k rows/s |
| **Speed, CDC catch-up** | OLake | Debezium + Kafka + Spark | Flink | Debezium + Kafka + Iceberg sink | 97k, 67k, 65k, 58k rows/s |
| **Run time on the same machine, full load** | Flink | OLake | Debezium + Kafka + Spark | Debezium + Kafka + Iceberg sink | 98, 129, 353, 444 s |
| **Run time on the same machine, CDC** | OLake | Debezium + Kafka + Spark | Flink | Debezium + Kafka + Iceberg sink | 103, 149, 154, 172 s |
| **CPU used per row, full load** | Debezium + Kafka + Iceberg sink | Flink | Debezium + Kafka + Spark | OLake | 21, 24, 39, 41 core-seconds per 1M rows |
| **CPU used per row, CDC** | Debezium + Kafka + Spark | Flink | Debezium + Kafka + Iceberg sink | OLake | 17, 21, 29, 48 core-seconds per 1M rows |
| **Peak memory, full load** (32 GB limit each) | OLake | Debezium + Kafka + Iceberg sink | Flink | Debezium + Kafka + Spark | 5.4, 14.2, 21.3, 23.1 GB |
| **Peak memory, CDC** (32 GB limit each) | OLake | Debezium + Kafka + Iceberg sink | Flink | Debezium + Kafka + Spark | 1.2, 9.8, 16.5, 23.9 GB |
| **Services running between syncs** | OLake | Flink | Debezium + Kafka + Iceberg sink | Debezium + Kafka + Spark | 0, 2, 2, 3 (none; JobManager and TaskManager; Kafka and Kafka Connect; Kafka, Kafka Connect and Spark) |

- **Speed:** single runs; differences under about 15% are noise. On CDC catch-up, places 2 to 4 are each within 15% of the next place (67k, 65k, 58k rows/s), so only OLake's lead is outside the noise. On full load all four places are outside the noise.
- **Cost is not one number here.** Total cost was not measured. Three measured parts are ranked separately:
  - *Run time on the same machine*: what you pay when renting a fixed machine by the hour.
  - *CPU used per row*: what you pay when billed per core used. Rough figure from 8 to 37 samples per run. On full load, places 1 and 2 (21 and 24) are within the noise of each other, and so are places 3 and 4 (39 and 41). Debezium + Kafka + Iceberg sink is lowest on full load and also the slowest: few cores for a long time.
  - *Services running between syncs*: what keeps billing when no data is moving. OLake exits after a sync; the other three keep services up. Their idle cost was not measured.
  A money figure needs a verified machine price, hours of running per day and data volume, none of which this comparison has.
- **No pipeline is called cheapest or most expensive overall, and no overall winner is computed:** that would need prices and weights this report does not have.

### Reliability observed

Facts from the runs, not a ranking:

| | OLake | Debezium + Kafka + Iceberg sink | Flink | Debezium + Kafka + Spark |
|---|---|---|---|---|
| Data correct after stop and resume | Yes | Yes | Yes | Yes |
| Workarounds needed in the runs | None | Upsert only in older unmaintained build | Job cancelled over REST, not with `flink cancel` | Custom merge code for updates and deletes |
| Commit and recovery mechanism (documentation) | Two-phase commit, state in the table | Commit coordination over Kafka control topic | Two-phase commit through checkpoints | Nothing documented |

## Measured test results

The Kafka Iceberg sink in all runs is the older Tabular build 0.6.19; its upsert results do not apply to the current Apache sink, which has no upsert. Flink ran in SQL mode, not YAML pipeline mode.

|                                                                                      | OLake                                                                                                              | Debezium + Kafka + Iceberg sink                                            | Flink (CDC source + Iceberg sink, one job)            | Debezium + Kafka + Spark           |
| ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------- | ----------------------------------------------------- | ---------------------------------- |
| **Data files written** (measured)                                                    | 11                                                                                                                 | 19                                                                         | 128                                                   | 304                                |
| **Data correct after resume** (measured)                                             | Yes                                                                                                                | Yes                                                                        | Yes                                                   | Yes                                |
| **Parallel load of one table** (measured, 16 threads requested for all)              | Yes                                                                                                                | No: `snapshot.max.threads=16` was set, Debezium 3.0.8 used 1 worker thread | Yes                                                   | No: same Debezium source           |
| **Decimal column in Iceberg** (measured, as configured)                              | decimal                                                                                                            | double                                                                     | decimal                                               | double                             |
| **Updates applied correctly** (measured)                                             | Yes                                                                                                                | Yes                                                                        | Yes                                                   | Yes                                |
| **Deleted rows** (measured)                                                          | Kept as tombstone rows (`_op_type = 'd'`, source columns null)                                                     | Removed                                                                    | Removed                                               | Removed                            |
| **Delete files written in upsert mode** (measured)                                   | Equality by default; position after `update_type = pos`; deletion vectors with `update_type = dv` (Snowflake test) | Equality                                                                   | Equality                                              | Position                           |
| **Getting rid of equality deletes** (measured)                                       | Built in: set `update_type = pos`, next sync rewrote them                                                          | Separate Spark maintenance job (`rewrite_data_files`)                      | Separate Spark maintenance job (`rewrite_data_files`) | Not needed                         |
| **New source column** (measured)                                                     | Added automatically                                                                                                | Added with `evolve-schema-enabled=true`                                    | Dropped silently, job kept running                    | Dropped silently, job kept running |
| **Append-only table read by Spark 3.5, Trino 476, Flink 1.19, PyIceberg** (measured) | Yes, all four                                                                                                      | Yes, all four                                                              | Yes, all four                                         | Yes, all four                      |
| **Upsert table read by Spark, Trino, Flink** (measured)                              | Correct                                                                                                            | Correct                                                                    | Correct                                               | Correct                            |
| **Upsert table read by PyIceberg** (measured)                                        | No with equality deletes; yes after `update_type = pos`                                                            | No; yes after Spark maintenance job                                        | No; yes after Spark maintenance job                   | Yes                                |
| **Iceberg format version** (measured, Glue tables)                                   | 2                                                                                                                  | 2                                                                          | 2                                                     | 2                                  |
| **Extra columns added to table** (measured)                                          | 5: `_olake_id`, `_olake_timestamp`, `_op_type`, `_cdc_timestamp`, `_cdc_lsn`                                       | None                                                                       | None                                                  | None                               |
| **Row identity declared in Iceberg** (measured)                                      | `_olake_id`                                                                                                        | None                                                                       | `c_custkey`                                           | None                               |
| **Source NOT NULL kept** (measured)                                                  | No, all source columns nullable                                                                                    | Yes, all columns                                                           | Key column only                                       | No                                 |
|                                                                                      |                                                                              

### Where the tests add to the documentation

- Debezium 3.0.8 with `snapshot.max.threads=16` logged `Creating snapshot worker pool with 1 worker thread(s)` for the one-table snapshot. The setting did not split the table in the release that was run. `features.md` row 7 describes the current documentation.
- Flink in SQL mode dropped a new source column, consistent with nothing being documented for that mode.
- Equality deletes written by the older Kafka sink build and by Flink were removed with Spark `rewrite_data_files`. Flink `ConvertEqualityDeletes` and OLake Fusion were not run.
- OLake kept deleted rows with `_op_type = 'd'`, as its documentation says.

### Iceberg interoperability (measured)

Every table was read back by four engines, each computing row count, distinct keys and `sum(c_acctbal)`:

| Reader | `olake_run3` | `dbz_kc_run3` | `flink_run3` | `dbz_spark_run3` |
|---|---|---|---|---|
| Spark 3.5.3 (Iceberg 1.7.1) | Correct | Correct | Correct | Correct |
| Trino 476 | Correct | Correct | Correct | Correct |
| Flink 1.19.3 (Iceberg 1.7.1) | Correct | Count correct; sum off by 0.06 | Correct | Count correct; sum off by 0.03 |
| PyIceberg 0.8.1 | Correct | Correct | Correct | Correct |

- "Correct" means 55,000,000 rows, 55,000,000 distinct keys and sum 247486409332.26, same as the source.
- The two small sum differences are in the Debezium tables, where `c_acctbal` is stored as double. Floating-point sums depend on the order an engine adds rows. The OLake and Flink tables store decimal and gave the exact sum in every engine.
- These reads are of the append-only tables, which contain no delete files. Reads of tables with delete files are in "Updates and deletes".
- The OLake table carries a table property `olake_2pc` holding sync state. All four engines ignored it.

Script: `scripts/interop/run.sh <tag>`. Raw output: `scripts/results/interop_<tag>/`.

## Snowflake

Snowflake was tested with OLake and it worked: OLake wrote a 1,000,000-row table into Snowflake's Horizon catalog with deletion vectors (CDC commit: `addedDVs=1`, 300,000 position deletes, `totalEqualityDeletes=0`). After 100,000 inserts, 100,000 deletes and 200,000 updates, Snowflake SQL returned the same row count, distinct keys and `sum(c_acctbal)` as the source (1,000,000; 1,000,000; 4,497,216,203.00), plus 100,000 tombstone rows.

## Updates and deletes

Test: new source table of 1,000,000 rows, all pipelines in upsert mode. After the full load: 100,000 inserts, 100,000 deletes, 200,000 updates. Source afterwards: 1,000,000 rows, `sum(c_acctbal)` 4,497,216,203.00. Script: `scripts/upsert_all.sh`.

| Table written by | Delete files | Rows read by Spark, Trino, Flink | Sum | PyIceberg 0.8.1 and 0.12.0 |
|---|---|---|---|---|
| OLake (`update_type = eq`, default) | 300,000 equality deletes | 1,100,000 | Matches source | Refuse: equality deletes not supported |
| Debezium + Kafka + Iceberg sink | 300,000 equality deletes | 1,000,000 | Matches source | Refuse |
| Flink (one job) | 300,000 equality deletes | 1,000,000 | Matches source | Refuse |
| Debezium + Kafka + Spark | 300,000 position deletes | 1,000,000 | Matches source | Correct |

- **Updates** arrived correctly in all four pipelines.
- **Kafka Iceberg sink result is for the older Tabular build.** The current Apache sink has no upsert or delete handling, so it could not have run this test.
- **OLake keeps deleted rows,** as its documentation states. Each deleted source row stays as a row with `_op_type = 'd'` and null source columns. That is why its table has 100,000 more rows. Sums match because nulls add nothing. A reader gets the source row count only with `WHERE _op_type <> 'd'`.

### Removing equality deletes

Run after the schema change test and further insert rounds on the same tables.

| Pipeline | Method | Result |
|---|---|---|
| OLake | Set stream `update_type` to `pos` and run the next sync | Log: `rewrote 2 equality delete file(s) as 301000 pos delete(s)`. Table then had 0 equality deletes and 301,000 position deletes. PyIceberg 0.8.1 and 0.12.0, Spark, Trino and Flink all read 1,122,000 rows with the source sum 4,501,637,450.21. |
| Debezium + Kafka + Iceberg sink | Separate Spark job: `rewrite_data_files` with `rewrite-all` and `remove-dangling-deletes` | 10 data files rewritten into 1, 0 delete files left. PyIceberg 0.8.1 and 0.12.0 then read the table: 1,011,000 rows, same rows and sum as Spark, Trino and Flink. |
| Flink (one job) | Same separate Spark job | 144 data files rewritten into 1, 0 delete files left. PyIceberg then read the table: 1,021,000 rows, same as the three engines. |

- 1,122,000 = 1,022,000 source rows at that point + 100,000 tombstone rows.
- The Kafka sink, Flink and Spark tables hold fewer rows than the source at that point (1,011,000, 1,021,000 and 1,016,000 against 1,022,000) because each streaming pipeline was parked after its own insert rounds and did not receive rows inserted afterwards. The gaps equal those later inserts exactly (11,000, 1,000 and 6,000 rows).
- For OLake the conversion is part of the writer. For the Kafka sink and Flink it is a separate job to schedule. Both writers keep producing equality deletes, so it has to run again after new changes. It was run once here, with the pipelines parked.
- Per the Iceberg documentation: `rewrite_position_delete_files` handles position deletes only; Flink table maintenance has a `ConvertEqualityDeletes` task that needs table format version 3. Neither was run here.
- Documented but not run: OLake Fusion Lite compaction, which converts equality deletes to position deletes on a schedule and also works on tables created outside OLake.

Scripts: `scripts/olake/set_update_type.sh`, `scripts/interop/maintain.sh`. Raw output: `scripts/results/interop_<tag>_eq/` and `scripts/results/interop_<tag>_pos/` (one copy per delete format), `scripts/results/maintenance/`.

## Schema change

Test: `ALTER TABLE ... ADD COLUMN c_tier int` on the source while pipelines were parked, then 1,000 updates and 1,000 inserts that set the column. Script: `scripts/schema_change.sh`.

| Pipeline | Column in Iceberg table | Rows with value | Pipeline after the change |
|---|---|---|---|
| OLake | Yes, added automatically | 2,000 of 2,000 | Ran normally |
| Debezium + Kafka + Iceberg sink | Yes, with `iceberg.tables.evolve-schema-enabled=true` | 2,000 of 2,000 | Ran normally |
| Flink (one job) | No | 0 | Ran normally; column ignored without warning |
| Debezium + Kafka + Spark | No | 0 | Ran normally; column ignored without warning |

Flink and Spark have the column list fixed in the job (SQL table definition, `from_json` schema). Only an added column was tested; dropped, renamed and retyped columns were not.

Flink documents schema evolution for YAML pipeline mode (Postgres pipeline source with `schema-change.enabled`, Iceberg pipeline sink), including a Postgres-to-Iceberg example with an added column. That mode was not run here; its Iceberg sink is documented as at-least-once. The SQL mode run here uses the Iceberg Flink sink, documented as exactly-once, with a source set to `scan.incremental.snapshot.backfill.skip=true`, which the Flink CDC documentation describes as at-least-once.

## Data validation

Source table after CDC workload, compared with each Iceberg table:

| Table | Rows | Distinct keys | sum(c_custkey) | sum(c_acctbal) | Data files | Size | Snapshots |
|---|---|---|---|---|---|---|---|
| Source (Postgres) | 55,000,000 | 55,000,000 | 2062500027500000 | 247486409332.26 | | | |
| `olake_run3` | 55,000,000 | 55,000,000 | 2062500027500000 | 247486409332.26 | 11 | 3.20 GB | 6 |
| `dbz_kc_run3` | 55,000,000 | 55,000,000 | 2062500027500000 | 247486409332.26 | 19 | 3.12 GB | 19 |
| `flink_run3` | 55,000,000 | 55,000,000 | 2062500027500000 | 247486409332.26 | 128 | 3.24 GB | 8 |
| `dbz_spark_run3` | 55,000,000 | 55,000,000 | 2062500027500000 | 247486409332.26 | 304 | 3.26 GB | 19 |

No loss and no duplicates in any pipeline, including across the stop and resume between full load and CDC.
OLake wrote the fewest files; Spark wrote the most.

## What was tested

- **Source:** RDS PostgreSQL 16.15 (staging), SSL `require`.
- **Source table:** `public.bench_customer_run3`, a new copy of `tpch.customer` (45,000,000 rows, 8 columns; 11 GB with index after the CDC workload).
- **Destination:** Iceberg on AWS Glue with an S3 warehouse, region `ap-south-1`, in a new Glue database `bench_run3`.
- **Credentials:** none in any Glue pipeline config. Every pipeline used the benchmark host's instance role. The Snowflake test used a Snowflake programmatic access token.
- **Full load:** each pipeline snapshots the 45M-row table, one pipeline at a time.
- **CDC:** all pipelines stopped, then 10,000,000 new rows inserted (100 transactions of 100,000 rows, 29.6 s). Each pipeline then resumed from its saved position and was timed until the backlog was committed.
- **Write mode:** append-only in all four pipelines for the speed runs; upsert in the feature tests.

## Objects created

**Source Postgres**

- Tables: `public.bench_customer_run3` (55,000,000 rows), `public.bench_smoke_run3` (2,500 rows), `public.bench_upsert_ups3` (1,022,000 rows, one added column `c_tier`), `public.bench_upsert_hz3` (1,000,000 rows).
- Publication: `bench_pub_run3`.
- Replication slots: 13 named `bench_*` were created (4 for each of the tags `run3`, `smoke3`, `ups3`; 1 for `hz3`) and dropped after the run, with the owner's agreement. The tags can no longer resume.
- Dropped before this run, with the owner's agreement: 17 inactive `bench_*` slots of earlier runs. Tables and publications of earlier runs were left in place.

**Glue / S3**

- Database `bench_run3`: four tables for each of the tags `run3`, `smoke3` and `ups3`.

**Snowflake**

- Table `olake_hz3` in schema `bench_hz` of database `OLAKE_BENCH_HZ`.

**Benchmark host**

- Benchmark folder with logs and results. Stopped containers `bench-dbz-*`, `bench-flink-*`, `spark_bench` hold Kafka data and can be restarted.

### Replication slots after the run

No benchmark slot is left on the source. The database has 2 of its 20 allowed slots (`max_replication_slots`) in use: two older inactive slots, not part of this comparison, holding 441 GB and 221 GB of WAL with `max_slot_wal_keep_size = -1`.

## Raw data

Raw result files are kept locally in `scripts/results/` and are not published with this report (git-ignored). The files listed below exist there after a run.

- `features.md`: single numbered feature list used for the comparison; new features are appended on every run.
- `scripts/results/summary_run3.json`: timings, CPU used per row and memory (from `scripts/tools/summarize.py`).
- `scripts/results/validate_run3.txt`, `scripts/results/validate_run3.source.txt`: validation figures.
- `scripts/results/<pipeline>_run3_<phase>.rows.csv`: committed row count over time.
- `scripts/results/<pipeline>_run3_<phase>.stats.csv`: CPU and memory samples.
- `scripts/results/upsert_ups3.*`, `scripts/results/schema_ups3.*`, `scripts/results/eqcheck_ups3.source.txt`: feature test output.
- `scripts/results/horizon_hz3.txt`: Snowflake test output.
- `scripts/results/interop_<tag>/`, `scripts/results/meta_<tag>*.jsonl`, `scripts/results/maintenance/`: cross-engine read, table metadata and maintenance output.
- `scripts/results/versions_run3.txt`: image IDs and the shared settings used by the run.
- `scripts/logs/`: full tool logs, on the benchmark host only.
