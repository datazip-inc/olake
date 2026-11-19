# Postgres to Iceberg (AWS Glue) benchmark: OLake, Debezium, Flink, Spark

Run date: 2026-11-18 (UTC). Benchmark host: one c7i.16xlarge (64 vCPU, 123 GiB) in the same region as source and destination.
Scripts and raw results are in `scripts/` next to this file (`pg-iceberg-benchmark/scripts/` in the olake repo). Full tool logs stay on the benchmark host. How to rerun: `SKILL.md`.

## Result

All four pipelines connected, loaded, resumed for CDC, and produced Iceberg tables that match the source exactly.
There was no authentication or permission failure in any pipeline.

Time is measured from tool start until the Iceberg table in Glue shows the full committed row count.

"Flink (CDC source + Iceberg sink, one job)" is a single Flink job that reads Postgres with the Flink CDC connector and writes Iceberg with the Iceberg Flink connector. There is no Kafka in that pipeline; the two connectors are separate add-on jars joined by one SQL statement.

Flink was fastest on full load; OLake was fastest on CDC catch-up.
These are single runs, so treat differences under about 15% as noise (see "Limits of this run").

### Ranking

Rank 1 is best. Rankings cover only what was measured: speed, memory usage and the measured parts of cost. Features are not ranked; they are compared in `features.md`.

| Ranked on | 1st | 2nd | 3rd | 4th | Numbers |
|---|---|---|---|---|---|
| **Speed, full load** | Flink | OLake | Debezium + Kafka + Spark | Debezium + Kafka + Iceberg sink | 485k, 350k, 160k, 108k rows/s |
| **Speed, CDC catch-up** | OLake | Debezium + Kafka + Spark | Flink | Debezium + Kafka + Iceberg sink | 97k, 70k, 63k, 56k rows/s |
| **Memory usage** (peak, full load) | OLake | Debezium + Kafka + Iceberg sink | Flink | Debezium + Kafka + Spark | 8.0, 12.3, 21.1, 26.8 GB |
| **Memory usage** (peak, CDC) | OLake | Debezium + Kafka + Iceberg sink | Flink | Debezium + Kafka + Spark | 2.7, 9.1, 16.2, 22.0 GB |
| **Run time on the same machine, full load** | Flink | OLake | Debezium + Kafka + Spark | Debezium + Kafka + Iceberg sink | 93, 128, 282, 418 s |
| **Run time on the same machine, CDC** | OLake | Debezium + Kafka + Spark | Flink | Debezium + Kafka + Iceberg sink | 103, 144, 159, 179 s |
| **CPU used per row, full load** | Debezium + Kafka + Iceberg sink | Flink | OLake | Debezium + Kafka + Spark | 22, 34, 42, 45 core-seconds per 1M rows |
| **CPU used per row, CDC** | Flink | Debezium + Kafka + Iceberg sink | Debezium + Kafka + Spark | OLake | 21, 29, 43, 46 core-seconds per 1M rows |
| **Services running between syncs** | OLake | Flink | Debezium + Kafka + Iceberg sink | Debezium + Kafka + Spark | 0, 2, 2, 3 (none; JobManager and TaskManager; Kafka and Kafka Connect; Kafka, Kafka Connect and Spark) |

- **Speed:** single runs with parallelism 16; differences under about 15% are noise.
- **Memory usage:** for Debezium, Flink and Spark it mostly reflects the heap size set for the run; OLake had no limit set.
- **Cost is not one number here.** Total cost was not measured. Three measured parts are ranked separately:
  - *Run time on the same machine*: what you pay when renting a fixed machine by the hour.
  - *CPU used per row*: what you pay when billed per core used. Rough figure from 8 to 34 samples per run. Debezium + Kafka + Iceberg sink is lowest on full load and also the slowest: few cores for a long time. OLake 46 against Spark 43 on CDC is inside the noise.
  - *Services running between syncs*: what keeps billing when no data is moving. OLake exits after a sync; the other three keep services up. Their idle cost was not measured.
  A money figure needs a verified machine price, hours of running per day and data volume, none of which this benchmark has.
- **No pipeline is called cheapest or most expensive overall, and no overall winner is computed:** that would need prices and weights this report does not have.

### Reliability observed

Facts from the runs, not a ranking:

| | OLake | Debezium + Kafka + Iceberg sink | Flink | Debezium + Kafka + Spark |
|---|---|---|---|---|
| Data correct after stop and resume | Yes | Yes | Yes | Yes |
| Same definition applied twice | No duplicates | No duplicates | Rows doubled, two jobs | No duplicates |
| Workarounds needed in the runs | None | First commit cycle lost after start; upsert only in older unmaintained build | `flink cancel` failed, REST cancel used | Custom merge code for updates and deletes |
| Commit and recovery mechanism (documentation) | Two-phase commit, state in the table | Commit coordination over Kafka control topic | Two-phase commit through checkpoints | Nothing documented |

Hard kills (`SIGKILL`) were tested for three pipelines: Flink and Spark while writing, OLake during a sync before its commit. All three matched the source after recovery. Details in "Crash recovery".

## Measured test results

Results of the feature tests, one row per test. Speed, memory, CPU and run time are in "Ranking" and are not repeated here. Feature claims from documentation are in `features.md`.

The Kafka Iceberg sink in all runs is the older Tabular build 0.6.19; its upsert results do not apply to the current Apache sink, which has no upsert. Flink ran in SQL mode, not YAML pipeline mode.

| | OLake | Debezium + Kafka + Iceberg sink | Flink (CDC source + Iceberg sink, one job) | Debezium + Kafka + Spark |
|---|---|---|---|---|
| **Data files written** (measured) | 11 | 18 | 128 | 368 |
| **Data correct after resume** (measured) | Yes | Yes | Yes | Yes |
| **Built-in test connection** (measured) | Source and destination | Source only | None | None |
| **Parallel load of one table** (measured, with settings used) | Yes | No, default `snapshot.max.threads` | Yes | No, default `snapshot.max.threads` |
| **Decimal column in Iceberg** (measured, as configured) | decimal | double | decimal | double |
| **Updates applied correctly** (measured) | Yes | Yes | Yes | Yes |
| **Deleted rows** (measured) | Kept as tombstone rows (`_op_type = 'd'`, source columns null) | Removed | Removed | Removed |
| **Delete files written in upsert mode** (measured) | Equality by default; position after `update_type = pos` | Equality | Equality | Position |
| **Getting rid of equality deletes** (measured) | Built in: set `update_type = pos`, next sync rewrote them | Separate Spark maintenance job (`rewrite_data_files`) | Separate Spark maintenance job (`rewrite_data_files`) | Not needed |
| **New source column** (measured) | Added automatically | Added with `evolve-schema-enabled=true` | Dropped silently, job kept running | Dropped silently, job kept running |
| **Live CDC latency, median** (measured) | 4.0 s (one sync run started after the change) | 23.2 s (30 s commit interval) | 21.7 s (30 s checkpoint) | 12.1 s (20 s trigger) |
| **GitOps supported** (measured) | Yes: JSON files and one command; applying again read 0 rows, no duplicates | Yes: JSON files sent with REST `PUT`; second `PUT` returned 200, no duplicates | Partly: SQL file; applying it twice started 2 jobs and doubled the rows | Partly: code and `spark-submit`; a second identical job kept running, no duplicates seen |
| **Append-only table read by Spark 3.5, Trino 476, Flink 1.19, PyIceberg** (measured) | Yes, all four | Yes, all four | Yes, all four | Yes, all four |
| **Upsert table read by Spark, Trino, Flink** (measured) | Correct | Correct | Correct | Correct |
| **Upsert table read by PyIceberg** (measured) | No with equality deletes; yes after `update_type = pos` | No; yes after Spark maintenance job | No; yes after Spark maintenance job | Yes |
| **Iceberg format version** (measured) | 2 | 2 | 2 | 2 |
| **Extra columns added to table** (measured) | 5: `_olake_id`, `_olake_timestamp`, `_op_type`, `_cdc_timestamp`, `_cdc_lsn` | None | None | None |
| **Row identity declared in Iceberg** (measured) | `_olake_id` | None | `c_custkey` | None |
| **Source NOT NULL kept** (measured) | No, all source columns nullable | Yes, all columns | Key column only | No |
| **Iceberg catalog used** (measured) | Glue | Glue | Glue | Glue |

### Where the tests add to the documentation

- Flink in SQL mode dropped a new source column, consistent with nothing being documented for that mode.
- Debezium loaded with one thread because `snapshot.max.threads` was left at its default of 1.
- Equality deletes written by the older Kafka sink build and by Flink were removed with Spark `rewrite_data_files`. Flink `ConvertEqualityDeletes` and OLake Fusion were not run.
- OLake kept deleted rows with `_op_type = 'd'`, as its documentation says.

### Iceberg interoperability (measured)

Every table was read back by four engines, each computing row count, distinct keys and `sum(c_acctbal)`:

| Reader | `olake_customer` | `dbz_kc_customer` | `flink_customer` | `dbz_spark_customer` |
|---|---|---|---|---|
| Spark 3.5.3 (Iceberg 1.7.1) | Correct | Correct | Correct | Correct |
| Trino 476 | Correct | Correct | Correct | Correct |
| Flink 1.19 (Iceberg 1.7.1) | Correct | Count correct; sum off by 0.07 | Correct | Count correct; sum off by 0.09 |
| PyIceberg 0.8.1 | Correct | Correct | Correct | Correct |

- "Correct" means 55,000,000 rows, 55,000,000 distinct keys and sum 247486409332.26, same as the source.
- The two small sum differences are in the Debezium tables, where `c_acctbal` is stored as double. Floating-point sums depend on the order an engine adds rows. The OLake and Flink tables store decimal and gave the exact sum in every engine.
- These reads are of the append-only benchmark tables, which contain no delete files. Reads of tables with delete files are in the next section.
- Only the Glue catalog was used, by all four pipelines.
- The OLake table carries a table property `olake_2pc` holding sync state. All four engines ignored it.

Script: `scripts/interop/run.sh <tag>`. Raw output: `scripts/results/interop/`.


## Snowflake

Snowflake was tested with OLake and it worked: OLake wrote the table into Snowflake with deletion vectors, and Snowflake returned the same rows and sums as the source after inserts, updates and deletes.

## Updates and deletes

Test: new source table of 1,000,000 rows, all pipelines in upsert mode. After the full load: 100,000 inserts, 100,000 deletes, 200,000 updates. Source afterwards: 1,000,000 rows, `sum(c_acctbal)` 4,497,216,203.00. Script: `scripts/upsert_all.sh`.

| Table written by | Delete files | Rows read by Spark, Trino, Flink | Sum | PyIceberg 0.8.1 |
|---|---|---|---|---|
| OLake (`update_type = eq`, default) | 300,000 equality deletes | 1,100,000 | Matches source | Refuses: equality deletes not supported |
| Debezium + Kafka + Iceberg sink | 300,000 equality deletes | 1,000,000 | Matches source | Refuses |
| Flink (one job) | 300,000 equality deletes | 1,000,000 | Matches source | Refuses |
| Debezium + Kafka + Spark | 300,000 position deletes | 1,000,000 | Matches source | Correct |

- **Updates** arrived correctly in all four pipelines.
- **Kafka Iceberg sink result is for the older Tabular build.** The current Apache sink has no upsert or delete handling, so it could not have run this test.
- **OLake keeps deleted rows,** as its documentation states. Each deleted source row stays as a row with `_op_type = 'd'` and null source columns. That is why its table has 100,000 more rows. Sums match because nulls add nothing. A reader gets the source row count only with `WHERE _op_type <> 'd'`.
- **Equality deletes block PyIceberg.** Both 0.8.1 and 0.12.0 refuse any table that has them.

### Removing equality deletes

| Pipeline | Method | Result |
|---|---|---|
| OLake | Set stream `update_type` to `pos` and run the next sync | Log: `rewrote 2 equality delete file(s) as 301000 pos delete(s)`. Table then had 0 equality deletes. PyIceberg 0.12.0, Spark, Trino and Flink all read 1,132,000 rows with the source sum. |
| Debezium + Kafka + Iceberg sink | Separate Spark job: `rewrite_data_files` with `rewrite-all` and `remove-dangling-deletes` | 10 data files rewritten into 1, 0 delete files left. PyIceberg 0.8.1 and 0.12.0 then read the table, same rows and sum as Spark before. |
| Flink (one job) | Same separate Spark job | 144 data files rewritten into 1, 0 delete files left. PyIceberg then read the table. |

- For OLake the conversion is part of the writer and stays in effect: later syncs wrote position deletes.
- For the Kafka sink and Flink it is a separate job to schedule. Both writers keep producing equality deletes, so it has to run again after new changes. It was run once here, with the pipelines parked.
- PyIceberg 0.8.1 returned 200 rows too many on the converted OLake table (rows inserted and then deleted or updated in one sync). PyIceberg 0.12.0 returned the correct result, so this is an old-reader fault.
- 1,132,000 = 1,031,900 source rows at that point + 100,100 tombstone rows.
- Per the Iceberg documentation: `rewrite_position_delete_files` handles position deletes only; Flink table maintenance has a `ConvertEqualityDeletes` task that needs table format version 3. Neither was run here.
- OLake `update_type = dv` (deletion vectors) was not run.
- Documented but not run: OLake Fusion Lite compaction, which converts equality deletes to position deletes on a schedule and also works on tables created outside OLake; Flink `ConvertEqualityDeletes`, which needs table format v3.

Scripts: `scripts/olake/set_update_type.sh`, `scripts/interop/maintain.sh`. Raw output: `scripts/results/interop_ups1_eq/`, `scripts/results/interop_ups1_pos/`, `scripts/results/maintenance_ups1/`.

## Schema change

Test: `ALTER TABLE ... ADD COLUMN c_tier int` on the source while pipelines were parked, then 1,000 updates and 1,000 inserts that set the column. Script: `scripts/schema_change.sh`.

| Pipeline | Column in Iceberg table | Rows with value | Pipeline after the change |
|---|---|---|---|
| OLake | Yes, added automatically | 2,000 of 2,000 | Ran normally |
| Debezium + Kafka + Iceberg sink | Yes, with `iceberg.tables.evolve-schema-enabled=true` | 2,000 of 2,000 | Ran normally |
| Flink (one job) | No | 0 | Ran normally; column ignored without warning |
| Debezium + Kafka + Spark | No | 0 | Ran normally; column ignored without warning |

Flink and Spark have the column list fixed in the job (SQL table definition, `from_json` schema). Only an added column was tested; dropped, renamed and retyped columns were not.

Flink documents schema evolution for YAML pipeline mode (Postgres pipeline source with `schema-change.enabled`, Iceberg pipeline sink), including a Postgres-to-Iceberg example with an added column. That mode was not run here; it is at-least-once, where the SQL mode run here is exactly-once.

## Live CDC latency

Test: pipeline running and caught up, then 5 rounds of 1,000 inserted rows. Latency is the time from the source commit until the rows are committed in the Iceberg table, polled every second. Script: `scripts/latency.sh`.

| Pipeline | Median | Range | Commit setting |
|---|---|---|---|
| OLake | 4.0 s | 4.0 to 4.1 s | One sync run per round |
| Debezium + Kafka + Spark | 12.1 s | 11.6 to 16.6 s | Micro-batch every 20 s |
| Flink (one job) | 21.7 s | 10.7 to 22.8 s | Checkpoint every 30 s |
| Debezium + Kafka + Iceberg sink | 23.2 s | 22.2 to 41.7 s | Commit every 30 s |

- The OLake figure is the duration of a sync started right after the change. OLake was not run as an always-on stream, so real latency is this plus the wait until the next scheduled run.
- The three streaming figures mostly reflect the commit interval chosen for this run. Shorter intervals would lower them and write more, smaller files.
- The Iceberg documentation recommends a Spark trigger interval of at least 1 minute, and the Kafka sink default commit interval is 5 minutes. With those settings both would show higher latency than measured here.

## GitOps: applying the same definition twice

Every pipeline in this benchmark is defined by files in this folder and started by a script, with no manual steps. This test applied each definition a second time while the pipeline existed, then compared table rows with source rows. Script: `scripts/gitops_reapply.sh`.

| Pipeline | Definition | Second apply | Rows, table vs source |
|---|---|---|---|
| OLake | JSON files, one `sync` command | Read 0 records and exited | 2,500 vs 2,500 |
| Debezium + Kafka + Iceberg sink | JSON files sent with REST `PUT` | `PUT` returned 200, connector unchanged | 2,600 vs 2,600 |
| Debezium + Kafka + Spark | Python job, `spark-submit` | Second identical job kept running on the same checkpoint | 2,700 vs 2,700 |
| Flink (one job) | SQL file, `sql-client` | Two jobs with the same name in state RUNNING | 5,400 vs 2,700, rows doubled |

- Flink SQL submission is not idempotent: a GitOps tool needs its own guard (for example the Flink Kubernetes Operator, not tested here).
- The Spark result is one observation on a small table. Two jobs sharing one checkpoint is not a safe setup even though no duplicate appeared.
- Kubernetes operators and Helm charts were not tested.
- This test used plain files and commands. It did not use the documented GitOps features: "GitOps for OLake Go" (files applied by Argo CD or Flux, or a watched folder), Strimzi and Debezium Operator resources, or Flink Kubernetes Operator resources. Those would be the supported way to avoid the Flink double-submit seen here.

## Crash recovery

Test: new rows inserted, then the writer of OLake, Flink and Spark pipelines killed with `SIGKILL` while it processed them, restarted, and the table compared with the source through Trino. The Debezium + Kafka + Iceberg sink pipeline is not part of this test. Script: `scripts/crash_test.sh`. The kill lands at a time offset, not at a chosen point inside a commit.

Source after the test: 3,031,900 rows, 3,031,900 distinct keys, `sum(c_acctbal)` 4,503,647,350.21.

| Pipeline | What was killed | Where the kill landed | Table after recovery | Recovery step |
|---|---|---|---|---|
| OLake | Sync container, twice | 6 s and 14 s into the sync, before its commit | Matches source (deleted rows excluded) | Run the same sync command again |
| Flink (one job) | TaskManager; then JobManager and TaskManager together | While writing: table at 1,197,636 rows, then at 2,430,404 rows | Matches source | TaskManager kill: none, job recovered itself. JobManager kill: job was lost and had to be resubmitted from the last complete checkpoint |
| Debezium + Kafka + Spark | Spark job, twice | While writing: table at 3,337,952 rows, then at 5,663,122 rows | Matches source (6,031,900 rows, sum equal) | Start the same job again; it continued from its checkpoint |

- No loss and no duplicates for OLake, Flink and Spark.
- Source row counts differ between rows of the table because the test ran in two rounds: 3,031,900 rows after the first, 6,031,900 after the second.
- The Spark row is from a second round: in the first round both kills came after Spark had finished writing, so that round did not count for Spark.
- Flink without high availability loses the job when the JobManager dies; the data was still correct after a manual resubmit.

Raw output: `scripts/results/crash_ups1.txt`.

## Data validation

Source benchmark table after CDC workload, compared with each Iceberg table:

| Table | Rows | Distinct keys | sum(c_custkey) | sum(c_acctbal) | Data files | Size | Snapshots |
|---|---|---|---|---|---|---|---|
| Source (Postgres) | 55,000,000 | 55,000,000 | 2062500027500000 | 247486409332.26 | | | |
| `olake_customer` | 55,000,000 | 55,000,000 | 2062500027500000 | 247486409332.26 | 11 | 3.20 GB | 6 |
| `dbz_kc_customer` | 55,000,000 | 55,000,000 | 2062500027500000 | 247486409332.26 | 18 | 3.12 GB | 18 |
| `flink_customer` | 55,000,000 | 55,000,000 | 2062500027500000 | 247486409332.26 | 128 | 3.23 GB | 8 |
| `dbz_spark_customer` | 55,000,000 | 55,000,000 | 2062500027500000 | 247486409332.26 | 368 | 3.26 GB | 23 |

No loss and no duplicates in any pipeline, including across the stop and resume between full load and CDC.
OLake wrote the fewest files; Spark wrote the most and would need compaction soonest.

## What was tested

- **Source:** RDS PostgreSQL 16.15 (staging), SSL `require`.
- **Source table:** `public.bench_customer_`*, a new copy of `tpch.customer` (45,000,000 rows, 8 columns, 9.4 GB with index).
- **Destination:** Iceberg on AWS Glue with an S3 warehouse, region `ap-south-1`, in a new Glue database `bench_`*.
- **Credentials:** none in any config. Every pipeline used the benchmark host's instance role.
- **Full load:** each pipeline snapshots the 45M-row table, one pipeline at a time.
- **CDC:** all pipelines stopped, then 10,000,000 new rows inserted (100 transactions of 100,000 rows, 31.5 s). Each pipeline then resumed from its saved position and was timed until the backlog was committed.
- **Write mode:** append-only in all four pipelines.

## Pipeline settings

| | OLake | Debezium + Iceberg sink | Flink (CDC source + Iceberg sink, one job) | Debezium + Spark |
|---|---|---|---|---|
| Version | v1.2.0 (`olakego/source-postgres:latest`) | Debezium 3.0.8, Kafka 3.9.0, Iceberg sink 0.6.19 (Iceberg 1.5.2) | Flink 1.19, Flink CDC 3.2.1, Iceberg 1.7.1 | Debezium 3.0.8, Kafka 3.9.0, Spark 3.5.3, Iceberg 1.7.1 |
| Parallelism | `max_threads` 16 | 1 source task, 16 sink tasks, 16 partitions | parallelism 16 | 1 source task, `local[16]`, 16 partitions |
| Memory limit | none set | Connect heap 24 GB, Kafka heap 4 GB | TaskManager 32 GB | driver 32 GB |
| Commit trigger | per chunk / end of CDC batch | every 30 s | checkpoint every 30 s | micro-batch every 20 s |
| Kafka format | not used | JSON with schema, lz4 | not used | JSON with schema, lz4 |
| Decimal column | native decimal | double | decimal(15,2) | double |

Flink also used `scan.incremental.snapshot.backfill.skip=true` and chunk size 100,000.

## Limits of this run

- **Single run per pipeline.** No repeats, so no variance figure.
- **Run order may favour later pipelines.** OLake ran first against a cold table; later pipelines read data already cached by Postgres.
- **Coarse timing.** Row counts are polled every 5 seconds and tools commit every 20 to 30 seconds, so each figure can be off by up to about 30 seconds. That is large relative to the 93 to 180 second runs.
- **CDC figure is backlog catch-up, not live latency.** Changes were generated while pipelines were stopped.
- **Speed numbers are insert-only.** The 45M-row benchmark ran append-only. Updates and deletes were tested for correctness on 1,000,000 rows, not for speed.
- **One narrow table.** 8 columns, no large text or JSON, no partitioning.
- **Debezium snapshot is single-threaded per table.** That caps both Debezium pipelines on full load.
- **Debezium pipelines used JSON with embedded schema.** Avro with a schema registry would likely be faster.
- **Debezium pipelines store** `c_acctbal` **as double,** not decimal. Sums matched here to 2 decimal places.
- **Iceberg Kafka Connect sink used was the Tabular/Databricks 0.6.19 build,** not the Apache Iceberg build, which has no ready runtime download. That repository is marked not maintained. It has upsert; the Apache build does not. Append-only speed numbers should carry over; upsert results do not.
- **Flink ran in SQL mode.** YAML pipeline mode has different features (schema evolution) and a different guarantee (at-least-once) and was not measured.
- **OLake Fusion was not run.** Maintenance for OLake is described from documentation only.

## Problems found on the way

- OLake destination `check` returns `SUCCEEDED` without writing a new file to S3. Only a real sync proves S3 write.
- OLake ignores `initial_wait_time` below 120 seconds and falls back to 10,800 seconds.
- Iceberg Kafka Connect sink loses its first commit cycle after start (workers miss the first trigger), which adds up to one commit timeout of delay.
- `flink cancel` CLI fails with `NoSuchMethodError` when the Hadoop uber jar is in `lib/`. REST cancel works.
- Containers need host networking to get instance-role credentials.
- Flink CDC incremental snapshot creates one temporary slot per reader unless backfill is skipped; with 16 readers that would exceed `max_replication_slots=20` on this database.

## Objects created (nothing existing was changed or deleted)

**Source Postgres**

- Tables: `public.bench_customer_*` (55,000,000 rows), `public.bench_smoke_*` (2,700 rows), `public.bench_upsert_ups1` (1,031,900 rows, one added column `c_tier`).
- Publication: `bench_pub_*`.
- Replication slots, all inactive: 16 named `bench_*` (4 per run tag: `customer`, `smoke`, `ups1`, `g1`). With 2 older slots the database now has 18 of its 20 allowed (`max_replication_slots`). One more full run needs 4, so slots must be dropped first.

**Glue / S3**

- Database `bench_*`: four tables for each of the tags `customer`, `smoke`, `ups1` and `g1`.
- Database `test_olake`: `kc_sink_conntest`, `flink_sink_conntest`, `olake_sink_conntest` (from connection tests).

**Benchmark host**

- Benchmark folder and connection-test folder. Stopped containers `bench-dbz-*`, `bench-flink-*`, `spark_bench` hold Kafka data and can be restarted.

### Open risk: replication slots

The 16 new slots are inactive and will hold WAL on the source until dropped.
This database already had two inactive slots holding 391 GB and 171 GB of WAL with `max_slot_wal_keep_size = -1`, so the new slots add no extra WAL while those exist.
They were left in place because the instruction was to delete nothing on the source. To drop only the benchmark slots:

```sql
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
WHERE slot_name LIKE 'bench\_%' AND NOT active;
```

## Raw data

Raw result files are kept locally in `scripts/results/` and are not published with this report (git-ignored). The files listed below exist there after a run.

- `features.md`: single numbered feature list used for the comparison; new features are appended on every run.
- `scripts/results/summary.json`: timings, CPU used per row and memory (from `scripts/tools/summarize.py results customer 45000000 55000000`).
- `scripts/results/validate.jsonl`, `scripts/results/source_agg.txt`: validation figures.
- `scripts/results/<pipeline>_customer_<phase>.rows.csv`: committed row count over time.
- `scripts/results/<pipeline>_customer_<phase>.stats.csv`: CPU and memory samples.
- `scripts/results/upsert_ups1.*`, `scripts/results/schema_ups1.*`, `scripts/results/latency_ups1*`, `scripts/results/gitops_g1.txt`: feature test output.
- `scripts/logs/`: full tool logs, on the benchmark host only.
