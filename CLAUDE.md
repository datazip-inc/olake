# OLake

OLake is an open-source (Apache-2.0) **EL tool**. It replicates databases, Kafka and S3 into **Apache Iceberg** tables or plain **Parquet** files. Users run it through the CLI, Docker images (`olakego/source-<driver>`) or OLake UI (a separate repo, `datazip-inc/olake-ui`, which consumes this repo's specs, catalogs and state).

The repo is public. Correctness of the data a sync writes, and safe resume after failure or upgrade, matter more than anything else here.

## Layout

```
connector.go             RegisterDriver(): CLI entry every driver's main.go calls
protocol/                Cobra commands: spec, check, discover, sync, clear
drivers/abstract/        Orchestration shared by all sources: backfill.go, incremental.go, cdc.go
drivers/<name>/          One Go module per source: postgres, mysql, mongodb, oracle, mssql, db2,
                         kafka, s3 (main.go, internal/, resources/spec.json, docker-compose.yml)
destination/interface.go Writer: the contract every destination implements
destination/writers.go   WriterPool / WriterThread: buffering, flush, commit, stats
destination/iceberg/     Iceberg destination: Go side + Java writer (olake-iceberg-java-writer/, gRPC)
  legacy-writer/         Default: Go flattens records -> gRPC rows -> Java writes
  arrow-writer/          arrow_writes=true: Go builds Arrow/Parquet -> Java registers files
  proto/                 gRPC contract shared by Go and Java
destination/parquet/     Plain Parquet to S3, GCS, Azure or local. No Java, no catalog
types/                   Stream, catalog (streams.json), State, DataType, delete modes, sync modes
utils/                   Concurrency (CxGroup, safego), typeutils (value reformatting), logger, telemetry
pkg/                     Protocol libraries: waljs (pg WAL), jdbc, kafka, objstorage, indexdb (pebble)
constants/               Global constants, state-versions.json (state version manifest)
tests/                   Separate Go module: integration, 2PC, performance, compatibility suites
```

`go.work` links the root module, each driver module and `tests/`. A change in the root module (`types`, `utils`, `destination`, `drivers/abstract`) reaches **every driver**.

## How a sync works

1. **discover**: the driver lists streams and infers schemas (`ProduceSchema`, or sampled schemas for Kafka/S3/MongoDB) into a catalog (`streams.json`). Re-running discover with an existing catalog **merges** into it. OLake UI does this when a job is edited after an upgrade.
2. **sync**: modes are `full_refresh`, `incremental` (cursor column), `cdc` (backfill, then change stream) and `strict_cdc` (change stream only).
   - **Backfill:** `GetOrSplitChunks` splits a table into chunks, and `ChunkIterator` reads each chunk in parallel. State keeps the pending chunks, and `RemoveChunk` runs only after the chunk's writer commits.
   - **CDC:** only some drivers support it (`CDCSupported()`), each with its own change source:
     - postgres: logical replication WAL via `pkg/waljs`
     - mysql: binlog
     - mongodb: change streams
     - mssql: SQL Server CDC change tables, LSN-based
     - kafka: consumer offsets

     **Oracle, DB2 and S3 have no CDC.** They support `full_refresh` and `incremental` only.

     `PreCDC` initializes state, and `PostCDC` saves the position.
   - **Every record carries OLake columns:** `_olake_id` (a hash of the PK), `_op_type` (`r` backfill, `c`/`i` insert, `u` update, `d` delete), `_olake_timestamp` and, in CDC mode, `_cdc_timestamp`.
3. **write**: `WriterPool.NewWriter` creates a WriterThread per chunk or CDC reader.
   - Records are flattened (`FlattenAndCleanData`) and the schema is evolved if needed (`EvolveSchema`).
   - Batches are written and `Close` commits them.
   - **Two-phase commit:** the commit stores `olake_2pc` metadata (latest thread ID, `full_refresh_committed_ids`, CDC positions, `dedup_inserts`) **atomically with the data**. For Iceberg it's the `olake_2pc` table property, set in the same transaction as the data files (`IcebergTableOperator.java`). For Parquet it's the `_olake_2pc/` marker files. On retry or resume, `NewWriter` returns that metadata, so chunks that are already committed are skipped and CDC resumes from the committed position.
4. **Deletes and updates (Iceberg):** `update_type` sets how they're written:
   - `eq`: equality deletes, the default, needs no index
   - `pos`: positional deletes
   - `dv`: deletion vectors, Iceberg v3 tables only

   `pos` and `dv` need the durable row index in `pkg/indexdb`. Some drivers restrict the modes they support (`UpdateTypeRestricter`; Kafka upserts are always positional).

## Invariants: breaking one is a real bug

- **Exactly-once.** Retrying a chunk, a CDC batch or a whole sync must not duplicate or drop rows. Never advance a source position (state cursor, chunk removal, LSN/binlog/offset ack) before the destination commit that covers it.
- **State is a public format.** Users resume from state files written by older releases. A change to how existing data is read, typed or written (type mapping, timezone, parsing, `_olake_id` hashing, column naming) must be **gated on `constants.LoadedStateVersion`**. It also needs a new entry in `constants/state-versions.json` (owned by `@datazip-inc/state-version-owners`, see `tests/testutils/compatibility/README.md`). Without the gate, existing destination columns change type or values on upgrade.
- **Destination schemas only widen.** Iceberg allows int -> long and float -> double (`promotionTransitions` in `destination/iceberg/iceberg.go`). New columns are added, never narrowed or retyped in place.
- **Catalog and config are user-facing contracts.** `streams.json`, the driver config and `resources/spec.json` are written by users and by OLake UI. New fields need defaults that keep current behavior. Renaming or removing a field breaks existing jobs. `types/legacy_catalog.go` keeps the old catalog shape working.
- **Go and Java change together.** Edits to `destination/iceberg/proto` must keep the Go and Java generated code in sync. Java changes need a JAR rebuild, and a stale root `olake-iceberg-java-writer.jar` silently wins.
- **Concurrency.** Launch goroutines through `utils.CxGroup` / `utils.Concurrent*` with `safego.Recovery`. Respect context cancellation. `State` and stream artifacts are shared across threads, so use their locking methods.

## Conventions

- Logging: `utils/logger` only, never `fmt.Println` / `log.Println`. Never log credentials, connection strings or record values.
- Errors: wrap with context, `fmt.Errorf("failed to <action>: %w", err)`. Mark failures that should not retry with `constants.ErrNonRetryable`. Retry classification is matched by message (`utils.go`), so keep its text intact.
- JSON tags: `snake_case`, with `omitempty` for optional config.
- Tests: integration tests are named `TestXxxIntegration` and performance tests `TestXxxPerformance`. Shared harness code lives in `tests/testutils`.
- Lint: `make lint` (golangci-lint for the root module and the tests module, plus a `go mod tidy -diff` check), and `make gofmt`. Commit the `go.sum` / `go.work.sum` changes that `make gomod` produces.

## Build and test

```bash
bash build.sh driver-<name> sync --config config.json --destination writer.json --catalog streams.json
make help                              # all targets
make test.unit
make test.integration.<driver>         # discover + sync + 2PC suites for one driver
make test.discover.<driver> | test.sync.<driver> | test.2pc.<driver>
make test.compatibility.<driver>       # upgrade-from-old-state suite
make iceberg.jar                       # rebuild the Java writer
```

Integration tests need Docker. `make olake.<driver>.up` / `olake.destination.all.up` start the source and destination stacks (MinIO, Iceberg catalogs).

## Workflow

- Feature PRs target **`staging`**. `staging` -> `master` PRs are releases of commits that were already reviewed.
- Both branches are protected by rulesets: review approval, code-owner review for state-version files and resolved review threads before merge.
- User-facing changes need docs at olake.io/docs (the `olake-docs` repo). The PR template asks for the link.
