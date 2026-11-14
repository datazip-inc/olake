# Review instructions

OLake is a data replication tool. A review exists to catch changes that would lose, duplicate or corrupt user data, break resume or upgrade, or break existing users' configs. Project context and invariants are in `CLAUDE.md`. Treat any violation of its "Invariants" section as Important.

## What Important means here

Report as 🔴 Important only when the finding names a concrete way this PR causes one of these:

- Data loss, duplication or corruption in the destination (wrong values, wrong types, dropped deletes or updates, out-of-order CDC operations).
- A source position (state cursor, chunk removal, LSN, binlog position, offset, resume token) advancing before the destination commit that covers it, or a retry or resume path that rewrites rows that were already committed.
- A change to how existing data is read, typed or written, with no `constants.LoadedStateVersion` gate and no new `constants/state-versions.json` entry.
- A destination schema change that narrows or retypes an existing column.
- A breaking change to `streams.json`, a driver config or `resources/spec.json` for existing jobs: a renamed or removed field, or a new required field without a default.
- A Go/Java gRPC contract mismatch in `destination/iceberg/proto`.
- A crash, deadlock, goroutine leak or unbounded memory growth on the sync path.
- Credentials, connection strings or record values written to logs.

Everything else is 🟡 Nit at most. Report **at most 3 Nits** per review, and only when they are worth an author's time.

## Evidence rules

These apply to every finding, and most strictly to Important ones.

1. **Trace it yourself.** Never write "verify this doesn't affect X", "check whether callers handle this" or "make sure this works with Y". Find the callers or implementers yourself. For a change in `drivers/abstract/`, `types/`, `destination/writers.go`, `utils/typeutils/` or `constants/`, check every driver or destination that uses it. Then report the specific break with `file:line` for both the change and the place it breaks. If you find no break, post nothing.
2. **Cite the code, not names.** A claim about behavior needs a `file:line` citation showing it. Don't infer behavior from function or variable names.
3. **External facts need a source.** For claims about GitHub Actions syntax, the Go toolchain, Iceberg spec behavior, a database's replication protocol or a library's API, confirm them from code in this repo or the dependency's source or docs. If you can't confirm, ask it as a question and mark it Nit, never Important. Your training data may predate the feature. Example: a reviewer once wrongly called the GitHub Actions step key `background: true` invalid.
4. **Never claim a file is missing** without checking the PR's full changed-file list. Generated files (`go.sum`, `go.work.sum`, `*.pb.go`) are often in the PR even when they aren't shown in the diff you are reading.
5. **Before posting, try to disprove the finding.** Look for the guard, test or caller that already handles the case. Drop the finding if you find one.

## Consistency across reviews

- Read the earlier review threads on this PR and the author's replies before reviewing.
- **Don't reverse an earlier suggestion that the author applied.** If code that follows an earlier suggestion now has a real bug, link the earlier thread and explain what changed. Don't flag the pattern itself.
- If the author replied with a reason, and the reason holds against the code, don't raise the same finding again.
- Configuration versus constants: values a user may need to change per deployment (timeouts, batch sizes, endpoints) belong in config. Protocol and format constants (OLake column names, state keys, Iceberg field IDs) belong in `constants/`. Apply this one rule. Don't argue both directions in the same PR.

## PR description

- The description is the stated intent of the PR. Compare the diff against it.
- In the review summary, once and not inline, say whether:
  - the description doesn't say **why** the change is needed,
  - the diff changes behavior the description doesn't mention,
  - a bug fix has no `Fixes #` issue link, or
  - "How has this been tested" is empty for a change to sync, CDC, type mapping or a writer.
- Don't comment on the description's grammar or formatting.

## Keep the review proportional

- Small PRs (under ~50 changed lines, or docs, CI or test-only changes) get a quick pass: report only Important findings. Don't explore unrelated modules.
- Release PRs (`staging` -> `master`) contain commits that were already reviewed. Report only Important findings, and only on code that differs from what was reviewed on `staging`.

## Always check

- **Type mapping changes** (`datatype_conversion.go`, `utils/typeutils/`, `types/data_types.go`): check edge values (unsigned max, negative, NaN/Inf, `[]byte`), timezone and precision handling, and nulls. A changed mapping for an existing type needs a state version gate.
- **Backfill:** chunk boundaries must not overlap or leave gaps, and a retried chunk must skip when its thread ID is in `FullRefreshCommittedIDs`.
- **Incremental and CDC:** cursor and position updates happen only after commit. Check how CDC handles a deleted row's before-image and a primary-key update. Check `_op_type` values and `_cdc_timestamp`.
- **Delete modes:** `pos` and `dv` need the table index. `dv` needs Iceberg format v3. Driver restrictions go through `UpdateTypeRestricter`.
- **Schema evolution:** new columns are nullable, and only int -> long and float -> double are allowed as promotions. Check `use_source_column_names` and normalized column names together.
- **Resources:** close rows, cursors, connections, gRPC streams and files on every path, including errors. Bound buffers (`MaxDestinationBatchBytes`).
- **Tests:** a new or changed type mapping updates the integration test data. A state version adds compatibility coverage.

## Do not report

- Anything `golangci-lint`, `gofmt` or `go vet` catch, and formatting or import order.
- **Security findings about read permissions, public URLs, or workflow and config files being visible.** This repository is public open source by design. Report security issues only for secret exposure (credentials in logs, errors or committed files), injection into SQL or shell built from user input, untrusted fork code running with secrets in CI, or authentication bypass.
- Generated code: `*.pb.go`, the generated protobuf Java in `destination/iceberg/olake-iceberg-java-writer/src/main/java/io/olake/iceberg/rpc/` (`*Grpc.java`, `RecordIngest.java`). The other files in that directory are hand-written and do get reviewed, `go.sum`, `go.work.sum`, `*.jar`.
- Test fixtures and test data under `tests/testdata/` and `drivers/*/examples/`, unless they break a test.
- Style or naming preferences not stated in `CLAUDE.md`.
- "Consider adding tests" without naming the specific untested case that could break.
- Pre-existing issues in code this PR doesn't touch. Mention one only if this PR makes it reachable or worse.
