# Tests for what the PR changes

## Which functions need unit tests

A function needs unit test cases when the PR adds it or changes its behavior. A rename, a move or a refactor that keeps behavior needs none, as long as existing cases cover the function.

Run all three searches before you flag a missing test:

1. Grep for `func Test<FunctionName>(` and, for a method, `func Test<Type><Method>(`. That is the team's naming rule.
2. Older tests don't always follow it, so also grep the package's `_test.go` files for calls to the function.
3. For an unexported function, find the exported function that reaches it and search for that one. Tests go through the exported API only. If no exported function reaches the change, a unit test can't pin it: see [Integration tests](#integration-tests).

If no test pins the changed behavior, flag 🟠 at the function's signature and name the cases the change needs. Cover the new or changed branch, its edge values (empty, nil, zero, the maximum, a negative value) and its error path, each with its input and expected result.

## Test shape

New and changed tests follow the team's rules, the same ones as the `olake-go-tests` skill. Flag 🟠 for each rule that new test code breaks:

- There is one `Test<FunctionName>` per function under test. A method's test is `Test<Type><Method>`, as in `TestTypeSchemaGetType`.
- A test is never named `Test<Function><Case>`, such as `TestReformatValueDecimal`. A new behavior is a new case in the function's own table. Staging still has some older tests named this way, such as `TestMaximumOnDataTypeInt64`. Flag only new ones.
- When `Test<FunctionName>` already exists, the PR adds cases to its table instead of a new test.
- Tests call the exported API, never an unexported helper.
- The cases are a slice run through `t.Run(tc.name, ...)`, not a run of hand-written assertions.
- Each case is a keyed struct literal with one field per line:

```go
	testCases := []struct {
		name  string
		input string
		want  int
	}{
		{
			name:  "counts every rune",
			input: "ab",
			want:  2,
		},
	}
```

- Each case asserts the result or effect the caller sees. A case that would still pass with the PR's change reverted pins nothing, so say which assertion is missing.

## Old code against new code

The `Unit Tests` workflow runs the tests this PR adds or changes twice. The first run uses the PR's code. The second uses the base branch's code with the PR's test files copied in. The results are in `/tmp/review/unit-test-baseline.tsv`, one row per test case: the package, the test, the result on the base and the result on the PR. A result is `pass`, `fail`, `skip`, or `no-build` when the test doesn't compile against the base.

Read each row:

| Base | PR | Meaning | Action |
|---|---|---|---|
| pass | pass | Behavior kept | None |
| fail | pass | The PR changed this behavior | Check that the PR description says so. If it doesn't, flag 🔴 Important with the case, its base result and its PR result, and ask whether the change is intended |
| pass | fail | The PR breaks the case | CI already fails, so don't repeat it |
| no-build | pass | The test calls an API the PR adds or changes | Nothing to compare, so judge the cases by reading them |

Also read the diff of the PR's test files. A deleted case, or a case whose expected value changed, is a behavior change too. Ask why, unless the description explains it.

If the PR adds or changes tests and the file is missing, say once in the summary that the old-code run was unavailable.

## Integration tests

Some code only runs against a live source database or destination: a driver reading chunks or a change stream, a writer committing to Iceberg or Parquet, schema evolution, or state and resume. When the PR changes such code and no unit test can reach it, guide the author to an integration test in one finding. Line 1 says what is untested. Line 2 names the suite to extend, what to seed and what to assert.

How the integration tests are laid out:

- Each driver has its own Go module in `tests/<driver>/`. Test functions are named `Test<Driver><Suite>`, for example `TestPostgresDiscover`, `TestPostgresSync`, `TestPostgres2PC`, `TestPostgresCompatibility` and `TestPostgresPerformance`. Feature suites follow the same pattern, such as `TestPostgresDecimalSync` and `TestPostgresUnchangedToastRecovery`.
- Each suite builds an `*integration.TestHandler` from `tests/testutils/integration` out of the driver's config and expected data, then calls a shared runner such as `TestSync` or `TestDiscover`.
- The driver's expected rows, destination schema and type mapping are maps in `tests/<driver>/<driver>_util_test.go`. For Postgres they are `ExpectedPostgresData`, `PostgresToDestinationSchema` and `PostgresTypeMapping`. The source, stream and destination configs are templates in `tests/<driver>/testdata/`.
- Shared harness code lives in `tests/testutils`. The `tests/` modules share no Go package with the root module.

Suggest the smallest of these that covers the change:

1. For a new column, type or value, extend the driver's seed data and its expected maps, so the existing Sync and Discover suites cover it.
2. For a variation of an existing suite, add a case to its table, as `TestPostgresDecimalSync` does.
3. Add a new Test function only for a flow no suite runs. Name the function and the shared runner it should call.

Name the make target that runs it:

- `make test.integration.<driver>` runs every suite of one driver except Performance and Compatibility, and starts the source and destination stacks it needs.
- `make test.compatibility.<driver>` covers a change gated on the state version.
- `make test.performance.<driver>` backs a speed or memory claim.
