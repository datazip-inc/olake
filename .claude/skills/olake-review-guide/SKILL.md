---
name: olake-review-guide
description: Code-quality and test rules for reviewing an OLake pull request. Checks for driver-specific logic in shared code, helpers that already exist, where new functions and files are placed, whether new functions read like text, and whether what the PR changes has unit or integration tests. Use when reviewing an OLake PR or diff, including your own change before you push it.
---

# OLake review guide

These rules judge only what the PR adds or changes. Code the PR doesn't touch is out of scope, even where it breaks a rule here: never ask a PR to clean up what it found. A PR that extends an existing violation, such as one more case in a driver switch or one more copy of a helper, is new code and gets flagged.

## Reporting

- A rule broken by new code is a 🟠 Change requested finding (`"severity": "change"`). Use 🟡 Nit only for an optional improvement.
- Each finding is two lines at most. Line 1: what is wrong and what it costs the reader or the next driver. Line 2: the fix in words, naming the existing helper, interface or file with `file:line`, or two or three candidate names.
- Trace before you flag, as REVIEW.md's evidence rules require. "There may be a helper for this" is not a finding: find the helper and cite it, or post nothing.

## 1. Shared code knows no driver

Drivers depend on shared code, and shared code never depends on a driver. Shared code is everything outside `drivers/<name>/`: `drivers/abstract`, `destination`, `types`, `utils`, `pkg`, `protocol` and `constants`. The same holds for destinations: code outside `destination/<name>/` names no destination.

Flag any of these on a line the PR adds outside `drivers/<name>/`:

- A driver's name: a string such as `"postgres"`, a constant such as `constants.MySQL`, or a comparison with `Type()`.
- A `switch` or map keyed by driver, or a new case in an existing one.
- An import of a package under `drivers/<name>/`.
- A field, option or parameter on a shared type that only one driver sets or reads.
- A comment such as "only MongoDB needs this", which marks the same leak.

A new driver still registers itself where the repo requires it today, such as its `constants.DriverType` value and its spec and UI schema entries. That is not a finding. Any new behavior keyed on the driver is.

The fix is an interface. Shared code calls a method, and each driver implements it, or a driver opts in by implementing an optional interface that shared code checks with a type assertion. The repo already does this:

```go
// drivers/abstract/interface.go
type UpdateTypeRestricter interface {
	SupportedUpdateTypes() []types.UpdateType
}

// drivers/abstract/abstract.go: shared code asks for the behavior, never for the driver
if restricter, ok := a.driver.(UpdateTypeRestricter); ok {
	return restricter.SupportedUpdateTypes()
}
```

The pattern to stop growing is `jdbc.SQLFilter` in `pkg/jdbc/jdbc.go`. It takes the driver's name as a string, switches on it to pick the SQL syntax, and falls back to Postgres for a name it doesn't know. A PR that adds a case there gets a 🟠: the driver passes in what differs, such as its identifier quoting and timestamp syntax, through an interface, so `pkg/jdbc` never names a driver.

## 2. Reuse what exists

For each function, type or constant the PR adds, search the codebase for one that already does the job before you accept it.

- Search by what it does, not only by its name. Grep for the operation at its core (`strings.EqualFold`, `time.Parse`, a retry or backoff loop, the error text). Then read what the likely homes export: `utils/` and its subpackages (`typeutils`, `safego`, `errs`), `pkg/`, `types/`, `drivers/abstract/` and `constants/`.
- Check the standard library too: `slices`, `maps`, `strings` and `errors` replace many hand-written loops.
- Flag a new helper when an existing one covers the case, or would after a small change such as one more parameter. Cite it with `file:line` and ask the author to use or extend it.
- Flag copied code: the same block of about five lines or more, with the same shape, in two drivers or twice in one package. It belongs in one place: a shared helper, or a method on the interface the drivers already implement.
- Common cases are a retry loop where `utils.RetryOnBackoff` exists, a `go func()` with its own `recover` where `safego` and `utils.Concurrent` exist, and a membership loop where `slices.Contains` does the job.
- Don't flag duplication the repo keeps on purpose. The `tests/` modules share no Go package with the root module, so `RetryOnBackoff` in `tests/testutils/utils.go` copies the one in `utils/utils.go` on purpose. Generated code is out of scope.

## 3. Placement of new functions and files

Ask of every new function, type and file: would a reader who knows the codebase look for it here?

- Package: logic for one driver goes in `drivers/<name>/internal/`, and logic every driver runs goes in `drivers/abstract/`. A library that several drivers or destinations share goes in `pkg/<x>/` (`jdbc`, `kafka`, `objstorage`, `indexdb`). Logic for one destination goes in `destination/<name>/`. Domain-free helpers go in `utils/`, the data model in `types/` and protocol constants in `constants/`.
- `utils/` holds no domain logic. A helper that knows about chunks, state, CDC positions or streams belongs with that concept.
- File: a function sits next to the type or flow it serves. A new file is for a new concept with several functions, and its name says the concept. Ask for a split when the PR grows a file past about 1000 lines.
- PR #1264 shows the shape. It split the TOAST value resolver by concept: `destination/iceberg/unavailable_values.go` holds the flow, `unavailable_values_read.go` reads from the table and `unavailable_values_memory.go` holds the caches.
- A method belongs to the type whose data it uses. A free function that reads three fields of one struct should be that struct's method.
- A name is exported only when another package uses it.
- Unit tests sit beside the code (`x_test.go` next to `x.go`). Integration tests live in `tests/<driver>/`.

## 4. Functions read like text

A reader who opens only the new function, not its helpers, can say what it does, in order, from the names alone. Run the text test in [references/readability.md](references/readability.md) on every function the PR adds and every function whose body it rewrites. That file has the rules and worked examples.

## 5. Unit tests for what the PR changes

Every function the PR adds, or whose behavior it changes, needs unit test cases that pin that behavior, in the team's test shape. Where none exist, ask for them and name the cases. The PR's tests also run against the base branch's code, which shows the behaviors the PR changed. A change the description doesn't mention is a regression until the author says otherwise. See [references/tests.md](references/tests.md).

## 6. Integration tests where unit tests can't reach

Some changes can only run against a live source database or destination, such as a change stream, a chunked read, a writer commit or schema evolution. For those, name the integration suite the author should extend, what to seed and what to assert. See [references/tests.md](references/tests.md#integration-tests).
