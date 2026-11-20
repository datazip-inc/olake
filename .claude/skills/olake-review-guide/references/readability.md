# Functions that read like text

A function that coordinates work should read like a short account of what happens, told by the names of the functions it calls. A reader who opens only that function, and none of its helpers, can say what it does and in what order. When they need a detail, a name tells them which helper holds it.

The bar is concise but clear. A name stays short and still says what its function does, so nobody has to open the function to find out.

## The text test

Run it on every function the PR adds and on every function whose body it rewrites.

1. Read the signature and the body. Don't open any helper yet.
2. Say each statement or block as a plain sentence, using the names as written: "find the unavailable values; if there are none and nothing is cached, every record is done".
3. Mark each place where you had to open a helper, or decode an expression, to finish the sentence. The mark is a finding when a name would fix it: a step, a condition or a piece of state that has no name, or a name that says less than the code it hides.
4. Mark each sentence that mixes a business step with mechanics, such as slice indexes, byte arithmetic, map edits or string building next to "read from the table". That function works at two levels.
5. Try to summarize the whole body in one sentence. If the sentence needs "and", the function does two jobs.
6. Now open the helpers. A helper whose body does less or more than its name promises is a finding too.

## Worked example: the TOAST value resolver

PR #1264 went through this review before it merged. Its first version of the resolver's main function, in commit ab4dbb5e on the PR branch, worked but had to be decoded:

```go
// Resolve fills the unavailable values of a prefix of records and returns its length (at least 1).
// It reads once, from the first record missing a value, and ends the prefix at a row that read did
// not get or before the prefix passes limit bytes. Only a lone first record may pass the limit.
func (r *unavailableValuesResolver) Resolve(ctx context.Context, records []types.RawRecord, limit int64) (int, error) {
	r.track(records)
	// no synced column was ever marked in this thread: nothing to fill, nothing to carry
	if len(r.tracked) == 0 {
		return len(records), nil
	}
	defer r.trim()

	size, read := int64(0), false
	for idx := range records {
		record := &records[idx]
		if !read && len(r.missing(record)) > 0 {
			read = true
			if err := r.readAhead(ctx, records[idx:], limit-size-r.fetchedBytes); err != nil {
				return 0, err
			}
			// the first record is read alone, whatever its size, so each call moves on
			if idx == 0 && len(r.missing(record)) > 0 {
				if err := r.readAhead(ctx, records[:1], math.MaxInt64); err != nil {
					return 0, err
				}
			}
		}

		recordSize := r.filledSize(record)
		if idx > 0 && (len(r.missing(record)) > 0 || size+recordSize > limit) {
			return idx, nil // the next call goes on from this record
		}
		r.fill(record)
		size += recordSize
	}

	return len(records), nil
}
```

The merged version, in `destination/iceberg/unavailable_values.go` on staging, reads as text:

```go
func (r *unavailableValuesResolver) RecoverUnavailableValues(ctx context.Context, records []types.RawRecord, maxBytes int64) (int, error) {
	hasUnavailableValues := r.findUnavailableValues(records)
	if !hasUnavailableValues && r.cachedValues.isEmpty() {
		return len(records), nil // nothing to fill, and nothing cached that these writes could make stale
	}
	defer r.keepCachedValuesWithinBudget()

	size, read := int64(0), false
	for idx := range records {
		record := &records[idx]
		if !read && r.needsRead(record) {
			read = true
			if err := r.readUncachedValuesFromTable(ctx, records, idx, maxBytes-size); err != nil {
				return 0, err
			}
		}

		recordSize := r.filledSize(record)
		if idx > 0 && (r.needsRead(record) || size+recordSize > maxBytes) {
			return idx, nil // the next call goes on from this record
		}
		r.fillRecord(record)
		size += recordSize
	}
	return len(records), nil
}
```

Read aloud, it says: find the unavailable values. If there are none and nothing is cached, every record is done. At the end, keep the cached values within their budget. Walk the records. At the first record that needs a read, read the uncached values from the table, once. Stop before a record that still needs a read or would pass `maxBytes`. Otherwise fill the record and count its size.

What changed, and the rule each change applies:

| Before | After | Rule |
|---|---|---|
| `Resolve(..., limit int64)` | `RecoverUnavailableValues(..., maxBytes int64)` | The exported name says what the caller gets, and the number's name carries its unit (R2, R4) |
| `r.track(records)`, then `if len(r.tracked) == 0` | `hasUnavailableValues := r.findUnavailableValues(records)` | The step says what it finds, and the condition gets a name before the `if` (R2, R3) |
| `len(r.missing(record)) > 0`, three times | `r.needsRead(record)` | A repeated expression becomes a predicate (R3) |
| `defer r.trim()` | `defer r.keepCachedValuesWithinBudget()` | The name says what it achieves, not how (R2) |
| The lone-first-record read with `math.MaxInt64`, nested in the loop | Moved into `readUncachedValuesFromTable` | One level of abstraction per function (R1) |
| `r.readAhead(ctx, records[idx:], limit-size-r.fetchedBytes)` | `r.readUncachedValuesFromTable(ctx, records, idx, maxBytes-size)` | The step says what it reads and from where, and its bookkeeping moved inside (R1, R2) |
| `r.fill`, and the fields `carry`, `fetched` and `tracked` | `r.fillRecord`, and `cachedValues`, `tableValues` and `unavailableColumns` | State is named for what it holds (R4) |

None of the new names is long, and none needs its body read to be understood. That is the bar.

## Rules

The examples below are illustrations with invented names. The resolver above is the real case.

### R1. One level of abstraction per function

A coordinating function lists steps. Indexes, byte math, map edits, string building and protocol details belong in the helper that owns each step. A function that mixes the two makes the reader change altitude on every line.

```go
// Before: the steps (encode, upload, clear) are buried in mechanics.
func (w *writer) flush(ctx context.Context) error {
	if len(w.buffer) == 0 {
		return nil
	}
	payload := make([]byte, 0, w.bufferedBytes)
	for _, record := range w.buffer {
		encoded, err := json.Marshal(record)
		if err != nil {
			return err
		}
		payload = append(append(payload, encoded...), '\n')
	}
	if err := w.client.Upload(ctx, w.nextFileName(), payload); err != nil {
		return err
	}
	w.buffer, w.bufferedBytes = w.buffer[:0], 0
	return nil
}

// After: each line is a step; the encoding lives in encodeAsJSONLines.
func (w *writer) flush(ctx context.Context) error {
	if w.isEmpty() {
		return nil
	}
	payload, err := encodeAsJSONLines(w.buffer)
	if err != nil {
		return fmt.Errorf("failed to encode buffered records: %w", err)
	}
	if err := w.client.Upload(ctx, w.nextFileName(), payload); err != nil {
		return fmt.Errorf("failed to upload buffered records: %w", err)
	}
	w.clearBuffer()
	return nil
}
```

### R2. Name each step for what it achieves

- Use a verb and its object: `fillRecord`, `readUncachedValuesFromTable`, `keepCachedValuesWithinBudget`.
- Flag verbs that say nothing on their own: `handle`, `process`, `do`, `manage`, `execute`, `perform`. Also flag a bare verb with no object: `track`, `trim`, `resolve`, `update`, `check`.
- An exported name says what the caller gets (`RecoverUnavailableValues`), not the internal mechanism (`Resolve`).
- A name doesn't repeat its package: `iceberg.Writer`, not `iceberg.IcebergWriter`.

### R3. Name every condition

- An expression used more than once becomes a predicate: `len(r.missing(record)) > 0` became `r.needsRead(record)`.
- A compound condition gets a named boolean or a predicate before the `if`, so the `if` reads as a sentence: `if !hasUnavailableValues && r.cachedValues.isEmpty()`.
- Predicates read as yes-or-no questions: `isDelete`, `hasAll`, `needsRead`.
- Avoid negative names such as `notReady` or `skipDisabled`, which force a reader through `!notReady`.

```go
// Before: the reader must decode why these three facts matter together.
if op != deleteOp && len(missing) > 0 && !appendOnly {

// After: the condition has the name of the question it answers.
if needsPreviousValues(record) {
```

### R4. Name state for what it holds

- Name a field or variable after the thing it holds, in words the reader already knows: `cachedValues`, `tableValues`, `unavailableColumns`, `maxBytes`.
- Flag mechanism metaphors (`carry`, `window`, `bucket`), bare past participles (`fetched`, `tracked`), abbreviations (`cv`, `tv`) and names that only give the type (`m`, `list`, `data`).
- A number's name says its unit unless its type does: `maxBytes`, and `retryDelay time.Duration`.
- Constants, types and helpers tied to the state share its vocabulary: `maxCachedBytes` goes with `cachedValues`. When the PR renames one, it renames the rest in the same change.

### R5. Short where the scope is short

- `idx`, `err`, `ctx` and `t` are fine in a short loop or a small function. A field, a parameter of an exported function or a package-level name spells out what it is.
- Short doesn't mean cryptic: `fillRecord` is short and complete, while `fr` and `doFill` are not.

### R6. Keep the main path flat

- Handle the trivial or failing case first and return, with the reason: `return len(records), nil // nothing to fill, and nothing cached that these writes could make stale`.
- Don't put an `else` after a `return`.
- More than two levels of nesting inside a loop, or a loop body taller than a screen, means the body is a step that needs a name.

### R7. Call sites read too

- Flag bare boolean or number arguments that switch behavior, such as `writeBatch(records, true, false)`. Use two functions, a named type or an options struct with field names.
- Flag long lists of positional parameters of the same type, such as `(string, string, string)`. A struct names them.

### R8. Don't over-extract

- A helper must hide detail its caller doesn't need. A one-line wrapper that only renames a call, a layer that passes its arguments straight through, or a single-use helper the reader must open to follow the flow adds a hop and saves nothing.
- `needsRead(record)` earns its place: it names a question the function asks three times. `func getValue(m map[string]any, k string) any { return m[k] }` doesn't.
- Put a step's helper in the same file, below its caller and in calling order, so the file reads top to bottom.

### R9. Comments carry the contract and the why

- At most two lines above a declaration: what the function returns or guarantees, and any condition the name can't carry, such as "returns at least 1" or "only a lone first record may pass maxBytes".
- An inline comment explains a non-obvious reason for that one line: `// the next call goes on from this record`.
- A comment that narrates what the next lines do means a name is missing. Ask for the name, not a better comment.

### R10. One job per function

- The one-sentence summary has no "and".
- A function the reader has to scroll, or one with more than about four parameters, usually does two jobs or is missing a type.
- Length alone is not a finding. A flat `switch` over types, or a table of cases, can be long and still read as text.

## Writing the finding

Quote the name or the line, say what the reader can't tell from it, then offer two or three candidate names with a few words each. The author picks the name. For example:

> `r.track(records)` hides that it drops unselected columns and reports whether any value is missing, so the caller can't be read without opening it.
> Name it for what it returns, such as `findUnavailableValues` or `collectUnavailableColumns`.

## Don't flag

- Code the PR doesn't add or rewrite.
- Generated code, test case tables and fixtures.
- A name that matches the established vocabulary of its package, even if you would pick another.
- Anything `gofmt` or `golangci-lint` already reports.
