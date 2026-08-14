package destination

import "errors"

// writeFailure marks an error raised by a writer itself — the record could not be encoded,
// flushed or committed.
//
// It carries no category. A full disk, a dropped upload and an unencodable value all surface at
// the same call sites, and classification takes the innermost answer, so a category attached
// here would outrank the specific cause. Each writer's classifier checks this marker last, after
// its own vendor and platform rules.
//
// Error() returns the wrapped error's message unchanged, so marking is invisible to logs and
// to anything matching on message text.
type writeFailure struct{ err error }

func (w writeFailure) Error() string { return w.err.Error() }
func (w writeFailure) Unwrap() error { return w.err }

// WriteFailure marks err as raised by the writer itself. Returns nil for nil so it can wrap a
// call that may not have failed.
func WriteFailure(err error) error {
	if err == nil {
		return nil
	}
	return writeFailure{err: err}
}

// IsWriteFailure reports whether err was marked by WriteFailure.
//
// Callers must check this *after* their vendor and platform rules, never before.
func IsWriteFailure(err error) bool {
	var marker writeFailure
	return errors.As(err, &marker)
}
