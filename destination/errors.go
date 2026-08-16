package destination

import "errors"

// writeFailure marks an error raised by the writer itself — a record that could not be encoded,
// flushed or committed. It carries no category on purpose: classification takes the innermost
// answer, so each writer's classifier checks this marker last, after its own vendor rules.
type writeFailure struct{ err error }

func (w writeFailure) Error() string { return w.err.Error() }
func (w writeFailure) Unwrap() error { return w.err }

// WriteFailure marks err as raised by the writer itself.
func WriteFailure(err error) error {
	if err == nil {
		return nil
	}
	return writeFailure{err: err}
}

// IsWriteFailure reports whether err was marked by WriteFailure.
func IsWriteFailure(err error) bool {
	var marker writeFailure
	return errors.As(err, &marker)
}
