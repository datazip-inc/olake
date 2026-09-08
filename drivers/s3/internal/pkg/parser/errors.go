package parser

import "errors"

// decodeFailure marks an error raised while decoding a file's contents. It carries no category:
// a truncated file arrives as a bare io.EOF and a bad header as an untyped fmt.Errorf, and a
// connection dropped mid-file surfaces here too, so a classifier checks this marker last.
type decodeFailure struct{ err error }

func (d decodeFailure) Error() string { return d.err.Error() }
func (d decodeFailure) Unwrap() error { return d.err }

// DecodeFailure marks err as raised while decoding.
func DecodeFailure(err error) error {
	if err == nil {
		return nil
	}
	return decodeFailure{err: err}
}

// IsDecodeFailure reports whether err was marked by DecodeFailure.
func IsDecodeFailure(err error) bool {
	var marker decodeFailure
	return errors.As(err, &marker)
}
