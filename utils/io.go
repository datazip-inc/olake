package utils

import "io"

// compositeReadCloser reads from Reader and closes all closers on Close.
type compositeReadCloser struct {
	io.Reader
	closers []io.Closer
}

// NewCompositeReadCloser returns a ReadCloser that reads from reader and, on
// Close, closes every closer in order, returning the first error. Use it when
// layering readers (e.g. a decompressor over a network body) where closing
// the outer reader does not close the inner one.
func NewCompositeReadCloser(reader io.Reader, closers ...io.Closer) io.ReadCloser {
	return &compositeReadCloser{Reader: reader, closers: closers}
}

func (c *compositeReadCloser) Close() error {
	var firstErr error
	for _, closer := range c.closers {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
