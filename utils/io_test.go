package utils

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordingCloser struct {
	closed bool
	err    error
}

func (r *recordingCloser) Close() error {
	r.closed = true
	return r.err
}

func TestNewCompositeReadCloser(t *testing.T) {
	inner := &recordingCloser{}
	outer := &recordingCloser{}
	rc := NewCompositeReadCloser(strings.NewReader("payload"), outer, inner)

	// Reads pass through to the reader
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "payload", string(data))

	// Close closes every closer
	require.NoError(t, rc.Close())
	require.True(t, outer.closed)
	require.True(t, inner.closed)
}

func TestNewCompositeReadCloserFirstError(t *testing.T) {
	first := &recordingCloser{err: errors.New("outer close failed")}
	second := &recordingCloser{err: errors.New("inner close failed")}
	rc := NewCompositeReadCloser(strings.NewReader(""), first, second)

	// First error wins, but all closers still run
	err := rc.Close()
	require.ErrorContains(t, err, "outer close failed")
	require.True(t, first.closed)
	require.True(t, second.closed)
}
