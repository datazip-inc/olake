// Package testhandler gives each unit test its own databases and destinations on shared containers, which make starts.
// It knows no driver or destination: drivers implement Source beside their tests, destinations implement DestinationKind.
package testhandler

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"
)

const (
	idPrefix       = "olake_ut_"
	idAlphabet     = "abcdefghijklmnopqrstuvwxyz234567"
	cleanupTimeout = 2 * time.Minute
	red            = "\033[31m"
	bold           = "\033[1m"
	reset          = "\033[0m"
)

// Handler hands one test its isolated databases and destinations and removes them when the test ends.
type Handler struct {
	t       testing.TB
	id      string
	mu      sync.Mutex
	handles map[any]any
}

// New returns a handler with a fresh test id. Nothing is created until the test asks for it.
func New(t testing.TB) *Handler {
	return &Handler{t: t, id: newID(), handles: map[any]any{}}
}

// ID names everything this test owns: its source databases and its destination database.
func (h *Handler) ID() string {
	return h.id
}

// newID returns a name that every database, namespace, replication slot and key prefix accepts.
func newID() string {
	random := make([]byte, 12)
	_, _ = rand.Read(random)
	for i, b := range random {
		random[i] = idAlphabet[b&31]
	}
	return idPrefix + string(random)
}

// once returns the handle stored under key and creates it on the first call.
func once[T any](h *Handler, key any, create func() T) T {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if handle, ok := h.handles[key]; ok {
		return handle.(T)
	}
	handle := create()
	// Logged once the test owns something, so a test whose container is missing prints only that line.
	if len(h.handles) == 0 {
		h.t.Logf("testhandler: test id %s", h.id)
	}
	h.handles[key] = handle
	return handle
}

// connect fails the test, naming the make target to run, when a service's container does not answer.
func (h *Handler) connect(s *Service) {
	h.t.Helper()
	ctx, cancel := h.untilDeadline()
	defer cancel()
	err := s.connect(ctx)
	if errors.Is(err, errContainerNotFound) {
		h.t.Fatalf(red+"container '%s' not found, run "+bold+"make %s"+reset+red+" in repo root and re run test"+reset, s.Container, s.Target)
	}
	if err != nil {
		h.t.Fatalf("testhandler: %s", err)
	}
}

// untilDeadline is the test's context cut short just before the test binary's deadline,
// so a wait that cannot end fails with its own message instead of a timeout panic.
func (h *Handler) untilDeadline() (context.Context, context.CancelFunc) {
	if t, ok := h.t.(interface{ Deadline() (time.Time, bool) }); ok {
		if deadline, set := t.Deadline(); set {
			return context.WithDeadline(h.t.Context(), deadline.Add(-10*time.Second))
		}
	}
	return context.WithCancel(h.t.Context())
}

// cleanup registers the drop of this test's database for the end of the test, when the test's own context is already canceled.
func (h *Handler) cleanup(what string, drop func(ctx context.Context, database string) error) {
	h.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := drop(ctx, h.id); err != nil {
			h.t.Errorf("testhandler: cleanup of %s for test id %s failed: %s", what, h.id, err)
		}
	})
}
