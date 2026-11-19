package testhandler

import (
	"context"
)

// Source is a database server that a driver's tests share. The driver implements it beside its tests,
// and the handler gives each test its own database on it, named by the test id.
type Source interface {
	// Service is the container the server runs in.
	Service() *Service
	// Create makes a test's database on the server.
	Create(ctx context.Context, database string) error
	// Drop removes a test's database with everything in it.
	Drop(ctx context.Context, database string) error
}

// Database returns the name of this test's own database on the source. Cleanup drops it.
func (h *Handler) Database(s Source) string {
	h.t.Helper()
	return once(h, s.Service(), func() string {
		h.connect(s.Service())
		h.cleanup("the database on "+s.Service().Container, s.Drop)
		ctx, cancel := h.untilDeadline()
		defer cancel()
		if err := s.Create(ctx, h.id); err != nil {
			h.t.Fatalf("testhandler: %s", err)
		}
		return h.id
	})
}
