package testhandler

import (
	"context"
	"testing"

	"github.com/datazip-inc/olake/types"
)

// DestinationKind is a destination tests write to. The destination implements it in its own test package,
// and the handler gives each test its own database in it, named by the test id.
type DestinationKind interface {
	// Name names the kind in failures; a test gets one destination per kind.
	Name() string
	// Services are the containers the kind needs; the handler checks each before Open.
	Services() []*Service
	// Open readies the kind for one test and returns its writer config, the same for every test of the kind.
	// It registers on t whatever it must release when the test ends.
	Open(ctx context.Context, t testing.TB) (*types.WriterConfig, error)
	// Drop removes a test's database from the destination, with everything stored under it.
	Drop(ctx context.Context, database string) error
}

// Destination is a writer config and the destination database that isolates one test in it.
type Destination struct {
	// Config is the same for every test of a kind, so it survives the writers' per-process caches.
	Config *types.WriterConfig
	// Database must be the destination database of every stream the test writes; cleanup removes it.
	Database string
}

// Destination returns the writer config of a kind and this test's own database in it. Cleanup removes that database.
func (h *Handler) Destination(kind DestinationKind) *Destination {
	h.t.Helper()
	return once(h, kind.Name(), func() *Destination {
		for _, service := range kind.Services() {
			h.connect(service)
		}
		ctx, cancel := h.untilDeadline()
		defer cancel()
		config, err := kind.Open(ctx, h.t)
		if err != nil {
			h.t.Fatalf("testhandler: %s", err)
		}
		h.cleanup(kind.Name()+" destination", kind.Drop)
		return &Destination{Config: config, Database: h.id}
	})
}
