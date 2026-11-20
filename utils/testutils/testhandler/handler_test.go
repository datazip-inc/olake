package testhandler

// Keep third-party imports to testify: go mod tidy follows this file's imports into the go.sum
// of every module whose tests import the package.
import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

// notFound is the line a test whose container is missing fails with: red, with the make command bold.
const notFound = "\033[31mcontainer 'olake-testhandler-absent' not found, run \033[1mmake testhandler.absent.start\033[0m\033[31m in repo root and re run test\033[0m"

func TestNew(t *testing.T) {
	testCases := []struct {
		name  string
		check func(t *testing.T, id string)
	}{
		{
			name: "the id is a lowercase identifier short enough for every system",
			check: func(t *testing.T, id string) {
				require.Regexp(t, `^[a-z][a-z0-9_]{0,29}$`, id)
			},
		},
		{
			name: "the id survives destination name normalization",
			check: func(t *testing.T, id string) {
				require.Equal(t, id, utils.Reformat(id))
			},
		},
		{
			name: "every handler gets its own id",
			check: func(t *testing.T, id string) {
				require.NotEqual(t, id, New(t).ID())
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.check(t, New(t).ID())
		})
	}
}

func TestHandlerDatabase(t *testing.T) {
	testCases := []struct {
		name      string
		source    Source
		wantFatal string
	}{
		{
			name:      "a source whose container is not running fails the test with the make target that starts it",
			source:    &fakeSource{service: absentService()},
			wantFatal: notFound,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.wantFatal, fatalMessage(t, func(tb testing.TB) { New(tb).Database(tc.source) }))
		})
	}
}

func TestHandlerDestination(t *testing.T) {
	testCases := []struct {
		name string
		// kinds are what one test asks for, in order.
		kinds []*fakeKind
		// wantFatal is the failure the handler must raise instead of handing out the destination.
		wantFatal string
	}{
		{
			name:  "the kind opens once per test and drops the test's database when the test ends",
			kinds: []*fakeKind{{}},
		},
		{
			name:  "kinds that share a name each open their own destination",
			kinds: []*fakeKind{{}, {}},
		},
		{
			name:      "a kind whose container is not running fails the test with the make target that starts it",
			kinds:     []*fakeKind{{services: []*Service{absentService()}}},
			wantFatal: notFound,
		},
		{
			name:      "a kind that cannot open fails the test with its reason",
			kinds:     []*fakeKind{{openErr: errors.New("no turn on the writer")}},
			wantFatal: "testhandler: no turn on the writer",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantFatal != "" {
				kind := tc.kinds[0]
				require.Equal(t, tc.wantFatal, fatalMessage(t, func(tb testing.TB) { New(tb).Destination(kind) }))
				require.Empty(t, kind.dropped, "a destination that was never handed out has nothing to drop")
				return
			}
			var id string
			t.Run("test", func(t *testing.T) {
				h := New(t)
				id = h.ID()
				for _, kind := range tc.kinds {
					dest := h.Destination(kind)
					require.Same(t, dest, h.Destination(kind))
					require.Equal(t, id, dest.Database)
					require.Empty(t, kind.dropped, "the drop waits for the end of the test")
				}
			})
			for _, kind := range tc.kinds {
				require.Equal(t, 1, kind.opens)
				require.Equal(t, []string{id}, kind.dropped)
			}
		})
	}
}

// absentService is a container no compose file starts.
func absentService() *Service {
	return &Service{Container: "olake-testhandler-absent", Port: 1, Target: "testhandler.absent.start"}
}

type fakeSource struct {
	service *Service
}

func (s *fakeSource) Service() *Service {
	return s.service
}

func (s *fakeSource) Create(context.Context, string) error {
	return nil
}

func (s *fakeSource) Drop(context.Context, string) error {
	return nil
}

// fakeKind counts what the handler asks of it.
type fakeKind struct {
	services []*Service
	openErr  error
	opens    int
	dropped  []string
}

func (k *fakeKind) Name() string {
	return "fake"
}

func (k *fakeKind) Services() []*Service {
	return k.services
}

func (k *fakeKind) Open(context.Context, testing.TB) (*types.WriterConfig, error) {
	k.opens++
	if k.openErr != nil {
		return nil, k.openErr
	}
	return &types.WriterConfig{}, nil
}

func (k *fakeKind) Drop(_ context.Context, database string) error {
	k.dropped = append(k.dropped, database)
	return nil
}

// fatalRecorder stands in for a test so that a Fatalf can be observed instead of failing the run.
type fatalRecorder struct {
	testing.TB
	message string
}

func (f *fatalRecorder) Fatalf(format string, args ...any) {
	f.message = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// fatalMessage runs body against a recorder and returns the Fatalf message it raised, if any.
func fatalMessage(t *testing.T, body func(tb testing.TB)) string {
	t.Helper()
	recorder := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		body(recorder)
	}()
	<-done
	return recorder.message
}
