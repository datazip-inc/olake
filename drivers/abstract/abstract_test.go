package abstract

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cleanupResult panics under the real handleWriterCleanup defer. An empty writer map is the no-op
// case of the switch, leaving the recovered panic as the only error; nil takes the default branch,
// so the close half of the function contributes "unsupported writer type" on top of it.
func cleanupResult(prior error, r any, threadID string, writer any) error {
	err := prior
	func() {
		defer handleWriterCleanup(context.Background(), func() {}, &err, writer, threadID, nil, nil)
		panic(r)
	}()
	return err
}

// writerArg picks the switch case a test wants: nil for the default branch, otherwise an empty
// map, which closes nothing.
func writerArg(nilWriter bool) any {
	if nilWriter {
		return nil
	}
	return map[string]*destination.WriterThread{}
}

func networkReset() error {
	return fmt.Errorf("read failed: %w", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET})
}

func TestHandleWriterCleanupClassification(t *testing.T) {
	classifiedPrior := errs.Precondition(errs.CDCPositionLost, "mssql.lsn_lost", errors.New("lsn gone"))

	testCases := []struct {
		name              string
		prior             error
		panicValue        any
		threadID          string
		expectedCategory  errs.Category
		expectedBy        string
		expectedCode      string
		expectedType      string
		expectedComponent string
		nilWriter         bool // takes the default branch, so closeErr wraps the panic
	}{
		// the panic is the only evidence, so it is classified at the raise site
		{
			name:              "no prior error",
			panicValue:        "assignment to entry in nil map",
			expectedCategory:  errs.InternalError,
			expectedBy:        errs.ClassifiedByPrecondition,
			expectedCode:      codeWriterPanicRecovered,
			expectedComponent: "sync",
		},
		// a runtime panic carries an error value rather than a string
		{
			name:              "runtime error value",
			panicValue:        errors.New("runtime error: index out of range"),
			expectedCategory:  errs.InternalError,
			expectedBy:        errs.ClassifiedByPrecondition,
			expectedCode:      codeWriterPanicRecovered,
			expectedComponent: "sync",
		},
		// thread[id] is wrapped after classification; From must still find the panic
		{
			name:              "thread wrap still classifies the panic",
			panicValue:        "boom",
			threadID:          "public.users_abc",
			expectedCategory:  errs.InternalError,
			expectedBy:        errs.ClassifiedByPrecondition,
			expectedCode:      codeWriterPanicRecovered,
			expectedComponent: "sync",
		},
		// a classified prior error is the cause; the panic may be a consequence of it
		{
			name:              "classified prior error keeps its category",
			prior:             classifiedPrior,
			panicValue:        "boom",
			expectedCategory:  errs.CDCPositionLost,
			expectedBy:        errs.ClassifiedByPrecondition,
			expectedCode:      "mssql.lsn_lost",
			expectedComponent: "mssql",
		},
		// the %w wrap is what keeps an unclassified prior reachable by the shared rules
		{
			name:             "stdlib prior error still reaches the shared rules",
			prior:            networkReset(),
			panicValue:       "boom",
			expectedCategory: errs.NetworkUnreachable,
			expectedBy:       errs.ClassifiedByStdlib,
			expectedCode:     "connection_reset",
		},
		// a cancellation is not a bug; wrapping a panic must not reclassify it as internal_error
		{
			name:             "canceled prior is not an internal error",
			prior:            context.Canceled,
			panicValue:       "boom",
			expectedCategory: errs.Canceled,
			expectedBy:       errs.ClassifiedByStdlib,
		},
		// Join is a tree; the classified branch must still win after the panic wrap
		{
			name:              "join prior, classified branch wins",
			prior:             errors.Join(errors.New("noise"), classifiedPrior),
			panicValue:        "boom",
			expectedCategory:  errs.CDCPositionLost,
			expectedBy:        errs.ClassifiedByPrecondition,
			expectedCode:      "mssql.lsn_lost",
			expectedComponent: "mssql",
		},
		// an unclassifiable prior leaves the concrete type as the only remaining clue
		{
			name:             "unclassifiable prior error",
			prior:            errors.New("something opaque"),
			panicValue:       "boom",
			expectedCategory: errs.Unclassified,
			expectedBy:       errs.ClassifiedByDefault,
			expectedType:     "*errors.errorString",
		},
		// a close failure wraps the panic with %w, so the panic must stay the classification
		{
			name:              "close error does not bury the panic",
			panicValue:        "boom",
			nilWriter:         true,
			expectedCategory:  errs.InternalError,
			expectedBy:        errs.ClassifiedByPrecondition,
			expectedCode:      codeWriterPanicRecovered,
			expectedComponent: "sync",
		},
		// and it must not bury a prior cause that outranks the panic either
		{
			name:              "close error does not bury a classified prior",
			prior:             classifiedPrior,
			panicValue:        "boom",
			nilWriter:         true,
			expectedCategory:  errs.CDCPositionLost,
			expectedBy:        errs.ClassifiedByPrecondition,
			expectedCode:      "mssql.lsn_lost",
			expectedComponent: "mssql",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := cleanupResult(tc.prior, tc.panicValue, tc.threadID, writerArg(tc.nilWriter))
			require.Error(t, err)

			got := errs.From(errs.Classify(err))
			assert.Equal(t, tc.expectedCategory, got.Category, "category")
			assert.Equal(t, tc.expectedBy, got.ClassifiedBy, "classified_by")
			assert.Equal(t, tc.expectedCode, got.Code, "code")
			assert.Equal(t, tc.expectedType, got.ErrorType, "error_type")
			assert.Equal(t, tc.expectedComponent, got.Component, "component")
		})
	}
}

func TestHandleWriterCleanupMessage(t *testing.T) {
	testCases := []struct {
		name       string
		prior      error
		panicValue any
		threadID   string
		contains   []string
		asOpError  bool
		nilWriter  bool
	}{
		// classification must not rewrite the panic text an operator reads
		{
			name:       "panic text is unchanged",
			panicValue: "nil map write",
			contains:   []string{"panic recovered: nil map write"},
		},
		// the prior error stays in the chain and in the message
		{
			name:       "prior error stays reachable",
			prior:      networkReset(),
			panicValue: "boom",
			contains:   []string{"panic recovered: boom", "read failed"},
			asOpError:  true,
		},
		// the thread prefix is added without flattening the chain
		{
			name:       "thread wrap is visible and unwraps",
			prior:      networkReset(),
			panicValue: "boom",
			threadID:   "public.users_abc",
			contains:   []string{"thread[public.users_abc]", "panic recovered: boom", "read failed"},
			asOpError:  true,
		},
		// the close half of the function: a writer it cannot close is reported alongside the panic
		{
			name:       "close error is reported with the panic",
			panicValue: "boom",
			nilWriter:  true,
			contains:   []string{"unsupported writer type", "prev error:", "panic recovered: boom"},
		},
		// and it still sits inside the thread prefix, with the prior error left reachable
		{
			name:       "close error keeps the thread prefix and the chain",
			prior:      networkReset(),
			panicValue: "boom",
			threadID:   "public.users_abc",
			nilWriter:  true,
			contains:   []string{"thread[public.users_abc]", "unsupported writer type", "prev error:", "read failed"},
			asOpError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := cleanupResult(tc.prior, tc.panicValue, tc.threadID, writerArg(tc.nilWriter))
			require.Error(t, err)

			for _, fragment := range tc.contains {
				assert.Contains(t, err.Error(), fragment)
			}
			if tc.asOpError {
				assert.True(t, errors.As(err, new(*net.OpError)), "the chain must not be flattened")
			}
		})
	}
}

func TestGenerateThreadID(t *testing.T) {
	testCases := []struct {
		name     string
		streamID string
		hash     string
		exact    string
		prefix   string
	}{
		// a supplied hash is used as the suffix, so retries of the same chunk are stable
		{
			name:     "hash is the suffix",
			streamID: "public.users",
			hash:     "chunk1",
			exact:    "public.users_chunk1",
		},
		// an empty stream id still joins with an underscore
		{
			name:     "empty stream id",
			streamID: "",
			hash:     "chunk1",
			exact:    "_chunk1",
		},
		// no hash: a ULID is generated; only the stream prefix is stable
		{
			name:     "empty hash uses a generated suffix",
			streamID: "public.users",
			prefix:   "public.users_",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := generateThreadID(tc.streamID, tc.hash)
			if tc.exact != "" {
				assert.Equal(t, tc.exact, got)
				return
			}
			assert.True(t, len(got) > len(tc.prefix), "generated suffix must be non-empty")
			assert.Equal(t, tc.prefix, got[:len(tc.prefix)])
		})
	}

	// two calls without a hash must not collide; the ULID is the uniqueness
	first := generateThreadID("public.users", "")
	second := generateThreadID("public.users", "")
	assert.NotEqual(t, first, second)
}

func TestSupportsCdcColumn(t *testing.T) {
	testCases := []struct {
		name         string
		driverType   string
		cdcSupported bool
		expected     bool
	}{
		// a cdc-capable relational driver adds the olake cdc timestamp column
		{name: "postgres with cdc", driverType: "postgres", cdcSupported: true, expected: true},
		// kafka has no cdc timestamp column even when cdc is on
		{name: "kafka with cdc", driverType: string(constants.Kafka), cdcSupported: true, expected: false},
		// cdc off means the column is not added
		{name: "postgres without cdc", driverType: "postgres", cdcSupported: false, expected: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			driver := NewAbstractDriver(context.Background(), stubDriver{
				typ:          tc.driverType,
				cdcSupported: tc.cdcSupported,
			})
			assert.Equal(t, tc.expected, driver.supportsCdcColumn())
		})
	}
}

func TestReadCDCNotConfigured(t *testing.T) {
	testCases := []struct {
		name             string
		driverType       string
		cdcSupported     bool
		cdcStreams       int
		expectedErr      bool
		expectedCategory errs.Category
		expectedCode     string
	}{
		// no cdc streams: the cdc branch is skipped
		{name: "no cdc streams", driverType: "postgres", cdcStreams: 0},
		// cdc selected but the source has no cdc config
		{
			name:             "cdc selected without config",
			driverType:       "postgres",
			cdcStreams:       1,
			expectedErr:      true,
			expectedCategory: errs.CDCPreconditionFailed,
			expectedCode:     "postgres.cdc_not_configured",
		},
		// the code prefix is the driver type, not a hardcoded postgres string
		{
			name:             "driver type is interpolated into the code",
			driverType:       "mysql",
			cdcStreams:       1,
			expectedErr:      true,
			expectedCategory: errs.CDCPreconditionFailed,
			expectedCode:     "mysql.cdc_not_configured",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			driver := NewAbstractDriver(context.Background(), stubDriver{typ: tc.driverType, cdcSupported: tc.cdcSupported})
			cdcStreams := make([]types.StreamInterface, tc.cdcStreams)

			err := driver.Read(context.Background(), nil, nil, cdcStreams, nil)
			if !tc.expectedErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)

			got := errs.From(errs.Classify(err))
			assert.Equal(t, tc.expectedCategory, got.Category, "category")
			assert.Equal(t, errs.ClassifiedByPrecondition, got.ClassifiedBy, "classified_by")
			assert.Equal(t, tc.expectedCode, got.Code, "code")
			assert.Equal(t, tc.driverType, got.Component, "component")
		})
	}
}

func TestDiscover(t *testing.T) {
	ctx := context.Background()
	networkErr := networkReset()

	testCases := []struct {
		name               string
		skipSchema         bool
		streamNamesErr     error
		expectedNilStreams bool
		expectedErr        bool
		expectedCategory   errs.Category
		expectedCode       string
	}{
		// skipSchema reuses the catalog schema and must not produce a new one
		{name: "skipSchema skips schema production", skipSchema: true, expectedNilStreams: true},
		// GetStreamNames still runs when skipSchema is set; a failure is not swallowed
		{
			name:             "skipSchema still reports a GetStreamNames failure",
			skipSchema:       true,
			streamNamesErr:   networkErr,
			expectedErr:      true,
			expectedCategory: errs.NetworkUnreachable,
			expectedCode:     "connection_reset",
		},
		// discover wraps GetStreamNames with %w so the cause stays classifiable
		{
			name:             "discover preserves a GetStreamNames cause",
			streamNamesErr:   networkErr,
			expectedErr:      true,
			expectedCategory: errs.NetworkUnreachable,
			expectedCode:     "connection_reset",
		},
		// a classified names error outranks the discover wrapper
		{
			name:             "discover preserves a classified names error",
			streamNamesErr:   errs.Precondition(errs.AuthFailed, "postgres.auth_failed", errors.New("bad password")),
			expectedErr:      true,
			expectedCategory: errs.AuthFailed,
			expectedCode:     "postgres.auth_failed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			driver := NewAbstractDriver(ctx, stubDriver{typ: "postgres", streamNamesErr: tc.streamNamesErr})
			streams, err := driver.Discover(ctx, 0, tc.skipSchema)

			if tc.expectedErr {
				require.Error(t, err)
				assert.Nil(t, streams)

				got := errs.From(errs.Classify(err))
				assert.Equal(t, tc.expectedCategory, got.Category, "category")
				assert.Equal(t, tc.expectedCode, got.Code, "code")
				return
			}

			require.NoError(t, err)
			if tc.expectedNilStreams {
				assert.Nil(t, streams)
			}
		})
	}
}

// stubDriver is a DriverInterface that returns zero values except where a test sets a field.
type stubDriver struct {
	typ            string
	cdcSupported   bool
	streamNamesErr error
}

func (s stubDriver) GetConfigRef() Config { return nil }
func (s stubDriver) Spec() any            { return nil }
func (s stubDriver) Type() string         { return s.typ }
func (s stubDriver) Setup(context.Context) error {
	return nil
}
func (s stubDriver) SetupState(*types.State) {}
func (s stubDriver) MaxConnections() int     { return 0 }
func (s stubDriver) MaxRetries() int         { return 0 }
func (s stubDriver) GetStreamNames(context.Context) ([]types.StreamID, error) {
	return nil, s.streamNamesErr
}
func (s stubDriver) ProduceSchema(context.Context, types.StreamID) (*types.Stream, error) {
	return &types.Stream{}, nil
}
func (s stubDriver) GetOrSplitChunks(context.Context, *destination.WriterPool, types.StreamInterface) (*types.Set[types.Chunk], error) {
	return types.NewSet[types.Chunk](), nil
}
func (s stubDriver) ChunkIterator(context.Context, types.StreamInterface, types.Chunk, BackfillMsgFn) error {
	return nil
}
func (s stubDriver) FetchMaxCursorValues(context.Context, types.StreamInterface) (any, any, error) {
	return nil, nil, nil
}
func (s stubDriver) StreamIncrementalChanges(context.Context, types.StreamInterface, BackfillMsgFn) error {
	return nil
}
func (s stubDriver) CDCSupported() bool { return s.cdcSupported }
func (s stubDriver) ChangeStreamConfig() (bool, bool, bool) {
	return false, false, false
}
func (s stubDriver) PreCDC(context.Context, []types.StreamInterface) error { return nil }
func (s stubDriver) StreamChanges(context.Context, int, map[string]any, CDCMsgFn) (any, error) {
	// the real drivers return a metadata state here; a stub that streams nothing has none
	return nil, nil //nolint:nilnil // no metadata state to report
}
func (s stubDriver) PostCDC(context.Context, int) error { return nil }

// discoverStreams are the streams stubSampler lists, all in one namespace.
var discoverStreams = []types.StreamID{{Namespace: "db", Name: "a"}, {Namespace: "db", Name: "b"}, {Namespace: "db", Name: "c"}}

// stubSampler is a sampling driver: each successful ProduceSampledSchema call returns a stream
// whose only sampled column names the tier, so a test can tell which tier a stream ended at.
type stubSampler struct {
	stubDriver
	streams []types.StreamID
	// sample decides one call's outcome; nil samples every tier successfully
	sample func(ctx context.Context, streamID types.StreamID, limit int) error

	mu       sync.Mutex
	calls    []int            // limits in the order the calls started
	byStream map[string][]int // limits each stream was sampled with
}

func newStubSampler(streams []types.StreamID, sample func(context.Context, types.StreamID, int) error) *stubSampler {
	return &stubSampler{
		stubDriver: stubDriver{typ: "mongodb"},
		streams:    streams,
		sample:     sample,
		byStream:   make(map[string][]int),
	}
}

func (s *stubSampler) GetStreamNames(context.Context) ([]types.StreamID, error) {
	return s.streams, nil
}

// MaxRetries is 1 because RetryOnBackoff never calls a function given 0 attempts.
func (s *stubSampler) MaxRetries() int { return 1 }

func (s *stubSampler) ProduceSampledSchema(ctx context.Context, streamID types.StreamID, limit int) (*types.Stream, error) {
	s.mu.Lock()
	s.calls = append(s.calls, limit)
	s.byStream[streamID.Name] = append(s.byStream[streamID.Name], limit)
	s.mu.Unlock()

	if s.sample != nil {
		if err := s.sample(ctx, streamID, limit); err != nil {
			return nil, err
		}
	}
	stream := types.NewStream(streamID.Name, streamID.Namespace, nil)
	stream.UpsertField(tierColumn(limit), types.String, true, false)
	return stream, nil
}

func tierColumn(limit int) string {
	return fmt.Sprintf("col_%d", limit)
}

// sampledColumns maps each discovered stream to the tier columns in its schema.
func sampledColumns(streams []*types.Stream) map[string][]string {
	columns := make(map[string][]string, len(streams))
	for _, stream := range streams {
		var tiers []string
		for _, column := range stream.Schema.ColumnNames() {
			if strings.HasPrefix(column, "col_") {
				tiers = append(tiers, column)
			}
		}
		columns[stream.Name] = tiers
	}
	return columns
}

// waitForTimeout blocks a sample until the discover timeout, the way a slow source does.
func waitForTimeout(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestDiscoverSampledTiers(t *testing.T) {
	first, second, last := DiscoverSampleTiers[0], DiscoverSampleTiers[1], DiscoverSampleTiers[len(DiscoverSampleTiers)-1]

	testCases := []struct {
		name   string
		sample func(ctx context.Context, streamID types.StreamID, limit int) error
		// expectedColumns is each returned stream's tier columns; streams absent from it must be skipped
		expectedColumns  map[string][]string
		expectedCalls    map[string][]int
		expectedErr      bool
		expectedDeadline bool
	}{
		// a discover that finishes in time ends every stream at the last tier, same as before tiers
		{
			name:            "all tiers complete within the timeout",
			expectedColumns: map[string][]string{"a": {tierColumn(last)}, "b": {tierColumn(last)}, "c": {tierColumn(last)}},
			expectedCalls:   map[string][]int{"a": DiscoverSampleTiers, "b": DiscoverSampleTiers, "c": DiscoverSampleTiers},
		},
		// each tier replaces the previous one, so an interrupted tier leaves the previous schema
		{
			name: "timeout during the second tier keeps the first tier",
			sample: func(ctx context.Context, _ types.StreamID, limit int) error {
				if limit == second {
					return waitForTimeout(ctx)
				}
				return nil
			},
			expectedColumns: map[string][]string{"a": {tierColumn(first)}, "b": {tierColumn(first)}, "c": {tierColumn(first)}},
		},
		// streams that finish the interrupted tier keep it; the rest keep the tier before
		{
			name: "streams that finish the interrupted tier keep it",
			sample: func(ctx context.Context, streamID types.StreamID, limit int) error {
				if limit == second && streamID.Name == "a" {
					return nil
				}
				if limit != first {
					return waitForTimeout(ctx)
				}
				return nil
			},
			expectedColumns: map[string][]string{"a": {tierColumn(second)}, "b": {tierColumn(first)}, "c": {tierColumn(first)}},
		},
		// only a stream that never completed the first tier is left without a schema; the others
		// wait for it at the tier barrier, so they keep the first tier
		{
			name: "timeout during the first tier skips unfinished streams",
			sample: func(ctx context.Context, streamID types.StreamID, _ int) error {
				if streamID.Name == "c" {
					return waitForTimeout(ctx)
				}
				return nil
			},
			expectedColumns: map[string][]string{"a": {tierColumn(first)}, "b": {tierColumn(first)}},
			expectedCalls:   map[string][]int{"a": {first}, "b": {first}, "c": {first}},
		},
		// with nothing to return, the timeout stays an error
		{
			name: "timeout before any stream completes the first tier fails",
			sample: func(ctx context.Context, _ types.StreamID, _ int) error {
				return waitForTimeout(ctx)
			},
			expectedErr:      true,
			expectedDeadline: true,
		},
		// a sample cut short by the timeout without an error (Kafka's poll deadline) is not trusted
		{
			name: "sample returned after the timeout does not replace the previous tier",
			sample: func(ctx context.Context, _ types.StreamID, limit int) error {
				if limit == second {
					<-ctx.Done()
				}
				return nil
			},
			expectedColumns: map[string][]string{"a": {tierColumn(first)}, "b": {tierColumn(first)}, "c": {tierColumn(first)}},
		},
		// errors other than the discover timeout still fail discover
		{
			name: "source error in a later tier fails discover",
			sample: func(_ context.Context, streamID types.StreamID, limit int) error {
				if limit == second && streamID.Name == "b" {
					return errors.New("not authorized")
				}
				return nil
			},
			expectedErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// the timeout only matters to cases that block; the rest finish well before it
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			sampler := newStubSampler(discoverStreams, tc.sample)
			streams, err := NewAbstractDriver(context.Background(), sampler).Discover(ctx, 10, false)

			if tc.expectedErr {
				require.Error(t, err)
				assert.Nil(t, streams)
				assert.Equal(t, tc.expectedDeadline, errors.Is(err, context.DeadlineExceeded), "deadline error")
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.expectedColumns, sampledColumns(streams))
			if tc.expectedCalls != nil {
				assert.Equal(t, tc.expectedCalls, sampler.byStream)
			}
		})
	}
}

// Every stream finishes a tier before any stream starts the next one.
func TestDiscoverSampledTiersRunBreadthFirst(t *testing.T) {
	streams := make([]types.StreamID, 20)
	for i := range streams {
		streams[i] = types.StreamID{Namespace: "db", Name: fmt.Sprintf("s%d", i)}
	}
	sampler := newStubSampler(streams, func(_ context.Context, _ types.StreamID, _ int) error {
		time.Sleep(time.Millisecond) // let calls of one tier overlap
		return nil
	})

	discovered, err := NewAbstractDriver(context.Background(), sampler).Discover(context.Background(), 4, false)

	require.NoError(t, err)
	assert.Len(t, discovered, len(streams))
	assert.Len(t, sampler.calls, len(streams)*len(DiscoverSampleTiers))
	assert.True(t, slices.IsSorted(sampler.calls), "a tier started before the previous tier finished: %v", sampler.calls)
}

// A canceled parent context (a signal, or sync --discover-schema) is not the discover timeout:
// discover fails instead of returning the streams sampled so far.
func TestDiscoverSampledTiersCanceledParentFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sampler := newStubSampler(discoverStreams, func(ctx context.Context, _ types.StreamID, limit int) error {
		if limit == DiscoverSampleTiers[1] {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})

	streams, err := NewAbstractDriver(context.Background(), sampler).Discover(ctx, 10, false)

	require.Error(t, err)
	assert.Nil(t, streams)
	assert.ErrorIs(t, err, context.Canceled)
}
