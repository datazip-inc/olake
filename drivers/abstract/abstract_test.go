package abstract

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

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
