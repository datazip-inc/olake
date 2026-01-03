package abstract

import (
	"context"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
)

// NoopWriter is a test writer that does nothing but satisfies the Writer interface
type NoopWriter struct {
	config *NoopConfig
}

type NoopConfig struct{}

func (c *NoopConfig) Validate() error {
	return nil
}

func (w *NoopWriter) GetConfigRef() destination.Config {
	return w.config
}

func (w *NoopWriter) Spec() any {
	return map[string]any{}
}

func (w *NoopWriter) Type() string {
	return "noop"
}

func (w *NoopWriter) Check(ctx context.Context) error {
	return nil
}

func (w *NoopWriter) Setup(ctx context.Context, stream types.StreamInterface, schema any, opts *destination.Options) (any, error) {
	return schema, nil
}

func (w *NoopWriter) Write(ctx context.Context, record []types.RawRecord) error {
	// Noop - just return success
	return nil
}

func (w *NoopWriter) FlattenAndCleanData(ctx context.Context, records []types.RawRecord) (bool, []types.RawRecord, any, error) {
	return false, records, nil, nil
}

func (w *NoopWriter) EvolveSchema(ctx context.Context, globalSchema, recordsSchema any) (any, error) {
	return globalSchema, nil
}

func (w *NoopWriter) DropStreams(ctx context.Context, dropStreams []types.StreamInterface) error {
	return nil
}

func (w *NoopWriter) Close(ctx context.Context) error {
	return nil
}

// Register the noop writer for tests
func init() {
	destination.RegisteredWriters["noop"] = func() destination.Writer {
		return &NoopWriter{config: &NoopConfig{}}
	}
}

// createTestWriterPool creates a real WriterPool with noop writers for testing
func createTestWriterPool(ctx context.Context, streams []string) (*destination.WriterPool, error) {
	config := &types.WriterConfig{
		Type:         "noop",
		WriterConfig: map[string]any{},
	}
	return destination.NewWriterPool(ctx, config, streams, 100)
}

// Mock implementations for testing

type MockDriver struct {
	getConfigRefFunc             func() Config
	specFunc                     func() any
	typeFunc                     func() string
	setupFunc                    func(ctx context.Context) error
	setupStateFunc               func(state *types.State)
	maxConnectionsFunc           func() int
	maxRetriesFunc               func() int
	getStreamNamesFunc           func(ctx context.Context) ([]string, error)
	produceSchemaFunc            func(ctx context.Context, stream string) (*types.Stream, error)
	getOrSplitChunksFunc         func(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error)
	chunkIteratorFunc            func(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error
	fetchMaxCursorValuesFunc     func(ctx context.Context, stream types.StreamInterface) (any, any, error)
	streamIncrementalChangesFunc func(ctx context.Context, stream types.StreamInterface, cb BackfillMsgFn) error
	cdcSupportedFunc             func() bool
	preCDCFunc                   func(ctx context.Context, streams []types.StreamInterface) error
	streamChangesFunc            func(ctx context.Context, stream types.StreamInterface, processFn CDCMsgFn) error
	postCDCFunc                  func(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error
}

func (m *MockDriver) GetConfigRef() Config {
	if m.getConfigRefFunc != nil {
		return m.getConfigRefFunc()
	}
	return nil
}

func (m *MockDriver) Spec() any {
	if m.specFunc != nil {
		return m.specFunc()
	}
	return nil
}

func (m *MockDriver) Type() string {
	if m.typeFunc != nil {
		return m.typeFunc()
	}
	return "mock"
}

func (m *MockDriver) Setup(ctx context.Context) error {
	if m.setupFunc != nil {
		return m.setupFunc(ctx)
	}
	return nil
}

func (m *MockDriver) SetupState(state *types.State) {
	if m.setupStateFunc != nil {
		m.setupStateFunc(state)
	}
}

func (m *MockDriver) MaxConnections() int {
	if m.maxConnectionsFunc != nil {
		return m.maxConnectionsFunc()
	}
	return 0
}

func (m *MockDriver) MaxRetries() int {
	if m.maxRetriesFunc != nil {
		return m.maxRetriesFunc()
	}
	return 3
}

func (m *MockDriver) GetStreamNames(ctx context.Context) ([]string, error) {
	if m.getStreamNamesFunc != nil {
		return m.getStreamNamesFunc(ctx)
	}
	return []string{}, nil
}

func (m *MockDriver) ProduceSchema(ctx context.Context, stream string) (*types.Stream, error) {
	if m.produceSchemaFunc != nil {
		return m.produceSchemaFunc(ctx, stream)
	}
	return &types.Stream{Name: stream}, nil
}

func (m *MockDriver) GetOrSplitChunks(ctx context.Context, pool *destination.WriterPool, stream types.StreamInterface) (*types.Set[types.Chunk], error) {
	if m.getOrSplitChunksFunc != nil {
		return m.getOrSplitChunksFunc(ctx, pool, stream)
	}
	return types.NewSet[types.Chunk](), nil
}

func (m *MockDriver) ChunkIterator(ctx context.Context, stream types.StreamInterface, chunk types.Chunk, processFn BackfillMsgFn) error {
	if m.chunkIteratorFunc != nil {
		return m.chunkIteratorFunc(ctx, stream, chunk, processFn)
	}
	return nil
}

func (m *MockDriver) FetchMaxCursorValues(ctx context.Context, stream types.StreamInterface) (any, any, error) {
	if m.fetchMaxCursorValuesFunc != nil {
		return m.fetchMaxCursorValuesFunc(ctx, stream)
	}
	return nil, nil, nil
}

func (m *MockDriver) StreamIncrementalChanges(ctx context.Context, stream types.StreamInterface, cb BackfillMsgFn) error {
	if m.streamIncrementalChangesFunc != nil {
		return m.streamIncrementalChangesFunc(ctx, stream, cb)
	}
	return nil
}

func (m *MockDriver) CDCSupported() bool {
	if m.cdcSupportedFunc != nil {
		return m.cdcSupportedFunc()
	}
	return false
}

func (m *MockDriver) PreCDC(ctx context.Context, streams []types.StreamInterface) error {
	if m.preCDCFunc != nil {
		return m.preCDCFunc(ctx, streams)
	}
	return nil
}

func (m *MockDriver) StreamChanges(ctx context.Context, stream types.StreamInterface, processFn CDCMsgFn) error {
	if m.streamChangesFunc != nil {
		return m.streamChangesFunc(ctx, stream, processFn)
	}
	return nil
}

func (m *MockDriver) PostCDC(ctx context.Context, stream types.StreamInterface, success bool, readerID string) error {
	if m.postCDCFunc != nil {
		return m.postCDCFunc(ctx, stream, success, readerID)
	}
	return nil
}

// MockKafkaDriver implements KafkaInterface
type MockKafkaDriver struct {
	MockDriver
	getReaderIDsFunc           func() []string
	partitionStreamChangesFunc func(ctx context.Context, readerID string, processFn CDCMsgFn) error
}

func (m *MockKafkaDriver) GetReaderIDs() []string {
	if m.getReaderIDsFunc != nil {
		return m.getReaderIDsFunc()
	}
	return []string{"reader1"}
}

func (m *MockKafkaDriver) PartitionStreamChanges(ctx context.Context, readerID string, processFn CDCMsgFn) error {
	if m.partitionStreamChangesFunc != nil {
		return m.partitionStreamChangesFunc(ctx, readerID, processFn)
	}
	return nil
}

// MockWriterThread implements destination.WriterThread
type MockWriterThread struct {
	pushFunc  func(ctx context.Context, record types.RawRecord) error
	closeFunc func(ctx context.Context) error
}

func (m *MockWriterThread) Push(ctx context.Context, record types.RawRecord) error {
	if m.pushFunc != nil {
		return m.pushFunc(ctx, record)
	}
	return nil
}

func (m *MockWriterThread) Close(ctx context.Context) error {
	if m.closeFunc != nil {
		return m.closeFunc(ctx)
	}
	return nil
}

// MockWriterPool implements destination.WriterPool methods for testing
type MockWriterPool struct {
	newWriterFunc func(ctx context.Context, stream types.StreamInterface, opts ...destination.WriterOption) (*destination.WriterThread, error)
}

func (m *MockWriterPool) NewWriter(ctx context.Context, stream types.StreamInterface, opts ...destination.WriterOption) (*destination.WriterThread, error) {
	if m.newWriterFunc != nil {
		return m.newWriterFunc(ctx, stream, opts...)
	}
	return &destination.WriterThread{}, nil
}

// toWriterPool converts MockWriterPool to a usable WriterPool pointer
// This is a workaround since we can't directly cast to the concrete type
func (m *MockWriterPool) toWriterPool() *destination.WriterPool {
	// Return nil - the actual functions should handle interface-based pools
	// For now, we'll need to modify test approach
	return nil
}

// MockConfig implements Config interface
type MockConfig struct {
	validateFunc func() error
}

func (m *MockConfig) Validate() error {
	if m.validateFunc != nil {
		return m.validateFunc()
	}
	return nil
}

// Helper functions

func createMockStream(name, namespace string, syncMode types.SyncMode) *types.Stream {
	stream := &types.Stream{
		Name:                    name,
		Namespace:               namespace,
		Schema:                  types.NewTypeSchema(),
		SyncMode:                syncMode,
		SupportedSyncModes:      types.NewSet(types.FULLREFRESH, types.INCREMENTAL),
		SourceDefinedPrimaryKey: types.NewSet("id"),
		CursorField:             "updated_at",
	}
	stream.Schema.Properties.Store("id", &types.Property{Type: types.NewSet(types.Int64)})
	stream.Schema.Properties.Store("updated_at", &types.Property{Type: types.NewSet(types.TimestampMicro)})
	return stream
}

// createConfiguredStream wraps a stream in ConfiguredStream for use in tests
func createConfiguredStream(name, namespace string, syncMode types.SyncMode) *types.ConfiguredStream {
	return &types.ConfiguredStream{
		Stream: createMockStream(name, namespace, syncMode),
	}
}
