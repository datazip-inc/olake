package types

type StreamInterface interface {
	ID() string
	Self() *ConfiguredStream
	Name() string
	Namespace() string
	Schema() *TypeSchema
	GetStream() *Stream
	GetSyncMode() SyncMode
	GetFilter() (FilterConfig, bool, error)
	SupportedSyncModes() *Set[SyncMode]
	Cursor() (string, string)
	Validate(source *Stream) error
	NormalizationEnabled() bool
	GetDestinationDatabase(icebergDB *string) string
	GetDestinationTable() string
	// Column selection helpers (driven by StreamMetadata.SelectedColumns)
	RetainSelectedColumns() func(map[string]interface{}) map[string]interface{}
	IsSelectedColumn() func(string) bool
	// ResolveColumnName returns the output column name based on the stream naming strategy, preserving the source name when use_source_column_names is enabled or applying utils.Reformat otherwise.
	ResolveColumnName(key string) string
}

type StateInterface interface {
	ResetStreams()
	SetType(typ StateType)
	GetCursor(stream *ConfiguredStream, key string) any
	SetCursor(stream *ConfiguredStream, key, value any)
	GetChunks(stream *ConfiguredStream) *Set[Chunk]
	SetChunks(stream *ConfiguredStream, chunks *Set[Chunk])
	RemoveChunk(stream *ConfiguredStream, chunk Chunk)
	SetGlobal(globalState any, streams ...string)
}

type Iterable interface {
	Next() bool
	Err() error
}
