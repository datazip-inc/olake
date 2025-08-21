package types

type StreamInterface interface {
	ID() string
	Self() *ConfiguredStream
	Name() string
	TargetName() string
	Namespace() string
	Schema() *TypeSchema
	GetStream() *Stream
	GetSyncMode() SyncMode
	GetFilter() (Filter, error)
	SupportedSyncModes() *Set[SyncMode]
	Cursor() (string, string)
	Validate(source *Stream) error
	NormalizationEnabled() bool
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
