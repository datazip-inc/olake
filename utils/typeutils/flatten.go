package typeutils

import (
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/types"
)

type Flattener interface {
	Flatten(json types.Record) (types.Record, error)
}

// FlattenerImpl holds a per-batch key cache (sync.Map) so the resolve function
// is called at most once per unique column name per batch, not once per record.
// Create one flattener per batch and reuse it across all records in that batch.
type FlattenerImpl struct {
	omitNilValues bool
	keyFn         func(string) string
	keyCache      sync.Map // batch-scoped: write-once per key, then read-only
}

// NewFlattener returns a flattener that resolves record keys using the provided
// function. Pass stream.ResolveColumnName to honour the stream's naming strategy:
// source-name mode returns keys unchanged, destination-name mode applies utils.Reformat.
// Create one instance per batch and reuse it across all records — the internal
// cache amortises repeated resolve calls for the same column names.
func NewFlattener(resolve func(string) string) Flattener {
	return &FlattenerImpl{omitNilValues: true, keyFn: resolve}
}

func (f *FlattenerImpl) Flatten(data types.Record) (types.Record, error) {
	destination := make(types.Record, len(data))

	for key, value := range data {
		if err := f.flatten(key, value, destination); err != nil {
			return nil, err
		}
	}

	return destination, nil
}

// resolveKey returns the output key for a column, using the instance-level cache
// to avoid calling keyFn more than once per unique column name per batch.
func (f *FlattenerImpl) resolveKey(key string) string {
	if cached, ok := f.keyCache.Load(key); ok {
		return cached.(string)
	}
	resolved := f.keyFn(key)
	f.keyCache.Store(key, resolved)
	return resolved
}

func (f *FlattenerImpl) flatten(key string, value any, destination types.Record) error {
	if value == nil {
		if !f.omitNilValues {
			destination[f.resolveKey(key)] = nil
		}
		return nil
	}

	outKey := f.resolveKey(key)

	// Type switch is faster than reflection for known types
	switch v := value.(type) {
	// json.Number is included because Kafka driver uses decoder.UseNumber() for number handling
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string, time.Time, json.Number:
		destination[outKey] = v
	case []byte:
		destination[outKey] = string(v)
	default:
		// Fallback for other types
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		destination[outKey] = string(b)
	}

	return nil
}
