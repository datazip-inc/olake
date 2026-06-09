package typeutils

import (
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type Flattener interface {
	Flatten(json types.Record) (types.Record, error)
}

type FlattenerImpl struct {
	omitNilValues bool
	keyFn         func(string) string
}

// keyCache caches reformatted keys to avoid repeated string operations
var keyCache sync.Map

func getCachedReformattedKey(key string) string {
	if cached, ok := keyCache.Load(key); ok {
		return cached.(string)
	}
	reformatted := utils.Reformat(key)
	keyCache.Store(key, reformatted)
	return reformatted
}

// NewFlattener returns a flattener that reformats record keys with utils.Reformat
// (destination column name mode — the default behaviour).
func NewFlattener() Flattener {
	return &FlattenerImpl{omitNilValues: true, keyFn: getCachedReformattedKey}
}

// NewFlattenerWith returns a flattener that resolves record keys using the
// provided function. Pass stream.ResolveColumnName to honour the stream's
// naming strategy; source-name mode returns keys unchanged, destination-name
// mode applies utils.Reformat.
func NewFlattenerWith(resolve func(string) string) Flattener {
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

func (f *FlattenerImpl) flatten(key string, value any, destination types.Record) error {
	if value == nil {
		if !f.omitNilValues {
			destination[f.keyFn(key)] = nil
		}
		return nil
	}

	outKey := f.keyFn(key)

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
