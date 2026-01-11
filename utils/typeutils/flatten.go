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
}

var flattenerPool = sync.Pool{
	New: func() interface{} {
		return &FlattenerImpl{omitNilValues: true}
	},
}

// keyCache caches reformatted keys to avoid repeated string operations
var keyCache sync.Map

func NewFlattener() Flattener {
	return flattenerPool.Get().(*FlattenerImpl)
}

func ReleaseFlattener(f Flattener) {
	if impl, ok := f.(*FlattenerImpl); ok {
		flattenerPool.Put(impl)
	}
}

func getReformattedKey(key string) string {
	if cached, ok := keyCache.Load(key); ok {
		return cached.(string)
	}
	reformatted := utils.Reformat(key)
	keyCache.Store(key, reformatted)
	return reformatted
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
			destination[getReformattedKey(key)] = nil
		}
		return nil
	}

	reformattedKey := getReformattedKey(key)

	// Type switch is faster than reflection for known types
	switch v := value.(type) {
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, string, time.Time:
		destination[reformattedKey] = v
	case []byte:
		destination[reformattedKey] = string(v)
	default:
		// Fallback for other types
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		destination[reformattedKey] = string(b)
	}

	return nil
}
