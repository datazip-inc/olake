package typeutils

import (
	"fmt"
	"reflect"
	"time"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
)

type Flattener interface {
	Flatten(json types.Record) (types.Record, error)
}

type FlattenerImpl struct {
	omitNilValues bool
}

func NewFlattener() Flattener {
	return &FlattenerImpl{
		omitNilValues: true,
	}
}

func (f *FlattenerImpl) Flatten(json types.Record) (types.Record, error) {
	destination := make(types.Record)

	for key, value := range json {
		err := f.flatten(key, value, destination)
		if err != nil {
			return nil, err
		}
	}

	return destination, nil
}

// Reformat key
func (f *FlattenerImpl) flatten(key string, value any, destination types.Record) error {
	key = utils.Reformat(key)
	t := reflect.ValueOf(value)
	switch t.Kind() {
	case reflect.Slice: // Preserve arrays as structured types
		destination[key] = value
	case reflect.Map: // Preserve maps as structured types (JSON/JSONB)
		destination[key] = value
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String:
		destination[key] = value
	default:
		if !f.omitNilValues || value != nil {
			// Handle time.Time values
			if tm, ok := value.(time.Time); ok {
				destination[key] = tm
			} else {
				destination[key] = fmt.Sprint(value)
			}
		}
	}

	return nil
}
