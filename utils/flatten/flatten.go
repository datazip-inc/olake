package flatten

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

type Flattener interface {
	Flatten(json map[string]any) (map[string]any, error)
}

type FlattenerImpl struct {
	omitNilValues bool
}

func NewFlattener() Flattener {
	return &FlattenerImpl{
		omitNilValues: true,
	}
}

func (f *FlattenerImpl) Flatten(json map[string]any) (map[string]any, error) {
	destination := make(map[string]any)

	for key, value := range json {
		err := f.flatten(key, value, destination)
		if err != nil {
			return nil, err
		}
	}

	return destination, nil
}

// Reformat key
func (f *FlattenerImpl) flatten(key string, value any, destination map[string]any) error {
	key = Reformat(key)
	t := reflect.ValueOf(value)
	switch t.Kind() {
	case reflect.Slice: // Stringify arrays
		b, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("error marshaling array with key %s: %v", key, err)
		}
		destination[key] = string(b)
	case reflect.Map: // Stringify nested maps
		b, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("error marshaling array with key[%s] and value %v: %v", key, value, err)
		}
		destination[key] = string(b)
	case reflect.Bool:
		boolValue, _ := value.(bool)
		destination[key] = boolValue
	default:
		if !f.omitNilValues || value != nil {
			destination[key] = value
		}
	}

	return nil
}

// Reformat makes all keys to lower case and replaces all special symbols with '_'
func Reformat(key string) string {
	key = strings.ToLower(key)
	var result strings.Builder
	for _, symbol := range key {
		if IsLetterOrNumber(symbol) {
			result.WriteByte(byte(symbol))
		} else {
			result.WriteRune('_')
		}
	}
	return result.String()
}

// IsLetterOrNumber returns true if input symbol is:
//
//	A - Z: 65-90
//	a - z: 97-122
func IsLetterOrNumber(symbol int32) bool {
	return ('a' <= symbol && symbol <= 'z') ||
		('A' <= symbol && symbol <= 'Z') ||
		('0' <= symbol && symbol <= '9')
}
