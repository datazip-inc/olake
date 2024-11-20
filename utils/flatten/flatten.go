package schema

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/datazip-inc/olake/logger"
	"github.com/datazip-inc/olake/utils"
)

type Flattener interface {
	// flats object and arrays both
	Flatten(json map[string]any, maxdepth int, skipNestedFlattening bool) ([]map[string]any, error)
	FlattenObject(json map[string]any, maxArrayDepth int, skipNestedFlattening bool) (map[string]any, error)
	ExplodeArrays(json map[string]any, depth int, maxdepth int) ([]map[string]any, error)
}

type FlattenerImpl struct {
	omitNilValues bool
}

func NewFlattener() Flattener {
	return &FlattenerImpl{
		omitNilValues: true,
	}
}

func (f *FlattenerImpl) Flatten(json map[string]any, maxdepth int, skipNestedFlattening bool) ([]map[string]any, error) {
	baseFlattening, err := f.FlattenObject(json, maxdepth, skipNestedFlattening)
	if err != nil {
		return nil, err
	}

	if maxdepth == 0 || skipNestedFlattening { // Never flatten arrays if nested flattening is disabled
		return []map[string]any{baseFlattening}, nil
	}

	return f.ExplodeArrays(baseFlattening, 0, maxdepth)
}

func (f *FlattenerImpl) ProcessJSONStrings(jsonmap map[string]any) map[string]any {
	for key, value := range jsonmap {
		switch value := value.(type) {
		case string:
			if utils.IsJSON(value) {
				var output any
				err := json.Unmarshal([]byte(value), &output)
				if err != nil {
					logger.Errorf("failed to process JSONString[%s] during flattening: %s", value, err)
				} else {
					jsonmap[key] = output
				}
			}
		}
	}

	return jsonmap
}

// FlattenObject flatten object e.g. from {"key1":{"key2":123}} to {"key1_key2":123}
// from {"$key1":1} to {"_key1":1}
// from {"(key1)":1} to {"_key1_":1}
func (f *FlattenerImpl) FlattenObject(json map[string]any, maxArrayDepth int, skipNestedFlatten bool) (map[string]any, error) {
	if maxArrayDepth > 0 {
		// preprocess json strings
		json = f.ProcessJSONStrings(json)
	}

	// flatten the processed
	flattenMap := make(map[string]any)
	for key, value := range json {
		err := f.flatten(key, value, flattenMap, maxArrayDepth, skipNestedFlatten)
		if err != nil {
			return nil, err
		}
	}

	emptyKeyValue, hasEmptyKey := flattenMap[""]
	if hasEmptyKey {
		flattenMap["_unnamed"] = emptyKeyValue
		delete(flattenMap, "")
	}
	return flattenMap, nil
}

// recursive function for flatten key (if value is inner object -> recursion call)
// Reformat key
func (f *FlattenerImpl) flatten(key string, value any, destination map[string]any, maxArrayDepth int, skipNestedFlatten bool) error {
	key = Reformat(key)
	t := reflect.ValueOf(value)
	switch t.Kind() {
	case reflect.Slice:
		if maxArrayDepth == 0 || skipNestedFlatten { // Never flatten arrays if nested flattening is disabled
			b, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("Error marshaling array with key %s: %v", key, err)
			}
			destination[key] = string(b)
		} else {
			destination[key] = value
		}
	case reflect.Map:
		unboxed := value.(map[string]any)
		if skipNestedFlatten {
			b, err := json.Marshal(value)
			if err != nil {
				return fmt.Errorf("Error marshaling array with key[%s] and value %v: %v", key, value, err)
			}
			destination[key] = string(b)
		} else {
			for k, v := range unboxed {
				newKey := k
				if key != "" {
					newKey = key + "_" + newKey
				}
				if err := f.flatten(newKey, v, destination, maxArrayDepth, skipNestedFlatten); err != nil {
					return err
				}
			}
		}
	case reflect.Bool:
		boolValue, _ := value.(bool)
		destination[key] = boolValue
	default:
		if !f.omitNilValues || value != nil {
			switch value.(type) {
			case string:
				strValue, _ := value.(string)

				destination[key] = strValue
			default:
				destination[key] = value
			}
		}
	}

	return nil
}

// Must run after basic flattening
// Explode arrays vertically
func (f *FlattenerImpl) ExplodeArrays(source map[string]any, depth int, maxdepth int) ([]map[string]any, error) {
	// Contains aftermath columns and their values after flattening an array
	flattenedArrays := make(map[string][]map[string]any)
	for key, value := range source {
		// re-add after formatting
		delete(source, key)
		key = Reformat(key)
		source[key] = value

		t := reflect.ValueOf(value)
		switch t.Kind() {
		case reflect.Slice:
			switch value := value.(type) {
			case []any:
				// if array empty; delete
				if len(value) == 0 {
					delete(source, key)
				}

				if depth < maxdepth {
					exploded, unexploded, err := f.explodeAnyArrays(value, depth, maxdepth)
					if err != nil {
						return nil, err
					}
					if len(exploded) > 0 {
						flattenedArrays[key] = exploded
						// delete the key after being exploded
						delete(source, key)
					}
					if len(unexploded) > 0 {
						// if found key with unexploded re-add the key with value
						b, err := json.Marshal(unexploded)
						if err != nil {
							return nil, fmt.Errorf("Error marshaling array with key %s: %v", key, err)
						}
						source[key] = string(b)
					}
				} else { // else case here since can't fallthrough in type switchcase
					b, err := json.Marshal(value)
					if err != nil {
						return nil, fmt.Errorf("Error marshaling array with key %s: %v", key, err)
					}
					source[key] = string(b)
				}
			case []map[string]any:
				if depth < maxdepth {
					flattenedObjects, err := f.explodeDynamicMapArrays(value, depth, maxdepth)
					if err != nil {
						return nil, err
					}
					if len(flattenedObjects) > 0 {
						// set into flattened arrays
						flattenedArrays[key] = flattenedObjects
						// delete the key after being exploded
						delete(source, key)
					} else {
						b, err := json.Marshal(value)
						if err != nil {
							return nil, fmt.Errorf("Error marshaling array with key %s: %v", key, err)
						}
						source[key] = string(b)
					}
				} else { // else case here since can't fallthrough in type switchcase
					b, err := json.Marshal(value)
					if err != nil {
						return nil, fmt.Errorf("Error marshaling array with key %s: %v", key, err)
					}
					source[key] = string(b)
				}
			default:
				b, err := json.Marshal(value)
				if err != nil {
					return nil, fmt.Errorf("Error marshaling array with key %s: %v", key, err)
				}
				source[key] = string(b)
			}
		}
	}

	// base case
	if len(flattenedArrays) == 0 {
		return []map[string]any{source}, nil
	}

	output := []map[string]any{}

	idx := 0
	continueIteration := true

	for continueIteration {
		// set continue iteration to false
		continueIteration = false

		// create a copy map with jsonutils
		destinationmap := map[string]any{}
		err := utils.Unmarshal(source, &destinationmap)
		if err != nil {
			return nil, err
		}

		for key, array := range flattenedArrays {
			if len(array) > idx {
				continueIteration = true
				// get the flattened map and merge
				flattenedMap := array[idx]
				for flatKey, value := range flattenedMap {
					newKey := Reformat(flatKey)
					destinationmap[key+"_"+newKey] = value
				}
			}
		}

		// only append incase we found new iteration of values
		if continueIteration {
			output = append(output, destinationmap)
		}
		// increase index
		idx++
	}

	return output, nil
}

// explodeAnyArrays either explode the whole array if any maps found else just returns the original array
func (f *FlattenerImpl) explodeAnyArrays(objects []any, depth int, maxdepth int) ([]map[string]any, []any, error) {
	// base case
	if depth >= maxdepth || len(objects) == 0 {
		return nil, objects, nil
	}

	exploded := []map[string]any{}
	unexploded := []any{}
	for _, object := range objects {
		switch object := object.(type) {
		case []map[string]any:
			flattenedObjects, err := f.explodeDynamicMapArrays(object, depth+1, maxdepth)
			if err != nil {
				return nil, nil, err
			}
			exploded = append(exploded, flattenedObjects...)
		case []any:
			subexploded, subunexploded, err := f.explodeAnyArrays(object, depth+1, maxdepth)
			if err != nil {
				return nil, nil, err
			}
			exploded = append(exploded, subexploded...)
			unexploded = append(unexploded, subunexploded...)
		case map[string]any:
			flattenedObject, err := f.FlattenObject(object, maxdepth, false) // Never flatten arrays if nested flattening is disabled
			if err != nil {
				return nil, nil, err
			}

			flattenedObjectWithExplodedArrays, err := f.ExplodeArrays(flattenedObject, depth+1, maxdepth)
			if err != nil {
				return nil, nil, err
			}

			exploded = append(exploded, flattenedObjectWithExplodedArrays...)
		default:
			unexploded = append(unexploded, object)
		}
	}

	return exploded, unexploded, nil
}

func (f *FlattenerImpl) explodeDynamicMapArrays(objects []map[string]any, depth int, maxdepth int) ([]map[string]any, error) {
	output := []map[string]any{}
	for _, object := range objects {
		flattenedObject, err := f.FlattenObject(object, maxdepth, false) // Never flatten arrays if nested flattening is disabled
		if err != nil {
			return nil, err
		}

		flattenedObjectWithExplodedArrays, err := f.ExplodeArrays(flattenedObject, depth+1, maxdepth)
		if err != nil {
			return nil, err
		}

		output = append(output, flattenedObjectWithExplodedArrays...)
	}

	return output, nil
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
