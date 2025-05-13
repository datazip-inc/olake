package utils

import (
	//nolint:gosec,G115
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/logger"
	"github.com/goccy/go-json"
	"github.com/oklog/ulid"

	"github.com/spf13/cobra"
)

var (
	ulidMutex = sync.Mutex{}
	entropy   = ulid.Monotonic(rand.Reader, 0)
)

func Absolute[T int | int8 | int16 | int32 | int64 | float32 | float64](value T) T {
	if value < 0 {
		return -value
	}

	return value
}

// IsValidSubcommand checks if the passed subcommand is supported by the parent command
func IsValidSubcommand(available []*cobra.Command, sub string) bool {
	for _, s := range available {
		if sub == s.Use || sub == s.CalledAs() {
			return true
		}
	}
	return false
}

func ExistInArray[T ~string | int | int8 | int16 | int32 | int64 | float32 | float64](set []T, value T) bool {
	_, found := ArrayContains(set, func(elem T) bool {
		return elem == value
	})

	return found
}

func ArrayContains[T any](set []T, match func(elem T) bool) (int, bool) {
	for idx, elem := range set {
		if match(elem) {
			return idx, true
		}
	}

	return -1, false
}

// returns cond ? a ; b
func Ternary(cond bool, a, b any) any {
	if cond {
		return a
	}
	return b
}

// Unmarshal serializes and deserializes any from into the object
// return error if occurred
func Unmarshal(from, object any) error {
	reformatted := reformatInnerMaps(from)
	b, err := json.Marshal(reformatted)
	if err != nil {
		return fmt.Errorf("error marshaling object: %v", err)
	}
	err = json.Unmarshal(b, object)
	if err != nil {
		return fmt.Errorf("error unmarshalling from object: %v", err)
	}

	return nil
}

func IsInstance(val any, typ reflect.Kind) bool {
	return reflect.ValueOf(val).Kind() == typ
}

// reformatInnerMaps converts all map[any]any into map[string]any
// because json.Marshal doesn't support map[any]any (supports only string keys)
// but viper produces map[any]any for inner maps
// return recursively converted all map[interface]any to map[string]any
func reformatInnerMaps(valueI any) any {
	switch value := valueI.(type) {
	case []any:
		for i, subValue := range value {
			value[i] = reformatInnerMaps(subValue)
		}
		return value
	case map[any]any:
		newMap := make(map[string]any, len(value))
		for k, subValue := range value {
			newMap[fmt.Sprint(k)] = reformatInnerMaps(subValue)
		}
		return newMap
	case map[string]any:
		for k, subValue := range value {
			value[k] = reformatInnerMaps(subValue)
		}
		return value
	default:
		return valueI
	}
}

func CheckIfFilesExists(files ...string) error {
	for _, file := range files {
		// Check if the file or directory exists
		_, err := os.Stat(file)
		if os.IsNotExist(err) {
			return fmt.Errorf("%s does not exist: %s", file, err)
		}

		_, err = os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("failed to read %s: %s", file, err)
		}
	}

	return nil
}

// func ReadFile(file string) any {
// 	content, _ := ReadFileE(file)

// 	return content
// }

func UnmarshalFile(file string, dest any) error {
	if err := CheckIfFilesExists(file); err != nil {
		return err
	}

	data, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("file not found : %s", err)
	}

	err = json.Unmarshal(data, dest)
	if err != nil {
		return fmt.Errorf("failed to unmarshal file[%s]: %s", file, err)
	}

	return nil
}

func IsOfType(object any, decidingKey string) (bool, error) {
	objectMap := make(map[string]any)
	if err := Unmarshal(object, &objectMap); err != nil {
		return false, err
	}

	if _, found := objectMap[decidingKey]; found {
		return true, nil
	}

	return false, nil
}

func StreamIdentifier(name, namespace string) string {
	if namespace != "" {
		return fmt.Sprintf("%s.%s", namespace, name)
	}

	return name
}

func IsSubset[T comparable](setArray, subsetArray []T) bool {
	set := make(map[T]bool)
	for _, item := range setArray {
		set[item] = true
	}

	for _, item := range subsetArray {
		if _, found := set[item]; !found {
			return false
		}
	}

	return true
}

func MaxDate(v1, v2 time.Time) time.Time {
	if v1.After(v2) {
		return v1
	}

	return v2
}

func ULID() string {
	return genULID(time.Now())
}

func genULID(t time.Time) string {
	ulidMutex.Lock()
	defer ulidMutex.Unlock()
	newUlid, err := ulid.New(ulid.Timestamp(t), entropy)
	if err != nil {
		logger.Fatalf("failed to generate ulid: %s", err)
	}
	return newUlid.String()
}

// Returns a timestamped
func TimestampedFileName(extension string) string {
	now := time.Now().UTC()
	return fmt.Sprintf("%d-%d-%d_%d-%d-%d_%s.%s", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), genULID(now), extension)
}

func IsJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}

// GetKeysHash returns md5 hashsum of concatenated map values (sort keys before)
func GetKeysHash(m map[string]interface{}, keys ...string) string {
	// If no primary key is present, the entire record is hashed to generate the olakeID.
	if len(keys) == 0 {
		return GetHash(m)
	}
	sort.Strings(keys)

	var str strings.Builder
	for _, k := range keys {
		str.WriteString(fmt.Sprint(m[k]))
		str.WriteRune('|')
	}
	//nolint:gosec,G115
	return fmt.Sprintf("%x", md5.Sum([]byte(str.String())))
}

// GetHash returns GetKeysHash result with keys from m
func GetHash(m map[string]interface{}) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	return GetKeysHash(m, keys...)
}

func AddConstantToInterface(val interface{}, increment int) (interface{}, error) {
	switch v := val.(type) {
	case int:
		return v + increment, nil
	case int64:
		return v + int64(increment), nil
	case float32:
		return v + float32(increment), nil
	case float64:
		return v + float64(increment), nil
	default:
		return nil, fmt.Errorf("failed to add contant values to interface, unsupported type %T", val)
	}
}

// return 0 for equal, -1 if a < b else 1 if a>b
func CompareInterfaceValue(a, b interface{}) int {
	switch a.(type) {
	case int, int64, float32, float64:
		af := 0.0
		if a != nil {
			af = reflect.ValueOf(a).Convert(reflect.TypeOf(float64(0))).Float()
		}
		bf := 0.0
		if b != nil {
			bf = reflect.ValueOf(b).Convert(reflect.TypeOf(float64(0))).Float()
		}
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
	case string:
		if a != nil && b != nil {
			return strings.Compare(a.(string), b.(string))
		}
		return Ternary(a == nil, -1, 1).(int)
	}
	return 0
}
func ConvertToString(value interface{}) string {
	switch v := value.(type) {
	case []byte:
		return string(v) // Convert byte slice to string
	case string:
		return v // Already a string
	default:
		return fmt.Sprintf("%v", v) // Fallback
	}
}
