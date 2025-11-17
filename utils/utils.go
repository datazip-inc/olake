package utils

import (
	"context"

	//nolint:gosec,G115
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/goccy/go-json"
	"github.com/oklog/ulid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/testcontainers/testcontainers-go"
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

// returns cond ? a ; b (note: it is not function ternary)
func Ternary(cond bool, a, b any) any {
	if cond {
		return a
	}
	return b
}

func ForEach[T any](set []T, action func(elem T) error) error {
	for _, elem := range set {
		err := action(elem)
		if err != nil {
			return err
		}
	}
	return nil
}

// Unmarshal serializes and deserializes any from into the object
// return error if occurred
func Unmarshal(from, object any) error {
	reformatted := reformatInnerMaps(from)
	b, err := json.Marshal(reformatted)
	if err != nil {
		return fmt.Errorf("error marshaling object: %s", err)
	}
	err = json.Unmarshal(b, object)
	if err != nil {
		return fmt.Errorf("error unmarshalling from object: %s", err)
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

func UnmarshalFile(file string, dest any, credsFile bool) error {
	if err := CheckIfFilesExists(file); err != nil {
		return err
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("file not found : %s", err)
	}
	decryptedJSON := data
	// Use the encryption package to decrypt JSON
	if credsFile && viper.GetString(constants.EncryptionKey) != "" {
		dConfig, err := Decrypt(string(data))
		if err != nil {
			return fmt.Errorf("failed to decrypt config file[%s]: %s", file, err)
		}
		decryptedJSON = []byte(dConfig)
	}
	err = json.Unmarshal(decryptedJSON, dest)
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
	// if single primary key is present use as it is
	if len(keys) == 1 {
		if _, ok := m[keys[0]]; ok {
			return fmt.Sprint(m[keys[0]])
		}
	}

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

func ComputeConfigHash(srcPath, destPath string) string {
	if srcPath == "" || destPath == "" {
		// no config or no destination → no meaningful hash
		return ""
	}
	a, err := os.ReadFile(srcPath)
	if err != nil {
		return ""
	}
	b, err := os.ReadFile(destPath)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(append(a, b...))
	return hex.EncodeToString(sum[:])
}

// Helper function to execute container commands
func ExecCommand(
	ctx context.Context,
	c testcontainers.Container,
	cmd string,
) (int, []byte, error) {
	code, reader, err := c.Exec(ctx, []string{"/bin/sh", "-c", cmd})
	if err != nil {
		return code, nil, err
	}
	output, _ := io.ReadAll(reader)
	return code, output, nil
}

func NormalizedEqual(strune1, strune2 string) bool {
	normalize := func(s string) (string, error) {
		// Slice out exactly from the first '{' to the last '}'
		start := strings.IndexRune(s, '{')
		end := strings.LastIndex(s, "}")
		if start < 0 || end < 0 || start > end {
			return "", fmt.Errorf("no valid JSON object found")
		}
		core := s[start : end+1]
		// remove whitespace
		core = strings.ReplaceAll(core, " ", "")
		core = strings.ReplaceAll(core, "\n", "")
		core = strings.ReplaceAll(core, "\t", "")
		return core, nil
	}

	c1, err := normalize(strune1)
	if err != nil {
		return false
	}
	c2, err := normalize(strune2)
	if err != nil {
		return false
	}

	rune1 := []rune(c1)
	rune2 := []rune(c2)
	if len(rune1) != len(rune2) {
		return false
	}
	sort.Slice(rune1, func(i, j int) bool { return rune1[i] < rune1[j] })
	sort.Slice(rune2, func(i, j int) bool { return rune2[i] < rune2[j] })
	return string(rune1) == string(rune2)
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

// GenerateDestinationDetails creates the default Iceberg database and table names.
// It combines prefix, source database, and namespace into a proper DB name.
func GenerateDestinationDetails(namespace, name string, sourceDatabase *string) (string, string) {
	parts := []string{}

	// Add destination database prefix if available
	if prefix := viper.GetString(constants.DestinationDatabasePrefix); prefix != "" {
		parts = append(parts, Reformat(prefix))
	}

	// Add source database if provided
	if sourceDatabase != nil && *sourceDatabase != "" {
		parts = append(parts, Reformat(*sourceDatabase))
	}

	// Join prefix + source database
	dbName := strings.Join(parts, "_")

	// Append namespace if provided
	if namespace != "" {
		dbName = fmt.Sprintf("%s:%s", dbName, Reformat(namespace))
	}

	// Final table name is always reformatted
	return dbName, Reformat(name)
}

// splitAndTrim splits a comma-separated string and trims whitespace
func SplitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// RetryOnBackoff retries the function f up to attempts times with a backoff sleep between attempts.
func RetryOnBackoff(attempts int, sleep time.Duration, f func(attempt int) error) (err error) {
	for cur := range attempts {
		if err = f(cur); err == nil {
			return nil
		}

		// check if error is non retryable
		for _, nonRetryableError := range constants.NonRetryableErrors {
			if strings.Contains(err.Error(), nonRetryableError) {
				break
			}
		}

		if attempts > 1 && cur != attempts-1 {
			logger.Infof("retry attempt[%d], retrying after %.2f seconds due to err: %s", cur+1, sleep.Seconds(), err)
			time.Sleep(sleep)
			sleep = sleep * 2
		}
	}

	return err
}
