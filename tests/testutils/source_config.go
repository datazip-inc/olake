package testutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// SourceConfig is a driver's source.json read as an untyped map.
//
// The integration tests hand source.json to the driver verbatim and only ever need a handful
// of connection fields back out of it to talk to the source directly. Reading it as a map
// keeps the tests from restating the driver's config schema: a field the tests do not read is
// none of their business, and one the driver adds or renames cannot silently drift out of
// sync with a copy kept here.
//
// Missing keys and type mismatches yield the zero value rather than failing. The fixtures are
// small and the immediate consequence is a connection error naming the source, which is where
// you would look anyway.
type SourceConfig map[string]any

// ReadSourceConfig loads path as an untyped source config.
func ReadSourceConfig(t *testing.T, path string) SourceConfig {
	t.Helper()
	config := SourceConfig{}
	require.NoError(t, UnmarshalFile(path, &config, false), "read source config %s", path)
	return config
}

// String returns key as a string.
func (c SourceConfig) String(key string) string {
	value, _ := c[key].(string)
	return value
}

// Int returns key as an int. encoding/json decodes every number into a float64 when the
// destination is an any, so this converts rather than type-asserting -- an int assertion on a
// JSON number never succeeds.
func (c SourceConfig) Int(key string) int {
	value, _ := c[key].(float64)
	return int(value)
}

// Strings returns key as a []string. JSON arrays decode to []any, so each element is asserted
// individually; non-string elements are skipped.
func (c SourceConfig) Strings(key string) []string {
	raw, _ := c[key].([]any)
	values := make([]string, 0, len(raw))
	for _, element := range raw {
		if value, ok := element.(string); ok {
			values = append(values, value)
		}
	}
	return values
}

// StringMap returns the nested object at key as a map[string]string, for config blocks whose
// keys are open-ended (jdbc_url_params and friends). Non-string values are skipped.
func (c SourceConfig) StringMap(key string) map[string]string {
	nested := c.Sub(key)
	if nested == nil {
		return nil
	}
	values := make(map[string]string, len(nested))
	for nestedKey, element := range nested {
		if value, ok := element.(string); ok {
			values[nestedKey] = value
		}
	}
	return values
}

// Sub returns the nested object at key, or nil when it is absent. The nil case stays usable:
// indexing a nil map is legal, so Sub("ssl").String("mode") on a config without an ssl block
// returns "" instead of panicking, and callers that need to tell "absent" from "present but
// empty" apart can compare the result against nil.
func (c SourceConfig) Sub(key string) SourceConfig {
	nested, ok := c[key].(map[string]any)
	if !ok {
		return nil
	}
	return SourceConfig(nested)
}
