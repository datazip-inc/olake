package testutils

import (
	"fmt"
	"net"
	"strings"
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
func ReadSourceConfig(path string) (SourceConfig, error) {
	config := SourceConfig{}
	if err := UnmarshalFile(path, &config, false); err != nil {
		return nil, fmt.Errorf("failed to read the source config at %s: %s", path, err)
	}
	return config, nil
}

// containerHost is how the driver container reaches the host's published ports. The harness runs
// on the host itself, where that name does not resolve.
const containerHost = "host.docker.internal"

// HostAddress rewrites an address the driver container uses into one the harness can dial. Host
// and "host:port" forms are both accepted; anything already reachable is returned unchanged.
func HostAddress(address string) string {
	if !strings.Contains(address, containerHost) {
		return address
	}
	if host, port, err := net.SplitHostPort(address); err == nil && host == containerHost {
		return net.JoinHostPort("127.0.0.1", port)
	}
	return strings.ReplaceAll(address, containerHost, "127.0.0.1")
}

// Host returns key as an address the harness can dial, translated out of the container's view.
func (c SourceConfig) Host(key string) string {
	return HostAddress(c.String(key))
}

// Hosts returns key as addresses the harness can dial, for the drivers that spell their host list
// as an array.
func (c SourceConfig) Hosts(key string) []string {
	raw := c.Strings(key)
	hosts := make([]string, 0, len(raw))
	for _, host := range raw {
		hosts = append(hosts, HostAddress(host))
	}
	return hosts
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
