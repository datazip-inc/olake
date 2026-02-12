package typeutils

import (
	"encoding/json"
	"math/big"

	"github.com/datazip-inc/olake/utils"
)

// ExtractAvroRecord recursively extracts Avro record to JSON-compatible map
func ExtractAvroRecord(record map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(record))
	for k, v := range record {
		result[k] = ExtractAvroValue(v)
	}
	return result
}

// ExtractAvroValue extracts Avro-decoded values to JSON-compatible types
func ExtractAvroValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case *big.Rat:
		// Avro decimal logical type → float64
		f, _ := val.Float64()
		return f
	case map[string]interface{}:
		// Handle Avro union types - goavro returns map with type name as key
		if len(val) == 1 {
			for _, unionVal := range val {
				return ExtractAvroValue(unionVal)
			}
		}
		return ExtractAvroRecord(val)
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, elem := range val {
			result[i] = ExtractAvroValue(elem)
		}
		return result
	default:
		return val
	}
}

// NormalizeAvroSchema parses Avro schema and normalizes all "name" and "namespace" fields
func NormalizeAvroSchema(schemaStr string) (string, error) {
	var schema interface{}
	if err := json.Unmarshal([]byte(schemaStr), &schema); err != nil {
		return "", err
	}

	schema = normalizeSchemaRecursive(schema)

	bytes, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// normalizeSchemaRecursive recursively normalizes all "name" and "namespace" fields in the schema
func normalizeSchemaRecursive(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		// normalize "name" and "namespace" if they exist
		if name, ok := val["name"].(string); ok {
			val["name"] = utils.NormalizeAvroValue(name)
		}
		if namespace, ok := val["namespace"].(string); ok {
			val["namespace"] = utils.NormalizeAvroValue(namespace)
		}

		// Recursively process other keys
		for key, item := range val {
			// Only recurse into values that are map or slice
			if _, isMap := item.(map[string]interface{}); isMap {
				val[key] = normalizeSchemaRecursive(item)
			} else if _, isSlice := item.([]interface{}); isSlice {
				val[key] = normalizeSchemaRecursive(item)
			}
		}
		return val

	case []interface{}:
		for index, item := range val {
			val[index] = normalizeSchemaRecursive(item)
		}
		return val

	default:
		// For primitive types (string, int, etc.) return as it is
		return val
	}
}
