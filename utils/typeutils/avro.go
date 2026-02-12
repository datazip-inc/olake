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
func NormalizeAvroSchema(schema string) (string, error) {
	var schemaMap map[string]interface{}
	if err := json.Unmarshal([]byte(schema), &schemaMap); err != nil {
		return "", err
	}

	if name, ok := schemaMap["name"].(string); ok {
		schemaMap["name"] = utils.Reformat(name)
	}

	if namespace, ok := schemaMap["namespace"].(string); ok {
		schemaMap["namespace"] = utils.Reformat(namespace)
	}

	bytes, err := json.Marshal(schemaMap)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
