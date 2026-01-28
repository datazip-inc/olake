package typeutils

import (
	"math/big"
)

// convertAvroRecord recursively converts Avro record to JSON-compatible map
func ConvertAvroRecord(record map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{}, len(record))
	for k, v := range record {
		result[k] = ConvertAvroValue(v)
	}
	return result
}

// convertAvroValue converts Avro-decoded values to JSON-compatible types
func ConvertAvroValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case *big.Rat:
		// Avro decimal logical type → float64
		f, _ := val.Float64()
		return f
	case []byte:
		return string(val)
	case map[string]interface{}:
		// Handle Avro union types - goavro returns map with type name as key
		if len(val) == 1 {
			for _, unionVal := range val {
				return ConvertAvroValue(unionVal)
			}
		}
		return ConvertAvroRecord(val)
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, elem := range val {
			result[i] = ConvertAvroValue(elem)
		}
		return result
	default:
		return val
	}
}
