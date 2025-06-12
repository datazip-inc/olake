package protocol

import (
	"bytes"         // Added for custom JSON marshaling
	"encoding/json" // Added for generic JSON manipulation
	"fmt"
	"sort" // Added for sorting slices
	"strings"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/jsonschema"
	"github.com/datazip-inc/olake/utils/logger"

	"github.com/spf13/cobra"
)

// OrderedProperties is a custom type to handle ordered JSON marshaling of properties.
// It stores the properties map and an explicit slice of keys in the desired order.
type OrderedProperties struct {
	Properties map[string]interface{}
	Order      []string // This slice will store the sorted keys
}

// MarshalJSON implements the json.Marshaler interface for OrderedProperties.
// It iterates through the `Order` slice to ensure properties are written to JSON
// in the specified sorted order.
func (op OrderedProperties) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteString("{")
	first := true
	for _, key := range op.Order {
		if !first {
			b.WriteString(",")
		}
		first = false

		// Marshal key (e.g., "aws_access_key")
		keyBytes, err := json.Marshal(key)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal property key %s: %w", key, err)
		}
		b.Write(keyBytes)
		b.WriteString(":")

		// Marshal value (the property's schema definition, which might itself be an OrderedProperties)
		valBytes, err := json.Marshal(op.Properties[key])
		if err != nil {
			return nil, fmt.Errorf("failed to marshal property value for key %s: %w", key, err)
		}
		b.Write(valBytes)
	}
	b.WriteString("}")
	return b.Bytes(), nil
}

// propertyEntry is a helper struct for sorting properties.
type propertyEntry struct {
	Key   string
	Value interface{} // Can be map[string]interface{} or OrderedProperties
	Order int         // Default -1 for properties without explicit order
}

// sortSchemaProperties recursively sorts properties within a JSON schema map.
// It handles 'properties', 'oneOf', 'anyOf', 'allOf', and 'items' keywords.
func sortSchemaProperties(schemaMap map[string]interface{}) {
	// Sort properties at the current level
	if propertiesMap, ok := schemaMap["properties"].(map[string]interface{}); ok {
		var entries []propertyEntry

		// Extract properties and their 'order' field
		for key, val := range propertiesMap {
			order := -1 // Default order if not found
			if propVal, isMap := val.(map[string]interface{}); isMap {
				if orderVal, exists := propVal["order"]; exists {
					if floatOrder, ok := orderVal.(float64); ok { // JSON numbers unmarshal as float64
						order = int(floatOrder)
					}
				}
			}
			entries = append(entries, propertyEntry{Key: key, Value: val, Order: order})
		}

		// Sort the entries based on the 'order' field and then by key
		sort.Slice(entries, func(i, j int) bool {
			// Prioritize properties with an 'order' field over those without
			if entries[i].Order != -1 && entries[j].Order == -1 {
				return true
			}
			if entries[i].Order == -1 && entries[j].Order != -1 {
				return false
			}

			// If both have 'order' or neither has 'order', compare based on 'order' value
			if entries[i].Order != entries[j].Order {
				return entries[i].Order < entries[j].Order // Sort by order ascending
			}

			// If 'order' values are equal (or both are -1), sort alphabetically by key
			return entries[i].Key < entries[j].Key
		})

		// Create a new map to store the potentially sorted nested schemas
		newPropertiesMap := make(map[string]interface{})
		sortedKeys := make([]string, 0, len(entries))

		// Populate the newPropertiesMap and sortedKeys, and recursively sort nested properties
		for _, entry := range entries {
			if nestedSchema, isMap := entry.Value.(map[string]interface{}); isMap {
				// Recursively sort properties within this nested object
				sortSchemaProperties(nestedSchema)
				newPropertiesMap[entry.Key] = nestedSchema
			} else {
				newPropertiesMap[entry.Key] = entry.Value
			}
			sortedKeys = append(sortedKeys, entry.Key)
		}

		// Replace the original "properties" value in the current schema map
		// with our custom OrderedProperties marshaler
		schemaMap["properties"] = OrderedProperties{
			Properties: newPropertiesMap,
			Order:      sortedKeys,
		}
	}

	// Handle 'oneOf' keyword: Iterate through each alternative schema and sort its properties
	if oneOfArray, ok := schemaMap["oneOf"].([]interface{}); ok {
		for i, item := range oneOfArray {
			if subSchema, isMap := item.(map[string]interface{}); isMap {
				sortSchemaProperties(subSchema) // Recursive call for each sub-schema
				oneOfArray[i] = subSchema       // Update the array with the potentially sorted sub-schema
			}
		}
		schemaMap["oneOf"] = oneOfArray // Ensure the updated array is put back
	}

	// Handle 'anyOf' keyword: Similar to 'oneOf'
	if anyOfArray, ok := schemaMap["anyOf"].([]interface{}); ok {
		for i, item := range anyOfArray {
			if subSchema, isMap := item.(map[string]interface{}); isMap {
				sortSchemaProperties(subSchema)
				anyOfArray[i] = subSchema
			}
		}
		schemaMap["anyOf"] = anyOfArray
	}

	// Handle 'allOf' keyword: Similar to 'oneOf'
	if allOfArray, ok := schemaMap["allOf"].([]interface{}); ok {
		for i, item := range allOfArray {
			if subSchema, isMap := item.(map[string]interface{}); isMap {
				sortSchemaProperties(subSchema)
				allOfArray[i] = subSchema
			}
		}
		schemaMap["allOf"] = allOfArray
	}

	// Handle 'items' keyword for array schemas (e.g., array of objects)
	if itemsVal, ok := schemaMap["items"].(map[string]interface{}); ok {
		sortSchemaProperties(itemsVal)
		schemaMap["items"] = itemsVal
	} else if itemsArr, ok := schemaMap["items"].([]interface{}); ok {
		for i, item := range itemsArr {
			if subSchema, isMap := item.(map[string]interface{}); isMap {
				sortSchemaProperties(subSchema)
				itemsArr[i] = subSchema
			}
		}
		schemaMap["items"] = itemsArr
	}
}

// specCmd represents the read command
var specCmd = &cobra.Command{
	Use:   "spec",
	Short: "spec command",
	RunE: func(_ *cobra.Command, _ []string) error {
		var config any
		if destinationConfigPath == "not-set" {
			config = connector.Spec()
		} else {
			// Create a writer config with the specified destination type
			writerConfig := types.WriterConfig{
				Type: types.AdapterType(strings.ToUpper(destinationConfigPath)),
			}

			// Get the new function for the destination type
			newFunc, found := destination.RegisteredWriters[writerConfig.Type]
			if !found {
				return fmt.Errorf("invalid destination type has been passed [%s]", writerConfig.Type)
			}

			writer := newFunc()
			config = writer.Spec()
		}

		schemaVal, err := jsonschema.Reflect(config)
		if err != nil {
			return fmt.Errorf("failed to reflect config: %v", err)
		}

		// Marshal the schemaVal to a generic map[string]interface{} to allow modification
		schemaBytes, err := json.Marshal(schemaVal)
		if err != nil {
			return fmt.Errorf("failed to marshal schemaVal for sorting: %v", err)
		}

		var genericSchema map[string]interface{}
		if err := json.Unmarshal(schemaBytes, &genericSchema); err != nil {
			return fmt.Errorf("failed to unmarshal schema to generic map for sorting: %v", err)
		}

		// Apply recursive sorting to the entire generic schema
		sortSchemaProperties(genericSchema)

		// The logger.FileLogger expects `interface{}`. The modified genericSchema
		// now contains our custom marshaler for "properties" at all levels.
		specSchema := map[string]interface{}{
			"spec": genericSchema,
		}

		logger.FileLogger(specSchema, "spec", ".json")

		return nil
	},
}
