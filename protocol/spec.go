package protocol

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/jsonschema"
	"github.com/datazip-inc/olake/utils/jsonschema/schema"
	"github.com/datazip-inc/olake/utils/logger"

	"github.com/spf13/cobra"
)

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

		// // Get just the properties from the schema
		// if objSchema, ok := schemaVal.(schema.ObjectSchema); ok {
		// 	// Create a new object schema with just the properties
		// 	newSchema := schema.NewObjectSchema(true)

		// 	// Copy properties and process them to remove refs and inline nested objects
		// 	props := objSchema.GetProperties()
		// 	processedProps := make(map[string]schema.JSONSchema)

		// 	for key, prop := range props {
		// 		// If the property is an object schema, process it
		// 		if objProp, ok := prop.(schema.ObjectSchema); ok {
		// 			// Create a new object schema for the nested property
		// 			nestedSchema := schema.NewObjectSchema(true)

		// 			// Copy all properties from the nested object
		// 			nestedProps := objProp.GetProperties()
		// 			if len(nestedProps) > 0 {
		// 				nestedSchema.SetProperties(nestedProps)
		// 			}

		// 			// Copy additionalProperties if it exists
		// 			if additionalProps := objProp.GetAdditionalProperties(); additionalProps != nil {
		// 				nestedSchema.SetAdditionalProperties(additionalProps)
		// 			}

		// 			// Copy required fields
		// 			for _, r := range objProp.GetRequired() {
		// 				nestedSchema.AddRequiredField(r)
		// 			}

		// 			processedProps[key] = nestedSchema
		// 		} else {
		// 			// For non-object properties, just copy them as is
		// 			processedProps[key] = prop
		// 		}
		// 	}

		// 	newSchema.SetProperties(processedProps)

		// 	// Copy required fields
		// 	required := objSchema.GetRequired()
		// 	if len(required) > 0 {
		// 		for _, r := range required {
		// 			newSchema.AddRequiredField(r)
		// 		}
		// 	}

		// 	// Use the new schema with just properties
		// 	schemaVal = newSchema
		// }
		specSchema := map[string]schema.JSONSchema{
			"spec": schemaVal,
		}

		logger.FileLogger(specSchema, "spec", ".json")

		return nil
	},
}
