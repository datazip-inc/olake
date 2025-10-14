package driver

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestFilterMongoObject_RecursiveHandling(t *testing.T) {
	tests := []struct {
		name     string
		input    bson.M
		expected func(bson.M) bool // Function to validate the result
	}{
		{
			name: "nested document with DateTime",
			input: bson.M{
				"topLevel": primitive.DateTime(0),
				"nested": bson.M{
					"level2DateTime": primitive.DateTime(0),
					"level3": bson.M{
						"deepDateTime": primitive.DateTime(0),
					},
				},
			},
			expected: func(result bson.M) bool {
				// Check that ALL DateTime fields are converted to time.Time
				if _, ok := result["topLevel"].(primitive.DateTime); ok {
					return false // Should be time.Time, not primitive.DateTime
				}
				nested, ok := result["nested"].(bson.M)
				if !ok {
					return false
				}
				if _, ok := nested["level2DateTime"].(primitive.DateTime); ok {
					return false // Should be time.Time
				}
				level3, ok := nested["level3"].(bson.M)
				if !ok {
					return false
				}
				if _, ok := level3["deepDateTime"].(primitive.DateTime); ok {
					return false // Should be time.Time
				}
				return true
			},
		},
		{
			name: "array with nested documents",
			input: bson.M{
				"arrayWithDocs": bson.A{
					bson.M{"arrayDocDateTime": primitive.DateTime(0)},
					bson.M{"anotherDoc": bson.M{
						"level3DateTime": primitive.DateTime(0),
					}},
				},
			},
			expected: func(result bson.M) bool {
				array, ok := result["arrayWithDocs"].(bson.A)
				if !ok || len(array) != 2 {
					return false
				}
				
				doc1, ok := array[0].(bson.M)
				if !ok {
					return false
				}
				if _, ok := doc1["arrayDocDateTime"].(primitive.DateTime); ok {
					return false // Should be time.Time
				}
				
				doc2, ok := array[1].(bson.M)
				if !ok {
					return false
				}
				anotherDoc, ok := doc2["anotherDoc"].(bson.M)
				if !ok {
					return false
				}
				if _, ok := anotherDoc["level3DateTime"].(primitive.DateTime); ok {
					return false // Should be time.Time
				}
				return true
			},
		},
		{
			name: "primitive.D document type",
			input: bson.M{
				"primitiveDoc": primitive.D{
					{Key: "primitiveDateTime", Value: primitive.DateTime(0)},
					{Key: "nestedPrimitive", Value: primitive.D{
						{Key: "innerPrimitiveDateTime", Value: primitive.DateTime(0)},
					}},
				},
			},
			expected: func(result bson.M) bool {
				primitiveDoc, ok := result["primitiveDoc"].(bson.M)
				if !ok {
					return false // Should be converted to bson.M
				}
				
				if _, ok := primitiveDoc["primitiveDateTime"].(primitive.DateTime); ok {
					return false // Should be time.Time
				}
				
				nestedPrimitive, ok := primitiveDoc["nestedPrimitive"].(bson.M)
				if !ok {
					return false // Should be converted to bson.M
				}
				
				if _, ok := nestedPrimitive["innerPrimitiveDateTime"].(primitive.DateTime); ok {
					return false // Should be time.Time
				}
				return true
			},
		},
		{
			name: "mixed types with NaN handling",
			input: bson.M{
				"topLevelDateTime": primitive.DateTime(0),
				"topLevelNaN":     math.NaN(),
				"nested": bson.M{
					"nestedNaN":     math.NaN(),
					"nestedDateTime": primitive.DateTime(0),
				},
			},
			expected: func(result bson.M) bool {
				// Check NaN handling
				if result["topLevelNaN"] != nil {
					return false // NaN should be converted to nil
				}
				
				nested, ok := result["nested"].(bson.M)
				if !ok {
					return false
				}
				if nested["nestedNaN"] != nil {
					return false // NaN should be converted to nil
				}
				
				return true
			},
		},
		{
			name: "all MongoDB primitive types",
			input: bson.M{
				"dateTime":     primitive.DateTime(0),
				"timestamp":    primitive.Timestamp{T: 1234567890, I: 1},
				"binary":       primitive.Binary{Subtype: 0x00, Data: []byte("test")},
				"decimal":      primitive.NewDecimal128(12345, 67890),
				"objectID":     primitive.NewObjectID(),
				"nullValue":    primitive.Null{},
				"infValue":     math.Inf(1),
			},
			expected: func(result bson.M) bool {
				// All primitive types should be converted appropriately
				if _, ok := result["dateTime"].(primitive.DateTime); ok {
					return false // Should be time.Time
				}
				if _, ok := result["timestamp"].(primitive.Timestamp); ok {
					return false // Should be uint32
				}
				if _, ok := result["binary"].(primitive.Binary); ok {
					return false // Should be string
				}
				if _, ok := result["decimal"].(primitive.Decimal128); ok {
					return false // Should be string
				}
				if _, ok := result["objectID"].(primitive.ObjectID); ok {
					return false // Should be string
				}
				if result["nullValue"] != nil {
					return false // Should be nil
				}
				if result["infValue"] != nil {
					return false // Should be nil (Infinite values converted to nil)
				}
				return true
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Call the recursive filterMongoObject
			tt.input = filterMongoObject(tt.input).(bson.M)
			
			// Validate the result
			require.True(t, tt.expected(tt.input), "Recursive filtering failed validation")
		})
	}
}

func TestFilterMongoObject_DeeplyNestedStructure(t *testing.T) {
	// Create a deeply nested structure with multiple levels
	malformedDateTime := primitive.DateTime(0)
	
	deepNestedDoc := bson.M{
		"level1": malformedDateTime,
		"level1Doc": bson.M{
			"level2": malformedDateTime,
			"level2Array": bson.A{
				bson.M{"level3": malformedDateTime},
				bson.M{"level3Doc": bson.M{
					"level4": malformedDateTime,
					"level4Array": bson.A{
						bson.M{"level5": malformedDateTime},
						primitive.D{
							{Key: "level5Primitive", Value: malformedDateTime},
							{Key: "level5Nested", Value: bson.M{
								"level6": malformedDateTime,
							}},
						},
					},
				}},
			},
		},
	}

	// Filter the deeply nested document
	deepNestedDoc = filterMongoObject(deepNestedDoc).(bson.M)
	
	// Verify no primitive.DateTime remains anywhere in the structure
	var hasPrimitiveDateTime func(interface{}) bool
	hasPrimitiveDateTime = func(val interface{}) bool {
		switch v := val.(type) {
		case primitive.DateTime:
			return true
		case bson.M:
			for _, item := range v {
				if hasPrimitiveDateTime(item) {
					return true
				}
			}
		case bson.A:
			for _, item := range v {
				if hasPrimitiveDateTime(item) {
					return true
				}
			}
		case primitive.D:
			for _, elem := range v {
				if hasPrimitiveDateTime(elem.Value) {
					return true
				}
			}
		case []interface{}:
			for _, item := range v {
				if hasPrimitiveDateTime(item) {
					return true
				}
			}
		}
		return false
	}
	
	assert.False(t, hasPrimitiveDateTime(deepNestedDoc), 
		"Should not have any primitive.DateTime values remaining in deeply nested structure")
}

func TestFilterMongoObject_JSONMarshalSafety(t *testing.T) {
	// Test that the filtered document can be safely marshaled to JSON
	// This was the original issue: malformed DateTime causing JSON marshal errors
	
	malformedDateTime := primitive.DateTime(0) // Represents year 0
	
	testDoc := bson.M{
		"topLevel": malformedDateTime,
		"nestedDoc": bson.M{
			"nestedDateTime": malformedDateTime,
			"nestedArray": bson.A{
				bson.M{"arrayDateTime": malformedDateTime},
				bson.M{"anotherNested": bson.M{
					"deepDateTime": malformedDateTime,
				}},
			},
		},
		"primitiveDoc": primitive.D{
			{Key: "primitiveDateTime", Value: malformedDateTime},
		},
	}

	// Filter the document
	testDoc = filterMongoObject(testDoc).(bson.M)
	
	// Attempt to marshal to JSON - this should not panic or error
	jsonBytes, err := bson.MarshalExtJSON(testDoc, true, false)
	
	require.NoError(t, err, "Should be able to marshal filtered document to JSON without errors")
	require.NotEmpty(t, jsonBytes, "JSON marshaling should produce non-empty result")
	
	// Verify that all DateTime fields are properly represented in JSON
	jsonStr := string(jsonBytes)
	assert.Contains(t, jsonStr, `"topLevel":{"$date":`, "Top level DateTime should be properly converted")
	assert.Contains(t, jsonStr, `"nestedDateTime":{"$date":`, "Nested DateTime should be properly converted")
	assert.Contains(t, jsonStr, `"arrayDateTime":{"$date":`, "Array DateTime should be properly converted") 
	assert.Contains(t, jsonStr, `"deepDateTime":{"$date":`, "Deep DateTime should be properly converted")
	assert.Contains(t, jsonStr, `"primitiveDateTime":{"$date":`, "Primitive DateTime should be properly converted")
}

func BenchmarkFilterMongoObject_Original(b *testing.B) {
	// Benchmark the original non-recursive version
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := bson.M{
			"topLevel": primitive.DateTime(0),
			"regularField": "test",
		}
		_ = filterMongoObjectOriginal(doc) // Assuming we have the original version available
	}
}

func BenchmarkFilterMongoObject_Recursive(b *testing.B) {
	// Benchmark the new recursive version
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		doc := bson.M{
			"nestedDoc": bson.M{
				"level2DateTime": primitive.DateTime(0),
				"level3Doc": bson.M{
					"deepDateTime": primitive.DateTime(0),
				},
			},
		}
		_ = filterMongoObject(doc)
	}
}

// Helper function to simulate the original non-recursive behavior for benchmarking
func filterMongoObjectOriginal(doc bson.M) bson.M {
	for key, value := range doc {
		delete(doc, key)
		switch value := value.(type) {
		case primitive.Timestamp:
			doc[key] = value.T
		case primitive.DateTime:
			doc[key] = value.Time()
		case primitive.Null:
			doc[key] = nil
		case primitive.Binary:
			doc[key] = fmt.Sprintf("%x", value.Data)
		case primitive.Decimal128:
			doc[key] = value.String()
		case primitive.ObjectID:
			doc[key] = value.Hex()
		case float64:
			if math.IsNaN(value) || math.IsInf(value, 0) {
				doc[key] = nil
			} else {
				doc[key] = value
			}
		default:
			doc[key] = value
		}
	}
	return doc
}
