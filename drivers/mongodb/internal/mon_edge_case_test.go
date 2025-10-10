package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestFilterMongoObject_ArrayWithPrimitiveDateTime(t *testing.T) {
	// Test case: array containing primitive.DateTime values directly (not in documents)
	malformedDateTime := primitive.DateTime(0)
	
	testDoc := bson.M{
		"arrayWithPrimitiveDateTimes": bson.A{
			malformedDateTime,
			"some string",
			malformedDateTime,
		},
		"nestedWithArrayOfDateTimes": bson.M{
			"dates": bson.A{
				malformedDateTime,
				malformedDateTime,
			},
		},
	}

	// Filter the document
	filterMongoObject(testDoc)
	
	// Verify that primitive.DateTime values in arrays are still present
	// This is the edge case - they should ideally be converted too
	array, ok := testDoc["arrayWithPrimitiveDateTimes"].(bson.A)
	assert.True(t, ok, "Array should still be bson.A")
	
	// Check if DateTime values in array are converted
	for i, item := range array {
		if _, ok := item.(primitive.DateTime); ok {
			t.Errorf("Array item at index %d is still primitive.DateTime, should be converted to time.Time", i)
		}
	}
	
	// Check nested array
	nested, ok := testDoc["nestedWithArrayOfDateTimes"].(bson.M)
	assert.True(t, ok, "Nested should be bson.M")
	
	dates, ok := nested["dates"].(bson.A)
	assert.True(t, ok, "Dates should be bson.A")
	
	for i, item := range dates {
		if _, ok := item.(primitive.DateTime); ok {
			t.Errorf("Nested array item at index %d is still primitive.DateTime, should be converted to time.Time", i)
		}
	}
}
