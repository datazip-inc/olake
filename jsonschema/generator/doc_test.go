package generator

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/brainicorn/ganno"
	"github.com/datazip-inc/olake/jsonschema/schema"

	"github.com/stretchr/testify/assert"
)

// StandardDocStruct has a proper synopsis.
// It also has a proper description
//
// @jsonSchema(id="me")
type StandardDocStruct struct{}

// StandardDocNoELStruct has a proper synopsis.
// It also has a proper description but no empty line before the anno
// @jsonSchema(id="me")
type StandardDocNoELStruct struct{}

// StandardDocNoDescNoELStruct has a proper synopsis but no empty line before the anno.
// @jsonSchema(id="me")
type StandardDocNoDescNoELStruct struct{}

// NewlineSynopsisDocStruct  is missing a period on the first line
// It also has a proper description
//
// @jsonSchema(id="me")
type NewlineSynopsisAndDescStruct struct{}

// NewlineSynopsisAndDescNoELStruct  is missing a period on the first line
// It also has a proper description
// @jsonSchema(id="me")
type NewlineSynopsisAndDescNoELStruct struct{}

// NewlineSynopsisNoDescStruct  is missing a period on the first line
//
// @jsonSchema(id="me")
type NewlineSynopsisNoDescStruct struct{}

// NewlineSynopsisDocStruct  is missing a period on the first line
// @jsonSchema(id="me")
type NewlineSynopsisNoDescNoELStruct struct{}

// RunonStruct is munged together @jsonSchema(id="me")
type RunonStruct struct{}

// @jsonSchema(id="me")
type JustAnnoStruct struct{}

// @jsonSchema(id="me")
type JustAnnoNoSpaceStruct struct{}

type NewlineSynopsisDocField struct {
	// DocField  is missing a period on the first line
	// soem desc
	// @jsonSchema(required=true)
	DocField string
}

func TestStandardDocStruct(t *testing.T) {
	t.Parallel()

	aparser := ganno.NewAnnotationParser()
	aparser.RegisterFactory("jsonSchema", &schemaAnnoFactory{})

	annos, errs := aparser.Parse(`// NewlineSynopsisDocStruct  is missing a period on the first line
	// @jsonSchema(id="me")`)

	assert.Equal(t, 0, len(errs))

	jsAnnos := annos.ByName("jsonSchema")

	jsAnno := jsAnnos[0].(*schemaAnno)

	assert.Equal(t, 1, len(jsAnno.Attributes()))
}

func TestNewlineSynopsisAndDescStruct(t *testing.T) {
	pkg := "github.com/datazip-inc/olake/jsonschema/generator"
	opts := NewOptions()
	opts.IncludeTests = true
	opts.LogLevel = VerboseLevel

	jsonSchema, err := GenerateIt(pkg, "NewlineSynopsisAndDescStruct", opts)

	assert.NoError(t, err)
	//jschema := jsonSchema.(schema.ObjectSchema)

	fmt.Println(schemaAsString(jsonSchema))
}

func TestNewlineSynopsisDocField(t *testing.T) {
	pkg := "github.com/datazip-inc/olake/jsonschema/generator"
	opts := NewOptions()
	opts.IncludeTests = true
	opts.LogLevel = VerboseLevel

	jsonSchema, err := GenerateIt(pkg, "NewlineSynopsisDocField", opts)
	if err != nil {
		t.Fatalf("Failed to generate schema: %v", err)
	}

	schemaStr := schemaAsString(jsonSchema)
	if schemaStr == "" {
		t.Fatal("Generated schema is empty")
	}

	// Verify the schema contains the required field
	assert.Contains(t, schemaStr, "DocField")
	assert.Contains(t, schemaStr, "required")
}

func GenerateIt(pkg, obj string, opts Options) (schema.JSONSchema, error) {
	if pkg == "" || obj == "" {
		return nil, fmt.Errorf("package and object name must be provided")
	}
	g := NewJSONSchemaGenerator(pkg, obj, opts)
	if g == nil {
		return nil, fmt.Errorf("failed to create schema generator")
	}
	schema, err := g.Generate()
	if err != nil {
		return nil, fmt.Errorf("failed to generate schema: %v", err)
	}
	if schema == nil {
		return nil, fmt.Errorf("generated schema is nil")
	}
	return schema, nil
}

func schemaAsString(s schema.JSONSchema) string {
	if s == nil {
		return ""
	}
	schemaBytes, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Sprintf("error marshaling schema: %v", err)
	}
	return string(schemaBytes)
}
