package generator

import (
	"fmt"
	"testing"

	"github.com/goccy/go-json"

	"github.com/brainicorn/ganno"
	"github.com/datazip-inc/olake/utils/jsonschema/schema"

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

	assert.NoError(t, err)

	fmt.Println(schemaAsString(jsonSchema))
}

func GenerateIt(pkg, obj string, opts Options) (schema.JSONSchema, error) {
	g := NewJSONSchemaGenerator(pkg, obj, opts)
	return g.Generate()
}

func schemaAsString(s schema.JSONSchema) string {
	schemaBytes, _ := json.MarshalIndent(s, "", "  ")

	return string(schemaBytes)
}
