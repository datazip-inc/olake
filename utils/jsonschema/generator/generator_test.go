package generator

import (
	"strings"
	"testing"

	"github.com/brainicorn/ganno"
	"github.com/datazip-inc/olake/utils/jsonschema/schema"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type GeneratorTestSuite struct {
	suite.Suite
}

// The entry point into the tests
func TestGeneratorSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(GeneratorTestSuite))
}

func (suite *GeneratorTestSuite) SetupSuite() {
}

func Generate(pkg, obj string, opts Options) (schema.JSONSchema, error) {
	g := NewJSONSchemaGenerator(pkg, obj, opts)
	return g.Generate()
}

type RecurseStruct struct {
	ID     string
	Myself *RecurseStruct
}

func (suite *GeneratorTestSuite) TestAttrsMap() {
	suite.T().Parallel()

	aparser := ganno.NewAnnotationParser()
	aparser.RegisterFactory("jsonSchema", &schemaAnnoFactory{})

	annos, errs := aparser.Parse("@jsonSchema(additionalProperties=true)")

	assert.Equal(suite.T(), 0, len(errs))

	jsAnnos := annos.ByName("jsonSchema")

	jsAnno := jsAnnos[0].(*schemaAnno)

	assert.Equal(suite.T(), 1, len(jsAnno.Attributes()))
}

func (suite *GeneratorTestSuite) TestIgnoreField() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/album"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "Album", opts)

	assert.NoError(suite.T(), err)
	_, hasField := jsonSchema.(schema.ObjectSchema).GetProperties()["RecordedAt"]

	assert.False(suite.T(), hasField, "Album has field RecordedAt but it should be ignored")
}

func (suite *GeneratorTestSuite) TestCommonAttrs() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/album"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "Album", opts)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), schema.SpecVersionDraftV4, jsonSchema.GetSchemaURI(), "wrong uri")
	assert.Equal(suite.T(), "http://github.com/schemas/album", jsonSchema.GetID(), "wrong id")
	assert.Equal(suite.T(), "An Album.", jsonSchema.GetTitle(), "wrong title")
	assert.Equal(suite.T(), "A thing we used to play with a player and now we get from the air", jsonSchema.GetDescription(), "wrong desc")
}

func (suite *GeneratorTestSuite) TestTagName() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/album"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "Album", opts)
	_, hasField := jsonSchema.(schema.ObjectSchema).GetProperties()["tracks"]

	assert.NoError(suite.T(), err)
	assert.True(suite.T(), hasField, "Album should have field 'tracks'")
}

func (suite *GeneratorTestSuite) TestDefaultName() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/album"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "Album", opts)
	_, hasField := jsonSchema.(schema.ObjectSchema).GetProperties()["Price"]

	assert.NoError(suite.T(), err)
	assert.True(suite.T(), hasField, "Album should have field 'Price'")
}

func (suite *GeneratorTestSuite) TestArrayAttrs() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/album"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "Album", opts)
	songs, hasField := jsonSchema.(schema.ObjectSchema).GetProperties()["tracks"]

	assert.NoError(suite.T(), err)
	assert.True(suite.T(), hasField, "Album should have field 'tracks'")

	songsAry := songs.(schema.ArraySchema)
	max := songsAry.GetMaxItems()
	min := songsAry.GetMinItems()
	add := songsAry.GetAdditionalItems()
	unq := songsAry.GetUniqueItems()

	numsongsAry := jsonSchema.(schema.ObjectSchema).GetProperties()["numtracks"].(schema.ArraySchema)
	numsongsType := strings.Join(numsongsAry.GetItems().GetType().Array, ",")

	assert.Equal(suite.T(), int64(50), max, "songs should have maxItems of 50 but got %d", max)
	assert.Equal(suite.T(), int64(1), min, "songs should have minItems of 1 but got %d", min)
	assert.Equal(suite.T(), false, add, "songs should have additionalItems of false but got %t", add)
	assert.Equal(suite.T(), true, unq, "songs should have uniqueItems of true but got %t", unq)
	assert.EqualValues(suite.T(), "string,number,integer", numsongsType, "numsongsType should have correct types %t", numsongsType)
}

func (suite *GeneratorTestSuite) TestStringAttrs() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/media"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "SocialMedia", opts)
	objSchema := jsonSchema.(schema.ObjectSchema)

	assert.NoError(suite.T(), err)

	twitter := objSchema.GetProperties()["twitter"].(schema.SimpleSchema)
	email := objSchema.GetProperties()["email"].(schema.StringSchema)
	mention := objSchema.GetProperties()["mention"].(schema.StringSchema)

	assert.Equal(suite.T(), "ipv4", twitter.GetFormat(), "twitter should have ipv4 format")
	assert.Equal(suite.T(), "email", email.GetFormat(), "email should have email format")
	assert.Equal(suite.T(), int64(2), mention.GetMinLength(), "mention should have minLength of 2, got %d", mention.GetMinLength())
	assert.Equal(suite.T(), int64(200), mention.GetMaxLength(), "mention should have maxLength of 200, got %d", mention.GetMaxLength())
	assert.Equal(suite.T(), "[A-Za-z0-9]", mention.GetPattern(), "mention should have pattern of [A-Za-z0-9], got %s", mention.GetPattern())
}

func (suite *GeneratorTestSuite) TestNumericAttrs() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/album"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "ForSale", opts)
	objSchema := jsonSchema.(schema.ObjectSchema)

	assert.NoError(suite.T(), err)

	price := objSchema.GetProperties()["Price"].(schema.NumericSchema)
	rating := objSchema.GetProperties()["rating"].(schema.NumericSchema)

	assert.Equal(suite.T(), float64(5), price.GetMultipleOf(), "price should be multiple of 5, got %f", price.GetMultipleOf())
	assert.Equal(suite.T(), float64(0), rating.GetMinimum(), "rating min should be 0, got %f", rating.GetMinimum())
	assert.Equal(suite.T(), float64(5), rating.GetMaximum(), "rating max should be 5, got %f", rating.GetMaximum())
	assert.Equal(suite.T(), true, rating.GetExclusiveMinimum(), "rating exclusive min should be true, got %t", rating.GetExclusiveMinimum())
	assert.Equal(suite.T(), false, rating.GetExclusiveMaximum(), "rating exclusive max should be false, got %t", rating.GetExclusiveMaximum())

}

func (suite *GeneratorTestSuite) TestObjectAttrs() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/artist"
	opts := NewOptions()
	opts.LogLevel = InfoLevel

	jsonSchema, err := Generate(pkg, "Individual", opts)
	objSchema := jsonSchema.(schema.ObjectSchema)

	assert.NoError(suite.T(), err)

	assert.Equal(suite.T(), int64(1), objSchema.GetMinProperties(), "min props should be 1, got %d", objSchema.GetMinProperties())
	assert.Equal(suite.T(), int64(20), objSchema.GetMaxProperties(), "max props should be 20, got %d", objSchema.GetMaxProperties())
	assert.Equal(suite.T(), true, objSchema.GetAdditionalProperties().Boolean, "additional props should be true, got %t", objSchema.GetAdditionalProperties())
	assert.Equal(suite.T(), []string{"firstName"}, objSchema.GetRequired(), "required props should be 'firstName, got %v", objSchema.GetRequired())

}

func (suite *GeneratorTestSuite) TestRecurseObject() {
	suite.T().Parallel()

	pkg := "github.com/datazip-inc/olake/jsonschema/generator"
	opts := NewOptions()
	opts.LogLevel = InfoLevel
	opts.IncludeTests = true

	jsonSchema, err := Generate(pkg, "RecurseStruct", opts)
	objSchema := jsonSchema.(schema.ObjectSchema)

	assert.NoError(suite.T(), err)

	assert.Equal(suite.T(), "#", objSchema.GetProperties()["Myself"].GetRef())
}

func (suite *GeneratorTestSuite) TestAllOf() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/xof"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "AllOf", opts)
	objSchema := jsonSchema.(schema.ObjectSchema)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "#/definitions/github_com-brainicorn-schematestobjects-xof-AThing", objSchema.GetAllOf()[0].GetRef(), "got %s", objSchema.GetAllOf()[0].GetRef())

}

func (suite *GeneratorTestSuite) TestAnyOf() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/xof"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "AnyOf", opts)
	objSchema := jsonSchema.(schema.ObjectSchema)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "#/definitions/github_com-brainicorn-schematestobjects-xof-AThing", objSchema.GetAnyOf()[0].GetRef(), "got %s", objSchema.GetAnyOf()[0].GetRef())

}

func (suite *GeneratorTestSuite) TestOneOf() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/xof"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "OneOf", opts)
	objSchema := jsonSchema.(schema.ObjectSchema)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "#/definitions/github_com-brainicorn-schematestobjects-xof-AThing", objSchema.GetOneOf()[0].GetRef(), "got %s", objSchema.GetOneOf()[0].GetRef())

}

func (suite *GeneratorTestSuite) TestNot() {
	suite.T().Parallel()

	pkg := "github.com/brainicorn/schematestobjects/xof"
	opts := NewOptions()
	opts.LogLevel = QuietLevel

	jsonSchema, err := Generate(pkg, "Not", opts)
	objSchema := jsonSchema.(schema.ObjectSchema)

	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), "#/definitions/github_com-brainicorn-schematestobjects-xof-AnotherThing", objSchema.GetNot().GetRef(), "got %s", objSchema.GetNot().GetRef())

}
