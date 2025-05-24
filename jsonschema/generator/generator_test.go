package generator

import (
	"testing"

	"github.com/brainicorn/ganno"
	"github.com/datazip-inc/olake/jsonschema/schema"

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
	suite.T().Skip("Skipping test that depends on external test objects")
}

func (suite *GeneratorTestSuite) TestCommonAttrs() {
	suite.T().Skip("Skipping test that depends on external test objects")
}

func (suite *GeneratorTestSuite) TestTagName() {
	suite.T().Skip("Skipping test that depends on external test objects")
}

func (suite *GeneratorTestSuite) TestDefaultName() {
	suite.T().Skip("Skipping test that depends on external test objects")
}

func (suite *GeneratorTestSuite) TestArrayAttrs() {
	suite.T().Skip("Skipping test that depends on external test objects")
}

func (suite *GeneratorTestSuite) TestStringAttrs() {
	suite.T().Skip("Skipping test that depends on external test objects")
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
	suite.T().Skip("Skipping test that depends on external test objects")
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
	suite.T().Skip("Skipping test that depends on external test objects")
}

func (suite *GeneratorTestSuite) TestAnyOf() {
	suite.T().Skip("Skipping test that depends on external test objects")
}

func (suite *GeneratorTestSuite) TestOneOf() {
	suite.T().Skip("Skipping test that depends on external test objects")
}

func (suite *GeneratorTestSuite) TestNot() {
	suite.T().Skip("Skipping test that depends on external test objects")
}
