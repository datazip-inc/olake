package generator

import (
	//"github.com/goccy/go-json"
	//"fmt"
	"testing"

	"github.com/datazip-inc/olake/utils/jsonschema/schema"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"golang.org/x/tools/go/loader"
)

type ErrorCaseTestSuite struct {
	suite.Suite
	program     *loader.Program
	options     Options
	basePackage string
}

// The entry point into the tests
func TestErrorCaseSuite(t *testing.T) {
	t.Parallel()

	suite.Run(t, new(ErrorCaseTestSuite))
}

func (suite *ErrorCaseTestSuite) SetupSuite() {
	opts := NewOptions()
	opts.LogLevel = QuietLevel
	opts.IncludeTests = true

	suite.options = opts

	suite.basePackage = "github.com/datazip-inc/olake/jsonschema/generator"

	p, _ := NewJSONSchemaGenerator("", "", opts).loadProgram(suite.basePackage, suite.options)

	suite.program = p
}

// ##### TEST OBJECTS #####

type BadMaximum struct {
	// @jsonSchema(maximum=!)
	SomeInt int
}

type BadMinimum struct {
	// @jsonSchema(minimum=!)
	SomeInt int
}

type BadExclusiveMaximum struct {
	// @jsonSchema(maximum=2, exclusiveMaximum=yup)
	SomeInt int
}

type BadExclusiveMinimum struct {
	// @jsonSchema(minimum=1, exclusiveMinimum=yup)
	SomeInt int
}

type BadMultipleOf struct {
	// @jsonSchema(multipleOf=five)
	SomeInt int
}

type BadMaxLength struct {
	// @jsonSchema(maxLength=one)
	SomeString string
}

type BadMinLength struct {
	// @jsonSchema(minLength=one)
	SomeString string
}

type BadMaxItems struct {
	// @jsonSchema(maxItems=one)
	SomeStrings []string
}

type BadMinItems struct {
	// @jsonSchema(minItems=one)
	SomeStrings []string
}

type BadUniqueItems struct {
	// @jsonSchema(uniqueItems=yup)
	SomeStrings []string
}

type BadAdditionalItems struct {
	// @jsonSchema(additionalItems=yup)
	SomeStrings []string
}

type BadRequired struct {
	// @jsonSchema(required=yup)
	SomeStrings []string
}

// @jsonSchema(maxProperties=two)
type BadMaxProps struct{}

// @jsonSchema(minProperties=one)
type BadMinProps struct{}

// @jsonSchema(additionalProperties=yup)
type BadAdditionalProps struct{}

// @jsonSchema(thisIsNotARealAttr=yup)
type BadAnnoAttr struct{}

// @jsonSchema(not="!this is not a package type")
type BadNot interface{}

// @jsonSchema(allOf=["!this is not a package type"])
type BadAllOf interface{}

// @jsonSchema(anyOf=["!this is not a package type"])
type BadAnyOf interface{}

// @jsonSchema(oneOf=["!this is not a package type"])
type BadOneOf interface{}

type StringArray struct {
	Aliases []string
}

func (suite *ErrorCaseTestSuite) TestArraySimpleType() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "StringArray", suite.options)
	generator.program = suite.program

	s, err := generator.Generate()
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), s)

	aliases := s.(schema.ObjectSchema).GetProperties()["Aliases"].(schema.ArraySchema)

	assert.Equal(suite.T(), "string", aliases.GetItems().GetType().String)
}

func (suite *ErrorCaseTestSuite) TestMaximumError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadMaximum", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestMinimumError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadMinimum", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestExclusiveMaximumError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadExclusiveMaximum", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestExclusiveMinimumError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadExclusiveMinimum", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestMultipleOfError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadMultipleOf", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestMaxLengthError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadMaxLength", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestMinLengthError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadMinLength", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestMaxItemsError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadMaxItems", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestMinItemsError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadMinItems", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestUniqueItemsError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadUniqueItems", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestAdditionalItemsError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadAdditionalItems", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestRequiredError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadRequired", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestMaxPropsError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadMaxProps", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestMinPropsError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadMinProps", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestAdditionalPropsError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadAdditionalProps", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestAnnoAttrError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadAnnoAttr", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestNotError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadNot", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestAllOfError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadAllOf", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestAnyOfError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadAnyOf", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestOneOfError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "BadOneOf", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.Error(suite.T(), err)
}

func (suite *ErrorCaseTestSuite) TestBadRootError() {
	suite.T().Parallel()

	generator := NewJSONSchemaGenerator(suite.basePackage, "NotFound", suite.options)
	generator.program = suite.program

	_, err := generator.Generate()
	assert.EqualError(suite.T(), err, "root not found")
}
