package jsonschema

import (
	"log"
	"reflect"

	"github.com/goccy/go-json"

	"github.com/datazip-inc/olake/utils/jsonschema/generator"
	"github.com/datazip-inc/olake/utils/jsonschema/schema"
	"sigs.k8s.io/yaml"
)

// Reflector is the main jsonschemagen command.
type Reflector struct {
	opts           generator.Options
	includeTests   bool
	inlineDefs     bool
	gen            *generator.JSONSchemaGenerator
	suppressXAttrs bool
}

func Reflect(v interface{}) (schema.JSONSchema, error) {
	r := Reflector{}
	basePackage := r.GetPackageName(v)
	typeOf := reflect.TypeOf(v)
	if typeOf.Kind() == reflect.Ptr {
		typeOf = typeOf.Elem()
	}

	opts := generator.NewOptions()
	opts.AutoCreateDefs = !r.inlineDefs
	opts.IncludeTests = r.includeTests
	opts.SupressXAttrs = r.suppressXAttrs
	opts.LogLevel = generator.VerboseLevel

	r.opts = opts
	r.gen = generator.NewJSONSchemaGenerator(basePackage, typeOf.Name(), opts)
	return r.gen.Generate()
}

func (r *Reflector) GetPackageName(s interface{}) string {
	t := reflect.TypeOf(s)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	pkgPath := t.PkgPath()

	return pkgPath
}

func ToJSONSchema(obj interface{}) (string, error) {
	schema, err := Reflect(obj)
	if err != nil {
		log.Fatal(err)
	}

	j, err := json.MarshalIndent(schema, "", " ")
	if err != nil {
		log.Fatal(err)
	}

	return string(j), nil
}

// func ToJSONSchema(obj interface{}) (string, error) {
// 	reflector := gojo.Reflector{}

// 	schema, err := reflector.Reflect(obj)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	j, err := json.MarshalIndent(schema, "", " ")
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	return string(j), nil
// }

func ToYamlSchema(obj interface{}) (string, error) {
	jsonSchema, err := ToJSONSchema(obj)
	if err != nil {
		return "", err
	}

	yamlData, err := yaml.JSONToYAML([]byte(jsonSchema))
	if err != nil {
		return "", err
	}

	return string(yamlData), nil
}
