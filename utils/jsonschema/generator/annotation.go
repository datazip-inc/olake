package generator

import (
	"fmt"
	"strconv"

	"github.com/brainicorn/ganno"
)

const (
	annotationName = "jsonSchema"
)

type boolOrPath struct {
	isBool  bool
	boolean bool
	path    string
}

type schemaAnno struct {
	attrs map[string][]string

	required             bool
	id                   string
	description          string
	definition           string
	format               string
	title                string
	defaultValue         string
	constValue           string
	maximum              float64
	exclusiveMaximum     bool
	minimum              float64
	exclusiveMinimum     bool
	maxLength            int64
	minLength            int64
	pattern              string
	maxItems             int64
	minItems             int64
	uniqueItems          bool
	multipleOf           float64
	maxProperties        int64
	minProperties        int64
	enum                 []string
	allOf                []string
	oneOf                []string
	anyOf                []string
	schemaType           []string
	not                  string
	additionalProperties *boolOrPath
	additionalItems      bool

	// TODO implement these somehow, maybe??
	//PatternProperties    ???
	//Dependencies         ???
}

func (a *schemaAnno) AnnotationName() string {
	return annotationName
}

func (a *schemaAnno) Attributes() map[string][]string {
	return a.attrs
}

type schemaAnnoFactory struct {
}

func (f *schemaAnnoFactory) ValidateAndCreate(name string, attrs map[string][]string) (ganno.Annotation, error) {
	anno := &schemaAnno{
		attrs:      attrs,
		allOf:      make([]string, 0),
		anyOf:      make([]string, 0),
		oneOf:      make([]string, 0),
		schemaType: make([]string, 0),
	}

	for k, v := range attrs {
		switch k {
		case "required":
			b, err := strconv.ParseBool(v[0])
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'required': %s", err)
			}
			anno.required = b
		case "id":
			if v[0] != "" {
				anno.id = v[0]
			}

		case "description":
			if v[0] != "" {
				anno.description = v[0]
			}

		case "definition":
			if v[0] != "" {
				anno.definition = v[0]
			}

		case "title":
			if v[0] != "" {
				anno.title = v[0]
			}

		case "format":
			if v[0] != "" {
				anno.format = v[0]
			}

		case "default":
			if v[0] != "" {
				anno.defaultValue = v[0]
			}

		case "const":
			if v[0] != "" {
				anno.constValue = v[0]
			}

		case "maximum":
			f, err := strconv.ParseFloat(v[0], 64)

			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'maximum': %s", err)
			}
			anno.maximum = f

		case "exclusivemaximum":
			b, err := strconv.ParseBool(v[0])
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'exclusiveMaximum': %s", err)
			}
			anno.exclusiveMaximum = b

		case "minimum":
			f, err := strconv.ParseFloat(v[0], 64)
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'minimum': %s", err)
			}
			anno.minimum = f

		case "exclusiveminimum":
			b, err := strconv.ParseBool(v[0])
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'exclusiveMinimum': %s", err)
			}
			anno.exclusiveMinimum = b

		case "maxlength":
			i, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'maxLength': %s", err)
			}
			anno.maxLength = i

		case "minlength":
			i, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'minLength': %s", err)
			}
			anno.minLength = i

		case "pattern":
			if v[0] != "" {
				anno.pattern = v[0]
			}

		case "maxitems":
			i, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'maxItems': %s", err)
			}
			anno.maxItems = i

		case "minitems":
			i, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'minItems': %s", err)
			}
			anno.minItems = i

		case "uniqueitems":
			b, err := strconv.ParseBool(v[0])
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'uniqueItems': %s", err)
			}
			anno.uniqueItems = b

		case "multipleof":
			f, err := strconv.ParseFloat(v[0], 64)
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'multipleOf': %s", err)
			}
			anno.multipleOf = f

		case "maxproperties":
			i, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'maxProperties': %s", err)
			}
			anno.maxProperties = i

		case "minproperties":
			i, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'minProperties': %s", err)
			}
			anno.minProperties = i

		case "enum":
			for _, item := range v {
				if isSelfRef(item) || isPackageType(item) {
					return nil, fmt.Errorf("error setting '%s' @jsonSchema 'enum': enum can not be a type selector", item)
				} else {
					anno.enum = append(anno.enum, item)
				}
			}

		case "allof":
			for _, item := range v {
				if isSelfRef(item) || isIdent(item) || isPackageType(item) {
					anno.allOf = append(anno.allOf, item)
				} else {
					return nil, fmt.Errorf("error setting @jsonSchema 'allOf': '%s' is not a valid ident or type selector", item)
				}
			}

		case "anyof":
			for _, item := range v {
				if isSelfRef(item) || isIdent(item) || isPackageType(item) {
					anno.anyOf = append(anno.anyOf, item)
				} else {
					return nil, fmt.Errorf("error setting @jsonSchema 'anyOf': '%s' is not a valid ident or type selector", item)
				}
			}

		case "oneof":
			for _, item := range v {
				if isSelfRef(item) || isIdent(item) || isPackageType(item) {
					anno.oneOf = append(anno.oneOf, item)
				} else {
					return nil, fmt.Errorf("error setting @jsonSchema 'oneOf': '%s' is not a valid ident or type selector", item)
				}
			}
		case "type":
			for _, item := range v {
				if isJSONType(item) {
					anno.schemaType = append(anno.schemaType, item)
				} else {
					return nil, fmt.Errorf("error setting @jsonSchema 'type': '%s' is not a valid JSON type", item)
				}
			}

		case "not":
			if isIdent(v[0]) || isPackageType(v[0]) {
				anno.not = v[0]
			} else {
				return nil, fmt.Errorf("error setting @jsonSchema 'not': '%s' is not a valid ident or type selector", v[0])
			}

		case "additionalproperties":
			b, err := strconv.ParseBool(v[0])
			if err == nil {
				anno.additionalProperties = &boolOrPath{
					isBool:  true,
					boolean: b,
				}
			} else if isPackageType(v[0]) || isJSONType(v[0]) {
				anno.additionalProperties = &boolOrPath{
					isBool: false,
					path:   v[0],
				}
			} else {
				return nil, fmt.Errorf("error setting @jsonSchema 'additionalProperties': %s", "must be a bool, jsonType, or typePath")
			}

		case "additionalitems":
			b, err := strconv.ParseBool(v[0])
			if err != nil {
				return nil, fmt.Errorf("error setting @jsonSchema 'additionalItems': %s", err)
			}
			anno.additionalItems = b

		default:
			return nil, fmt.Errorf("unknown @jsonSchema attribute '%s'", k)
		}
	}

	return anno, nil
}
