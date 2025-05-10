package schema

import (
	"strings"

	"github.com/goccy/go-json"
)

const (
	// DefinitionRoot is the root of the definition ref
	DefinitionRoot = "#/definitions/"
)

// StringOrArray holds a string or a slice of strings for values that can take either.
// This is used for things like type
type StringOrArray struct {
	String string
	Array  []string
}

// NewStringOrArray creates a *StringOrArray based on the given interface.
func NewStringOrArray(v interface{}) *StringOrArray {
	s, ok := v.(string)
	if ok {
		return &StringOrArray{
			String: s,
		}
	}

	a, ok := v.([]string)
	if ok {
		return &StringOrArray{
			Array: a,
		}
	}

	return &StringOrArray{}
}

// MarshalJSON convert this object to JSON
func (soa *StringOrArray) MarshalJSON() ([]byte, error) {
	if len(soa.Array) > 0 {
		return json.Marshal(soa.Array)
	}

	return json.Marshal(soa.String)
}

// UnmarshalJSON converts this bool or schema object from a JSON structure
func (soa *StringOrArray) UnmarshalJSON(data []byte) error {
	var sa StringOrArray
	if data[0] == '[' {
		var a []string
		if err := json.Unmarshal(data, &a); err != nil {
			return err
		}
		sa.Array = a
	} else {
		sa.String = string(data)
	}

	*soa = sa
	return nil
}

// JSONSchema is the base interface that represents common json-schema attributes.
type JSONSchema interface {
	Clone() JSONSchema
	GetSchemaURI() string
	GetID() string
	GetType() *StringOrArray
	GetRef() string
	GetTitle() string
	GetDescription() string
	GetAllOf() []JSONSchema
	GetAnyOf() []JSONSchema
	GetOneOf() []JSONSchema
	GetNot() JSONSchema
	GetDefinitions() map[string]JSONSchema
	GetDefault() interface{}

	AddDefinition(key string, def JSONSchema)
	SetSchemaURI(uri string)
	SetID(id string)
	SetRef(ref string)
	SetTitle(title string)
	SetDescription(description string)
	SetEnum(items []string)
	SetIntEnum(items []string) error
	SetAllOf(items []JSONSchema)
	SetAnyOf(items []JSONSchema)
	SetOneOf(items []JSONSchema)
	SetNot(not JSONSchema)
	SetDefault(def string) error
	SetConstant(def interface{})
	SetType(typeList string)
}

// Definitions hold schema definitions.
// http://json-schema.org/latest/json-schema-validation.html#rfc.section.5.26
// RFC draft-wright-json-schema-validation-00, section 5.26
type Definitions map[string]*JSONSchema

// BasicSchema is the base implementation of the JSONSchema interface.
type basicSchema struct {
	SchemaURI    string                `json:"$schema,omitempty"`
	ID           string                `json:"id,omitempty"`
	Ref          string                `json:"$ref,omitempty"`
	JSONType     *StringOrArray        `json:"type,omitempty"`
	Title        string                `json:"title,omitempty"`
	Description  string                `json:"description,omitempty"`
	AllOf        []JSONSchema          `json:"allOf,omitempty"`
	AnyOf        []JSONSchema          `json:"anyOf,omitempty"`
	OneOf        []JSONSchema          `json:"oneOf,omitempty"`
	Not          JSONSchema            `json:"not,omitempty"`
	Definitions  map[string]JSONSchema `json:"definitions,omitempty"`
	DefaultValue interface{}           `json:"default,omitempty"`
	Const        interface{}           `json:"const,omitempty"`
}

// FromJSON returns a JSONSchema object from the given json bytes.
func FromJSON(js []byte) (JSONSchema, error) {
	var err error
	var obj JSONSchema

	var stuff map[string]interface{}

	err = json.Unmarshal(js, &stuff)

	if err != nil {
		return nil, err
	}

	if jsontype, ok := stuff["type"]; ok {
		switch jsontype {
		case SchemaTypeObject:
			obj = &defaultObjectSchema{}
			err = json.Unmarshal(js, obj)
			return obj, err
		case SchemaTypeString:
			obj = &defaultStringSchema{}
			err = json.Unmarshal(js, obj)
		case SchemaTypeArray:
			obj = &defaultArraySchema{}
			err = json.Unmarshal(js, obj)
		case SchemaTypeInteger, SchemaTypeNumber:
			obj = &defaultNumericSchema{}
			err = json.Unmarshal(js, obj)
		case SchemaTypeBoolean:
			obj = &defaultSimpleSchema{}
			err = json.Unmarshal(js, obj)
		default:
			obj = &defaultSimpleSchema{}
			err = json.Unmarshal(js, obj)
		}
	}

	if ref, ok := stuff["$ref"]; ok {
		obj = &basicSchema{}
		obj.(*basicSchema).Ref = ref.(string)
	}

	return obj, err

}

// strings enums not supported
func (s *basicSchema) SetEnum(enums []string) {
}

// int enums not supported
func (s *basicSchema) SetIntEnum(enums []string) error {
	return nil
}

// NewBasicSchema creates a new BasicSchema
func NewBasicSchema(jsonType string) JSONSchema {
	bs := &basicSchema{
		AllOf:       make([]JSONSchema, 0),
		AnyOf:       make([]JSONSchema, 0),
		OneOf:       make([]JSONSchema, 0),
		Definitions: make(map[string]JSONSchema),
	}

	bs.SetType(jsonType)

	return bs
}

func (s *basicSchema) UnmarshalJSON(b []byte) error {
	var err error
	var stuff map[string]interface{}

	s.AllOf = make([]JSONSchema, 0)
	s.AnyOf = make([]JSONSchema, 0)
	s.OneOf = make([]JSONSchema, 0)
	s.Definitions = make(map[string]JSONSchema)

	err = json.Unmarshal(b, &stuff)

	if err == nil {
		for k, v := range stuff {
			switch k {
			case "$schema":
				s.SchemaURI = v.(string)
			case "id":
				s.ID = v.(string)
			case "$ref":
				s.Ref = v.(string)
			case "type":
				s.JSONType = NewStringOrArray(v)
			case "title":
				s.Title = v.(string)
			case "description":
				s.Description = v.(string)
			case "allOf":
				for _, xo := range v.([]interface{}) {
					xb, xerr := json.Marshal(xo)
					if xerr != nil {
						return xerr
					}
					xs, xerr := FromJSON(xb)
					if xerr != nil {
						return xerr
					}
					s.AllOf = append(s.AllOf, xs)
				}

			case "anyOf":
				for _, xo := range v.([]interface{}) {
					xb, xerr := json.Marshal(xo)
					if xerr != nil {
						return xerr
					}
					xs, xerr := FromJSON(xb)
					if xerr != nil {
						return xerr
					}
					s.AnyOf = append(s.AnyOf, xs)
				}

			case "oneOf":
				for _, xo := range v.([]interface{}) {
					xb, xerr := json.Marshal(xo)
					if xerr != nil {
						return xerr
					}
					xs, xerr := FromJSON(xb)
					if xerr != nil {
						return xerr
					}
					s.OneOf = append(s.OneOf, xs)
				}

			case "not":
				xb, xerr := json.Marshal(v)
				if xerr != nil {
					return err
				}
				xs, xerr := FromJSON(xb)
				if xerr != nil {
					return xerr
				}
				s.Not = xs
			case "definitions":
				for mk, mv := range v.(map[string]interface{}) {
					mb, xerr := json.Marshal(mv)
					if xerr != nil {
						return xerr
					}
					ms, xerr := FromJSON(mb)
					if xerr != nil {
						return xerr
					}
					s.Definitions[mk] = ms
				}
				//			case "default":
				//				s.Description = v.(string)
			}
		}
	}

	return err
}

func (s *basicSchema) Clone() JSONSchema {
	s2 := &basicSchema{}
	*s2 = *s

	return s2
}

func (s *basicSchema) GetSchemaURI() string {
	return s.SchemaURI
}

func (s *basicSchema) GetID() string {
	return s.ID
}

func (s *basicSchema) GetType() *StringOrArray {
	return s.JSONType
}

func (s *basicSchema) GetRef() string {
	return s.Ref
}

func (s *basicSchema) GetTitle() string {
	return s.Title
}

func (s *basicSchema) GetDescription() string {
	return s.Description
}

func (s *basicSchema) GetAllOf() []JSONSchema {
	return s.AllOf
}

func (s *basicSchema) GetAnyOf() []JSONSchema {
	return s.AnyOf
}

func (s *basicSchema) GetOneOf() []JSONSchema {
	return s.OneOf
}

func (s *basicSchema) GetNot() JSONSchema {
	return s.Not
}

func (s *basicSchema) GetDefinitions() map[string]JSONSchema {
	return s.Definitions
}

func (s *basicSchema) GetDefault() interface{} {
	return s.DefaultValue
}

func (s *basicSchema) AddDefinition(key string, def JSONSchema) {
	s.Definitions[key] = def
}
func (s *basicSchema) SetSchemaURI(uri string) {
	s.SchemaURI = uri
}

func (s *basicSchema) SetID(id string) {
	s.ID = id
}

func (s *basicSchema) SetRef(ref string) {
	s.Ref = ref
}

func (s *basicSchema) SetTitle(title string) {
	s.Title = title
}

func (s *basicSchema) SetDescription(description string) {
	s.Description = description
}

func (s *basicSchema) SetAllOf(items []JSONSchema) {
	s.AllOf = items
}

func (s *basicSchema) SetAnyOf(items []JSONSchema) {
	s.AnyOf = items
}

func (s *basicSchema) SetOneOf(items []JSONSchema) {
	s.OneOf = items
}

func (s *basicSchema) SetNot(not JSONSchema) {
	s.Not = not
}

func (s *basicSchema) SetDefault(def string) error {
	s.DefaultValue = def

	return nil
}

func (s *basicSchema) SetConstant(constant interface{}) {
	s.Const = constant
}

func (s *basicSchema) SetType(typeList string) {
	if len(strings.TrimSpace(typeList)) < 1 {
		return
	}

	types := strings.Split(typeList, ",")

	if len(types) > 1 {
		s.JSONType = &StringOrArray{Array: types}
	} else {
		s.JSONType = &StringOrArray{String: types[0]}
	}
}
