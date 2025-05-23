package schema

import "github.com/goccy/go-json"

// BoolOrSchema holds a bool or a JSONSchema for values that can take either.
// This is used for things like additionalProperties
type BoolOrSchema struct {
	Boolean bool
	Schema  JSONSchema
}

// NewBoolOrSchema creates a *BoolOrSchema based on the given interface.
func NewBoolOrSchema(v interface{}) *BoolOrSchema {
	s, ok := v.(JSONSchema)
	if ok {
		return &BoolOrSchema{
			Schema: s,
		}
	}

	b, ok := v.(bool)
	if ok {
		return &BoolOrSchema{
			Boolean: b,
		}
	}

	return &BoolOrSchema{}
}

// MarshalJSON convert this object to JSON
func (b *BoolOrSchema) MarshalJSON() ([]byte, error) {
	if b.Schema != nil {
		return json.Marshal(b.Schema)
	}

	if b.Schema == nil && !b.Boolean {
		return []byte("false"), nil
	}
	return []byte("true"), nil
}

// UnmarshalJSON converts this bool or schema object from a JSON structure
func (b *BoolOrSchema) UnmarshalJSON(data []byte) error {
	var bs BoolOrSchema
	if data[0] == '{' {
		var s JSONSchema
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		bs.Schema = s
	}
	bs.Boolean = !(data[0] == 'f' && data[1] == 'a' && data[2] == 'l' && data[3] == 's' && data[4] == 'e')
	*b = bs
	return nil
}

// ObjectSchema represents a JSON object schema.
type ObjectSchema interface {
	JSONSchema
	GetProperties() map[string]JSONSchema
	SetProperties(props map[string]JSONSchema)
	GetRequired() []string
	GetMaxProperties() int64
	GetMinProperties() int64
	GetAdditionalProperties() *BoolOrSchema
	//GetPatternProperties() map[string]JSONSchema
	//GetDependencies() map[string]StringArrayOrObject

	SetMaxProperties(maxProperties int64)
	SetMinProperties(minProperties int64)
	SetAdditionalProperties(additionalProperties *BoolOrSchema)
	AddRequiredField(fieldName string)

	SetGoPath(string)
	GetGoPath() string
}

type defaultObjectSchema struct {
	*basicSchema
	Properties           map[string]JSONSchema `json:"properties,omitempty"`
	Required             []string              `json:"required,omitempty"`
	MaxProperties        int64                 `json:"maxProperties,omitempty"`
	MinProperties        int64                 `json:"minProperties,omitempty"`
	AdditionalProperties *BoolOrSchema         `json:"additionalProperties,omitempty"`
	GoPath               string                `json:"x-go-path,omitempty"`
	suppressXAttrs       bool
	//PatternProperties    map[string]JSONSchema `json:"patternProperties,omitempty"`
	//Dependencies         map[string]StringArrayOrObject `json:"dependencies,omitempty"`
}

// NewObjectSchema creates a new object schema
func NewObjectSchema(suppressXAttrs bool) ObjectSchema {
	return &defaultObjectSchema{
		basicSchema:    NewBasicSchema(SchemaTypeObject).(*basicSchema),
		Properties:     make(map[string]JSONSchema),
		Required:       make([]string, 0),
		suppressXAttrs: suppressXAttrs,
		//PatternProperties: make(map[string]JSONSchema),
		//		Dependencies:      make(map[string]StringArrayOrObject),
	}
}

func (s *defaultObjectSchema) UnmarshalJSON(b []byte) error {
	var err error
	var stuff map[string]interface{}

	bs := &basicSchema{}
	err = json.Unmarshal(b, bs)

	if err == nil {
		s.basicSchema = bs

		s.Properties = make(map[string]JSONSchema)
		s.Required = make([]string, 0)
	}

	err = json.Unmarshal(b, &stuff)

	if err == nil {
		for k, v := range stuff {
			switch k {
			case "maxProperties":
				s.MaxProperties = int64(v.(float64))
			case "minProperties":
				s.MinProperties = int64(v.(float64))
			case "additionalProperties":
				s.AdditionalProperties = NewBoolOrSchema(v)
			case "properties":
				for mk, mv := range v.(map[string]interface{}) {
					mb, xerr := json.Marshal(mv)
					if xerr != nil {
						return xerr
					}
					ms, xerr := FromJSON(mb)
					if xerr != nil {
						return xerr
					}
					s.Properties[mk] = ms
				}

			case "required":
				for _, rs := range v.([]interface{}) {
					s.Required = append(s.Required, rs.(string))
				}
			}
		}
	}

	return err

}

func (s *defaultObjectSchema) Clone() JSONSchema {
	s2 := &defaultObjectSchema{}
	*s2 = *s

	s2.basicSchema = s.basicSchema.Clone().(*basicSchema)
	return s2
}

func (s *defaultObjectSchema) GetProperties() map[string]JSONSchema {
	return s.Properties
}

func (s *defaultObjectSchema) SetProperties(props map[string]JSONSchema) {
	s.Properties = props
}

func (s *defaultObjectSchema) GetRequired() []string {
	return s.Required
}

func (s *defaultObjectSchema) AddRequiredField(fieldName string) {
	s.Required = append(s.Required, fieldName)
}

func (s *defaultObjectSchema) GetMaxProperties() int64 {
	return s.MaxProperties
}

func (s *defaultObjectSchema) GetMinProperties() int64 {
	return s.MinProperties
}

func (s *defaultObjectSchema) GetAdditionalProperties() *BoolOrSchema {
	return s.AdditionalProperties
}

//func (s *defaultObjectSchema) GetPatternProperties() map[string]JSONSchema {
//	return s.PatternProperties
//}

//func (s *DefaultObjectSchema) GetDependencies() map[string]StringArrayOrObject {
//	return s.Dependencies
//}

func (s *defaultObjectSchema) SetMaxProperties(maxProperties int64) {
	s.MaxProperties = maxProperties
}

func (s *defaultObjectSchema) SetMinProperties(minProperties int64) {
	s.MinProperties = minProperties
}

func (s *defaultObjectSchema) SetAdditionalProperties(additionalProperties *BoolOrSchema) {
	s.AdditionalProperties = additionalProperties
}

func (s *defaultObjectSchema) SetGoPath(path string) {
	if s.suppressXAttrs {
		s.GoPath = ""
	} else {
		s.GoPath = path
	}
}

func (s *defaultObjectSchema) GetGoPath() string {
	return s.GoPath
}
