package schema

import (
	"github.com/goccy/go-json"
)

// StringSchema represents the schema for a JSON string.
type StringSchema interface {
	SimpleSchema
	SetPattern(pattern string)
	SetMaxLength(maxLength int64)
	SetMinLength(minLength int64)

	GetPattern() string
	GetMaxLength() int64
	GetMinLength() int64
}

type defaultStringSchema struct {
	*defaultSimpleSchema
	Pattern   string   `json:"pattern,omitempty"`
	MaxLength int64    `json:"maxLength,omitempty"`
	Enums     []string `json:"enum,omitempty"`
	MinLength int64    `json:"minLength,omitempty"`
}

// NewStringSchema creates a new string schema.
func NewStringSchema() StringSchema {
	return &defaultStringSchema{
		defaultSimpleSchema: &defaultSimpleSchema{
			basicSchema: NewBasicSchema(SchemaTypeString).(*basicSchema),
		},
	}
}

func (s *defaultStringSchema) SetEnum(enums []string) {
	s.Enums = append(s.Enums, enums...)
}

func (s *defaultStringSchema) UnmarshalJSON(b []byte) error {
	var err error
	var stuff map[string]interface{}

	ss := &defaultSimpleSchema{}
	err = json.Unmarshal(b, ss)

	if err == nil {
		s.defaultSimpleSchema = ss

		err = json.Unmarshal(b, &stuff)
	}

	if err == nil {
		for k, v := range stuff {
			switch k {
			case "pattern":
				s.Pattern = v.(string)
			case "maxLength":
				s.MaxLength = int64(v.(float64))
			case "minLength":
				s.MinLength = int64(v.(float64))
			}
		}
	}

	return err
}

func (s *defaultStringSchema) Clone() JSONSchema {
	s2 := &defaultStringSchema{}
	*s2 = *s

	s2.defaultSimpleSchema = s.defaultSimpleSchema.Clone().(*defaultSimpleSchema)
	return s2
}

func (s *defaultStringSchema) SetPattern(pattern string) {
	s.Pattern = pattern
}

func (s *defaultStringSchema) SetMaxLength(maxLength int64) {
	s.MaxLength = maxLength
}

func (s *defaultStringSchema) SetMinLength(minLength int64) {
	s.MinLength = minLength
}

func (s *defaultStringSchema) GetPattern() string {
	return s.Pattern
}

func (s *defaultStringSchema) GetMaxLength() int64 {
	return s.MaxLength
}

func (s *defaultStringSchema) GetMinLength() int64 {
	return s.MinLength
}
