package schema

import (
	"github.com/goccy/go-json"
)

// SimpleSchema is the schema for simple JSON types such as a string, number or boolean.
type SimpleSchema interface {
	JSONSchema
	SetFormat(format string)
	GetFormat() string
}

type defaultSimpleSchema struct {
	*basicSchema
	Format string `json:"format,omitempty"`
}

// NewSimpleSchema creates a simple schema with the specified jsonType.
func NewSimpleSchema(jsonType string) SimpleSchema {
	return &defaultSimpleSchema{
		basicSchema: NewBasicSchema(jsonType).(*basicSchema),
	}
}

// enums not supported in simple schema
func (s *defaultSimpleSchema) SetEnum(enums []string) {
}

// enums not supported in simple schema
func (s *defaultSimpleSchema) SetIntEnum(enums []string) error {
	return nil
}

func (s *defaultSimpleSchema) UnmarshalJSON(b []byte) error {
	var err error
	var stuff map[string]interface{}

	bs := &basicSchema{}
	err = json.Unmarshal(b, bs)

	if err == nil {
		s.basicSchema = bs
	}

	err = json.Unmarshal(b, &stuff)

	if err == nil {
		for k, v := range stuff {
			switch k {
			case "format":
				s.Format = v.(string)
			}
		}
	}

	return err
}

func (s *defaultSimpleSchema) Clone() JSONSchema {
	s2 := &defaultSimpleSchema{}
	*s2 = *s

	s2.basicSchema = s.basicSchema.Clone().(*basicSchema)
	return s2
}

func (s *defaultSimpleSchema) SetFormat(format string) {
	s.Format = format
}

func (s *defaultSimpleSchema) GetFormat() string {
	return s.Format
}
