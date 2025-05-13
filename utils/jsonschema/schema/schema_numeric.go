package schema

import (
	"fmt"
	"strconv"

	"github.com/goccy/go-json"
)

// NumericSchema represents the schema for a JSON number.
type NumericSchema interface {
	SimpleSchema
	GetMultipleOf() float64
	GetMaximum() float64
	GetMinimum() float64
	GetExclusiveMaximum() bool
	GetExclusiveMinimum() bool

	SetMultipleOf(multipleOf float64)
	SetMaximum(maximum float64)
	SetMinimum(minimum float64)
	SetExclusiveMaximum(exclusiveMaximum bool)
	SetExclusiveMinimum(exclusiveMinimum bool)
}

type defaultNumericSchema struct {
	*defaultSimpleSchema
	Maximum          float64 `json:"maximum,omitempty"`
	Minimum          float64 `json:"minimum,omitempty"`
	Enums            []int   `json:"enum,omitempty"`
	MultipleOf       float64 `json:"multipleOf,omitempty"`
	ExclusiveMaximum bool    `json:"exclusiveMaximum,omitempty"`
	ExclusiveMinimum bool    `json:"exclusiveMinimum,omitempty"`
}

// NewNumericSchema creates a new numeric schema.
func NewNumericSchema(jsonType string) NumericSchema {
	return &defaultNumericSchema{
		defaultSimpleSchema: &defaultSimpleSchema{
			basicSchema: NewBasicSchema(jsonType).(*basicSchema),
		},
	}
}

// int enums not supported in arrays
func (s *defaultNumericSchema) SetIntEnum(enums []string) error {
	for _, item := range enums {
		val, err := strconv.Atoi(item)
		if err != nil {
			return fmt.Errorf("failed to parse enum[%s] in integer: %s", item, err)
		}

		s.Enums = append(s.Enums, val)
	}

	return nil
}

func (s *defaultNumericSchema) SetDefault(def string) error {
	reformat, err := strconv.Atoi(def)
	if err != nil {
		return fmt.Errorf("failed to reformate to int: %s", err)
	}

	s.DefaultValue = reformat

	return nil
}

func (s *defaultNumericSchema) UnmarshalJSON(b []byte) error {
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
			case "maximum":
				s.Maximum = v.(float64)
			case "minimum":
				s.Minimum = v.(float64)
			case "multipleOf":
				s.MultipleOf = v.(float64)
			case "exclusiveMaximum":
				s.ExclusiveMaximum = v.(bool)
			case "exclusiveMinimum":
				s.ExclusiveMinimum = v.(bool)
			}
		}
	}

	return err
}

func (s *defaultNumericSchema) Clone() JSONSchema {
	s2 := &defaultNumericSchema{}
	*s2 = *s

	s2.defaultSimpleSchema = s.defaultSimpleSchema.Clone().(*defaultSimpleSchema)
	return s2
}

func (s *defaultNumericSchema) GetMultipleOf() float64 {
	return s.MultipleOf
}

func (s *defaultNumericSchema) GetMaximum() float64 {
	return s.Maximum
}

func (s *defaultNumericSchema) GetMinimum() float64 {
	return s.Minimum
}

func (s *defaultNumericSchema) GetExclusiveMaximum() bool {
	return s.ExclusiveMaximum
}

func (s *defaultNumericSchema) GetExclusiveMinimum() bool {
	return s.ExclusiveMinimum
}

func (s *defaultNumericSchema) SetMultipleOf(multipleOf float64) {
	s.MultipleOf = multipleOf
}

func (s *defaultNumericSchema) SetMaximum(maximum float64) {
	s.Maximum = maximum
}

func (s *defaultNumericSchema) SetMinimum(minimum float64) {
	s.Minimum = minimum
}

func (s *defaultNumericSchema) SetExclusiveMaximum(exclusiveMaximum bool) {
	s.ExclusiveMaximum = exclusiveMaximum
}

func (s *defaultNumericSchema) SetExclusiveMinimum(exclusiveMinimum bool) {
	s.ExclusiveMinimum = exclusiveMinimum
}
