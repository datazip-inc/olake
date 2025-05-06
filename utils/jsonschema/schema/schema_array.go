package schema

import (
	"github.com/goccy/go-json"
)

// ArraySchema represents the schema for a JSON array.
type ArraySchema interface {
	JSONSchema
	GetItems() JSONSchema
	GetMaxItems() int64
	GetMinItems() int64
	GetAdditionalItems() bool
	GetUniqueItems() bool

	SetItems(items JSONSchema)
	SetMaxItems(maxItems int64)
	SetMinItems(minItems int64)
	SetAdditionalItems(additionalItems bool)
	SetUniqueItems(uniqueItems bool)
}

type defaultArraySchema struct {
	*basicSchema
	Items           JSONSchema `json:"items,omitempty"`
	MaxItems        int64      `json:"maxItems,omitempty"`
	MinItems        int64      `json:"minItems,omitempty"`
	AdditionalItems bool       `json:"additionalItems,omitempty"`
	UniqueItems     bool       `json:"uniqueItems,omitempty"`
}

// NewArraySchema creates a new array schema
func NewArraySchema() ArraySchema {
	return &defaultArraySchema{
		basicSchema: NewBasicSchema(SchemaTypeArray).(*basicSchema),
	}
}

func (s *defaultArraySchema) UnmarshalJSON(b []byte) error {
	var err error
	var stuff map[string]interface{}

	bs := &basicSchema{}
	err = json.Unmarshal(b, bs)

	if err == nil {
		s.basicSchema = bs
		err = json.Unmarshal(b, &stuff)
	}

	if err == nil {
		for k, v := range stuff {
			switch k {
			case "items":
				mb, xerr := json.Marshal(v)
				if xerr != nil {
					return xerr
				}
				ms, xerr := FromJSON(mb)
				if xerr != nil {
					return xerr
				}
				s.Items = ms
			case "maxItems":
				s.MaxItems = int64(v.(float64))
			case "minItems":
				s.MinItems = int64(v.(float64))
			case "additionalItems":
				s.AdditionalItems = v.(bool)
			case "uniqueItems":
				s.UniqueItems = v.(bool)
			}
		}
	}

	return err
}

func (s *defaultArraySchema) Clone() JSONSchema {
	s2 := &defaultArraySchema{}
	*s2 = *s

	s2.basicSchema = s.basicSchema.Clone().(*basicSchema)
	return s2
}

func (s *defaultArraySchema) GetItems() JSONSchema {
	return s.Items
}

func (s *defaultArraySchema) GetMaxItems() int64 {
	return s.MaxItems
}

func (s *defaultArraySchema) GetMinItems() int64 {
	return s.MinItems
}

func (s *defaultArraySchema) GetAdditionalItems() bool {
	return s.AdditionalItems
}

func (s *defaultArraySchema) GetUniqueItems() bool {
	return s.UniqueItems
}

func (s *defaultArraySchema) SetItems(items JSONSchema) {
	s.Items = items
}

func (s *defaultArraySchema) SetMaxItems(maxItems int64) {
	s.MaxItems = maxItems
}

func (s *defaultArraySchema) SetMinItems(minItems int64) {
	s.MinItems = minItems
}

func (s *defaultArraySchema) SetAdditionalItems(additionalItems bool) {
	s.AdditionalItems = additionalItems
}

func (s *defaultArraySchema) SetUniqueItems(uniqueItems bool) {
	s.UniqueItems = uniqueItems
}
