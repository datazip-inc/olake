package schema

// NewMapSchema creates a new object schema with additionalProperties set to true.
func NewMapSchema(suppressXAttrs bool) ObjectSchema {
	s := NewObjectSchema(suppressXAttrs)
	s.SetAdditionalProperties(NewBoolOrSchema(true))

	return s
}
