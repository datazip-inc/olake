package schema

// SpecVersion is the type for specifying which spec version you adhere to.
type SpecVersion string

const (
	// SpecVersionCurrent is the lastest spec
	SpecVersionCurrent SpecVersion = "http://json-schema.org/schema#"
	// SpecVersionCurrentHyper is the latest hyper spec
	SpecVersionCurrentHyper = "http://json-schema.org/hyper-schema#"
	// SpecVersionDraftV4 is the draft-04 spec
	SpecVersionDraftV4 = "http://json-schema.org/draft-04/schema#"
	// SpecVersionDraftV4 is the final 2020-12 spec
	SpecVersion202012 = "https://json-schema.org/draft/2020-12/schema"
	// SpecVersionDraftV4Hyper is the draft-04 hyper spec
	SpecVersionDraftV4Hyper = "http://json-schema.org/draft-04/hyper-schema#"
)

const (
	// SchemaTypeObject is the object type
	SchemaTypeObject string = "object"
	// SchemaTypeInteger is the integer type
	SchemaTypeInteger = "integer"
	// SchemaTypeNumber is the number type
	SchemaTypeNumber = "number"
	// SchemaTypeBoolean is the boolean type
	SchemaTypeBoolean = "boolean"
	// SchemaTypeString is the string type
	SchemaTypeString = "string"
	// SchemaTypeArray is the array type
	SchemaTypeArray = "array"
)
