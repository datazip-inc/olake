package typeutils

import "github.com/datazip-inc/olake/types"

func Resolve(stream *types.Stream, objects ...map[string]interface{}) error {
	allfields := Fields{}

	for _, object := range objects {
		fields := Fields{}
		// apply default typecast and define column types
		for k, v := range object {
			fields[k] = NewField(TypeFromValue(v))
		}

		for fieldName, field := range allfields {
			if _, found := object[fieldName]; !found {
				field.setNullable()
			}
		}

		allfields.Merge(fields)
	}

	for column, field := range allfields {
		// Use getType() instead of *dataType because Merge() sets dataType to nil
		// when a field has multiple type occurrences across records
		stream.UpsertField(column, field.getType(), field.isNullable())
	}

	return nil
}
