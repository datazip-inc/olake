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
		stream.UpsertField(column, *field.dataType, field.isNullable())
	}

	return nil
}

// infers the schema of JSON messages
func ResolveJSONMessages(stream *types.Stream, messages []map[string]interface{}) error {
	jsonMessageStats := make(map[string]*JSONTypeTrack)
	for _, msg := range messages {
		for key, val := range msg {
			if _, ok := jsonMessageStats[key]; !ok {
				jsonMessageStats[key] = &JSONTypeTrack{}
			}
			DetectJSONType(val, jsonMessageStats[key])
		}
	}

	finalSchema := make(map[string]types.DataType)
	for key, stats := range jsonMessageStats {
		finalSchema[key] = InferJSONType(*stats)
	}

	for key, dataType := range finalSchema {
		stream.UpsertField(key, dataType, true) // nullable true by default
	}

	return nil
}
