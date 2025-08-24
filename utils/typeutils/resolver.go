/*
 * Copyright 2025 Olake By Datazip
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
