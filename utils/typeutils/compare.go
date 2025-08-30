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

import "github.com/datazip-inc/olake/utils"

func Compare(a, b any) int {
	// Handle nil cases first
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	aTime, aOk := a.(Time)
	bTime, bOk := b.(Time)

	if aOk && bOk {
		return aTime.Compare(bTime)
	}
	return utils.CompareInterfaceValue(a, b)
}
