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

import (
	"strings"
	"time"
)

type Time struct {
	time.Time
}

// UnmarshalJSON overrides the default unmarshalling for CustomTime
func (ct *Time) UnmarshalJSON(b []byte) error {
	// Remove the quotes around the date string
	str := strings.Trim(string(b), "\"")
	time, err := parseStringTimestamp(str)
	if err != nil {
		return err
	}

	*ct = Time{time}
	return nil
}

// Before reports whether the time instant ct is before u
func (ct Time) Before(u Time) bool {
	return ct.Time.Before(u.Time)
}

// After reports whether the time instant ct is after u
func (ct Time) After(u Time) bool {
	return ct.Time.After(u.Time)
}

// Equal reports whether ct and u represent the same time instant
func (ct Time) Equal(u Time) bool {
	return ct.Time.Equal(u.Time)
}

// Compare compares the time instant ct with u. If ct is before u, it returns -1;
// if ct is after u, it returns +1; if they're the same, it returns 0.
func (ct Time) Compare(u Time) int {
	if ct.Before(u) {
		return -1
	}
	if ct.After(u) {
		return 1
	}
	return 0
}
