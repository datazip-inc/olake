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
	str := strings.Trim(string(b), "\"")
	time, err := parseStringTimestamp(str)
	if err != nil {
		return err
	}

	*ct = Time{time}
	return nil
}
