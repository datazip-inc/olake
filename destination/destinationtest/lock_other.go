//go:build !unix

package destinationtest

import (
	"context"
	"errors"
)

// lockFile has no implementation off unix, where tests cannot take turns on the Iceberg writer.
func lockFile(context.Context, string) (func(), error) {
	return nil, errors.New("destinationtest needs a unix host: it coordinates test processes with flock")
}
