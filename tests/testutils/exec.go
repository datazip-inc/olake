package testutils

import (
	"context"
	"io"

	"github.com/testcontainers/testcontainers-go"
)

// ExecCommand runs cmd inside the container through /bin/sh.
func ExecCommand(ctx context.Context, c testcontainers.Container, cmd string) (int, []byte, error) {
	code, reader, err := c.Exec(ctx, []string{"/bin/sh", "-c", cmd})
	if err != nil {
		return code, nil, err
	}
	output, _ := io.ReadAll(reader)
	return code, output, nil
}
