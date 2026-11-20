package testhandler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Host is the address tests reach every service's published port on.
const Host = "127.0.0.1"

const commandTimeout = 2 * time.Minute

// Service is a container the tests share. A make target starts it from a compose file and waits until it
// is ready; tests only connect to it.
type Service struct {
	// Container is the container_name in the compose file.
	Container string
	// Port is the host port the compose file publishes.
	Port int
	// Target is the make target that starts the container.
	Target string
	// Ready reports whether the running container can serve tests.
	Ready func(ctx context.Context, s *Service) error

	mu sync.Mutex
	up bool
}

// Addr is the host address of the service's published port.
func (s *Service) Addr() string {
	return net.JoinHostPort(Host, strconv.Itoa(s.Port))
}

// Exec runs a command inside the container; flags are extra `docker exec` flags.
func (s *Service) Exec(ctx context.Context, flags []string, command ...string) (string, error) {
	args := append([]string{"exec"}, flags...)
	return docker(ctx, append(append(args, s.Container), command...)...)
}

// docker runs one docker CLI command and returns its trimmed stdout. The timeout turns a stalled
// daemon into an error instead of a hung test.
func docker(ctx context.Context, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, commandTimeout)
	defer cancel()
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()+stdout.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

// dockerReady checks once per process that the docker CLI exists and the daemon answers.
var dockerReady = sync.OnceValue(func() error {
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("this test needs Docker and the docker CLI is not on PATH: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := docker(ctx, "version", "--format", "{{.Server.Version}}"); err != nil {
		return fmt.Errorf("this test needs Docker and the daemon is not answering: %w", err)
	}
	return nil
})

// errContainerNotFound means the service's container is not running, so its make target has to start it.
var errContainerNotFound = errors.New("container not found")

// connect checks once per process that the container runs and is ready. A container that is not running gives errContainerNotFound.
func (s *Service) connect(ctx context.Context) error {
	if err := dockerReady(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.up {
		return nil
	}
	running, err := docker(ctx, "ps", "--quiet", "--filter", "name=^"+s.Container+"$")
	if err != nil {
		return err
	}
	if running == "" {
		return errContainerNotFound
	}
	if err := s.Ready(ctx, s); err != nil {
		return fmt.Errorf("container %s is not ready, run make %s: %w", s.Container, s.Target, err)
	}
	s.up = true
	return nil
}
