package testutils

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

// runInTestContainer starts a disposable base-image container, mounts the project root at
// /test-olake, builds the driver in-container from source (via build.sh) and runs testFn
// inside its PostReadies lifecycle hook, terminating the container when done. Individual
// olake commands are executed with ExecCommand against the running container.
func runInTestContainer(
	ctx context.Context,
	t *testing.T,
	cfg *TestConfig,
	testFn func(ctx context.Context, c testcontainers.Container) error,
) {
	t.Helper()
	baseImage := ensureTestBaseImage(t, cfg.HostRootPath)
	containerReady := trackPhaseTiming(t, cfg.Driver, "container ready")

	req := testcontainers.ContainerRequest{
		Image:         baseImage,
		ImagePlatform: cfg.ImagePlatform,
		HostConfigModifier: func(hc *container.HostConfig) {
			hc.Binds = []string{
				fmt.Sprintf("%s:/test-olake:rw", cfg.HostRootPath),
				fmt.Sprintf("%s:/test-olake/drivers/%s/internal/testdata:rw", cfg.HostTestDataPath, cfg.Driver),
				goModCacheMount,
				goBuildCacheMount,
			}
			hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
		},
		ConfigModifier: func(config *container.Config) {
			config.WorkingDir = "/test-olake"
		},
		Env: map[string]string{
			"TELEMETRY_DISABLED":  "true",
			"OLAKE_SKIP_MOD_TIDY": "1",
		},
		LifecycleHooks: []testcontainers.ContainerLifecycleHooks{
			{
				PostReadies: []testcontainers.ContainerHook{
					func(ctx context.Context, c testcontainers.Container) error {
						containerReady()
						defer trackPhaseTiming(t, cfg.Driver, "in-container work")()
						return testFn(ctx, c)
					},
				},
			},
		},
		Cmd: []string{"tail", "-f", "/dev/null"},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	require.NoError(t, err, "test container run failed")
	defer func() {
		// PID 1 here is `tail -f /dev/null` (req.Cmd), which never exits on SIGTERM — the
		// kernel skips default signal handling for PID 1 — so a graceful stop just burns
		// Terminate's default 10s grace period before the SIGKILL. All in-container work is
		// already done by the time this deferred call runs, so kill immediately.
		if err := container.Terminate(ctx, testcontainers.StopTimeout(0)); err != nil {
			t.Logf("warning: failed to terminate container: %v", err)
		}
	}()
}

// ExecCommand runs cmd inside the container through /bin/sh.
func ExecCommand(
	ctx context.Context,
	c testcontainers.Container,
	cmd string,
) (int, []byte, error) {
	code, reader, err := c.Exec(ctx, []string{"/bin/sh", "-c", cmd})
	if err != nil {
		return code, nil, err
	}
	output, _ := io.ReadAll(reader)
	return code, output, nil
}

func syncCommand(config TestConfig, useState bool, destinationType string, flags ...string) string {
	baseCmd := fmt.Sprintf("/test-olake/build.sh driver-%s sync --config %s --catalog %s", config.Driver, config.SourcePath, config.CatalogPath)

	switch destinationType {
	case "iceberg":
		baseCmd = fmt.Sprintf("%s --destination %s", baseCmd, config.IcebergDestinationPath)
	case "parquet":
		baseCmd = fmt.Sprintf("%s --destination %s", baseCmd, config.ParquetDestinationPath)
	}

	if useState {
		baseCmd = fmt.Sprintf("%s --state %s", baseCmd, config.StatePath)
	}

	if len(flags) > 0 {
		baseCmd = fmt.Sprintf("%s %s", baseCmd, strings.Join(flags, " "))
	}
	return baseCmd
}

// pass flags as `--flag1, flag1 value, --flag2, flag2 value...`
func discoverCommand(config TestConfig, flags ...string) string {
	baseCmd := fmt.Sprintf("/test-olake/build.sh driver-%s discover --config %s", config.Driver, config.SourcePath)
	if len(flags) > 0 {
		baseCmd = fmt.Sprintf("%s %s", baseCmd, strings.Join(flags, " "))
	}
	return baseCmd
}
