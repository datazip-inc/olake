package testutils

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	// containerTestDataDir is where the driver's testdata is mounted in the container; every
	// olake input and output lives under it, since the CLI writes next to --config
	containerTestDataDir = "/testdata"

	// driverImageEnvVar pins the image to run instead of olake/source-<driver>:local, and means
	// the caller already built exactly what it wants tested (see getOrBuildDriverImage)
	driverImageEnvVar = "OLAKE_DRIVER_IMAGE"

	// containerRowIndexDir is where the row index database is mounted in the container
	containerRowIndexDir = "/testdata/olake-row-index"
)

// driverImageRef returns the image the harness runs, `olake/source-<driver>:local` as
// built by `make docker.<driver>.build`; OLAKE_DRIVER_IMAGE overrides it.
func driverImageRef(driver string) string {
	if ref := os.Getenv(driverImageEnvVar); ref != "" {
		return ref
	}
	return fmt.Sprintf("olake/source-%s:local", driver)
}

var (
	ensureImageOnce sync.Once
	ensureImageErr  error
	containerSeq    atomic.Int64
)

// getOrBuildDriverImage returns the driver image, rebuilding it via `make docker.<driver>.build`
// so a local run tests current code. OLAKE_DRIVER_IMAGE suppresses the build; sync.Once bounds it.
func getOrBuildDriverImage(t *testing.T, cfg *TestConfig) string {
	t.Helper()
	ref := driverImageRef(cfg.Driver)
	if os.Getenv(driverImageEnvVar) != "" {
		return ref
	}
	ensureImageOnce.Do(func() {
		t.Logf("building driver image %s with `make docker.%s.build` to pick up the latest local changes", ref, cfg.Driver)
		defer trackPhaseTiming(t, "driver-image", ref)()
		cmd := exec.Command("make", fmt.Sprintf("docker.%s.build", cfg.Driver))
		cmd.Dir = cfg.HostRootPath
		if out, err := cmd.CombinedOutput(); err != nil {
			ensureImageErr = fmt.Errorf("failed to build driver image %s (the iceberg jar must be built first, see destination/iceberg/olake-iceberg-java-writer): %w\n%s", ref, err, out)
		}
	})
	require.NoError(t, ensureImageErr, "driver image unavailable")
	return ref
}

// dockerRunArgs builds the `docker run` args that invoke the image's ENTRYPOINT with olakeArgs,
// testdata bind-mounted so host and container share the config/catalog/state files.
func dockerRunArgs(cfg *TestConfig, extraFlags []string, olakeArgs []string) []string {
	args := []string{
		"run", "--rm",
		"-v", fmt.Sprintf("%s:%s", cfg.HostTestDataPath, containerTestDataDir),
		"--tmpfs", fmt.Sprintf("%s/logs", containerTestDataDir),
		"-e", "TELEMETRY_DISABLED=true",
		"-e", "OLAKE_TIMING=1",
		"-e", fmt.Sprintf("OLAKE_INDEX_DB_DIR=%s", containerRowIndexDir),
	}

	if cfg.ImagePlatform != "" {
		args = append(args, "--platform", cfg.ImagePlatform)
	}
	args = append(args, extraFlags...)
	args = append(args, driverImageRef(cfg.Driver))
	return append(args, olakeArgs...)
}

// runOlake runs the driver image once as a user would and returns the container's exit code and
// combined output. err is non-nil only when docker itself fails to launch.
func runOlake(ctx context.Context, t *testing.T, cfg *TestConfig, olakeArgs ...string) (int, []byte, error) {
	t.Helper()
	getOrBuildDriverImage(t, cfg)
	defer trackPhaseTiming(t, cfg.Driver, olakeArgs[0]+" run")()

	name := fmt.Sprintf("olake-it-%s-%d-%d", cfg.Driver, os.Getpid(), containerSeq.Add(1))
	t.Cleanup(func() {
		if exec.Command("docker", "rm", "-f", name).Run() == nil {
			t.Logf("reaped leaked container %s", name)
		}
	})
	args := dockerRunArgs(cfg, []string{"--add-host", "host.docker.internal:host-gateway", "--name", name}, olakeArgs)

	runCtx, cancel := context.WithTimeout(ctx, SyncTimeout)
	defer cancel()
	out, err := exec.CommandContext(runCtx, "docker", args...).CombinedOutput()
	logContainerTimings(t, out)
	if runCtx.Err() == context.DeadlineExceeded {
		err = exec.Command("docker", "rm", "-f", name).Run()
		if err != nil {
			t.Logf("error stopping docker container after timeout: %v", err)
		}
		return -1, out, fmt.Errorf("olake %s run timed out after %s", olakeArgs[0], SyncTimeout)
	}
	return dockerExitResult(out, err, olakeArgs[0])
}

// logContainerTimings re-emits the `[timing]` lines the driver wrote inside the container, which a
// successful `docker run` would otherwise drop, leaving every sync as one opaque span.
func logContainerTimings(t *testing.T, out []byte) {
	t.Helper()
	for _, line := range strings.Split(string(out), "\n") {
		if idx := strings.Index(line, "[timing]"); idx >= 0 {
			t.Logf("  container %s", strings.TrimSpace(line[idx:]))
		}
	}
}

// dockerExitResult normalizes `docker run`'s outcome: a non-zero container exit is carried in
// exitCode, and only a failure to launch docker itself comes back as err.
func dockerExitResult(out []byte, err error, what string) (int, []byte, error) {
	if err == nil {
		return 0, out, nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode(), out, nil
	}
	return -1, out, fmt.Errorf("docker run (%s) failed to execute: %w", what, err)
}

// syncArgs builds the `olake sync ...` argument vector run against the driver image.
func syncArgs(config TestConfig, useState bool, destinationType string, flags ...string) []string {
	args := []string{"sync", "--config", config.SourcePath, "--catalog", config.CatalogPath}
	switch destinationType {
	case "iceberg":
		args = append(args, "--destination", config.IcebergDestinationPath)
	case "parquet":
		args = append(args, "--destination", config.ParquetDestinationPath)
	}
	if useState {
		args = append(args, "--state", config.StatePath)
	}
	return append(args, flags...)
}

// discoverArgs builds the `olake discover ...` argument vector run against the driver image.
func discoverArgs(config TestConfig, flags ...string) []string {
	return append([]string{"discover", "--config", config.SourcePath}, flags...)
}
