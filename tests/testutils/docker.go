package testutils

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	// containerTestDataDir is where the driver's testdata directory is mounted inside the
	// driver container; all olake inputs and outputs (streams.json, state.json, stats.json,
	// logs) live under it since the CLI writes next to --config.
	containerTestDataDir = "/testdata"

	// skipDestinationCheckEnvVar mirrors destination.SkipDestinationCheckEnvVar. Declared here
	// rather than imported so this module keeps no dependency on the root one.
	skipDestinationCheckEnvVar = "OLAKE_SKIP_DESTINATION_CHECK"

	// driverImageEnvVar pins the image to run instead of the conventional
	// `olake/source-<driver>:local`. Setting it also means "this image is already what I want" —
	// see getOrBuildDriverImage.
	driverImageEnvVar = "OLAKE_DRIVER_IMAGE"
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
)

// getOrBuildDriverImage returns the driver image to run, (re)building it via
// `make docker.<driver>.build` unconditionally so a local run always exercises the current
// code -- docker's layer cache makes that near-free when nothing changed.
//
// OLAKE_DRIVER_IMAGE suppresses the build: pinning an image means the caller already produced
// exactly the artifact it wants tested. CI relies on that -- it builds through buildx with a
// shared layer cache the plain `docker build` here would not reach, so rebuilding would both
// waste the cache and risk testing a different image than the one it checked.
//
// Guarded by sync.Once so parallel tests trigger the (slow) build at most once and all share
// its result.
func getOrBuildDriverImage(t *testing.T, cfg *TestConfig) string {
	t.Helper()
	ref := driverImageRef(cfg.Driver)
	if os.Getenv(driverImageEnvVar) != "" {
		return ref
	}
	ensureImageOnce.Do(func() {
		t.Logf("building driver image %s with `make docker.%s.build` to pick up the latest local changes", ref, cfg.Driver)
		// wall-clock via trackPhaseTiming, not cmd.ProcessState.SystemTime() (that reports make's
		// kernel CPU time — a misleading ~87ms even when the docker build actually took far longer).
		defer trackPhaseTiming(t, "driver-image", ref)()
		// Called plain: the image's platform comes from drivers/platforms.conf via
		// local_driver_platforms, so cross-arch pins (db2 is amd64-only) stay in make.
		cmd := exec.Command("make", fmt.Sprintf("docker.%s.build", cfg.Driver))
		cmd.Dir = cfg.HostRootPath
		if out, err := cmd.CombinedOutput(); err != nil {
			ensureImageErr = fmt.Errorf("failed to build driver image %s (the iceberg jar must be built first, see destination/iceberg/olake-iceberg-java-writer): %w\n%s", ref, err, out)
		}
	})
	require.NoError(t, ensureImageErr, "driver image unavailable")
	return ref
}

// dockerRunArgs builds the `docker run` argument list that invokes the driver image exactly
// as a user would: the image's ENTRYPOINT (./olake) runs with olakeArgs appended. The
// driver's testdata directory is mounted at /testdata so the config/catalog/state files are
// shared with the host and the CLI writes its outputs (streams.json, state.json, ...) back
// there. extraFlags carries per-invocation docker flags (host gateway, network, name).
func dockerRunArgs(cfg *TestConfig, extraFlags []string, olakeArgs []string) []string {
	args := []string{
		"run", "--rm",
		"-v", fmt.Sprintf("%s:%s", cfg.HostTestDataPath, containerTestDataDir),
		"-e", "TELEMETRY_DISABLED=true",
		"-e", "OLAKE_TIMING=1",
	}

	if v := os.Getenv(skipDestinationCheckEnvVar); v != "" {
		args = append(args, "-e", skipDestinationCheckEnvVar+"="+v)
	}

	if cfg.ImagePlatform != "" {
		args = append(args, "--platform", cfg.ImagePlatform)
	}
	args = append(args, extraFlags...)
	args = append(args, driverImageRef(cfg.Driver))
	return append(args, olakeArgs...)
}

// runOlake runs the driver image once, exactly like a real user would:
//
//	docker run --rm -v <testdata>:/testdata olake/source-<driver>:local <olakeArgs...>
//
// It exercises the image's real ENTRYPOINT (no exec-into-a-parked-container) and returns the
// container's exit code and combined stdout+stderr. err is non-nil only when docker itself
// fails to launch — a non-zero olake exit is reported via the code, mirroring a user's
// experience at the CLI.
func runOlake(ctx context.Context, t *testing.T, cfg *TestConfig, olakeArgs ...string) (int, []byte, error) {
	t.Helper()
	getOrBuildDriverImage(t, cfg)
	defer trackPhaseTiming(t, cfg.Driver, olakeArgs[0]+" run")()

	args := dockerRunArgs(cfg, []string{"--add-host", "host.docker.internal:host-gateway"}, olakeArgs)
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	logContainerTimings(t, out)
	return dockerExitResult(out, err, olakeArgs[0])
}

// logContainerTimings re-emits the `[timing]` lines the driver wrote inside the container. A
// successful `docker run`'s output is otherwise dropped on the floor, so without this the
// in-container breakdown is invisible and every sync reads as one opaque span. The leading
// log prefix is trimmed so the forwarded lines line up with the harness's own.
func logContainerTimings(t *testing.T, out []byte) {
	t.Helper()
	for _, line := range strings.Split(string(out), "\n") {
		if idx := strings.Index(line, "[timing]"); idx >= 0 {
			t.Logf("  container %s", strings.TrimSpace(line[idx:]))
		}
	}
}

// dockerExitResult normalizes `docker run`'s outcome into (exitCode, output, err): a non-zero
// container exit is a normal result carried in exitCode; only a failure to launch docker
// itself is returned as err.
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
