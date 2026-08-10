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
	// containerTestDataDir is where the driver's testdata directory is mounted inside the
	// driver container; all olake inputs and outputs (streams.json, state.json, stats.json,
	// logs) live under it since the CLI writes next to --config.
	containerTestDataDir = "/testdata"

	// driverImageEnvVar pins the image to run instead of the conventional
	// `olake/source-<driver>:local`. Setting it also means "this image is already what I want" —
	// see getOrBuildDriverImage.
	driverImageEnvVar = "OLAKE_DRIVER_IMAGE"

	// containerMemoryEnvVar overrides the per-container memory cap; "0" or "off" removes it.
	containerMemoryEnvVar = "OLAKE_TEST_CONTAINER_MEMORY"

	// defaultContainerMemory bounds one sync container. Measured peak on the compat fixture is
	// ~1.06GiB, so this clears it while keeping the caps from over-subscribing the machine: the
	// suite runs two containers per driver, and the sweep runs two drivers, so four of these must
	// coexist with spark, the source DBs and minio. A cap alone cannot prevent an OOM -- 1200m was
	// too tight and containers hit their own cgroup during a JVM start, 2048m was loose enough
	// that six of them together exceeded the VM. Concurrency is the other half; see the sweep.
	defaultContainerMemory = "1536m"

	// defaultWriterHeap pins the Iceberg writer's max heap. Without it the JVM derives the heap
	// from -XX:MaxRAMPercentage=75.0 (destination/iceberg/java_client.go), which means raising the
	// container cap silently raises the heap too -- so the cap could never buy headroom. olake
	// starts one JVM per destination check, per backfill chunk and per CDC phase, and the compat
	// fixture is a handful of rows, so a small fixed heap is both sufficient and predictable.
	defaultWriterHeap = "-Xmx512m"

	// writerHeapEnvVar overrides that heap; "off" leaves the JVM's own sizing alone.
	writerHeapEnvVar = "OLAKE_TEST_WRITER_HEAP"
)

// writerHeapOpts is the JAVA_TOOL_OPTIONS value handed to sync containers, which the JVM applies
// ahead of its own ergonomics.
func writerHeapOpts() string {
	switch opts := strings.TrimSpace(os.Getenv(writerHeapEnvVar)); opts {
	case "":
		return defaultWriterHeap
	case "off", "none":
		return ""
	default:
		return opts
	}
}

// containerMemoryLimit is the --memory value for sync containers, overridable per run.
func containerMemoryLimit() string {
	switch limit := strings.TrimSpace(os.Getenv(containerMemoryEnvVar)); limit {
	case "":
		return defaultContainerMemory
	case "0", "off", "none":
		return ""
	default:
		return limit
	}
}

// imageRef returns the image cfg runs, `olake/source-<driver>:local` as built by
// `make docker.<driver>.build`. cfg.DriverImage wins over OLAKE_DRIVER_IMAGE, which wins over
// the default -- the compat suite pins a config to a released baseline image that way, without
// disturbing the env every other suite reads.
func imageRef(cfg *TestConfig) string {
	if cfg.DriverImage != "" {
		return cfg.DriverImage
	}
	if ref := os.Getenv(driverImageEnvVar); ref != "" {
		return ref
	}
	return fmt.Sprintf("olake/source-%s:local", cfg.Driver)
}

var (
	ensureImageOnce sync.Once
	ensureImageErr  error
	containerSeq    atomic.Int64
)

// getOrBuildDriverImage returns the driver image to run, (re)building it via
// `make docker.<driver>.build` unconditionally so a local run always exercises the current
// code -- docker's layer cache makes that near-free when nothing changed.
//
// OLAKE_DRIVER_IMAGE and cfg.DriverImage both suppress the build: pinning an image means the
// caller already produced exactly the artifact it wants tested. CI relies on that -- it builds
// through buildx with a shared layer cache the plain `docker build` here would not reach, so
// rebuilding would both waste the cache and risk testing a different image than the one it
// checked. The compat suite relies on it too: a pinned baseline must never be rebuilt from the
// working tree, or it would stop being a baseline.
//
// Guarded by sync.Once so parallel tests trigger the (slow) build at most once and all share
// its result. That Once is keyed to the CANDIDATE image only -- baseline images are pulled or
// built by compat.go and never tagged olake/source-<driver>:local, since a baseline built under
// the candidate's tag would replace it and the suite would compare an image with itself.
func getOrBuildDriverImage(t *testing.T, cfg *TestConfig) string {
	t.Helper()
	ref := imageRef(cfg)
	if cfg.DriverImage != "" || os.Getenv(driverImageEnvVar) != "" {
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
// there. extraFlags carries per-invocation docker flags (host gateway, network, name); image is
// explicit rather than derived so one suite can hand successive syncs to different images.
func dockerRunArgs(cfg *TestConfig, image string, extraFlags []string, olakeArgs []string) []string {
	args := []string{
		"run", "--rm",
		"-v", fmt.Sprintf("%s:%s", cfg.HostTestDataPath, containerTestDataDir),
		"-e", "TELEMETRY_DISABLED=true",
		"-e", "OLAKE_TIMING=1",
	}

	// Without a limit the container sees the whole Docker VM, and the Iceberg writer's
	// -XX:MaxRAMPercentage=75.0 (destination/iceberg/java_client.go) sizes each JVM's max heap to
	// 75% of it -- measured at 5.82GiB on a 7.75GiB VM. olake starts one JVM per destination
	// check, per backfill chunk and per CDC phase, so concurrent suites OOM-kill each other on a
	// handful of rows. Capping the container makes the JVM container-aware instead.
	if limit := containerMemoryLimit(); limit != "" {
		args = append(args, "--memory", limit, "--memory-swap", limit)
	}
	if heap := writerHeapOpts(); heap != "" {
		args = append(args, "-e", "JAVA_TOOL_OPTIONS="+heap)
	}

	if cfg.ImagePlatform != "" {
		args = append(args, "--platform", cfg.ImagePlatform)
	}
	args = append(args, extraFlags...)
	args = append(args, image)
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
	return runOlakeWithImage(ctx, t, cfg, getOrBuildDriverImage(t, cfg), olakeArgs...)
}

// runOlakeWithImage is runOlake against an explicit image, without the build-if-missing step.
// The compat suite uses it to run one scenario across two binaries -- the baseline image for the
// initial load, the candidate for everything after it -- which is the whole upgrade being tested.
// Callers are responsible for the image existing (see ensureImagePresent).
func runOlakeWithImage(ctx context.Context, t *testing.T, cfg *TestConfig, image string, olakeArgs ...string) (int, []byte, error) {
	t.Helper()
	defer trackPhaseTiming(t, cfg.Driver, olakeArgs[0]+" run")()

	name := fmt.Sprintf("olake-it-%s-%d-%d", cfg.Driver, os.Getpid(), containerSeq.Add(1))
	t.Cleanup(func() {
		if exec.Command("docker", "rm", "-f", name).Run() == nil {
			t.Logf("reaped leaked container %s", name)
		}
	})
	args := dockerRunArgs(cfg, image, []string{"--add-host", "host.docker.internal:host-gateway", "--name", name}, olakeArgs)

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

// ensureImagePresent pulls image unless the local daemon already has it. Used for compat
// baselines, which come from a registry rather than from a build; platform is passed through so
// an amd64-only baseline can be pulled on an arm64 host.
//
// A pull failure is returned rather than fataled: a baseline tag older than the driver itself
// legitimately has no image, and the caller turns that into a skip, not a failure.
func ensureImagePresent(t *testing.T, image, platform string) error {
	t.Helper()
	if err := exec.Command("docker", "image", "inspect", image).Run(); err == nil {
		return nil
	}
	args := []string{"pull"}
	if platform != "" {
		args = append(args, "--platform", platform)
	}
	args = append(args, image)
	t.Logf("pulling compat baseline image %s", image)
	defer trackPhaseTiming(t, "compat-baseline", image)()
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to pull %s: %s\n%s", image, err, out)
	}
	return nil
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
	return append(args, dropUnsupportedFlags(config.InputGeneration, flags)...)
}

// discoverArgs builds the `olake discover ...` argument vector run against the driver image.
func discoverArgs(config TestConfig, flags ...string) []string {
	return append([]string{"discover", "--config", config.SourcePath}, dropUnsupportedFlags(config.InputGeneration, flags)...)
}
