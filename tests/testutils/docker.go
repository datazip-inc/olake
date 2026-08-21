package testutils

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/stretchr/testify/require"
)

const (
	// containerTestDataDir is where the driver's testdata is mounted in the container; every
	// olake input and output lives under it, since the CLI writes next to --config
	containerTestDataDir = "/testdata"

	// driverVersionEnvVar is used to specify what is the version of the driver to run the test for
	// by default the driver version is the current code, which is built as `local`
	driverVersionEnvVar = "OLAKE_DRIVER_VERSION"

	// currentDriverVersion refers to the version of driver image of current code
	currentDriverVersion = "local"

	// containerMemoryEnvVar overrides the per-container memory cap; "0" or "off" removes it.
	containerMemoryEnvVar = "OLAKE_TEST_CONTAINER_MEMORY"

	// defaultContainerMemory bounds one sync container. Measured peak on the compatibility fixture is
	// ~1.06GiB, so this clears it while keeping the caps from over-subscribing the machine: the
	// suite runs two containers per driver, and the sweep runs two drivers, so four of these must
	// coexist with spark, the source DBs and minio. A cap alone cannot prevent an OOM -- 1200m was
	// too tight and containers hit their own cgroup during a JVM start, 2048m was loose enough
	// that six of them together exceeded the VM. Concurrency is the other half; see the sweep.
	defaultContainerMemory = "1536m"

	// kafka is sized differently: it starts one Iceberg writer JVM PER PARTITION reader (the
	// fixture topic has 5 partitions) plus the destination-check writer -- six JVMs where every
	// other driver runs one. Six 512m heaps cannot fit a 1536m container, so a kafka sync was
	// SIGKILLed by its own cgroup no matter the host. Wider cap, smaller per-writer share.
	kafkaContainerMemory = "3g"
	kafkaWriterHeap      = "-Xmx256m"

	// db2 gets the kafka treatment for a different reason: baselines predating the
	// single-JVM-per-process change (#962) start one writer JVM per backfill chunk, and the db2
	// image is amd64-only, so on ARM hosts every JVM also carries emulation overhead that -Xmx
	// cannot bound -- two concurrent chunk writers blew the 1536m cap (SIGKILL on the compatibility
	// full loads) even at 256m heaps. Wider cap, smaller per-writer share.
	db2ContainerMemory = "3g"
	db2WriterHeap      = "-Xmx256m"

	// defaultWriterHeap pins the Iceberg writer's max heap. Without it the JVM derives the heap
	// from -XX:MaxRAMPercentage=75.0 (destination/iceberg/java_client.go), which means raising the
	// container cap silently raises the heap too -- so the cap could never buy headroom. olake
	// starts one JVM per destination check, per backfill chunk and per CDC phase, and the compatibility
	// fixture is a handful of rows, so a small fixed heap is both sufficient and predictable.
	defaultWriterHeap = "-Xmx512m"

	// writerHeapEnvVar overrides that heap; "off" leaves the JVM's own sizing alone.
	writerHeapEnvVar = "OLAKE_TEST_WRITER_HEAP"
)

// writerHeapOpts is the JAVA_TOOL_OPTIONS value handed to sync containers, which the JVM applies
// ahead of its own ergonomics. The env var overrides every driver alike; the defaults are
// per-driver (see kafkaWriterHeap).
func writerHeapOpts(driver string) string {
	switch opts := strings.TrimSpace(os.Getenv(writerHeapEnvVar)); opts {
	case "":
		if driver == string(constants.Kafka) {
			return kafkaWriterHeap
		}
		if driver == string(constants.DB2) {
			return db2WriterHeap
		}
		return defaultWriterHeap
	case "off", "none":
		return ""
	default:
		return opts
	}
}

// containerMemoryLimit is the --memory value for sync containers, overridable per run. The env
// var overrides every driver alike; the defaults are per-driver (see kafkaContainerMemory).
func containerMemoryLimit(driver string) string {
	switch limit := strings.TrimSpace(os.Getenv(containerMemoryEnvVar)); limit {
	case "":
		if driver == string(constants.Kafka) {
			return kafkaContainerMemory
		}
		if driver == string(constants.DB2) {
			return db2ContainerMemory
		}
		return defaultContainerMemory
	case "0", "off", "none":
		return ""
	default:
		return limit
	}
}

func getDriverImage(driver, version string) string {
	return fmt.Sprintf("olake/source-%s:%s", driver, version)
}

var (
	ensureImageOnce sync.Once
	containerSeq    atomic.Int64
)

// buildDriverImage builds the driver image if needed.
func buildDriverImage(t *testing.T, cfg *TestConfig) {
	t.Helper()
	ensureImageOnce.Do(func() {
		defer TrackPhaseTiming(t, "build driver image", cfg.Driver)()

		t.Logf("building driver image with `make docker.%s.build` to pick up the latest local changes", cfg.Driver)

		cmd := exec.Command("make", fmt.Sprintf("docker.%s.build", cfg.Driver), fmt.Sprintf("IMAGE_TAG=%s", currentDriverVersion))
		cmd.Dir = cfg.OlakeRootPath

		out, err := cmd.CombinedOutput()
		require.NoErrorf(t, err, "failed to build driver image: %w\n%s", err, out)
	})
}

// DockerRunArgs builds the `docker run` argument list that invokes the driver image exactly
// as a user would: the image's ENTRYPOINT (./olake) runs with olakeArgs appended. The
// driver's testdata directory is mounted at /testdata so the config/catalog/state files are
// shared with the host and the CLI writes its outputs (streams.json, state.json, ...) back
// there. extraFlags carries per-invocation docker flags (host gateway, network, name); image is
// explicit rather than derived so one suite can hand successive syncs to different images.
func DockerRunArgs(cfg *TestConfig, image string, extraFlags []string, olakeArgs []string) []string {
	args := []string{
		"run", "--rm",
		"-v", fmt.Sprintf("%s:%s", cfg.TestWorkingDir, containerTestDataDir),
		"--tmpfs", fmt.Sprintf("%s/logs", containerTestDataDir),
		"-e", "TELEMETRY_DISABLED=true",
		"-e", "OLAKE_TIMING=1",
	}

	// Without a limit the container sees the whole Docker VM, and the Iceberg writer's
	// -XX:MaxRAMPercentage=75.0 (destination/iceberg/java_client.go) sizes each JVM's max heap to
	// 75% of it -- measured at 5.82GiB on a 7.75GiB VM. olake starts one JVM per destination
	// check, per backfill chunk and per CDC phase, so concurrent suites OOM-kill each other on a
	// handful of rows. Capping the container makes the JVM container-aware instead.
	if limit := containerMemoryLimit(cfg.Driver); limit != "" {
		args = append(args, "--memory", limit, "--memory-swap", limit)
	}
	if heap := writerHeapOpts(cfg.Driver); heap != "" {
		args = append(args, "-e", "JAVA_TOOL_OPTIONS="+heap)
	}

	if cfg.ImagePlatform != "" {
		args = append(args, "--platform", cfg.ImagePlatform)
	}
	args = append(args, extraFlags...)
	args = append(args, image)
	return append(args, olakeArgs...)
}

func generateUniqueContainerName(cfg *TestConfig) string {
	return fmt.Sprintf("olake-it-%s-%s-%d-%d", cfg.Driver, cfg.Suite, os.Getpid(), containerSeq.Add(1))
}

// RunOlake runs the driver image once, exactly like a real user would:
//
//	docker run --rm -v <testdata>:/testdata olake/source-<driver>:local <olakeArgs...>
func RunOlake(ctx context.Context, t *testing.T, cfg *TestConfig, olakeArgs ...string) (int, []byte, error) {
	t.Helper()
	defer TrackPhaseTiming(t, cfg.Driver, olakeArgs[0]+" run")()

	name := generateUniqueContainerName(cfg)
	args := DockerRunArgs(cfg, cfg.DriverImage, []string{"--add-host", "host.docker.internal:host-gateway", "--name", name}, olakeArgs)

	runCtx, cancel := context.WithTimeout(ctx, SyncTimeout)
	defer cancel()

	out, err := exec.CommandContext(runCtx, "docker", args...).CombinedOutput()
	logContainerTimings(t, out)
	if runCtx.Err() == context.DeadlineExceeded {
		err := exec.Command("docker", "rm", "-f", name).Run()
		if err != nil {
			t.Logf("error stopping docker container after timeout: %v", err)
		}

		t.Logf("reaped leaked container %s", name)
		return -1, out, fmt.Errorf("olake %s run timed out after %s", olakeArgs[0], SyncTimeout)
	}

	return DockerExitResult(out, err, olakeArgs[0])
}

// ensureImagePresent pulls image unless the local daemon already has it. Used for compatibility
// baselines, which come from a registry rather than from a build; platform is passed through so
// an amd64-only baseline can be pulled on an arm64 host.
//
// A pull failure is returned rather than fataled: a baseline tag older than the driver itself
// legitimately has no image, and the caller turns that into a skip, not a failure.
func EnsureImagePresent(t *testing.T, image, platform string) error {
	t.Helper()
	if err := exec.Command("docker", "image", "inspect", image).Run(); err == nil {
		return nil
	}
	args := []string{"pull"}
	if platform != "" {
		args = append(args, "--platform", platform)
	}
	args = append(args, image)
	t.Logf("pulling compatibility baseline image %s", image)
	defer TrackPhaseTiming(t, "compatibility-baseline", image)()
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

// DockerExitResult normalizes `docker run`'s outcome into (exitCode, output, err): a non-zero
// container exit is a normal result carried in exitCode; only a failure to launch docker
// itself is returned as err.
func DockerExitResult(out []byte, err error, what string) (int, []byte, error) {
	if err == nil {
		return 0, out, nil
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		return exitErr.ExitCode(), out, nil
	}
	return -1, out, fmt.Errorf("docker run (%s) failed to execute: %w", what, err)
}

func ContainerPath(fileName string) string {
	return filepath.Join(containerTestDataDir, fileName)
}

// SyncArgs builds the `olake sync ...` argument vector run against the driver image.
func SyncArgs(config TestConfig, useState bool, destinationType string, flags ...string) []string {
	p := ContainerPath
	args := []string{"sync", "--config", p("source.json"), "--catalog", p("test_streams.json")}
	switch destinationType {
	case "iceberg":
		// Which iceberg config -- base or the derived arrow one -- is the suite's choice; see
		// TestConfig.IcebergDestinationFile and testIcebergWriter.
		args = append(args, "--destination", p(config.IcebergDestinationFile))
	case "parquet":
		args = append(args, "--destination", p("parquet_destination.json"))
	}
	if useState {
		args = append(args, "--state", p("state.json"))
	}
	if config.FilterFlags != nil {
		flags = config.FilterFlags(flags)
	}
	return append(args, flags...)
}

// DiscoverArgs builds the `olake discover ...` argument vector run against the driver image.
func DiscoverArgs(flags ...string) []string {
	p := ContainerPath
	return append([]string{"discover", "--config", p("source.json")}, flags...)
}
