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
)

const (
	// containerTestDataDir is where the driver's testdata is mounted in the container; every
	// olake input and output lives under it, since the CLI writes next to --config
	containerTestDataDir = "/testdata"

	// driverImageEnvVar pins the image under test, so a caller that has already built or pulled it
	// (CI does) is not made to build it again.
	driverImageEnvVar = "OLAKE_DRIVER_IMAGE"

	// driverVersionEnvVar is used to specify what is the version of the driver to run the test for
	// by default the driver version is the current code, which is built as `local`
	driverVersionEnvVar = "OLAKE_DRIVER_VERSION"

	// currentDriverVersion refers to the version of driver image of current code
	currentDriverVersion = "local"
)

func getDriverImage(driver, version string) string {
	return fmt.Sprintf("olake/source-%s:%s", driver, version)
}

var (
	ensureImageOnce sync.Once
	ensureImageErr  error
	containerSeq    atomic.Int64
)

// buildDriverImage builds the driver image if needed.
func buildDriverImage(cfg *TestConfig) error {
	ensureImageOnce.Do(func() {
		cmd := exec.Command("make", fmt.Sprintf("docker.%s.build", cfg.Driver), fmt.Sprintf("IMAGE_TAG=%s", currentDriverVersion))
		cmd.Dir = cfg.OlakeRootPath

		out, err := cmd.CombinedOutput()
		if err != nil {
			ensureImageErr = fmt.Errorf("`make docker.%s.build` failed in %s: %s\n%s", cfg.Driver, cfg.OlakeRootPath, err, out)
		}
	})
	return ensureImageErr
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
func RunOlake(ctx context.Context, cfg *TestConfig, olakeArgs ...string) (int, []byte, error) {
	name := generateUniqueContainerName(cfg)
	args := DockerRunArgs(cfg, cfg.DriverImage, []string{"--add-host", "host.docker.internal:host-gateway", "--name", name}, olakeArgs)

	runCtx, cancel := context.WithTimeout(ctx, SyncTimeout)
	defer cancel()

	out, err := exec.CommandContext(runCtx, "docker", args...).CombinedOutput()
	if runCtx.Err() == context.DeadlineExceeded {
		if rmErr := exec.Command("docker", "rm", "-f", name).Run(); rmErr != nil {
			return -1, out, fmt.Errorf("olake %s of driver %s timed out after %s, and its container %s could not be removed: %s",
				olakeArgs[0], cfg.Driver, SyncTimeout, name, rmErr)
		}
		return -1, out, fmt.Errorf("olake %s of driver %s timed out after %s; container %s was removed", olakeArgs[0], cfg.Driver, SyncTimeout, name)
	}

	return DockerExitResult(out, err, olakeArgs[0])
}

// ensureImagePresent pulls image unless the local daemon already has it. Used for compatibility
// baselines, which come from a registry rather than from a build; platform is passed through so
// an amd64-only baseline can be pulled on an arm64 host.
//
// A pull failure is returned rather than fataled: a baseline tag older than the driver itself
// legitimately has no image, and the caller turns that into a skip, not a failure.
func EnsureImagePresent(image, platform string) error {
	if err := exec.Command("docker", "image", "inspect", image).Run(); err == nil {
		return nil
	}
	args := []string{"pull"}
	if platform != "" {
		args = append(args, "--platform", platform)
	}
	args = append(args, image)
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("failed to pull %s: %s\n%s", image, err, out)
	}
	return nil
}

// logContainerTimings re-emits the `[timing]` lines the driver wrote inside the container. A
// successful `docker run`'s output is otherwise dropped on the floor, so without this the
// in-container breakdown is invisible and every sync reads as one opaque span. The leading
// log prefix is trimmed so the forwarded lines line up with the harness's own.
func ContainerTimings(out []byte) []string {
	var timings []string
	for _, line := range strings.Split(string(out), "\n") {
		if idx := strings.Index(line, "[timing]"); idx >= 0 {
			timings = append(timings, strings.TrimSpace(line[idx:]))
		}
	}
	return timings
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
func SyncArgs(useState bool, destinationFile string, flags ...string) []string {
	p := ContainerPath
	args := []string{"sync", "--config", p("source.json"), "--catalog", p("streams.json")}

	args = append(args, "--destination", p(destinationFile))

	if useState {
		args = append(args, "--state", p("state.json"))
	}

	return append(args, flags...)
}

// DiscoverArgs builds the `olake discover ...` argument vector run against the driver image.
func DiscoverArgs(flags ...string) []string {
	p := ContainerPath
	return append([]string{"discover", "--config", p("source.json")}, flags...)
}
