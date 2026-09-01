package testutils

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	// CurrentDriverVersion refers to the version of driver image of current code
	CurrentDriverVersion = "local"

	// containerTestDataDir is where the driver's testdata is mounted in the container; every
	// olake input and output lives under it, since the CLI writes next to --config
	containerTestDataDir = "/testdata"

	// containerTableIndexDir is where the table index database is mounted in the container
	containerTableIndexDir = containerTestDataDir + "/olake-table-index"

	// driverVersionEnvVar is used to specify what is the version of the driver to run the test for
	// by default the driver version is the current code, which is built as `local`
	driverVersionEnvVar = "OLAKE_DRIVER_VERSION"

	// preBuiltImageEnvVar means the caller already built the local image and the harness must not
	// rebuild it. CI sets it after its own build step; a local run leaves it unset, so the build
	// still runs and picks up current code.
	preBuiltImageEnvVar = "OLAKE_PRE_BUILT_IMAGE"
)

var (
	// imageResolutions runs each image's resolution once per process, keyed by image ref, so the
	// configs of concurrent subtests that name the same version share one build or pull.
	imageResolutions sync.Map
	containerSeq     atomic.Int64
)

type imageResolution struct {
	once sync.Once
	err  error
}

// resolveImageOnce hands every caller naming image the result of the one resolve that ran.
func resolveImageOnce(image string, resolve func() error) error {
	entry, _ := imageResolutions.LoadOrStore(image, &imageResolution{})
	resolution := entry.(*imageResolution)
	resolution.once.Do(func() { resolution.err = resolve() })
	return resolution.err
}

// buildDriverImage builds the driver image if needed.
func buildDriverImage(t *testing.T, cfg *TestConfig) error {
	t.Helper()
	image := cfg.GetDriverImage()
	if os.Getenv(preBuiltImageEnvVar) != "" {
		t.Logf("skipping the build of %s: %s is set, so the caller already built it", image, preBuiltImageEnvVar)
		return nil
	}
	return resolveImageOnce(image, func() error {
		t.Logf("building driver image %s with `make docker.%s.build` to pick up the latest local changes", image, cfg.Driver)
		defer TrackPhaseTiming(t, "driver-image", "build "+image)()
		cmd := exec.Command("make", fmt.Sprintf("docker.%s.build", cfg.Driver), fmt.Sprintf("IMAGE_TAG=%s", CurrentDriverVersion))
		cmd.Dir = cfg.OlakeRootPath

		if out, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("`make docker.%s.build` failed in %s: %s\n%s", cfg.Driver, cfg.OlakeRootPath, err, out)
		}
		return nil
	})
}

// buildBaselineFromCommit builds a driver image from a detached worktree at sha.
func buildImageFromCommit(t *testing.T, cfg *TestConfig, commitID string) error {
	t.Helper()
	imageTag := cfg.GetDriverImage()
	return resolveImageOnce(imageTag, func() error {
		if exec.Command("docker", "image", "inspect", imageTag).Run() == nil {
			t.Logf("driver image %s is already present; not rebuilding it from %s", imageTag, commitID)
			return nil
		}
		t.Logf("building driver image %s from a worktree at %s", imageTag, commitID)
		defer TrackPhaseTiming(t, "driver-image", "build "+imageTag)()

		worktree := filepath.Join(cfg.TestWorkingDir, "olake-compatibility-"+commitID)
		// Each step is minutes long -- maven, then a full image build off an old tree -- so time
		// them separately; without it the whole thing is one silent span.
		run := func(what string, name string, args ...string) error {
			t.Logf("  %s (%s)", what, commitID)
			defer TrackPhaseTiming(t, "driver-image", what)()
			cmd := exec.Command(name, args...)
			out, err := cmd.CombinedOutput()
			if err != nil {
				return fmt.Errorf("error during %s: %v\noutput: %s", what, err, out)
			}

			return nil
		}

		err := run("create the worktree", "git", "-C", cfg.OlakeRootPath, "worktree", "add", "--detach", worktree, commitID)
		if err != nil {
			return err
		}
		// The working dir it lives in is removed when the test ends, which would leave the
		// registration behind for every later `git worktree` call in the repo.
		defer func() {
			_ = run("remove the worktree", "git", "-C", cfg.OlakeRootPath, "worktree", "remove", "--force", worktree)
		}()
		if err := run("build the iceberg writer jar", "make", "-C", worktree, "iceberg.jar"); err != nil {
			return err
		}
		return run("build the image", "docker", "build", "--build-arg", "DRIVER_NAME="+cfg.Driver, "-t", imageTag, worktree)
	})
}

func ensureImagePresent(t *testing.T, image string) error {
	t.Helper()
	return resolveImageOnce(image, func() error {
		defer TrackPhaseTiming(t, "driver-image", "ensure "+image)()
		if err := exec.Command("docker", "image", "inspect", image).Run(); err == nil {
			return nil
		}
		t.Logf("pulling driver image %s", image)
		defer TrackPhaseTiming(t, "driver-image", "pull "+image)()
		args := []string{"pull", image}
		if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
			return fmt.Errorf("failed to pull %s: %s\n%s", image, err, out)
		}
		return nil
	})
}

// DockerRunArgs builds the `docker run` argument list that invokes the driver image exactly
// as a user would: the image's ENTRYPOINT (./olake) runs with olakeArgs appended.
func DockerRunArgs(cfg *TestConfig, extraFlags []string, olakeArgs []string) []string {
	args := []string{
		"run", "--rm",
		"-v", fmt.Sprintf("%s:%s", cfg.TestWorkingDir, containerTestDataDir),
		"--tmpfs", fmt.Sprintf("%s/logs", containerTestDataDir),
		"-e", "TELEMETRY_DISABLED=true",
		"-e", "OLAKE_TIMING=1",
		"-e", fmt.Sprintf("OLAKE_INDEX_DB_DIR=%s", containerTableIndexDir),
	}

	if u, err := user.Current(); err == nil {
		args = append(args, "--user", u.Uid+":"+u.Gid)
	}

	if cfg.ImagePlatform != "" {
		args = append(args, "--platform", cfg.ImagePlatform)
	}
	args = append(args, extraFlags...)
	args = append(args, cfg.GetDriverImage())
	return append(args, olakeArgs...)
}

func generateUniqueContainerName(cfg *TestConfig) string {
	return fmt.Sprintf("olake-it-%s-%s-%d-%d", cfg.Driver, cfg.Suite, os.Getpid(), containerSeq.Add(1))
}

func RunOlake(ctx context.Context, cfg *TestConfig, olakeArgs ...string) (int, []byte, error) {
	name := generateUniqueContainerName(cfg)
	args := DockerRunArgs(cfg, []string{"--add-host", "host.docker.internal:host-gateway", "--name", name}, olakeArgs)

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

// logContainerTimings re-emits the `[timing]` lines the driver wrote inside the container.
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
