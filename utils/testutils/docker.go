package testutils

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/mod/modfile"
)

const (
	goModCacheMount   = "go-mod:/go/pkg/mod"
	goBuildCacheMount = "go-build:/root/.cache/go-build"
)

var (
	buildBaseImageOnce sync.Once
	buildBaseImageErr  error
)

// baseImageRef returns the integration-test base image ref (olakego/base:build-go<version>).
// The version is read from the go directive in go.mod — the same line `make docker.base.build`
// derives the tag (and the baked-in toolchain) from — so the image that gets built and the image
// the harness looks up can never drift, with no separate tag file to bump.
func baseImageRef(t *testing.T, rootPath string) string {
	t.Helper()
	workFile := filepath.Join(rootPath, "go.mod")
	data, err := os.ReadFile(workFile)
	require.NoError(t, err, "read go.mod to derive the base image tag")
	f, err := modfile.Parse(workFile, data, nil)
	require.NoError(t, err, "parse go.mod to derive the base image tag")
	require.NotNil(t, f.Go, "no go directive found in %s", workFile)
	return "olakego/base:build-go" + f.Go.Version
}

// ensureTestBaseImage guarantees the prebaked integration-test base image exists in the local
// docker daemon, building it via `make docker.base.build` if it is missing, and returns its ref.
// The image is local-only and never pulled from a registry, so testcontainers reuses the local
// build. Guarded by sync.Once so that parallel tests trigger the (slow) build at most once and
// all share its result. rootPath is the olake repo root, where the Makefile, base.Dockerfile and
// go.work live.
//
// A non-empty platform ("linux/amd64") is passed through as PLATFORMS, overriding the default set
// docker.base.build reads from drivers/platforms.conf so that the ref holds that platform alone.
// It has to be settled at build time: testcontainers decides whether an image needs (re)pulling by
// inspecting it *without* a platform, which always resolves the host's variant, so against the
// default multi-platform index an arm64 host reports arm64 and a request pinned to linux/amd64
// looks stale. The image is local-only, so the pull that follows fails and the container is never
// created.
func ensureTestBaseImage(t *testing.T, rootPath, platform string) string {
	t.Helper()
	image := baseImageRef(t, rootPath)
	buildBaseImageOnce.Do(func() {
		t.Logf("Building test base image %s (platform: %s)...", image, platform)
		// wall-clock via trackPhaseTiming, not build.ProcessState.SystemTime() (that reports make's
		// kernel CPU time — a misleading ~87ms even when the docker build actually took far longer).
		defer trackPhaseTiming(t, "base-image", image)()
		args := []string{"docker.base.build"}
		if platform != "" {
			args = append(args, "PLATFORMS="+platform)
		}
		build := exec.Command("make", args...)
		build.Dir = rootPath
		if out, err := build.CombinedOutput(); err != nil {
			buildBaseImageErr = fmt.Errorf("failed to build base image %s: %w\n%s", image, err, out)
		}
	})
	require.NoError(t, buildBaseImageErr, "test base image unavailable")
	return image
}
