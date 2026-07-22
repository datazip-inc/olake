package testutils

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

	pinBaseImageOnce sync.Once
	pinBaseImageErr  error
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
// go.work live. A non-empty platform ("linux/amd64") returns a ref pinned to that platform
// instead — see pinBaseImagePlatform.
func ensureTestBaseImage(t *testing.T, rootPath, platform string) string {
	t.Helper()
	image := baseImageRef(t, rootPath)
	buildBaseImageOnce.Do(func() {
		t.Logf("Building test base image %s...", image)
		// wall-clock via trackPhaseTiming, not build.ProcessState.SystemTime() (that reports make's
		// kernel CPU time — a misleading ~87ms even when the docker build actually took far longer).
		defer trackPhaseTiming(t, "base-image", image)()
		build := exec.Command("make", "docker.base.build")
		build.Dir = rootPath
		if out, err := build.CombinedOutput(); err != nil {
			buildBaseImageErr = fmt.Errorf("failed to build base image %s: %w\n%s", image, err, out)
		}
	})
	require.NoError(t, buildBaseImageErr, "test base image unavailable")
	if platform == "" {
		return image
	}
	return pinBaseImagePlatform(t, image, platform)
}

// pinBaseImagePlatform re-tags a single platform of the multi-platform base image under its own
// ref and returns it, so that a ContainerRequest.ImagePlatform pinned to a non-host platform is
// actually honoured.
// The derived build is a bare `FROM <base>`: it re-exports the existing layers under a new tag, so
// it costs a fraction of a second and touches the network not at all.
func pinBaseImagePlatform(t *testing.T, baseImage, platform string) string {
	t.Helper()
	pinned := fmt.Sprintf("%s-%s", baseImage, strings.ReplaceAll(platform, "/", "-"))
	pinBaseImageOnce.Do(func() {
		t.Logf("Pinning test base image %s to %s as %s...", baseImage, platform, pinned)
		defer trackPhaseTiming(t, "base-image", pinned)()
		// --provenance=false keeps the result a plain single-platform image rather than an index
		// carrying an extra attestation manifest.
		build := exec.Command("docker", "build", "--platform", platform, "--provenance=false", "-t", pinned, "-")
		build.Stdin = strings.NewReader("FROM " + baseImage + "\n")
		if out, err := build.CombinedOutput(); err != nil {
			pinBaseImageErr = fmt.Errorf("failed to pin base image %s to platform %s: %w\n%s", baseImage, platform, err, out)
		}
	})
	require.NoError(t, pinBaseImageErr, "platform-pinned test base image unavailable")
	return pinned
}
