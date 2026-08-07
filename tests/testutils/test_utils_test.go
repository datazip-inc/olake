package testutils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeedCatalog(t *testing.T) {
	newConfig := func(dir string) *TestConfig {
		return &TestConfig{
			HostTestDataPath:    dir,
			HostTestCatalogPath: filepath.Join(dir, "test_streams.json"),
			HostCatalogPath:     filepath.Join(dir, "streams.json"),
		}
	}

	t.Run("copies the tracked catalog", func(t *testing.T) {
		cfg := newConfig(t.TempDir())
		require.NoError(t, os.WriteFile(cfg.HostTestCatalogPath, []byte(`{"streams":["a"]}`), 0600))

		require.NoError(t, seedCatalog(cfg))

		got, err := os.ReadFile(cfg.HostCatalogPath)
		require.NoError(t, err)
		require.Equal(t, `{"streams":["a"]}`, string(got))
	})

	t.Run("truncates a longer stale catalog", func(t *testing.T) {
		cfg := newConfig(t.TempDir())
		require.NoError(t, os.WriteFile(cfg.HostTestCatalogPath, []byte(`{"streams":["a"]}`), 0600))
		require.NoError(t, os.WriteFile(cfg.HostCatalogPath, []byte(`{"streams":["stale","longer"]}`), 0600))

		require.NoError(t, seedCatalog(cfg))

		got, err := os.ReadFile(cfg.HostCatalogPath)
		require.NoError(t, err)
		require.Equal(t, `{"streams":["a"]}`, string(got))
	})

	t.Run("writes the catalog as 0600", func(t *testing.T) {
		cfg := newConfig(t.TempDir())
		require.NoError(t, os.WriteFile(cfg.HostTestCatalogPath, []byte(`{}`), 0600))

		require.NoError(t, seedCatalog(cfg))

		info, err := os.Stat(cfg.HostCatalogPath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	})

	t.Run("refuses a catalog symlinked out of the test data dir", func(t *testing.T) {
		outside := filepath.Join(t.TempDir(), "outside.json")
		require.NoError(t, os.WriteFile(outside, []byte(`untouched`), 0600))

		cfg := newConfig(t.TempDir())
		require.NoError(t, os.WriteFile(cfg.HostTestCatalogPath, []byte(`{}`), 0600))
		require.NoError(t, os.Symlink(outside, cfg.HostCatalogPath))

		require.Error(t, seedCatalog(cfg))

		got, err := os.ReadFile(outside)
		require.NoError(t, err)
		require.Equal(t, "untouched", string(got))
	})
}
