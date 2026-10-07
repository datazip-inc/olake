package utils

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/logger"
	s3util "github.com/datazip-inc/olake/utils/s3"
	"github.com/spf13/viper"
)

const s3URIPrefix = "s3://"

// s3PathMapping tracks an S3 URI and its downloaded local copy.
type s3PathMapping struct {
	localPath string
}

// resolveS3Path downloads an s3:// path into s3LocalPath() as the object basename. Local paths are returned unchanged.
func resolveS3Path(ctx context.Context, s3Path string) (s3PathMapping, error) {
	if !isS3Path(s3Path) {
		return s3PathMapping{localPath: s3Path}, nil
	}

	bucket, key, err := parseS3URI(s3Path)
	if err != nil {
		return s3PathMapping{}, err
	}

	resp, err := s3util.GetObject(ctx, bucket, key)
	if err != nil {
		return s3PathMapping{}, fmt.Errorf("failed to download %s: %s", s3Path, err)
	}
	defer resp.Body.Close()

	localPath := filepath.Join(s3LocalPath(), path.Base(key))
	file, err := os.Create(localPath)
	if err != nil {
		return s3PathMapping{}, fmt.Errorf("failed to create local file for %s: %s", s3Path, err)
	}

	if _, err = io.Copy(file, resp.Body); err != nil {
		file.Close()
		return s3PathMapping{}, fmt.Errorf("failed to write local file for %s: %s", s3Path, err)
	}
	if err := file.Close(); err != nil {
		return s3PathMapping{}, fmt.Errorf("failed to close local file for %s: %s", s3Path, err)
	}

	return s3PathMapping{localPath: localPath}, nil
}

// parseS3URI parses an s3:// URI into a bucket and key.
// Caller must ensure uri is an s3:// path.
func parseS3URI(uri string) (bucket, key string, err error) {
	rest := strings.TrimPrefix(uri, s3URIPrefix)
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid s3 uri: %s", uri)
	}

	return parts[0], parts[1], nil
}

// s3LocalPath is the temp dir that holds downloaded s3:// flag files for this process.
func s3LocalPath() string {
	return os.TempDir()
}

// isS3Path returns true if the path is an S3 URI.
func isS3Path(path string) bool {
	return strings.HasPrefix(path, s3URIPrefix)
}

// applyConfigFolder rewrites flag paths to live under configFolder, keeping only the filename.
// The worker sets OLAKE_S3_CONFIG_FOLDER to the execution dir (/mnt/config or s3://bucket/[prefix/]hash)
// so schedule-time hashes baked into CLI args are ignored.
func applyConfigFolder(configFolder string, pathSets ...[]*string) {
	if configFolder == "" {
		return
	}
	configFolder = strings.TrimSpace(strings.TrimRight(configFolder, "/"))
	for _, flagPaths := range pathSets {
		for _, flagPath := range flagPaths {
			if flagPath == nil || *flagPath == "" || *flagPath == "not-set" {
				continue
			}
			*flagPath = configFolder + "/" + path.Base(*flagPath)
		}
	}
}

// ResolveS3Paths initializes storage and downloads s3:// flag paths into s3LocalPath().
func ResolveS3Paths(ctx context.Context, flagPaths []*string, telemetryFiles []*string) error {
	configFolder := os.Getenv(constants.EnvS3ConfigFolder)
	applyConfigFolder(configFolder, flagPaths, telemetryFiles)

	if err := s3util.Init(ctx); err != nil {
		return err
	}

	for _, flagPath := range flagPaths {
		if err := resolveS3PathFlag(ctx, flagPath); err != nil {
			return err
		}
	}

	for _, telemetryPath := range telemetryFiles {
		_ = resolveS3PathFlag(ctx, telemetryPath)
	}
	return nil
}

func resolveS3PathFlag(ctx context.Context, flagPath *string) error {
	if *flagPath == "" || *flagPath == "not-set" {
		return nil
	}

	s3PathMap, err := resolveS3Path(ctx, *flagPath)
	if err != nil {
		return err
	}
	*flagPath = s3PathMap.localPath
	return nil
}

// FinalizeS3Upload uploads local artifacts after a successful run. Callers invoke it
// from PersistentPostRunE, which Cobra skips when RunE failed, so a partial local
// write cannot overwrite remote streams/state.
func FinalizeS3Upload(ctx context.Context, noSave bool) error {
	if noSave || s3util.JobBucket == "" {
		return nil
	}

	files := []struct {
		local string
		name  string
	}{
		{viper.GetString(constants.StreamsPath), "streams.json"},
		{viper.GetString(constants.AvailableStreamsPath), "available_streams.json"},
		{viper.GetString(constants.SelectedStreamsPath), "selected_streams.json"},
		{viper.GetString(constants.StatePath), "state.json"},
		{viper.GetString(constants.DifferencePath), "difference_streams.json"},
	}

	for _, file := range files {
		if file.local == "" {
			continue
		}
		if _, statErr := os.Stat(file.local); statErr != nil {
			continue
		}

		s3Key := path.Join(s3util.JobPrefix, file.name)
		if uploadErr := s3util.UploadFileToS3(ctx, file.local, s3util.JobBucket, s3Key); uploadErr != nil {
			return fmt.Errorf("failed to upload config folder artifacts to S3: %s", uploadErr)
		}
		logger.Infof("uploaded %s to s3://%s/%s", file.name, s3util.JobBucket, s3Key)
	}
	return nil
}
