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

// S3PathMapping tracks an S3 URI and its downloaded local copy.
type S3PathMapping struct {
	OriginalPath string
	LocalPath    string
	IsS3         bool
	Bucket       string
	Key          string
}

// ResolveS3Path downloads an s3:// path to a local temp file. Local paths are returned unchanged.
func ResolveS3Path(ctx context.Context, path string) (S3PathMapping, error) {
	if !IsS3Path(path) {
		return S3PathMapping{OriginalPath: path, LocalPath: path}, nil
	}

	bucket, key, err := ParseS3URI(path)
	if err != nil {
		return S3PathMapping{}, err
	}

	resp, err := s3util.GetObject(ctx, bucket, key)
	if err != nil {
		return S3PathMapping{}, fmt.Errorf("failed to download %s: %s", path, err)
	}
	defer resp.Body.Close()

	localPath := localPathForS3URI(bucket, key)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return S3PathMapping{}, fmt.Errorf("failed to create temp dir for %s: %s", path, err)
	}

	file, err := os.Create(localPath)
	if err != nil {
		return S3PathMapping{}, fmt.Errorf("failed to create local file for %s: %s", path, err)
	}

	if _, err = io.Copy(file, resp.Body); err != nil {
		file.Close()
		return S3PathMapping{}, fmt.Errorf("failed to write local file for %s: %s", path, err)
	}
	if err := file.Close(); err != nil {
		return S3PathMapping{}, fmt.Errorf("failed to close local file for %s: %s", path, err)
	}

	return S3PathMapping{
		OriginalPath: path,
		LocalPath:    localPath,
		IsS3:         true,
		Bucket:       bucket,
		Key:          key,
	}, nil
}

// ParseS3URI parses an s3:// URI into a bucket and key.
// Caller must ensure uri is an s3:// path.
func ParseS3URI(uri string) (bucket, key string, err error) {
	rest := strings.TrimPrefix(uri, s3URIPrefix)
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid s3 uri: %s", uri)
	}

	return parts[0], parts[1], nil
}

func localPathForS3URI(bucket, key string) string {
	return filepath.Join(os.TempDir(), "olake", "s3", bucket, key)
}

// IsS3Path returns true if the path is an S3 URI.
func IsS3Path(path string) bool {
	return strings.HasPrefix(path, s3URIPrefix)
}

// ResolveS3Paths initializes storage and downloads s3:// flag paths to local temp files.
func ResolveS3Paths(ctx context.Context, flagPaths []*string) error {
	if err := s3util.Init(ctx); err != nil {
		return err
	}

	for _, flagPath := range flagPaths {
		if err := resolveS3PathFlag(ctx, flagPath); err != nil {
			return err
		}
	}
	return nil
}

func resolveS3PathFlag(ctx context.Context, flagPath *string) error {
	if *flagPath == "" || *flagPath == "not-set" {
		return nil
	}

	s3PathMap, err := ResolveS3Path(ctx, *flagPath)
	if err != nil {
		return err
	}
	*flagPath = s3PathMap.LocalPath
	return nil
}

// FinalizeS3Upload uploads local artifacts after a successful run. Deferred with
// a named return so a failed upload is not dropped. Failures skip the upload so
// a partial local write cannot overwrite remote streams/state.
func FinalizeS3Upload(ctx context.Context, err *error, noSave bool) {
	if *err != nil || noSave || s3util.JobBucket == "" {
		return
	}

	statsPath := ""
	if configFolder := viper.GetString(constants.ConfigFolder); configFolder != "" {
		statsPath = filepath.Join(configFolder, "stats.json")
	}

	files := []struct {
		local string
		name  string
	}{
		{viper.GetString(constants.StreamsPath), "streams.json"},
		{viper.GetString(constants.StatePath), "state.json"},
		{statsPath, "stats.json"},
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
			*err = fmt.Errorf("failed to upload config folder artifacts to S3: %s", uploadErr)
			return
		}
		logger.Infof("uploaded %s to s3://%s/%s", file.name, s3util.JobBucket, s3Key)
	}
}
