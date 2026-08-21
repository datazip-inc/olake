package protocol

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/spf13/viper"
)

// s3 job folder derived from the first resolved s3:// flag path (e.g. prod/{hash}).
var s3JobBucket string
var s3JobKeyPrefix string

func resolveS3Paths(ctx context.Context) error {
	if err := resolveS3PathFlag(ctx, &configPath); err != nil {
		return err
	}
	if err := resolveS3PathFlag(ctx, &destinationConfigPath); err != nil {
		return err
	}
	if err := resolveS3PathFlag(ctx, &streamsPath); err != nil {
		return err
	}
	if err := resolveS3PathFlag(ctx, &statePath); err != nil {
		return err
	}
	if err := resolveS3PathFlag(ctx, &differencePath); err != nil {
		return err
	}

	return nil
}

func resolveS3PathFlag(ctx context.Context, flagPath *string) error {
	if *flagPath == "" || *flagPath == "not-set" {
		return nil
	}

	s3PathMap, err := utils.ResolveS3Path(ctx, *flagPath)
	if err != nil {
		return err
	}
	if s3PathMap.IsS3 && s3JobBucket == "" {
		s3JobBucket = s3PathMap.Bucket
		s3JobKeyPrefix = path.Dir(s3PathMap.Key)
	}
	*flagPath = s3PathMap.LocalPath
	return nil
}

func finalizeS3Upload(ctx context.Context) error {
	if noSave || s3JobBucket == "" {
		return nil
	}

	configFolder := viper.GetString(constants.ConfigFolder)
	if configFolder == "" {
		return nil
	}

	files := []struct {
		local string
		name  string
	}{
		{filepath.Join(configFolder, "streams.json"), "streams.json"},
		{filepath.Join(configFolder, "state.json"), "state.json"},
		{filepath.Join(configFolder, "stats.json"), "stats.json"},
		{viper.GetString(constants.DifferencePath), "difference_streams.json"},
	}

	for _, file := range files {
		if file.local == "" {
			continue
		}
		if _, err := os.Stat(file.local); err != nil {
			continue
		}

		s3Key := path.Join(s3JobKeyPrefix, file.name)
		s3URI := fmt.Sprintf("s3://%s/%s", s3JobBucket, s3Key)
		err := utils.UploadFileToS3(ctx, utils.S3PathMapping{
			OriginalPath: s3URI,
			LocalPath:    file.local,
			IsS3:         true,
			Bucket:       s3JobBucket,
			Key:          s3Key,
		})
		if err != nil {
			return fmt.Errorf("failed to upload config folder artifacts to S3: %s", err)
		}
		logger.Infof("uploaded %s to %s", file.name, s3URI)
	}

	return nil
}
