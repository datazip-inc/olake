package parquet

import (
	"fmt"

	"github.com/datazip-inc/olake/destination/parquet/azure"
	"github.com/datazip-inc/olake/destination/parquet/s3"
)

func newObjectStore(cfg *Config) (objectStore, error) {
	switch {
	case cfg.usingAzure():
		return azure.New(azure.Config{
			AuthType:         cfg.AzureAuthType,
			AccountName:      cfg.AzureStorageAccountName,
			AccountKey:       cfg.AzureStorageAccountKey,
			Container:        cfg.AzureContainerName,
			Prefix:           cfg.AzurePath,
			Endpoint:         cfg.AzureEndpoint,
			SASToken:         cfg.AzureSASToken,
			ConnectionString: cfg.AzureConnectionString,
		})
	case cfg.usingS3():
		return s3.New(s3.Config{
			Bucket:     cfg.Bucket,
			Region:     cfg.Region,
			AccessKey:  cfg.AccessKey,
			SecretKey:  cfg.SecretKey,
			Prefix:     cfg.Prefix,
			S3Endpoint: cfg.S3Endpoint,
		})
	default:
		return nil, fmt.Errorf("no object store configured")
	}
}
