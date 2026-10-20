package parquet

import (
	"errors"
	"fmt"

	"github.com/datazip-inc/olake/destination/parquet/azure"
	"github.com/datazip-inc/olake/destination/parquet/s3"
)

// storeKind reads the Kind string from the concrete store.
func storeKind(store objectStore) string {
	switch s := store.(type) {
	case *s3.Store:
		return s.Kind
	case *azure.Store:
		return s.Kind
	default:
		return ""
	}
}

// objectNotFound reports a missing object. S3 and Azure classify their SDK errors;
// any other store uses errObjectNotFound.
func objectNotFound(store objectStore, err error) bool {
	switch s := store.(type) {
	case *s3.Store:
		return s.IsNotFound(err)
	case *azure.Store:
		return s.IsNotFound(err)
	default:
		return errors.Is(err, errObjectNotFound)
	}
}

// objectRateLimited reports a throttle response from the concrete store.
func objectRateLimited(store objectStore, err error) bool {
	switch s := store.(type) {
	case *s3.Store:
		return s.IsRateLimitError(err)
	case *azure.Store:
		return s.IsRateLimitError(err)
	default:
		return false
	}
}

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
