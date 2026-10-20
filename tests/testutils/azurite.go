package testutils

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

const (
	// AzuriteContainer is azure_container_name in parquet_azure_destination.json.
	AzuriteContainer = "warehouse"
	// AzuritePath is azure_path in parquet_azure_destination.json.
	AzuritePath = "olake"
)

// NewAzuriteClient returns a client for the Azurite instance backing the parquet Azure destination in tests
func NewAzuriteClient() (*azblob.Client, error) {
	cred, err := azblob.NewSharedKeyCredential(
		"devstoreaccount1",
		"Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create azurite client: %s", err)
	}
	client, err := azblob.NewClientWithSharedKeyCredential("http://127.0.0.1:11000/devstoreaccount1", cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create azurite client: %s", err)
	}
	return client, nil
}
