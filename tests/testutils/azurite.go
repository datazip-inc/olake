package testutils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	pqgo "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

const (
	// azuriteAccountName, azuriteAccountKey, azuriteContainer, and azuriteHostURL must match the
	// azurite service in olake/destination/iceberg/local-test/docker-compose.yml
	azuriteAccountName = "devstoreaccount1"
	azuriteAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	azuriteContainer   = "warehouse"
	azuriteHostURL     = "http://127.0.0.1:11000/devstoreaccount1"
)

// azuriteTestDrivers lists the drivers wired for the Azurite sub-test inside the common Sync flow
var azuriteTestDrivers = []constants.DriverType{constants.Postgres}

// hasAzuriteTest reports whether the driver participates in the Azurite sub-test
func hasAzuriteTest(driver string) bool {
	return slices.Contains(azuriteTestDrivers, constants.DriverType(driver))
}

// newAzuriteClient creates a new azurite client.
func newAzuriteClient() (*azblob.Client, error) {
	cred, err := azblob.NewSharedKeyCredential(azuriteAccountName, azuriteAccountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create azurite client: %w", err)
	}
	client, err := azblob.NewClientWithSharedKeyCredential(azuriteHostURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create azurite client: %w", err)
	}
	return client, nil
}

// requireAzurite creates the azurite container and returns the client
func requireAzurite(t *testing.T) *azblob.Client {
	t.Helper()
	client, err := newAzuriteClient()
	require.NoError(t, err)

	ctx := context.Background()
	_, err = client.CreateContainer(ctx, azuriteContainer, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		t.Skipf("azurite not running on :11000: %v", err)
	}
	return client
}

// azureParquetPrefix returns the prefix of the parquet objects
func azureParquetPrefix(azurePath, destDB, table string) string {
	azurePath = strings.Trim(azurePath, "/")
	if azurePath == "" {
		return fmt.Sprintf("%s/%s/", destDB, table)
	}
	return fmt.Sprintf("%s/%s/%s/", azurePath, destDB, table)
}

// testAzureBlob tests the parquet sync by seeding the catalog, executing the query, and verifying the rows
func (cfg *IntegrationTest) testAzureBlob(ctx context.Context, t *testing.T, testTable string) error {
	t.Helper()
	seedCatalogFromTestStreams(t, cfg.TestConfig, testTable)

	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "drop")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "create")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "clean")
	cfg.ExecuteQuery(ctx, t, cfg.TestConfig, "add")

	require.NoError(t, updateSelectedStreams(cfg.TestConfig, cfg.Namespace, "", "", []string{testTable}, cfg.ColumnToExclude))
	// Full refresh - make azure independent of CDC
	require.NoError(t, updateStreamConfig(cfg.TestConfig, cfg.Namespace, testTable, "full_refresh", ""))
	requireAzurite(t)
	// Full load (no --state) with a small buffer so rolled files hug the threshold
	code, out, err := runOlake(ctx, t, cfg.TestConfig, syncArgs(*cfg.TestConfig, false, "parquet-azure")...)
	require.NoError(t, err)
	require.Zerof(t, code, "sync failed:\n%s", out)

	cfg.verifyAzureParquetSync(t, testTable)
	return nil
}

// verifyAzureParquetSync verifies the parquet sync by listing the objects and verifying the rows
func (cfg *IntegrationTest) verifyAzureParquetSync(t *testing.T, table string) {
	t.Helper()
	ctx := t.Context()

	client, err := newAzuriteClient()
	require.NoError(t, err)

	prefix := azureParquetPrefix("olake", cfg.DestinationDB, table)
	objects, err := listParquetObjectsAzureClient(ctx, client, cfg.DestinationDB, table)
	require.NoError(t, err)
	require.NotEmpty(t, objects, "no parquet blobs under %s", prefix)

	var totalRows int64
	for _, obj := range objects {
		resp, err := client.DownloadStream(ctx, azuriteContainer, obj, nil)
		require.NoError(t, err, "failed to download blob %s", obj)
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, err)
		pf, oerr := pqgo.OpenFile(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, oerr)
		totalRows += pf.NumRows()
	}
	require.Greater(t, totalRows, int64(0), "parquet files under %s had 0 rows", prefix)
	t.Logf("azure parquet OK: %d files, %d rows, prefix %s", len(objects), totalRows, prefix)
}

// listParquetObjectsAzureClient lists the parquet objects under the given prefix.
func listParquetObjectsAzureClient(ctx context.Context, client *azblob.Client, destDB, table string) ([]string, error) {
	prefix := azureParquetPrefix("olake", destDB, table)
	var objects []string

	pager := client.NewListBlobsFlatPager(azuriteContainer, &container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name != nil && strings.HasSuffix(*item.Name, ".parquet") {
				objects = append(objects, *item.Name)
			}
		}
	}
	return objects, nil
}
