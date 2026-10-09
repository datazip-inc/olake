package testutils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	pqgo "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

type azureDestWriter struct {
	AccountName   string `json:"azure_storage_account_name"`
	AccountKey    string `json:"azure_storage_account_key"`
	ContainerName string `json:"azure_container_name"`
	Path          string `json:"azure_path"`
	Endpoint      string `json:"azure_endpoint"`
}

func loadAzureDest(t *testing.T, cfg *TestConfig) azureDestWriter {
	t.Helper()
	var doc struct {
		Writer azureDestWriter `json:"writer"`
	}
	path := filepath.Join(cfg.HostTestDataPath, "parquet_azure_destination.json")
	require.NoError(t, UnmarshalFile(path, &doc, false), "read azure dest %s", path)
	doc.Writer.Endpoint = strings.Replace(doc.Writer.Endpoint, "host.docker.internal", "127.0.0.1", 1)
	return doc.Writer
}

// newAzuriteClient creates a new azurite client.
func newAzuriteClient(dest azureDestWriter) (*azblob.Client, error) {
	cred, err := azblob.NewSharedKeyCredential(dest.AccountName, dest.AccountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create azurite client: %w", err)
	}
	return azblob.NewClientWithSharedKeyCredential(dest.Endpoint, cred, nil)
}

// requireAzurite creates the azurite container and returns the client
func requireAzurite(t *testing.T, dest azureDestWriter) *azblob.Client {
	t.Helper()
	client, err := newAzuriteClient(dest)
	require.NoError(t, err)
	_, err = client.CreateContainer(context.Background(), dest.ContainerName, nil)
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

	dest := loadAzureDest(t, cfg.TestConfig)
	requireAzurite(t, dest)

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

	dest := loadAzureDest(t, cfg.TestConfig)
	client, err := newAzuriteClient(dest)
	require.NoError(t, err)

	prefix := azureParquetPrefix(dest.Path, cfg.DestinationDB, table)
	objects, err := listParquetObjectsAzureClient(ctx, client, dest, cfg.DestinationDB, table)
	require.NoError(t, err)
	require.NotEmpty(t, objects, "no parquet blobs under %s", prefix)

	var totalRows int64
	for _, obj := range objects {
		resp, err := client.DownloadStream(ctx, dest.ContainerName, obj, nil)
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
func listParquetObjectsAzureClient(ctx context.Context, client *azblob.Client, dest azureDestWriter, destDB, table string) ([]string, error) {
	prefix := azureParquetPrefix(dest.Path, destDB, table)
	var objects []string

	pager := client.NewListBlobsFlatPager(dest.ContainerName, &container.ListBlobsFlatOptions{
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
