package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/require"
	pqgo "github.com/parquet-go/parquet-go"
)

// requireAzurite returns a client for local Azurite and ensures the dest container exists.
// HTTP errors from a live Azurite fail the test; a down daemon skips.
func requireAzurite(t *testing.T) *azblob.Client {
	t.Helper()
	client, err := testutils.NewAzuriteClient()
	require.NoError(t, err)
	_, err = client.CreateContainer(context.Background(), testutils.AzuriteContainer, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) {
			require.NoError(t, err, "azurite reachable but CreateContainer failed")
		}
		t.Skipf("azurite not running on :11000: %v", err)
	}
	return client
}

func azureParquetPrefix(destDB, table string) string {
	return path.Join(testutils.AzuritePath, destDB, table) + "/"
}

// testAzureBlob tests the parquet sync by seeding the catalog, executing the query, and verifying the rows
func (th *TestHandler) testAzureBlob(ctx context.Context, t *testing.T, testTable string) error {
	t.Helper()

	client := requireAzurite(t)
	seed := testutils.Ternary(len(th.DedupKeys) > 0, testutils.Ternary(th.UpsertAddOp != "", th.UpsertAddOp, "upsert_add"), "add").(string)
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "drop")
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "create")
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, "clean")
	th.TestConfig.ExecuteQuery(ctx, t, th.TestConfig, seed)

	th.resetCatalog(t)
	if err := testutils.UpdateSelectedStreams(th.TestConfig, th.Namespace, "", "", []string{testTable}, []string{th.ColumnToExclude}); err != nil {
		return fmt.Errorf("failed to clear partition regex and filter config in streams.json: %s", err)
	}
	syncMode := testutils.Ternary(th.TestConfig.Driver == string(constants.Kafka), "strict_cdc", "full_refresh").(string)
	if err := updateStreamConfig(th.TestConfig, th.Namespace, testTable, syncMode, ""); err != nil {
		return fmt.Errorf("failed to set sync mode %s: %s", syncMode, err)
	}

	prefix := azureParquetPrefix(th.DestinationDB, testTable)
	if err := deleteAzureParquetPrefix(ctx, client, prefix); err != nil {
		return fmt.Errorf("failed to reset azurite prefix %s: %w", prefix, err)
	}

	if err := testutils.RunSync(ctx, t, th.TestConfig, parquetAzureDestinationFile, false); err != nil {
		return err
	}

	th.verifyAzureParquetSync(t, testTable)
	return nil
}

func (th *TestHandler) verifyAzureParquetSync(t *testing.T, table string) {
	t.Helper()
	ctx := t.Context()

	client := requireAzurite(t)
	prefix := azureParquetPrefix(th.DestinationDB, table)

	objects, err := listParquetObjectsAzureClient(ctx, client, th.DestinationDB, table)
	require.NoError(t, err)
	require.NotEmpty(t, objects, "no parquet blobs under %s", prefix)

	var totalRows int64
	for _, obj := range objects {
		resp, err := client.DownloadStream(ctx, testutils.AzuriteContainer, obj, nil)
		require.NoError(t, err, "failed to download blob %s", obj)
		data, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, err)
		pf, oerr := pqgo.OpenFile(bytes.NewReader(data), int64(len(data)))
		require.NoError(t, oerr)
		totalRows += pf.NumRows()
	}
	require.Greaterf(t, totalRows, int64(0), "parquet files under %s had 0 rows", prefix)
	t.Logf("azure parquet OK: %d files, %d rows, prefix %s", len(objects), totalRows, prefix)
}

type azureParquetBlob struct {
	name string
	size int64
}

func listParquetObjectsAzureClient(ctx context.Context, client *azblob.Client, destDB, table string) ([]string, error) {
	blobs, err := listAzureParquetBlobs(ctx, client, destDB, table)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(blobs))
	for _, blob := range blobs {
		names = append(names, blob.name)
	}
	return names, nil
}

// listAzureParquetBlobs lists .parquet blobs written directly into the table folder.
func listAzureParquetBlobs(ctx context.Context, client *azblob.Client, destDB, table string) ([]azureParquetBlob, error) {
	prefix := azureParquetPrefix(destDB, table)
	var objects []azureParquetBlob
	pager := client.NewListBlobsFlatPager(testutils.AzuriteContainer, &container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil || !strings.HasSuffix(*item.Name, ".parquet") {
				continue
			}
			// Flat listing includes _olake_2pc. Rolling assertions count only the promoted files.
			if strings.Contains(strings.TrimPrefix(*item.Name, prefix), "/") {
				continue
			}
			if item.Properties == nil || item.Properties.ContentLength == nil {
				return nil, fmt.Errorf("blob %s has no content length", *item.Name)
			}
			objects = append(objects, azureParquetBlob{name: *item.Name, size: *item.Properties.ContentLength})
		}
	}
	return objects, nil
}

func deleteAzureParquetPrefix(ctx context.Context, client *azblob.Client, prefix string) error {
	pager := client.NewListBlobsFlatPager(testutils.AzuriteContainer, &container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name == nil {
				continue
			}
			_, err := client.DeleteBlob(ctx, testutils.AzuriteContainer, *item.Name, nil)
			if err != nil && !bloberror.HasCode(err, bloberror.BlobNotFound) {
				return fmt.Errorf("delete azurite blob %s: %w", *item.Name, err)
			}
		}
	}
	return nil
}
