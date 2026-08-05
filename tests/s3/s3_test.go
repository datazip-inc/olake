package s3

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

func TestS3Integration(t *testing.T) {
	t.Parallel()

	t.Run("Variants", func(t *testing.T) {
		for _, variant := range S3TestVariants {
			t.Run(variant.Name, func(t *testing.T) {
				// No t.Parallel(): variants share the bind-mounted checkout, so a concurrent
				// build of drivers/s3/olake fails with "Text file busy" (same as kafka).
				// TODO: Add t.Parallel() back once we update the testfamework to use driver docker images
				cfg := &testutils.IntegrationTest{
					TestConfig:                testutils.GetTestConfig(string(constants.S3), variant.DataFormat),
					Namespace:                 "s3",
					ExpectedData:              variant.ExpectedData,
					ExpectedUpdatedData:       variant.ExpectedUpdatedData,
					DestinationDataTypeSchema: variant.DestinationSchema,
					// The "evolve-schema" operation ships a file carrying a column discover
					// has not seen (see S3TestVariant.BuildEvolvedFile), so the update sync
					// must land it in the destination as a string column.
					UpdatedDestinationDataTypeSchema: variant.UpdatedDestinationSchema,
					ExecuteQuery:                     ExecuteQueryFactory(variant),
					ColumnToExclude:                  excludedColumn,
					DestinationDB:                    S3DestinationDB,
					CursorField:                      S3CursorField,
					PartitionRegex:                   S3PartitionRegex,
					FilterConfig:                     S3FilterConfig,
				}
				cfg.TestIntegration(t)
			})
		}
	})
}
