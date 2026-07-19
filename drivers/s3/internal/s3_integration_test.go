package driver

import (
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestS3Integration(t *testing.T) {
	t.Parallel()

	t.Run("Variants", func(t *testing.T) {
		for _, variant := range S3TestVariants {
			t.Run(variant.Name, func(t *testing.T) {
				t.Parallel()

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
