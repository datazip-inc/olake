package s3

import (
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
)

// s3BaseConfig returns an IntegrationTest for one source format variant. Each variant owns a
// testdata/<DataFormat>/ directory, which is what GetTestConfig's third argument selects.
func s3BaseConfig(t *testing.T, variant S3TestVariant) *testutils.IntegrationTest {
	return &testutils.IntegrationTest{
		TestConfig:                testutils.GetTestConfig(t, string(constants.S3), variant.DataFormat),
		Namespace:                 "s3",
		ExpectedData:              variant.ExpectedData,
		DestinationDataTypeSchema: variant.DestinationSchema,
		ExecuteQuery:              ExecuteQueryFactory(variant),
		ColumnToExclude:           excludedColumn,
		DestinationDB:             S3DestinationDB,
		CursorField:               S3CursorField,
		PartitionRegex:            S3PartitionRegex,
		FilterConfig:              S3FilterConfig,
	}
}

func TestS3Discover(t *testing.T) {
	for _, variant := range S3TestVariants {
		t.Run(variant.Name, func(t *testing.T) {
			s3BaseConfig(t, variant).TestDiscover(t)
		})
	}
}

func TestS3Sync(t *testing.T) {
	t.Parallel()
	for _, variant := range S3TestVariants {
		t.Run(variant.Name, func(t *testing.T) {
			t.Parallel()
			cfg := s3BaseConfig(t, variant)
			cfg.IsolateSuite(t, variant.Name)
			cfg.ExpectedUpdatedData = variant.ExpectedUpdatedData
			// The "evolve-schema" operation ships a file carrying a column discover has not
			// seen (see S3TestVariant.BuildEvolvedFile), so the update sync must land it in
			// the destination as a string column.
			cfg.UpdatedDestinationDataTypeSchema = variant.UpdatedDestinationSchema
			cfg.TestSync(t)
		})
	}
}
