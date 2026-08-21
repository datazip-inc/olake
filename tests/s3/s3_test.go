package s3

import (
	"strings"
	"testing"

	"github.com/datazip-inc/olake/tests/testutils"
	"github.com/datazip-inc/olake/tests/testutils/constants"
	"github.com/datazip-inc/olake/tests/testutils/integration"
)

// s3BaseConfig returns an IntegrationTest for one source format variant. Each variant owns a
// testdata/<DataFormat>/ directory, which is what DataFormat selects.
func s3BaseConfig(variant S3TestVariant) *integration.Test {
	cfg := &integration.Test{
		TestConfig:                &testutils.TestConfig{Driver: string(constants.S3), DataFormat: variant.DataFormat},
		ExpectedData:              variant.ExpectedRowData(seedValues),
		ExpectedUpdatedData:       variant.ExpectedRowData(updatedValues),
		DestinationDataTypeSchema: variant.DestinationSchema,
	}
	cfg.TestConfig.Namespace = "s3"
	cfg.TestConfig.ColumnToExclude = excludedColumn
	cfg.TestConfig.DestinationDB = S3DestinationDB
	cfg.TestConfig.CursorField = S3CursorField
	cfg.TestConfig.PartitionRegex = S3PartitionRegex
	cfg.TestConfig.FilterConfig = S3FilterConfig
	cfg.TestConfig.ExecuteQuery = ExecuteQueryFactory(variant, cfg)
	return cfg
}

func TestS3Discover(t *testing.T) {
	for _, variant := range S3TestVariants {
		t.Run(variant.Name, func(t *testing.T) {
			s3BaseConfig(variant).TestDiscover(t)
		})
	}
}

func TestS3Sync(t *testing.T) {
	t.Parallel()
	for _, variant := range S3TestVariants {
		t.Run(variant.Name, func(t *testing.T) {
			t.Parallel()
			cfg := s3BaseConfig(variant)
			cfg.Suite = strings.ToLower(variant.Name)
			// The "evolve-schema" operation ships a file carrying a column discover has not
			// seen (see S3TestVariant.BuildEvolvedFile), so the update sync must land it in
			// the destination as a string column.
			cfg.UpdatedDestinationDataTypeSchema = variant.UpdatedDestinationSchema
			cfg.TestSync(t)
		})
	}
}
