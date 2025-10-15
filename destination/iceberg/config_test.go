package iceberg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid glue config",
			config: Config{
				Region:         "us-west-2",
				CatalogType:   GlueCatalog,
				CatalogName:   "test_catalog",
				IcebergS3Path: "s3://bucket/path",
			},
			wantErr: false,
		},
		{
			name: "invalid glue config - missing region",
			config: Config{
				CatalogType:   GlueCatalog,
				CatalogName:   "test_catalog",
				IcebergS3Path: "s3://bucket/path",
			},
			wantErr: true,
			errMsg:  "aws_region is required when catalog_type is glue",
		},
		{
			name: "valid jdbc config",
			config: Config{
				CatalogType:   JDBCCatalog,
				CatalogName:   "test_catalog",
				JDBCUrl:      "jdbc:postgresql://localhost:5432/db",
				JDBCUsername: "user",
				JDBCPassword: "pass",
				IcebergS3Path: "s3://bucket/path",
			},
			wantErr: false,
		},
		{
			name: "invalid jdbc config - missing url",
			config: Config{
				CatalogType:   JDBCCatalog,
				CatalogName:   "test_catalog",
				JDBCUsername: "user",
				JDBCPassword: "pass",
				IcebergS3Path: "s3://bucket/path",
			},
			wantErr: true,
			errMsg:  "jdbc_url is required when catalog_type is jdbc",
		},
		{
			name: "invalid jdbc url format",
			config: Config{
				CatalogType:   JDBCCatalog,
				CatalogName:   "test_catalog",
				JDBCUrl:      "invalid-url",
				JDBCUsername: "user",
				JDBCPassword: "pass",
				IcebergS3Path: "s3://bucket/path",
			},
			wantErr: true,
			errMsg:  "invalid JDBC URL format: must start with 'jdbc:'",
		},
		{
			name: "valid rest config with token auth",
			config: Config{
				CatalogType:    RestCatalog,
				CatalogName:    "test_catalog",
				RestCatalogURL: "https://rest-catalog.example.com",
				RestAuthType:   "token",
				RestToken:      "my-token",
				IcebergS3Path:  "s3://bucket/path",
			},
			wantErr: false,
		},
		{
			name: "invalid rest config - missing token",
			config: Config{
				CatalogType:    RestCatalog,
				CatalogName:    "test_catalog",
				RestCatalogURL: "https://rest-catalog.example.com",
				RestAuthType:   "token",
				IcebergS3Path:  "s3://bucket/path",
			},
			wantErr: true,
			errMsg:  "token is required when using token authentication",
		},
		{
			name: "valid rest config with oauth2",
			config: Config{
				CatalogType:     RestCatalog,
				CatalogName:     "test_catalog",
				RestCatalogURL:  "https://rest-catalog.example.com",
				RestAuthType:    "oauth2",
				RestOAuthURI:    "https://auth.example.com",
				RestScope:       "catalog.read",
				RestCredential:  "client-id:secret",
				IcebergS3Path:   "s3://bucket/path",
			},
			wantErr: false,
		},
		{
			name: "invalid rest config - incomplete oauth2",
			config: Config{
				CatalogType:    RestCatalog,
				CatalogName:    "test_catalog",
				RestCatalogURL: "https://rest-catalog.example.com",
				RestAuthType:   "oauth2",
				RestOAuthURI:   "https://auth.example.com",
				IcebergS3Path:  "s3://bucket/path",
			},
			wantErr: true,
			errMsg:  "oauth2_uri, scope, and credential are required for OAuth2 authentication",
		},
		{
			name: "valid hive config",
			config: Config{
				CatalogType:   HiveCatalog,
				CatalogName:   "test_catalog",
				HiveURI:      "thrift://localhost:9083",
				IcebergS3Path: "s3://bucket/path",
			},
			wantErr: false,
		},
		{
			name: "invalid hive config - wrong uri format",
			config: Config{
				CatalogType:   HiveCatalog,
				CatalogName:   "test_catalog",
				HiveURI:      "invalid://localhost:9083",
				IcebergS3Path: "s3://bucket/path",
			},
			wantErr: true,
			errMsg:  "invalid Hive URI format: must start with 'thrift://'",
		},
		{
			name: "invalid hive config - missing uri",
			config: Config{
				CatalogType:   HiveCatalog,
				CatalogName:   "test_catalog",
				IcebergS3Path: "s3://bucket/path",
			},
			wantErr: true,
			errMsg:  "hive_uri is required when catalog_type is hive",
		},
		{
			name: "invalid aws credentials - partial keys",
			config: Config{
				CatalogType:   GlueCatalog,
				CatalogName:   "test_catalog",
				Region:       "us-west-2",
				AccessKey:    "AKIAXXXXXXXX",
				IcebergS3Path: "s3://bucket/path",
			},
			wantErr: true,
			errMsg:  "both aws_access_key and aws_secret_key must be provided when using explicit AWS credentials",
		},
		{
			name: "missing s3 path",
			config: Config{
				CatalogType: GlueCatalog,
				CatalogName: "test_catalog",
				Region:     "us-west-2",
			},
			wantErr: true,
			errMsg:  "s3_path is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
