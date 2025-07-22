package iceberg

type SpecConfig struct {
	// Common S3 configuration
	S3Config

	// Catalog configuration
	//
	// @jsonSchema(
	//   title="Catalog",
	//   description="Type of catalog to use",
	//   oneOf=[JDBCCatalogSpec, HiveCatalogSpec, RestCatalogSpec, GlueCatalogSpec]
	// )
	CatalogType interface{} `json:"catalog_type"`

	// Common Iceberg configuration
	//
	// @jsonSchema(
	//   title="Iceberg Database",
	//   description="Specifies the name of the Iceberg database to be used in the destination",
	//   type="string",
	//   required=true
	// )
	IcebergDatabase string `json:"iceberg_db,omitempty"`

	// Iceberg S3 Path
	//
	// @jsonSchema(
	//   title="Iceberg S3 Path",
	//   description="Specifies the S3 path in AWS where the iceberg data will be stored",
	//   type="string",
	//   required=true
	// )
	IcebergS3Path string `json:"iceberg_s3_path"`

	CatalogName string `json:"catalog_name,omitempty"`

	JarPath string `json:"sink_jar_path,omitempty"`

	DebugMode bool `json:"debug_mode,omitempty"`
}

// JDBC catalog configuration
//
// @jsonSchema(
//
//	title="JDBC Catalog",
//	description="JDBC catalog configuration"
//
// )
type JDBCCatalogSpec struct {
	//  JDBCUrl
	//
	// @jsonSchema(
	//   title="JDBC URL",
	//   description="Specifies the JDBC URL used to connect to the PostgreSQL catalog",
	//   type="string",
	//   required=true
	// )
	JDBCUrl string `json:"jdbc_url"`

	// JDBCUsername
	//
	// @jsonSchema(
	//   title="JDBC Username",
	//   description="Specifies the username used to authenticate with the PostgreSQL catalog",
	//   type="string",
	//   required=true
	// )
	JDBCUsername string `json:"jdbc_username"`

	// JDBCPassword
	//
	// @jsonSchema(
	//   title="JDBC Password",
	//   description="Specifies the password used to authenticate with the PostgreSQL catalog",
	//   type="string",
	//   format="password",
	//   required=true
	// )
	JDBCPassword string `json:"jdbc_password"`

	// S3Endpoint
	//
	// @jsonSchema(
	//   title="S3 Endpoint",
	//   description="Specifies the endpoint URL for S3 services (e.g., MinIO)",
	//   type="string"
	// )
	S3Endpoint string `json:"s3_endpoint,omitempty"`

	// UseSSL
	//
	// @jsonSchema(
	//   title="Use SSL for S3",
	//   description="Indicates if SSL is enabled for S3 connections; 'false' disables SSL",
	//   type="boolean",
	//   default=false
	// )
	UseSSL bool `json:"s3_use_ssl,omitempty"`

	// UsePathStyle
	//
	// @jsonSchema(
	//   default=true,
	//   description="Specifies whether path-style access is used for S3; 'true' enables path-style addressing instead of the default virtual-hosted style",
	//   title="Use Path Style for S3",
	//   type="boolean",
	//   default=true
	// )
	UsePathStyle bool `json:"s3_path_style,omitempty"`
}

// Hive catalog configuration
//
// @jsonSchema(
//
//	title="Hive Catalog",
//	description="Hive catalog configuration"
//
// )
type HiveCatalogSpec struct {
	//
	// @jsonSchema(
	//   title="Hive URI",
	//   description="Specifies the URI of the Hive Metastore service used for catalog interactions by the writer",
	//   type="string",
	//   required=true
	// )
	HiveURI string `json:"hive_uri"`

	//
	// @jsonSchema(
	//   title="Hive Clients",
	//   description="Specifies the number of Hive clients allocated to handle interactions with the Hive Metastore",
	//   type="integer"
	// )
	HiveClients int `json:"hive_clients,omitempty"`

	//
	// @jsonSchema(
	//   title="Enable SASL for Hive",
	//   description="Indicates if SASL authentication is enabled for the Hive connection; 'false' means SASL is disabled",
	//   type="boolean",
	//   default=false
	// )
	HiveSaslEnabled bool `json:"hive_sasl_enabled,omitempty"`

	//
	// @jsonSchema(
	//   title="Use SSL for S3",
	//   description="Indicates if SSL is enabled for S3 connections; 'false' disables SSL",
	//   type="boolean",
	//   default=false
	// )
	UseSSL bool `json:"s3_use_ssl,omitempty"`

	//
	// @jsonSchema(
	//   default=true,
	//   description="Specifies whether path-style access is used for S3; 'true' enables path-style addressing instead of the default virtual-hosted style",
	//   title="Use Path Style for S3",
	//   type="boolean",
	//   default=true
	// )
	UsePathStyle bool `json:"s3_path_style,omitempty"`
}

// Rest catalog configuration
//
// @jsonSchema(
//
//	title="Rest Catalog",
//	description="REST catalog configuration"
//
// )
type RestCatalogSpec struct {
	//RestCatalogURL
	//
	// @jsonSchema(
	//   title="REST Catalog URI",
	//   description="Specifies the endpoint URI for the REST catalog service the writer will connect to",
	//   type="string",
	//   required=true
	// )
	RestCatalogURL string `json:"rest_catalog_url"`

	//RestSigningName
	//
	// @jsonSchema(
	//   title="REST Signing Name",
	//   description="Optional AWS signing name to be used when authenticating REST requests",
	//   type="string"
	// )
	RestSigningName string `json:"rest_signing_name,omitempty"`

	//RestSigningRegion
	//
	// @jsonSchema(
	//   title="REST Signing Region",
	//   description="AWS region used for signing REST requests",
	//   type="string"
	// )
	RestSigningRegion string `json:"rest_signing_region,omitempty"`

	//RestSigningV4
	//
	// @jsonSchema(
	//   title="REST Enable Signature V4",
	//   description="Enable AWS signature version 4 for REST request signing",
	//   type="boolean"
	// )
	RestSigningV4 bool `json:"rest_signing_v_4,omitempty"`

	//RestToken
	//
	// @jsonSchema(
	//   title="Token",
	//   description="Optional token used for authenticating with the REST catalog",
	//   type="string"
	// )
	RestToken string `json:"token,omitempty"`

	//RestOAuthURI
	//
	// @jsonSchema(
	//   title="OAuth2 Auth URI",
	//   description="OAuth2 authorization URI for obtaining access tokens",
	//   type="string"
	// )
	RestOAuthURI string `json:"oauth2_uri,omitempty"`

	//RestAuthType
	//
	// @jsonSchema(
	//   title="REST Auth Type",
	//   description="Type of authentication to use with the REST catalog. Need to use it for only `oauth2",
	//   type="string"
	// )
	RestAuthType string `json:"rest_auth_type,omitempty"`

	//RestScope
	//
	// @jsonSchema(
	//   title="Scope (OAuth2)",
	//   description="OAuth2 scope to be used during token acquisition",
	//   type="string"
	// )
	RestScope string `json:"scope,omitempty"`

	//RestCredential
	//
	// @jsonSchema(
	//   title="Credential (OAuth2)",
	//   description="Optional credential used for authenticating REST requests",
	//   type="string"
	// )
	RestCredential string `json:"credential,omitempty"`

	//NoIdentifierFields
	//
	// @jsonSchema(
	//   title="Disable Identifier Table",
	//   description="Disable creation of Iceberg identifier tables for this catalog, Needed for environments which doesn't support equality deletes based updates (Ex, Databricks unity managed Iceberg tables)",
	//   type="boolean",
	//   default=false
	// )
	NoIdentifierFields bool `json:"no_identifier_fields,omitempty"`
}

// Glue catalog configuration
//
// @jsonSchema(
//
//	title="Glue Catalog",
//	description="Glue catalog configuration"
//
// )
type GlueCatalogSpec struct {
	//
	// @jsonSchema(
	//   title="Server Host",
	//   description="Host address of the gRPC server",
	//   type="string",
	//   default="localhost",
	//   required=true
	// )
	ServerHost string `json:"sink_rpc_server_host,omitempty"`

	//
	// @jsonSchema(
	//   title="gRPC Port",
	//   description="Port on which the gRPC server listens (mostly 50051)",
	//   type="integer",
	//   default=50051,
	//   required=true
	// )
	GrpcPort int `json:"grpc_port,omitempty"`
}

// Config for spec command
type S3Config struct {
	// Region
	//
	// @jsonSchema(
	//   title="AWS Region",
	//   description="Specify the AWS region where the S3 bucket is hosted",
	//   type="string",
	//   required=true
	// )
	Region string `json:"aws_region,omitempty"`

	// AccessKey
	//
	// @jsonSchema(
	//   title="AWS Access Key",
	//   description="The AWS access key for authenticating S3 requests, typically a 20-character alphanumeric string",
	//   type="string",
	//   format="password"
	// )
	AccessKey string `json:"aws_access_key,omitempty"`

	// SecretKey
	//
	// @jsonSchema(
	//   title="AWS Secret Key",
	//   description="The AWS secret key for S3 authentication—typically 40+ characters long",
	//   type="string",
	//   format="password"
	// )
	SecretKey string `json:"aws_secret_key,omitempty"`

	SessionToken string `json:"aws_session_token,omitempty"`
	ProfileName  string `json:"aws_profile,omitempty"`
	S3Endpoint   string `json:"s3_endpoint,omitempty"`
}
