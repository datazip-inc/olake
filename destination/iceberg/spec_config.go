package iceberg

// SpecConfig struct for JSON schema generation
type SpecConfig struct {
	// Common S3 configuration
	S3Config

	// Catalog configuration
	//
	// @jsonSchema(
	//   title="Catalog Type",
	//   description="Type of catalog to use",
	//   oneOf=[JDBCCatalogSpec, HiveCatalogSpec, RestCatalogSpec, GlueCatalogSpec],
	//   order=2
	// )
	CatalogType interface{} `json:"catalog_type"`

	CatalogName string `json:"catalog_name,omitempty"`

	// Common Iceberg configuration
	//
	// @jsonSchema(
	//   title="Iceberg Database",
	//   description="Specifies the name of the Iceberg database to be used in the destination",
	//   type="string",
	//   order=13
	// )
	IcebergDatabase string `json:"iceberg_db,omitempty"`

	//
	// @jsonSchema(
	//   title="Iceberg S3 Path",
	//   description="Specifies the S3 path for storing Iceberg data, such as 's3a://warehouse/', representing the target bucket or directory",
	//   type="string",
	//   required=true
	// )
	IcebergS3Path string `json:"iceberg_s3_path"`

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
	//
	// @jsonSchema(
	//   title="JDBC URL",
	//   description="Specifies the JDBC URL used to connect to the PostgreSQL catalog",
	//   type="string",
	//   order=3,
	//   required=true
	// )
	JDBCUrl string `json:"jdbc_url"`

	//
	// @jsonSchema(
	//   title="JDBC Username",
	//   description="Specifies the username used to authenticate with the PostgreSQL catalog",
	//   type="string",
	//   order=4,
	//   required=true
	// )
	JDBCUsername string `json:"jdbc_username"`

	//
	// @jsonSchema(
	//   title="JDBC Password",
	//   description="Specifies the password used to authenticate with the PostgreSQL catalog",
	//   type="string",
	//   format="password",
	//   order=5,
	//   required=true
	// )
	JDBCPassword string `json:"jdbc_password"`
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
	//   order=8,
	//   required=true
	// )
	HiveURI string `json:"hive_uri"`

	//
	// @jsonSchema(
	//   title="Hive Clients",
	//   description="Specifies the number of Hive clients allocated to handle interactions with the Hive Metastore",
	//   type="integer",
	//   order=11
	// )
	HiveClients int `json:"hive_clients,omitempty"`

	//
	// @jsonSchema(
	//   title="Enable SASL for Hive",
	//   description="Indicates if SASL authentication is enabled for the Hive connection; 'false' means SASL is disabled",
	//   type="boolean",
	//   order=12
	// )
	HiveSaslEnabled bool `json:"hive_sasl_enabled,omitempty"`
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
	//
	// @jsonSchema(
	//   title="REST Catalog URL",
	//   description="Specifies the endpoint URL for the REST catalog service the writer will connect to",
	//   type="string",
	//   order=3,
	//   required=true
	// )
	RestCatalogURL string `json:"rest_catalog_url"`
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
	//   order=9,
	//   required=true
	// )
	ServerHost string `json:"sink_rpc_server_host,omitempty"`

	//
	// @jsonSchema(
	//   title="gRPC Port",
	//   description="Port on which the gRPC server listens (mostly 50051)",
	//   type="integer",
	//   default=50051,
	//   order=8,
	//   required=true
	// )
	GrpcPort int `json:"grpc_port,omitempty"`
}

// Config for spec command
type S3Config struct {
	// Region is the AWS region
	//
	// @jsonSchema(
	//   title="AWS Region",
	//   description="Specify the AWS region where the S3 bucket is hosted",
	//   type="string",
	//   order=4,
	//   required=true
	// )
	Region string `json:"aws_region,omitempty"`

	// AccessKey is the AWS access key
	//
	// @jsonSchema(
	//   title="AWS Access Key",
	//   description="The AWS access key for authenticating S3 requests, typically a 20-character alphanumeric string",
	//   type="string",
	//   format="password",
	//   order=5,
	//   required=true
	// )
	AccessKey string `json:"aws_access_key,omitempty"`

	// SecretKey is the AWS secret key
	//
	// @jsonSchema(
	//   title="AWS Secret Key",
	//   description="The AWS secret key for S3 authentication—typically 40+ characters long",
	//   type="string",
	//   format="password",
	//   order=6,
	//   required=true
	// )
	SecretKey string `json:"aws_secret_key,omitempty"`

	SessionToken string `json:"aws_session_token,omitempty"`
	ProfileName  string `json:"aws_profile,omitempty"`
	S3Endpoint   string `json:"s3_endpoint,omitempty"`
}
