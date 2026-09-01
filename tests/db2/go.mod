module github.com/datazip-inc/olake/tests/db2

go 1.25.13

require (
	github.com/apache/arrow-go/v18 v18.7.0
	github.com/datazip-inc/olake/tests/testutils v0.0.0-00010101000000-000000000000
	github.com/ibmdb/go_ibm_db v0.4.5
	github.com/jmoiron/sqlx v1.4.0
	github.com/stretchr/testify v1.12.1
)

require (
	github.com/andybalholm/brotli v1.2.2 // indirect
	github.com/parquet-go/bitpack v1.0.0 // indirect
	github.com/parquet-go/jsonlite v1.0.0 // indirect
	github.com/parquet-go/parquet-go v0.32.0 // indirect
	github.com/twpayne/go-geom v1.6.1 // indirect
)

require (
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	github.com/apache/spark-connect-go/v35 v35.0.0-20250317154112-ffd832059443 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-errors/errors v1.5.1 // indirect
	github.com/goccy/go-json v0.10.6 // indirect
	github.com/google/flatbuffers v25.12.19+incompatible // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/ibmruntimes/go-recordio/v2 v2.0.0-20240416213906-ae0ad556db70 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/klauspost/cpuid/v2 v2.4.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.3.0 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.27 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/tinylib/msgp v1.6.4 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/exp v0.0.0-20260112195511-716be5621a96 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
	gopkg.in/ini.v1 v1.67.3 // indirect
)

replace (
	github.com/datazip-inc/olake/tests/testutils => ../testutils
	github.com/ibmdb/go_ibm_db => github.com/datazip-inc/go_ibm_db v0.0.1
	google.golang.org/genproto => google.golang.org/genproto v0.0.0-20240123012728-ef4313101c80
)
