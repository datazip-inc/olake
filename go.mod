module github.com/datazip-inc/olake

go 1.23.0

toolchain go1.23.7

require (
	github.com/go-mysql-org/go-mysql v1.11.0
	github.com/go-playground/locales v0.14.1
	github.com/go-playground/universal-translator v0.18.1
	github.com/go-playground/validator/v10 v10.25.0
	github.com/goccy/go-json v0.10.5
	github.com/jackc/pglogrepl v0.0.0-20250322012620-f1e2b1498ed6
	github.com/jackc/pgx/v5 v5.7.3
	github.com/jmoiron/sqlx v1.4.0
	github.com/parquet-go/parquet-go v0.25.0
	github.com/rs/zerolog v1.34.0
	github.com/spf13/cobra v1.9.1
	github.com/spf13/viper v1.20.0
	github.com/stretchr/testify v1.10.0
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20241021075129-b732d2ac9c9b
	go.mongodb.org/mongo-driver v1.17.3
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d
	google.golang.org/grpc v1.69.0
	google.golang.org/protobuf v1.36.4
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	sigs.k8s.io/yaml v1.4.0
)

require (
	github.com/Masterminds/semver v1.5.0 // indirect
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/apache/thrift v0.21.0 // indirect
	github.com/brainicorn/goblex v0.0.0-20220304181919-81f017b0ee95 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/fsnotify/fsnotify v1.8.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.8 // indirect
	github.com/go-viper/mapstructure/v2 v2.2.1 // indirect
	github.com/google/pprof v0.0.0-20250317173921-a4b03ec1a45e // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/olekukonko/tablewriter v0.0.5 // indirect
	github.com/pelletier/go-toml/v2 v2.2.3 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pingcap/errors v0.11.5-0.20240311024730-e056997136bb // indirect
	github.com/pingcap/log v1.1.1-0.20230317032135-a0d097d16e22 // indirect
	github.com/pingcap/tidb/pkg/parser v0.0.0-20250324075712-6dd95c94c498 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/sagikazarmark/locafero v0.8.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/siddontang/go-log v0.0.0-20190221022429-1e957dd83bed // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spf13/afero v1.14.0 // indirect
	github.com/spf13/cast v1.7.1 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/crypto v0.36.0 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/aws/aws-sdk-go v1.55.6
	github.com/brainicorn/ganno v0.0.0-20220304182003-e638228cd865
	github.com/felixge/fgprof v0.9.5
	github.com/gorilla/mux v1.8.1
	github.com/hashicorp/go-multierror v1.1.1
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/mitchellh/hashstructure v1.1.0
	github.com/oklog/ulid v1.3.1
	github.com/spf13/pflag v1.0.6 // indirect
	golang.org/x/sync v0.12.0
)

replace (
	google.golang.org/genproto => google.golang.org/genproto v0.0.0-20240123012728-ef4313101c80
	google.golang.org/genproto/googleapis/rpc => google.golang.org/genproto/googleapis/rpc v0.0.0-20240123012728-ef4313101c80
)
