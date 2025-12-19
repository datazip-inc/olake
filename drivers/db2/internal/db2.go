package driver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/drivers/abstract"
	"github.com/datazip-inc/olake/pkg/jdbc"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	_ "github.com/ibmdb/go_ibm_db"
	"github.com/jmoiron/sqlx"
)

type DB2 struct {
	client *sqlx.DB
	config *Config
	state  *types.State
}

func (d *DB2) CDCSupported() bool {
	return false
}

func (d *DB2) Setup(ctx context.Context) error {
	if err := d.config.Validate(); err != nil {
		return err
	}

	// Build DSN
	dsn := d.config.BuildDSN()
	client, err := sqlx.Open("go_ibm_db", dsn)
	if err != nil {
		return fmt.Errorf("failed to open db2 connection: %w", err)
	}

	client.SetMaxOpenConns(d.config.MaxThreads)

	// Verify connection
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := client.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping db2: %w", err)
	}

	d.client = client
	d.config.RetryCount = utils.Ternary(d.config.RetryCount <= 0, 1, d.config.RetryCount+1).(int)
	return nil
}

func (d *DB2) StateType() types.StateType {
	return types.GlobalType
}

func (d *DB2) SetupState(state *types.State) {
	d.state = state
}

func (d *DB2) GetConfigRef() abstract.Config {
	d.config = &Config{}
	return d.config
}

func (d *DB2) Spec() any {
	return Config{}
}

func (d *DB2) CloseConnection() {
	if d.client != nil {
		if err := d.client.Close(); err != nil {
			logger.Error("failed to close db2 connection: %s", err)
		}
	}
}

func (d *DB2) Type() string {
	return string(constants.DB2)
}

func (d *DB2) MaxConnections() int {
	return d.config.MaxThreads
}

func (d *DB2) MaxRetries() int {
	return d.config.RetryCount
}

func (d *DB2) GetStreamNames(ctx context.Context) ([]string, error) {
	logger.Infof("Starting discover for DB2 database %s", d.config.Database)

	var tables []struct {
		Schema string `db:"TABLE_SCHEMA"`
		Name   string `db:"TABLE_NAME"`
	}

	if err := d.client.SelectContext(ctx, &tables, jdbc.DB2DiscoveryQuery()); err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	streamNames := make([]string, 0, len(tables))
	for _, t := range tables {
		streamNames = append(
			streamNames,
			fmt.Sprintf("%s.%s", t.Schema, t.Name),
		)
	}

	return streamNames, nil
}

func (d *DB2) ProduceSchema(ctx context.Context, streamName string) (*types.Stream, error) {
	populateStreams := func(ctx context.Context, streamName string) (*types.Stream, error) {
		logger.Infof("producing type schema for stream [%s]", streamName)

		parts := strings.Split(streamName, ".")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid stream name format: %s", streamName)
		}

		schemaName, tableName := parts[0], parts[1]
		stream := types.NewStream(tableName, schemaName, &d.config.Database)

		rows, err := d.client.QueryContext(ctx, jdbc.DB2TableSchemaAndPrimaryKeysQuery(), schemaName, tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to query column metadata: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var (
				columnName string
				dataType   string
				isNullable string
				pkColumn   *string
			)

			if err := rows.Scan(&columnName, &dataType, &isNullable, &pkColumn); err != nil {
				return nil, fmt.Errorf("failed to scan column: %w", err)
			}

			stream.WithCursorField(columnName)
			datatype := types.Unknown

			if val, found := db2TypeToDataTypes[strings.ToLower(dataType)]; found {
				datatype = val
			} else {
				logger.Debugf("unsupported DB2 type '%s' for column '%s.%s', defaulting to String", dataType, streamName, columnName)
				datatype = types.String
			}
			stream.UpsertField(columnName, datatype, isNullable == "Y")

			if pkColumn != nil {
				stream.WithPrimaryKey(columnName)
			}
		}
		return stream, rows.Err()
	}

	stream, err := populateStreams(ctx, streamName)
	if err != nil && ctx.Err() == nil {
		return nil, fmt.Errorf("failed to process table[%s]: %w", streamName, err)
	}
	return stream, nil
}

func (d *DB2) dataTypeConverter(value interface{}, columnType string) (interface{}, error) {
	if value == nil {
		return nil, typeutils.ErrNullValue
	}
	olakeType := typeutils.ExtractAndMapColumnType(columnType, db2TypeToDataTypes)
	return typeutils.ReformatValue(olakeType, value)
}
