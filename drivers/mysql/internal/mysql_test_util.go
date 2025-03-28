// testMySQLClient
package driver

import (
	"database/sql"
	"testing"

	"github.com/datazip-inc/olake/drivers/base"
	"github.com/datazip-inc/olake/types"
	_ "github.com/go-sql-driver/mysql" // MySQL driver
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver
	"github.com/stretchr/testify/require"
)

func testMySQLClient(t *testing.T) (*sql.DB, Config, *MySQL) {
	t.Helper()

	config := Config{
		Username: "olake_user",
		Host:     "localhost",
		Port:     3306,
		Password: "olake_password",
		Database: "olake",
	}

	d := &MySQL{
		Driver: base.NewBase(),
		config: &config,
	}

	state := types.NewState(types.GlobalType)
	d.SetupState(state)

	err := d.Setup()
	require.NoError(t, err, "Failed to set up MySQL client")

	return d.client, *d.config, d
}
