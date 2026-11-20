package driver

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

func TestPostgresGetStreamNames(t *testing.T) {
	testCases := []struct {
		name   string
		tables []string
		want   []types.StreamID
	}{
		{
			name: "lists the tables of the configured database",
			tables: []string{
				`CREATE TABLE users (id int PRIMARY KEY)`,
				`CREATE TABLE orders (id int PRIMARY KEY)`,
			},
			want: []types.StreamID{
				{Namespace: "public", Name: "users"},
				{Namespace: "public", Name: "orders"},
			},
		},
		{
			name: "a database without tables has no streams",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p := &Postgres{config: testConfig(testhandler.New(t))}
			require.NoError(t, p.Setup(t.Context()))
			t.Cleanup(p.CloseConnection)
			for _, table := range tc.tables {
				_, err := p.client.ExecContext(t.Context(), table)
				require.NoError(t, err)
			}

			got, err := p.GetStreamNames(t.Context())
			require.NoError(t, err)
			require.ElementsMatch(t, tc.want, got)
		})
	}
}
