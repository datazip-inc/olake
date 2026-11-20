package driver

import (
	"context"
	"fmt"

	"github.com/datazip-inc/olake/utils/testutils/testhandler"
)

// The root user of drivers/mongodb/docker-compose.yml, whose replica set the integration suites share.
const (
	mongoUser     = "admin"
	mongoPassword = "password"
)

var mongoService = &testhandler.Service{
	Container: "primary_mongo",
	Port:      27017,
	Target:    "olake.mongodb.start",
	Ready: func(ctx context.Context, s *testhandler.Service) error {
		_, err := mongosh(ctx, s, `if (!db.hello().isWritablePrimary) { throw new Error("no primary yet") }`)
		return err
	},
}

// mongoSource gives each test its own database on the shared single-node replica set, so change streams work.
type mongoSource struct{}

func (mongoSource) Service() *testhandler.Service {
	return mongoService
}

// Create does nothing: MongoDB creates a database on its first write.
func (mongoSource) Create(context.Context, string) error {
	return nil
}

func (mongoSource) Drop(ctx context.Context, database string) error {
	_, err := mongosh(ctx, mongoService, fmt.Sprintf("db.getSiblingDB(%q).dropDatabase()", database))
	return err
}

// testConfig reaches this test's own database, which cleanup drops. It connects to the member directly:
// the replica set advertises host.docker.internal, which a client on the host may not resolve.
func testConfig(h *testhandler.Handler) *Config {
	return &Config{
		Hosts:            []string{mongoService.Addr()},
		AuthDB:           "admin",
		Database:         h.Database(mongoSource{}),
		Username:         mongoUser,
		Password:         mongoPassword,
		AdditionalParams: map[string]string{"directConnection": "true"},
	}
}

// mongosh runs a script on the server as root; a thrown error fails the command.
func mongosh(ctx context.Context, s *testhandler.Service, script string) (string, error) {
	return s.Exec(ctx, nil, "mongosh", "--port", "27017", "--quiet",
		"--username", mongoUser, "--password", mongoPassword, "--authenticationDatabase", "admin", "--eval", script)
}
