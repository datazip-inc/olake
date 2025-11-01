package driver

import (
	"testing"
	"context"
	"os"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils/testutils"
)

func TestMongodbIntegration(t *testing.T) {
	t.Parallel()
	testConfig := &testutils.IntegrationTest{
		TestConfig:         testutils.GetTestConfig(string(constants.MongoDB)),
		Namespace:          "olake_mongodb_test",
		ExpectedData:       ExpectedMongoData,
		ExpectedUpdateData: ExpectedUpdatedMongoData,
		DataTypeSchema:     MongoToIcebergSchema,
		ExecuteQuery:       ExecuteQuery,
		IcebergDB:          "mongodb_olake_mongodb_test",
	}
	testConfig.TestIntegration(t)
}

func TestMongodbPerformance(t *testing.T) {
	config := &testutils.PerformanceTest{
		TestConfig:      testutils.GetTestConfig(string(constants.MongoDB)),
		Namespace:       "twitter_data",
		BackfillStreams: []string{"tweets"},
		CDCStreams:      []string{"tweets_cdc"},
		ExecuteQuery:    ExecuteQuery,
	}

	config.TestPerformance(t)
}

func TestMongoSSHConnection(t *testing.T) {
    ctx := context.Background()

    keyBytes, err := os.ReadFile("test_key.pem")
    if err != nil {
        t.Fatalf("Failed to read SSH key: %v", err)
    }

    driver := &Mongo{
        config: &Config{
            SSHEnabled:    true,
            SSHHost:       "localhost",
            SSHPort:       2222,
            SSHUser:       "testuser",
            SSHPrivateKey: string(keyBytes),
            Database:      "testdb",
            Hosts:         []string{"dummy"},
            Username:      "mongo",
            Password:      "mongo",
        },
    }

    err = driver.Setup(ctx)
    if err != nil {
        t.Fatalf("SSH tunnel setup failed: %v", err)
    }

    defer driver.Close(ctx)
    if !driver.CDCSupported() {
        t.Errorf("Expected CDC Support to be true")
    }
}
