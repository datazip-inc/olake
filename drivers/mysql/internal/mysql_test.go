package driver

import (
	"testing"

	"github.com/datazip-inc/olake/utils"
	"github.com/stretchr/testify/assert"
)

// Test functions using base utilities
func TestMySQLSetup(t *testing.T) {
	_, abstractDriver := testAndBuildAbstractDriver(t)
	abstractDriver.TestSetup(t)
}

func TestMySQLDiscover(t *testing.T) {
	conn, abstractDriver := testAndBuildAbstractDriver(t)
	abstractDriver.TestDiscover(t, conn, ExecuteQuery)
	// TODO : Add MySQL-specific schema verification if needed
}

func TestMySQLRead(t *testing.T) {
	conn, abstractDriver := testAndBuildAbstractDriver(t)
	abstractDriver.TestRead(t, conn, ExecuteQuery)
}

func TestURIWithJDBCParams(t *testing.T) {
	config := &Config{
		Host:     "localhost",
		Port:     3306,
		Username: "user",
		Password: "pass",
		Database: "testdb",
		JDBCURLParams: map[string]string{
			"connectTimeout": "30000",
			"useSSL":         "true",
		},
	}

	uri := config.URI()
	assert.Contains(t, uri, "connectTimeout=30000")
	assert.Contains(t, uri, "useSSL=true")
}

func TestURIWithSSLConfig(t *testing.T) {
	// Test with SSLModeRequire
	config := &Config{
		Host:     "localhost",
		Port:     3306,
		Username: "user",
		Password: "pass",
		Database: "testdb",
		SSLConfig: &utils.SSLConfig{
			Mode: utils.SSLModeRequire,
		},
	}

	uri := config.URI()
	assert.Contains(t, uri, "tls=true")

	// Test with SSLModeDisable
	config.SSLConfig.Mode = utils.SSLModeDisable
	uri = config.URI()
	assert.Contains(t, uri, "tls=false")

	// Test with TLSSkipVerify
	config = &Config{
		Host:          "localhost",
		Port:          3306,
		Username:      "user",
		Password:      "pass",
		Database:      "testdb",
		TLSSkipVerify: true,
	}

	uri = config.URI()
	assert.Contains(t, uri, "tls=skip-verify")
}

func TestConfigValidation(t *testing.T) {
	// Test with valid SSL config
	config := &Config{
		Host:     "localhost",
		Port:     3306,
		Username: "user",
		Password: "pass",
		Database: "testdb",
		SSLConfig: &utils.SSLConfig{
			Mode:       utils.SSLModeVerifyCA,
			ServerCA:   "ca-cert",
			ClientCert: "client-cert",
			ClientKey:  "client-key",
		},
	}

	err := config.Validate()
	assert.NoError(t, err)

	// Test with invalid SSL config (missing required fields)
	config.SSLConfig = &utils.SSLConfig{
		Mode: utils.SSLModeVerifyCA,
		// Missing ServerCA, ClientCert, ClientKey
	}

	err = config.Validate()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid SSL configuration")
}
