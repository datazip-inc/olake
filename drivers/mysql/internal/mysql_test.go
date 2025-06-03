package driver

import (
	"testing"

	"github.com/datazip-inc/olake/utils/logger"
)

// Test functions using base utilities
func TestMySQLSetup(t *testing.T) {
	logger.Init()
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
