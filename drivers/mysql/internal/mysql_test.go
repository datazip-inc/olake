package driver

import (
	"testing"
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
