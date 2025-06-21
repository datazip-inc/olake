package driver

import (
	"testing"
)

// Test functions using base utilities
func TestOracleSetup(t *testing.T) {
	_, abstractDriver := testAndBuildAbstractDriver(t)
	abstractDriver.TestSetup(t)
}

func TestOracleDiscover(t *testing.T) {
	conn, abstractDriver := testAndBuildAbstractDriver(t)
	abstractDriver.TestDiscover(t, conn, ExecuteQuery)
	// TODO : Add Oracle-specific schema verification if needed
}

func TestOracleRead(t *testing.T) {
	conn, abstractDriver := testAndBuildAbstractDriver(t)
	abstractDriver.TestRead(t, conn, ExecuteQuery)
} 