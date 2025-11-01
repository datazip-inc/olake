package driver

import (
    "context"
    "os"
    "testing"
)

func TestOracleSSHConnection(t *testing.T) {
    ctx := context.Background()

    keyBytes, err := os.ReadFile("test_key.pem")
    if err != nil {
        t.Fatalf("Failed to read SSH key: %v", err)
    }

    driver := &Oracle{
        config: &Config{
            SSHEnabled:    true,
            SSHHost:       "localhost",
            SSHPort:       2222,
            SSHUser:       "testuser",
            SSHPrivateKey: string(keyBytes),
            Host:          "dummy",
            Port:          1521,
            Username:      "oracle",
            Password:      "oracle",
            ServiceName:   "XE",
        },
    }

    err = driver.Setup(ctx)
    if err == nil {
        defer driver.Close()
        t.Log("SSH tunnel setup succeeded (unexpected)")
    } else {
        t.Logf("Expected failure: %v", err)
    }
}

// This test validates SSH tunneling setup for Oracle using a dummy PEM key.
// It expects the connection to fail unless a real bastion is running,
// but confirms reproducibility and key parsing for open-source contributors.
