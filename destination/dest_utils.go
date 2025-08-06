package destination

import (
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/datazip-inc/olake/utils/logger"
)

var portMap sync.Map

// findAvailablePort finds an available port for the RPC server
func FindAvailablePort(serverHost string) (int, error) {
	for p := 50051; p <= 59051; p++ {
		// Try to store port in map - returns false if already exists
		if _, loaded := portMap.LoadOrStore(p, true); !loaded {
			// Check if the port is already in use by another process
			conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", serverHost, p), time.Second)
			if err == nil {
				// Port is in use, close our test connection
				conn.Close()

				// Find the process using this port
				cmd := exec.Command("lsof", "-i", fmt.Sprintf(":%d", p), "-t")
				output, err := cmd.Output()
				if err != nil {
					// Failed to find process, continue to next port
					portMap.Delete(p)
					continue
				}

				// Get the PID
				pid := strings.TrimSpace(string(output))
				if pid == "" {
					// No process found, continue to next port
					portMap.Delete(p)
					continue
				}

				// Kill the process
				killCmd := exec.Command("kill", "-9", pid)
				err = killCmd.Run()
				if err != nil {
					logger.Warnf("Failed to kill process using port %d: %v", p, err)
					portMap.Delete(p)
					continue
				}

				logger.Infof("Killed process %s that was using port %d", pid, p)

				// Wait a moment for the port to be released
				time.Sleep(time.Second * 5)
			}
			return p, nil
		}
	}
	return 0, fmt.Errorf("no available ports found between 50051 and 59051")
}
