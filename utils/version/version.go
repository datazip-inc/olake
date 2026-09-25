package version

import "os"

// GetOlakeCLIVersion() extracts the olake version from the ENV embedded in the olake image
func GetOlakeCLIVersion() string {
	version := os.Getenv("DRIVER_VERSION")
	if version == "" {
		return "Not Available"
	}
	return version
}
