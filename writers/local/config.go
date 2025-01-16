package local

import (
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	BaseFilePath string `json:"path"`
}

// TODO: Add go struct validation in Config
func (c *Config) Validate() error {
	return utils.Validate(c)
}
