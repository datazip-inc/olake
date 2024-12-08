package local

import "github.com/piyushsingariya/relec"

type Config struct {
	BaseFilePath string `json:"path"`
}

// TODO: Add go struct validation in Config
func (c *Config) Validate() error {
	return relec.Validate(c)
}
