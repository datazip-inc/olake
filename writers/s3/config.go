package s3

import "github.com/piyushsingariya/relec"

type Config struct {
	Bucket    string `json:"bucket" validate:"required"`
	Region    string `json:"region" validate:"required"`
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`
}

func (c *Config) Validate() error {
	return relec.Validate(c)
}
