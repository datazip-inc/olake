package s3

import "github.com/piyushsingariya/relec"

type Config struct {
	Bucket string `json:"bucket" validate:"required"`
	Region string `json:"region" validate:"required"`
}

func (c *Config) Validate() error {
	return relec.Validate(c)
}
