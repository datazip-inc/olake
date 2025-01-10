package local

import "github.com/piyushsingariya/relec"

type Config struct {
	// Local file path (for local file system usage)
	Path      string `json:"path"`
	Bucket    string `json:"s3_bucket" validate:"omitempty"`
	Region    string `json:"s3_region" validate:"omitempty"`
	AccessKey string `json:"s3_access_key,omitempty"`
	SecretKey string `json:"s3_secret_key,omitempty"`
	Prefix    string `json:"s3_prefix,omitempty"`
}

func (c *Config) Validate() error {
	return relec.Validate(c)
}
