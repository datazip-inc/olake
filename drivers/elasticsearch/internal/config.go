package driver

import "fmt"

type Config struct {
    Host  string `json:"host"`
    Index string `json:"index"`
}

func (c *Config) Validate() error {
    if c == nil {
        return nil
    }
    if c.Host == "" {
        return fmt.Errorf("host is required")
    }
    if c.Index == "" {
        return fmt.Errorf("index is required")
    }
    return nil
}
