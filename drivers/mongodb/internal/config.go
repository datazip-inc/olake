package driver

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Hosts            []string          `json:"hosts"`
	Username         string            `json:"username"`
	Password         string            `json:"password"`
	AuthDB           string            `json:"authdb"`
	ReplicaSet       string            `json:"replica_set"`
	ReadPreference   string            `json:"read_preference"`
	Srv              bool              `json:"srv"`
	ServerRAM        uint              `json:"server_ram"`
	MaxThreads       int               `json:"max_threads"`
	Database         string            `json:"database"`
	RetryCount       int               `json:"backoff_retry_count"`
	ChunkingStrategy string            `json:"chunking_strategy"`
	UseIAM           bool              `json:"use_iam"`
	SSLConfiguration *utils.SSLConfig  `json:"ssl"`
	SSHConfig        *utils.SSHConfig  `json:"ssh_config"`
	AdditionalParams map[string]string `json:"additional_params"`
}

func (c *Config) URI() string {
	connectionPrefix := "mongodb"
	if c.Srv {
		connectionPrefix = "mongodb+srv"
	}

	// Build query parameters
	query := url.Values{}

	if c.UseIAM {
		query.Set("authSource", "$external")
		query.Set("authMechanism", "MONGODB-AWS")
	} else {
		query.Set("authSource", c.AuthDB)
	}

	if c.ReplicaSet != "" {
		query.Set("replicaSet", c.ReplicaSet)
		if c.ReadPreference == "" {
			c.ReadPreference = constants.DefaultReadPreference
		}
		query.Set("readPreference", c.ReadPreference)
	}

	host := strings.Join(c.Hosts, ",")

	sslEnabled := c.SSLConfiguration != nil && c.SSLConfiguration.Mode != utils.SSLModeDisable
	for key, value := range c.AdditionalParams {
		if sslEnabled && (key == "tlsCAFile" || key == "tlsCertificateKeyFile") {
			continue
		}
		query.Set(key, value)
	}
	if sslEnabled && query.Get("tls") == "" {
		query.Set("tls", "true")
	}

	// Construct final URI using url.URL
	u := &url.URL{
		Scheme:   connectionPrefix,
		Host:     host,
		Path:     "/",
		RawQuery: query.Encode(),
	}

	if !c.UseIAM {
		u.User = utils.Ternary(c.Password != "", url.UserPassword(c.Username, c.Password), url.User(c.Username)).(*url.Userinfo)
	}

	return u.String()
}

func (c *Config) buildTLSConfig() (*tls.Config, error) {
	if c.SSLConfiguration == nil || c.SSLConfiguration.Mode == utils.SSLModeDisable {
		return nil, nil
	}
	return utils.BuildTLSConfig("", c.SSLConfiguration)
}

func (c *Config) Validate() error {
	if !c.Srv && len(c.Hosts) == 0 {
		return fmt.Errorf("hosts is required")
	}

	if c.Database == "" {
		return fmt.Errorf("database is required")
	}

	if !c.UseIAM {
		if c.Username == "" {
			return fmt.Errorf("username is required")
		}
		if c.Password == "" {
			return fmt.Errorf("password is required")
		}
		if c.AuthDB == "" {
			return fmt.Errorf("authdb is required")
		}
	}

	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}

	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	if c.SSLConfiguration == nil {
		c.SSLConfiguration = &utils.SSLConfig{
			Mode: utils.SSLModeDisable,
		}
	}

	if err := c.SSLConfiguration.Validate(); err != nil {
		return fmt.Errorf("failed to validate ssl config: %w", err)
	}

	if _, err := c.buildTLSConfig(); err != nil {
		return err
	}
	return utils.Validate(c)
}
