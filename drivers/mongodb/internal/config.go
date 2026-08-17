package driver

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
)

type Config struct {
	Hosts            []string          `json:"hosts"`
	Username         string            `json:"username"`
	Password         string            `json:"password"`
	AuthDB           string            `json:"authdb"`
	AuthMechanism    string            `json:"auth_mechanism"`
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

// applyAuthDefaults sets derived auth fields in one place so URI() and Validate()
// can read c.AuthMechanism / c.AuthDB / c.ReadPreference without per-field helpers.
func (c *Config) applyAuthDefaults() {
	if c.UseIAM {
		c.AuthMechanism = AuthMechanismAWS
	}
	if slices.Contains(externalAuthMechanisms, c.AuthMechanism) {
		c.AuthDB = "$external"
	}
	if c.ReplicaSet != "" && c.ReadPreference == "" {
		c.ReadPreference = constants.DefaultReadPreference
	}
}

func (c *Config) URI() string {
	c.applyAuthDefaults()

	connectionPrefix := "mongodb"
	if c.Srv {
		connectionPrefix = "mongodb+srv"
	}

	query := url.Values{}
	query.Set("authSource", c.AuthDB)
	if c.AuthMechanism != "" {
		query.Set("authMechanism", c.AuthMechanism)
	}

	if c.ReplicaSet != "" {
		query.Set("replicaSet", c.ReplicaSet)
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

	u := &url.URL{
		Scheme:   connectionPrefix,
		Host:     host,
		Path:     "/",
		RawQuery: query.Encode(),
	}

	switch {
	case c.AuthMechanism == AuthMechanismAWS:
		// IAM credentials come from the environment, not the URI userinfo.
	case slices.Contains(passwordlessAuthMechanisms, c.AuthMechanism):
		if c.Username != "" {
			u.User = url.User(c.Username)
		}
	default:
		u.User = utils.Ternary(c.Password != "", url.UserPassword(c.Username, c.Password), url.User(c.Username)).(*url.Userinfo)
	}

	return u.String()
}

func (c *Config) buildTLSConfig() (*tls.Config, error) {
	// Pass "" so we don't hardcode one hostname for TLS verify-full. The mongo
	// driver fills ServerName from whichever host this connection is dialing.
	return utils.BuildTLSConfig("", c.SSLConfiguration)
}

func (c *Config) Validate() error {
	if len(c.Hosts) == 0 {
		return fmt.Errorf("hosts is required")
	}

	if c.Database == "" {
		return fmt.Errorf("database is required")
	}

	if c.UseIAM && c.AuthMechanism != "" {
		return fmt.Errorf("auth_mechanism cannot be set when use_iam is enabled; IAM authentication uses MONGODB-AWS")
	}

	if !c.UseIAM {
		if c.Username == "" {
			return fmt.Errorf("username is required")
		}
		if c.AuthDB == "" {
			return fmt.Errorf("authdb is required")
		}
		// Password is optional — password-less URIs (X509, OIDC, username-only) are valid.
		// MongoDB rejects at connect time if the mechanism actually needs a password.
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

	c.applyAuthDefaults()
	return utils.Validate(c)
}
