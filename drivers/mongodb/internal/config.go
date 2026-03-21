package driver

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/logger"
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
	SSHConfig        *utils.SSHConfig  `json:"ssh_config"`
	AdditionalParams map[string]string `json:"additional_params"`
	AuthMechanism    AuthMechanism     `json:"auth_mechanism"`
	TLSConfig        TLSConfig         `json:"tls_config"`
}

func (c *Config) URI() string {
	connectionPrefix := "mongodb"
	if c.Srv {
		connectionPrefix = "mongodb+srv"
	}

	if c.MaxThreads == 0 {
		// set default threads
		logger.Info("setting max threads to default[%d]", constants.DefaultThreadCount)
		c.MaxThreads = constants.DefaultThreadCount
	}

	// Build query parameters
	query := url.Values{}

	if c.AuthMechanism != "" {
		query.Set("authMechanism", string(c.AuthMechanism))
	}

	switch c.AuthMechanism {
	case AuthMechanismX509, AuthMechanismPLAINTEXT, AuthMechanismGSSAPI, AuthMechanismMONGOAWS:
		query.Set("authSource", "$external")
	default:
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

	for key, value := range c.AdditionalParams {
		query.Set(key, value)
	}

	// Construct final URI using url.URL
	u := &url.URL{
		Scheme:   connectionPrefix,
		Host:     host,
		Path:     "/",
		RawQuery: query.Encode(),
	}

	skipUser := c.AuthMechanism == AuthMechanismX509 ||
		c.AuthMechanism == AuthMechanismPLAINTEXT ||
		c.AuthMechanism == AuthMechanismGSSAPI ||
		c.AuthMechanism == AuthMechanismMONGOAWS

	if !skipUser {
		u.User = utils.Ternary(c.Password != "", url.UserPassword(c.Username, c.Password), url.User(c.Username)).(*url.Userinfo)
	}

	return u.String()
}

// TODO: Add go struct validation in Config
func (c *Config) Validate() error {
	if err := utils.Validate(c); err != nil {
		return err
	}

	// Validations for specific Auth Mechanisms
	switch c.AuthMechanism {
	case AuthMechanismMONGODBCR:
		logger.Warn("MONGODB-CR is deprecated, consider using SCRAM-SHA-256")
	case AuthMechanismX509:
		if c.Username == "" {
			return fmt.Errorf("username is required for X.509 authentication")
		}
		if c.Password != "" {
			logger.Warn("password is ignored for X.509 authentication")
		}
	case AuthMechanismSCRAMSHA1, AuthMechanismSCRAMSHA256, "":
		if c.Username == "" {
			return fmt.Errorf("username is required for %s authentication", c.AuthMechanism)
		}
		if c.Password == "" {
			return fmt.Errorf("password is required for %s authentication", c.AuthMechanism)
		}
	}

	// TLS validation
	if err := c.TLSConfig.Validate(); err != nil {
		return err
	}

	return nil
}
