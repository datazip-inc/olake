package driver

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"slices"
	"strconv"
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

type mongoTLSCaps struct {
	inlineSSL     bool
	tlsEnabled    bool
	hasClientCert bool
}
// read mechanism before Validate writes it
func resolveMechanism(c *Config) string {
	if c.AuthMechanism != "" {
		return c.AuthMechanism
	}
	if c.AdditionalParams != nil {
		return c.AdditionalParams["authMechanism"]
	}
	return ""
}
// parse tls=true / ssl=true in additional_params
func additionalParamTrue(params map[string]string, key string) bool {
	if params == nil {
		return false
	}
	value, ok := params[key]
	if !ok {
		return false
	}
	enabled, err := strconv.ParseBool(value)
	return err == nil && enabled
}

// computes the TLS capabilities of the configuration by checking the SSLConfiguration and additional_params
func computeTLSCaps(c *Config) mongoTLSCaps {
	inlineSSL := c.SSLConfiguration != nil && c.SSLConfiguration.Mode != utils.SSLModeDisable
	caps := mongoTLSCaps{
		inlineSSL:  inlineSSL,
		tlsEnabled: inlineSSL || c.Srv,
	}
	if !caps.tlsEnabled {
		caps.tlsEnabled = additionalParamTrue(c.AdditionalParams, "tls") ||
			additionalParamTrue(c.AdditionalParams, "ssl")
	}
	if c.SSLConfiguration != nil &&
		c.SSLConfiguration.ClientCert != "" &&
		c.SSLConfiguration.ClientKey != "" {
		caps.hasClientCert = true
	} else if c.AdditionalParams != nil && c.AdditionalParams["tlsCertificateKeyFile"] != "" {
		caps.hasClientCert = true
	}
	return caps
}

// enforces the authentication policy by checking the username, password, TLS capabilities, and client certificate
func enforceAuthPolicy(c *Config, mechanism string, policy authPolicy, caps mongoTLSCaps) error {
	if policy.RequireUsername && c.Username == "" {
		return fmt.Errorf("username is required")
	}
	if policy.ForbidPassword && c.Password != "" {
		switch mechanism {
		case AuthMechanismX509:
			return fmt.Errorf("password must be empty for MONGODB-X509")
		case AuthMechanismOIDC:
			return fmt.Errorf("password must be empty for MONGODB-OIDC")
		default:
			return fmt.Errorf("password must be empty for %s", mechanism)
		}
	}
	if policy.RequireTLS && !caps.tlsEnabled {
		switch mechanism {
		case AuthMechanismPLAIN:
			return fmt.Errorf("TLS is required for PLAIN authentication")
		case AuthMechanismX509:
			return fmt.Errorf("TLS is required for MONGODB-X509")
		default:
			return fmt.Errorf("TLS is required for %s", mechanism)
		}
	}
	if policy.RequireClientCert && !caps.hasClientCert {
		return fmt.Errorf("a client certificate is required for MONGODB-X509")
	}
	return nil
}

// URI builds the MongoDB connection string from an already-validated config.
// It does not mutate Config: call Validate() first so AuthMechanism, AuthDB, and defaults are set.
func (c *Config) URI() string {
	caps := computeTLSCaps(c)
	policy, _ := authPolicyFor(c.AuthMechanism)

	query := url.Values{}
	for key, value := range c.AdditionalParams {
		if caps.inlineSSL && slices.Contains(tlsFileParams, key) {
			continue
		}
		query.Set(key, value)
	}
	if c.AuthDB != "" {
		query.Set("authSource", c.AuthDB)
	}
	if c.AuthMechanism != "" {
		query.Set("authMechanism", c.AuthMechanism)
	}
	if c.ReplicaSet != "" {
		query.Set("replicaSet", c.ReplicaSet)
		query.Set("readPreference", utils.Ternary(c.ReadPreference != "", c.ReadPreference, constants.DefaultReadPreference).(string))
	}
	if caps.inlineSSL {
		query.Set("tls", "true")
	}

	scheme := "mongodb"
	if c.Srv {
		scheme = "mongodb+srv"
	}

	u := &url.URL{
		Scheme:   scheme,
		Host:     strings.Join(c.Hosts, ","),
		Path:     "/",
		RawQuery: query.Encode(),
	}

	switch {
	case c.Username == "" || policy.SkipUserinfo:
		// No userinfo. AWS credentials come from the environment; X509/OIDC may omit username.
	case c.Password == "" || policy.ForbidPassword:
		u.User = url.User(c.Username)
	default:
		u.User = url.UserPassword(c.Username, c.Password)
	}

	return u.String()
}

func (c *Config) buildTLSConfig() (*tls.Config, error) {
	// Pass "" so we don't hardcode one hostname for TLS verify-full. The mongo
	// driver fills ServerName from whichever host this connection is dialing.
	return utils.BuildTLSConfig("", c.SSLConfiguration)
}

// Validate normalizes auth fields, applies defaults, and checks mechanism-specific rules.
// It is the single write path for AuthMechanism and AuthDB; Setup() calls Validate() then URI().
func (c *Config) Validate() error {
	if len(c.Hosts) == 0 {
		return fmt.Errorf("hosts is required")
	}
	if c.Database == "" {
		return fmt.Errorf("database is required")
	}

	mechanism := resolveMechanism(c)

	if c.UseIAM {
		if mechanism != "" && mechanism != AuthMechanismAWS {
			return fmt.Errorf("auth_mechanism cannot be set when use_iam is enabled; IAM authentication uses MONGODB-AWS")
		}
		mechanism = AuthMechanismAWS
	} else if mechanism == AuthMechanismAWS {
		return fmt.Errorf("MONGODB-AWS must be configured through use_iam in this connector")
	}

	if mechanism == AuthMechanismGSSAPI {
		return fmt.Errorf("GSSAPI is not supported due to low market adoption")
	}

	policy, known := authPolicyFor(mechanism)
	if !known {
		return fmt.Errorf("unsupported auth_mechanism %q", mechanism)
	}
	if mechanism != "" && mechanism != AuthMechanismAWS && !policy.Supported {
		return fmt.Errorf("unsupported auth_mechanism %q", mechanism)
	}

	c.AuthMechanism = mechanism
	if c.AdditionalParams != nil {
		if _, ok := c.AdditionalParams["authMechanism"]; ok {
			if mechanism == "" {
				delete(c.AdditionalParams, "authMechanism")
			} else {
				c.AdditionalParams["authMechanism"] = mechanism
			}
		}
	}

	if policy.ExternalAuthDB {
		c.AuthDB = externalAuthDB
	} else if c.AuthDB == "" {
		return fmt.Errorf("authdb is required")
	}

	if c.SSLConfiguration == nil {
		c.SSLConfiguration = &utils.SSLConfig{
			Mode: utils.SSLModeDisable,
		}
	}
	if err := c.SSLConfiguration.Validate(); err != nil {
		return fmt.Errorf("failed to validate ssl config: %w", err)
	}

	caps := computeTLSCaps(c)
	if caps.inlineSSL {
		for _, key := range []string{"tls", "ssl"} {
			value, ok := c.AdditionalParams[key]
			if !ok {
				continue
			}
			enabled, err := strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("additional_params.%s must be true or false", key)
			}
			if !enabled {
				return fmt.Errorf("additional_params.%s=false conflicts with enabled ssl configuration", key)
			}
		}
	}

	if err := enforceAuthPolicy(c, mechanism, policy, caps); err != nil {
		return err
	}

	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}
	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	return utils.Validate(c)
}
