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
	inlineSSL      bool
	tlsEnabled     bool
	hasClientCert  bool
	explicitTLS    bool
	explicitTLSSet bool
	explicitTLSKey string
}

func isTLSFileParam(key string) bool {
	for _, candidate := range tlsFileParams {
		if strings.EqualFold(candidate, key) {
			return true
		}
	}
	return false
}

func inlineSSLEnabled(c *Config) bool {
	return c.SSLConfiguration != nil && c.SSLConfiguration.Mode != utils.SSLModeDisable
}

// computeTLSCaps resolves the effective TLS state from SSLConfiguration, SRV, and additional_params.
func computeTLSCaps(c *Config, reserved reservedParams) (mongoTLSCaps, error) {
	explicitTLS, explicitTLSSet, explicitTLSKey, err := reserved.explicitTLSSetting()
	if err != nil {
		return mongoTLSCaps{}, err
	}

	inlineSSL := inlineSSLEnabled(c)
	caps := mongoTLSCaps{
		inlineSSL:      inlineSSL,
		tlsEnabled:     inlineSSL || c.Srv,
		explicitTLS:    explicitTLS,
		explicitTLSSet: explicitTLSSet,
		explicitTLSKey: explicitTLSKey,
	}
	if explicitTLSSet {
		caps.tlsEnabled = explicitTLS
	}
	if inlineSSL &&
		c.SSLConfiguration.ClientCert != "" &&
		c.SSLConfiguration.ClientKey != "" {
		caps.hasClientCert = true
	}
	if reserved.hasCombinedCertFile || (reserved.hasCertFile && reserved.hasKeyFile) {
		caps.hasClientCert = true
	}
	return caps, nil
}

func skipAdditionalParamKey(key string, inlineSSL bool, authDB, authMechanism string) bool {
	if inlineSSL && isTLSFileParam(key) {
		return true
	}
	if authDB != "" && strings.EqualFold(key, "authSource") {
		return true
	}
	if authMechanism != "" && strings.EqualFold(key, "authMechanism") {
		return true
	}
	return false
}

// URI builds the MongoDB connection string from an already-validated config.
// It does not mutate Config: call Validate() first so AuthMechanism, AuthDB, and defaults are set.
func (c *Config) URI() string {
	inlineSSL := inlineSSLEnabled(c)
	policy, _ := authPolicyFor(c.AuthMechanism)

	query := url.Values{}
	for key, value := range c.AdditionalParams {
		if skipAdditionalParamKey(key, inlineSSL, c.AuthDB, c.AuthMechanism) {
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
	if inlineSSL {
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

	reserved, err := parseReservedParams(c.AdditionalParams)
	if err != nil {
		return err
	}

	mechanism, err := resolveAuthMechanism(c.UseIAM, c.AuthMechanism, reserved.AuthMechanism)
	if err != nil {
		return err
	}

	policy, known := authPolicyFor(mechanism)
	if !known {
		return fmt.Errorf("unsupported auth_mechanism %q", mechanism)
	}

	c.AuthMechanism = mechanism
	if c.AdditionalParams != nil {
		for key := range c.AdditionalParams {
			if strings.EqualFold(key, "authMechanism") {
				delete(c.AdditionalParams, key)
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

	caps, err := computeTLSCaps(c, reserved)
	if err != nil {
		return err
	}
	if caps.inlineSSL && caps.explicitTLSSet && !caps.explicitTLS {
		return fmt.Errorf("additional_params.%s=false conflicts with enabled ssl configuration", caps.explicitTLSKey)
	}

	if err := enforceAuthPolicy(c, mechanism, policy, caps); err != nil {
		return err
	}
	if mechanism == AuthMechanismOIDC {
		if err := validateOIDCProperties(reserved.AuthMechanismProperties); err != nil {
			return err
		}
	}

	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}
	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	return utils.Validate(c)
}
