package driver

import (
	"crypto/tls"
	"fmt"
	"net/url"
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
	inlineSSL      bool
	tlsEnabled     bool
	hasClientCert  bool
	explicitTLS    bool
	explicitTLSSet bool
	explicitTLSKey string
}

var reservedAdditionalParamGroups = [][]string{
	{"authMechanism"},
	{"authSource"},
	{"authMechanismProperties"},
	{"tls", "ssl"},
	{"tlsCAFile", "sslCertificateAuthorityFile"},
	{"tlsCertificateKeyFile", "sslClientCertificateKeyFile"},
	{"tlsCertificateFile"},
	{"tlsPrivateKeyFile"},
}

// additionalParam returns one case-insensitive reserved parameter or rejects duplicates and aliases.
func additionalParam(params map[string]string, names ...string) (value string, found bool, err error) {
	for candidate, candidateValue := range params {
		matches := false
		for _, name := range names {
			if strings.EqualFold(candidate, name) {
				matches = true
				break
			}
		}
		if !matches {
			continue
		}
		if found {
			return "", false, fmt.Errorf("additional parameter %q is configured more than once", names[0])
		}
		value, found = candidateValue, true
	}
	return value, found, nil
}

func validateReservedAdditionalParams(params map[string]string) error {
	for _, names := range reservedAdditionalParamGroups {
		if _, _, err := additionalParam(params, names...); err != nil {
			return err
		}
	}
	return nil
}

// resolveMechanism reads the mechanism before Validate writes its canonical value.
func resolveMechanism(c *Config) (string, error) {
	if c.AuthMechanism != "" {
		return c.AuthMechanism, nil
	}
	mechanism, _, err := additionalParam(c.AdditionalParams, "authMechanism")
	return mechanism, err
}

// explicitTLSSetting parses an explicit tls/ssl override in additional_params.
func explicitTLSSetting(params map[string]string) (enabled bool, set bool, key string, err error) {
	raw, set, err := additionalParam(params, "tls", "ssl")
	if err != nil || !set {
		return false, set, "", err
	}
	enabled, err = strconv.ParseBool(raw)
	if err != nil {
		return false, false, "", fmt.Errorf("additional_params.tls must be true or false")
	}
	return enabled, true, "tls", nil
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
func computeTLSCaps(c *Config) (mongoTLSCaps, error) {
	explicitTLS, explicitTLSSet, explicitTLSKey, err := explicitTLSSetting(c.AdditionalParams)
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
	_, hasCombinedCertFile, err := additionalParam(
		c.AdditionalParams,
		"tlsCertificateKeyFile",
		"sslClientCertificateKeyFile",
	)
	if err != nil {
		return mongoTLSCaps{}, err
	}
	_, hasCertFile, err := additionalParam(c.AdditionalParams, "tlsCertificateFile")
	if err != nil {
		return mongoTLSCaps{}, err
	}
	_, hasKeyFile, err := additionalParam(c.AdditionalParams, "tlsPrivateKeyFile")
	if err != nil {
		return mongoTLSCaps{}, err
	}
	if hasCombinedCertFile || (hasCertFile && hasKeyFile) {
		caps.hasClientCert = true
	}
	return caps, nil
}

// enforceAuthPolicy checks the username, password, TLS, and certificate requirements.
func enforceAuthPolicy(c *Config, mechanism string, policy authPolicy, caps mongoTLSCaps) error {
	if policy.RequireUsername && c.Username == "" {
		return fmt.Errorf("username is required")
	}
	if policy.RequirePassword && c.Password == "" {
		if mechanism == "" {
			return fmt.Errorf("password is required")
		}
		return fmt.Errorf("password is required for %s", mechanism)
	}
	if policy.ForbidPassword && c.Password != "" {
		return fmt.Errorf("password must be empty for %s", mechanism)
	}
	if policy.RequireTLS && !caps.tlsEnabled {
		return fmt.Errorf("TLS is required for %s authentication", mechanism)
	}
	if policy.RequireClientCert && !caps.hasClientCert {
		return fmt.Errorf("a client certificate is required for MONGODB-X509")
	}
	return nil
}

func validateOIDCConfig(c *Config) error {
	raw, found, err := additionalParam(c.AdditionalParams, "authMechanismProperties")
	if err != nil {
		return err
	}
	if !found || raw == "" {
		return fmt.Errorf("MONGODB-OIDC requires additional_params.authMechanismProperties")
	}

	properties := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(pair, ":")
		if !ok || strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return fmt.Errorf("invalid MONGODB-OIDC authMechanismProperties entry %q", pair)
		}
		properties[strings.ToUpper(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}

	environment := strings.ToLower(properties["ENVIRONMENT"])
	if environment != "azure" && environment != "gcp" {
		return fmt.Errorf("MONGODB-OIDC ENVIRONMENT must be azure or gcp")
	}
	if properties["TOKEN_RESOURCE"] == "" {
		return fmt.Errorf("MONGODB-OIDC TOKEN_RESOURCE is required for %s", environment)
	}
	return nil
}

// URI builds the MongoDB connection string from an already-validated config.
// It does not mutate Config: call Validate() first so AuthMechanism, AuthDB, and defaults are set.
func (c *Config) URI() string {
	inlineSSL := inlineSSLEnabled(c)
	policy, _ := authPolicyFor(c.AuthMechanism)

	query := url.Values{}
	for key, value := range c.AdditionalParams {
		if inlineSSL && isTLSFileParam(key) {
			continue
		}
		if c.AuthDB != "" && strings.EqualFold(key, "authSource") {
			continue
		}
		if c.AuthMechanism != "" && strings.EqualFold(key, "authMechanism") {
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

	if err := validateReservedAdditionalParams(c.AdditionalParams); err != nil {
		return err
	}

	mechanism, err := resolveMechanism(c)
	if err != nil {
		return err
	}

	if c.UseIAM {
		if mechanism != "" && mechanism != AuthMechanismAWS {
			return fmt.Errorf("auth_mechanism cannot be set when use_iam is enabled; IAM authentication uses MONGODB-AWS")
		}
		mechanism = AuthMechanismAWS
	} else if mechanism == AuthMechanismAWS {
		return fmt.Errorf("MONGODB-AWS must be configured through use_iam in this connector")
	}

	if mechanism == AuthMechanismGSSAPI {
		return fmt.Errorf("GSSAPI is not supported by the OLake MongoDB connector")
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

	caps, err := computeTLSCaps(c)
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
		if err := validateOIDCConfig(c); err != nil {
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
