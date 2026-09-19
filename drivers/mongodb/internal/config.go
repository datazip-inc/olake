package driver

import (
	"crypto/tls"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
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

// URI builds the MongoDB connection string from an already-validated config.
// It does not mutate Config: call Validate() first so AuthMechanism, AuthDB, AdditionalParams
// and defaults are already normalized — URI() only reads that state.
func (c *Config) URI() string {
	policy := mechanismPolicies[c.AuthMechanism]

	query := url.Values{}
	for key, value := range c.AdditionalParams {
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
	if c.SSLConfiguration != nil && c.SSLConfiguration.Mode != utils.SSLModeDisable {
		query.Set("tls", "true")
	}

	u := &url.URL{
		Scheme:   utils.Ternary(c.Srv, "mongodb+srv", "mongodb").(string),
		Host:     strings.Join(c.Hosts, ","),
		Path:     "/",
		RawQuery: query.Encode(),
	}

	switch {
	case c.Username == "" || policy.SkipUserinfo:
		// No userinfo. AWS credentials come from the environment; X509/OIDC may omit username.
	case c.Password == "" || policy.Password == passwordForbidden:
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
// It is the single write path for AuthMechanism, AuthDB and AdditionalParams; Setup() calls
// Validate() then URI().
func (c *Config) Validate() error {
	if len(c.Hosts) == 0 {
		return errs.Precondition(errs.ConfigInvalid, codeHostsMissing, fmt.Errorf("hosts is required"))
	}
	if c.Database == "" {
		return errs.Precondition(errs.ConfigInvalid, codeDatabaseMissing, fmt.Errorf("database is required"))
	}

	reserved, err := c.normalizeAdditionalParams()
	if err != nil {
		return err
	}

	mechanism, policy, err := c.resolveAuthMechanism(reserved)
	if err != nil {
		return err
	}
	c.AuthMechanism = mechanism
	delete(c.AdditionalParams, "authMechanism")

	switch {
	case policy.ExternalAuthDB:
		c.AuthDB = externalAuthDB
	case reserved["authSource"] != "":
		// additional_params.authSource is the legacy way to name the auth database; honor it
		// over the typed field so configs written before auth_mechanism existed keep working.
		logger.Warnf("mongodb: using additional_params.authSource %q as authdb", reserved["authSource"])
		c.AuthDB = reserved["authSource"]
	case c.AuthDB == "":
		return errs.Precondition(errs.ConfigInvalid, codeAuthDBMissing, fmt.Errorf("authdb is required"))
	}
	delete(c.AdditionalParams, "authSource")

	if c.SSLConfiguration == nil {
		c.SSLConfiguration = &utils.SSLConfig{
			Mode: utils.SSLModeDisable,
		}
	}
	if err := c.SSLConfiguration.Validate(); err != nil {
		return fmt.Errorf("failed to validate ssl config: %w", err)
	}

	// Effective TLS state. Inline SSL and SRV both imply it; an explicit additional_params
	// tls/ssl option decides it outright, and may not contradict inline SSL.
	inlineSSL := c.SSLConfiguration.Mode != utils.SSLModeDisable
	tlsEnabled := inlineSSL || c.Srv
	if raw, ok := reserved["tls"]; ok {
		explicit, parseErr := strconv.ParseBool(raw)
		if parseErr != nil {
			return errs.Precondition(errs.ConfigInvalid, codeAdditionalParamInvalid,
				fmt.Errorf("additional_params tls/ssl must be true or false, got %q", raw))
		}
		if inlineSSL && !explicit {
			return errs.Precondition(errs.ConfigInvalid, codeTLSConflict,
				fmt.Errorf("additional_params tls/ssl=false conflicts with ssl.mode=%s", c.SSLConfiguration.Mode))
		}
		tlsEnabled = explicit
	}

	// A client certificate can come from inline ssl.* PEM content or from a passthrough file
	// path. Inline SSL ships PEM content, not a file the driver container has on disk, so a
	// passthrough file-path option would point nowhere once inline SSL is active — only the
	// inline pair counts then, and the file options are dropped below rather than left to
	// reference files that were never provided.
	hasClientCert := reserved["tlsCertificateKeyFile"] != "" ||
		(reserved["tlsCertificateFile"] != "" && reserved["tlsPrivateKeyFile"] != "")
	if inlineSSL {
		hasClientCert = c.SSLConfiguration.ClientCert != "" && c.SSLConfiguration.ClientKey != ""
		for option := range tlsFileOptions {
			delete(c.AdditionalParams, option)
		}
	}

	if err := c.enforceAuthPolicy(policy, tlsEnabled, hasClientCert); err != nil {
		return err
	}
	if mechanism == AuthMechanismOIDC {
		if err := validateOIDCProperties(reserved["authMechanismProperties"]); err != nil {
			return err
		}
	}

	if c.MaxThreads <= 0 {
		c.MaxThreads = constants.DefaultThreadCount
	}
	if c.RetryCount <= 0 {
		c.RetryCount = constants.DefaultRetryCount
	}

	return errs.Precondition(errs.ConfigInvalid, codeConfigValidationFailed, utils.Validate(c))
}
