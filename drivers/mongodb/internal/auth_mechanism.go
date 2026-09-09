package driver

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/datazip-inc/olake/utils/errs"
)

// MongoDB authentication mechanism constants.
const (
	AuthMechanismSCRAMSHA1   = "SCRAM-SHA-1"
	AuthMechanismSCRAMSHA256 = "SCRAM-SHA-256"
	AuthMechanismPLAIN       = "PLAIN"
	AuthMechanismX509        = "MONGODB-X509"
	AuthMechanismOIDC        = "MONGODB-OIDC"
	AuthMechanismAWS         = "MONGODB-AWS"
	AuthMechanismGSSAPI      = "GSSAPI"

	externalAuthDB = "$external"
)

// authPolicy declares validation and URI rules for one MongoDB auth mechanism.
type authPolicy struct {
	ExternalAuthDB    bool // force authSource=$external
	RequireUsername   bool
	RequirePassword   bool
	ForbidPassword    bool // no password in config or URI user:pass
	RequireTLS        bool
	RequireClientCert bool
	SkipUserinfo      bool // IAM: credentials from environment
}

// mechanismPolicies is the single source of truth for per-mechanism auth rules.
var mechanismPolicies = map[string]authPolicy{
	"": {
		RequireUsername: true,
		RequirePassword: true,
	},
	AuthMechanismSCRAMSHA1: {
		RequireUsername: true,
		RequirePassword: true,
	},
	AuthMechanismSCRAMSHA256: {
		RequireUsername: true,
		RequirePassword: true,
	},
	AuthMechanismPLAIN: {
		ExternalAuthDB:  true,
		RequireUsername: true,
		RequirePassword: true,
		RequireTLS:      true,
	},
	AuthMechanismX509: {
		ExternalAuthDB:    true,
		ForbidPassword:    true,
		RequireTLS:        true,
		RequireClientCert: true,
	},
	AuthMechanismOIDC: {
		ExternalAuthDB: true,
		ForbidPassword: true,
	},
	AuthMechanismAWS: {
		ExternalAuthDB: true,
		SkipUserinfo:   true,
	},
}

// tlsFileParams are connection string file paths that conflict with inline SSL PEMs.
var tlsFileParams = []string{
	"tlsCAFile",
	"sslCertificateAuthorityFile",
	"tlsCertificateKeyFile",
	"sslClientCertificateKeyFile",
	"tlsCertificateFile",
	"tlsPrivateKeyFile",
}

func authPolicyFor(mechanism string) (authPolicy, bool) {
	policy, ok := mechanismPolicies[mechanism]
	return policy, ok
}

// reservedParams holds parsed additional_params keys that overlap with first-class config fields.
type reservedParams struct {
	AuthMechanism           string
	AuthMechanismProperties string
	TLS                     string
	hasAuthMechanism        bool
	hasAuthSource           bool
	hasAuthMechanismProps   bool
	hasTLS                  bool
	hasCAFile               bool
	hasCombinedCertFile     bool
	hasCertFile             bool
	hasKeyFile              bool
}

func (r reservedParams) explicitTLSSetting() (enabled bool, set bool, key string, err error) {
	if !r.hasTLS {
		return false, false, "", nil
	}
	enabled, err = strconv.ParseBool(r.TLS)
	if err != nil {
		return false, false, "", fmt.Errorf("additional_params.tls must be true or false")
	}
	return enabled, true, "tls", nil
}

func parseReservedParams(params map[string]string) (reservedParams, error) {
	var r reservedParams
	if params == nil {
		return r, nil
	}

	for key, value := range params {
		switch {
		case strings.EqualFold(key, "authMechanism"):
			if r.hasAuthMechanism {
				return r, fmt.Errorf("additional parameter %q is configured more than once", "authMechanism")
			}
			r.AuthMechanism = value
			r.hasAuthMechanism = true
		case strings.EqualFold(key, "authSource"):
			if r.hasAuthSource {
				return r, fmt.Errorf("additional parameter %q is configured more than once", "authSource")
			}
			r.hasAuthSource = true
		case strings.EqualFold(key, "authMechanismProperties"):
			if r.hasAuthMechanismProps {
				return r, fmt.Errorf("additional parameter %q is configured more than once", "authMechanismProperties")
			}
			r.AuthMechanismProperties = value
			r.hasAuthMechanismProps = true
		case strings.EqualFold(key, "tls"), strings.EqualFold(key, "ssl"):
			if r.hasTLS {
				return r, fmt.Errorf("additional parameter %q is configured more than once", "tls")
			}
			r.TLS = value
			r.hasTLS = true
		case strings.EqualFold(key, "tlsCAFile"), strings.EqualFold(key, "sslCertificateAuthorityFile"):
			if r.hasCAFile {
				return r, fmt.Errorf("additional parameter %q is configured more than once", "tlsCAFile")
			}
			r.hasCAFile = true
		case strings.EqualFold(key, "tlsCertificateKeyFile"), strings.EqualFold(key, "sslClientCertificateKeyFile"):
			if r.hasCombinedCertFile {
				return r, fmt.Errorf("additional parameter %q is configured more than once", "tlsCertificateKeyFile")
			}
			r.hasCombinedCertFile = true
		case strings.EqualFold(key, "tlsCertificateFile"):
			if r.hasCertFile {
				return r, fmt.Errorf("additional parameter %q is configured more than once", "tlsCertificateFile")
			}
			r.hasCertFile = true
		case strings.EqualFold(key, "tlsPrivateKeyFile"):
			if r.hasKeyFile {
				return r, fmt.Errorf("additional parameter %q is configured more than once", "tlsPrivateKeyFile")
			}
			r.hasKeyFile = true
		}
	}

	return r, nil
}

func resolveAuthMechanism(useIAM bool, fieldMechanism, legacyMechanism string) (string, error) {
	mechanism := fieldMechanism
	if mechanism == "" {
		mechanism = legacyMechanism
	}

	switch {
	case useIAM && mechanism != "" && mechanism != AuthMechanismAWS:
		return "", fmt.Errorf("auth_mechanism cannot be set when use_iam is enabled; IAM authentication uses MONGODB-AWS")
	case useIAM:
		return AuthMechanismAWS, nil
	case mechanism == AuthMechanismAWS:
		return "", fmt.Errorf("MONGODB-AWS must be configured through use_iam in this connector")
	case mechanism == AuthMechanismGSSAPI:
		return "", fmt.Errorf("GSSAPI is not supported by the OLake MongoDB connector")
	default:
		return mechanism, nil
	}
}

// enforceAuthPolicy checks the username, password, TLS, and certificate requirements.
func enforceAuthPolicy(c *Config, mechanism string, policy authPolicy, caps mongoTLSCaps) error {
	if policy.RequireUsername && c.Username == "" {
		return errs.Precondition(errs.ConfigInvalid, codeAuthUsernameRequired, fmt.Errorf("username is required"))
	}
	if policy.RequirePassword && c.Password == "" {
		if mechanism == "" {
			return errs.Precondition(errs.ConfigInvalid, codeAuthPasswordRequired, fmt.Errorf("password is required"))
		}
		return errs.Precondition(errs.ConfigInvalid, codeAuthPasswordRequired, fmt.Errorf("password is required for %s", mechanism))
	}
	if policy.ForbidPassword && c.Password != "" {
		return errs.Precondition(errs.ConfigInvalid, codeAuthPasswordForbidden, fmt.Errorf("password must be empty for %s", mechanism))
	}
	if policy.RequireTLS && !caps.tlsEnabled {
		return errs.Precondition(errs.ConfigInvalid, codeAuthTLSRequired, fmt.Errorf("TLS is required for %s authentication", mechanism))
	}
	if policy.RequireClientCert && !caps.hasClientCert {
		return errs.Precondition(errs.ConfigInvalid, codeAuthClientCertRequired, fmt.Errorf("a client certificate is required for MONGODB-X509"))
	}
	return nil
}

func validateOIDCProperties(raw string) error {
	if raw == "" {
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
