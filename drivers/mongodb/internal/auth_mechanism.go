package driver

import (
	"fmt"
	"strings"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/datazip-inc/olake/utils/logger"
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

// tlsRequirement is how much transport security a mechanism needs. The levels are ordered:
// a client certificate implies TLS, so one field covers both checks and the two cannot
// disagree the way separate booleans could.
type tlsRequirement int

const (
	tlsOptional   tlsRequirement = iota // mechanism works with or without TLS
	tlsRequired                         // encrypted transport, server-authenticated only
	tlsClientCert                       // encrypted transport plus a client certificate
)

// passwordRule is how a mechanism treats Config.Password. The three states are mutually
// exclusive, which separate require/forbid booleans could not express.
type passwordRule int

const (
	passwordOptional passwordRule = iota
	passwordRequired
	passwordForbidden
)

// authPolicy declares validation and URI rules for one MongoDB auth mechanism.
type authPolicy struct {
	ExternalAuthDB  bool // force authSource=$external
	RequireUsername bool
	Password        passwordRule
	TLS             tlsRequirement
	SkipUserinfo    bool // IAM: credentials from environment
}

// scramPolicy covers the negotiated default ("") and both explicit SCRAM mechanisms:
// SCRAM always authenticates with a username and password.
var scramPolicy = authPolicy{RequireUsername: true, Password: passwordRequired}

// mechanismPolicies is the single source of truth for per-mechanism auth rules.
var mechanismPolicies = map[string]authPolicy{
	// Negotiated default: the driver negotiates SCRAM, so the same rules apply as the
	// explicit SCRAM entries below.
	"":                       scramPolicy,
	AuthMechanismSCRAMSHA1:   scramPolicy,
	AuthMechanismSCRAMSHA256: scramPolicy,
	AuthMechanismPLAIN: {
		ExternalAuthDB:  true,
		RequireUsername: true,
		Password:        passwordRequired,
		TLS:             tlsRequired,
	},
	AuthMechanismX509: {
		ExternalAuthDB: true,
		Password:       passwordForbidden,
		TLS:            tlsClientCert,
	},
	AuthMechanismOIDC: {
		ExternalAuthDB: true,
		Password:       passwordForbidden,
	},
	AuthMechanismAWS: {
		ExternalAuthDB: true,
		SkipUserinfo:   true,
	},
}

// reservedAliases maps every accepted spelling of a MongoDB connection-string option that
// overlaps a first-class Config field, or needs special handling, onto one canonical name.
// Lookup is lowercased because MongoDB treats connection-string option names case-insensitively.
var reservedAliases = map[string]string{
	"authmechanism":               "authMechanism",
	"authsource":                  "authSource",
	"authmechanismproperties":     "authMechanismProperties",
	"tls":                         "tls",
	"ssl":                         "tls",
	"tlscafile":                   "tlsCAFile",
	"sslcertificateauthorityfile": "tlsCAFile",
	"tlscertificatekeyfile":       "tlsCertificateKeyFile",
	"sslclientcertificatekeyfile": "tlsCertificateKeyFile",
	"tlscertificatefile":          "tlsCertificateFile",
	"tlsprivatekeyfile":           "tlsPrivateKeyFile",
}

// tlsFileOptions are the canonical reserved keys that name a certificate/key file on disk.
// Inline ssl.* ships PEM content instead, so these conflict with it and are dropped whenever
// inline SSL is active — see (*Config).Validate.
var tlsFileOptions = map[string]bool{
	"tlsCAFile":             true,
	"tlsCertificateKeyFile": true,
	"tlsCertificateFile":    true,
	"tlsPrivateKeyFile":     true,
}

// normalizeAdditionalParams canonicalizes any additional_params key that aliases a reserved
// MongoDB connection-string option — case-insensitively, and across documented aliases such as
// tls/ssl — rejects the same option being set twice under different spellings, and returns the
// reserved subset for the caller to resolve. c.AdditionalParams is rewritten in place to
// canonical spelling; everything else is left untouched.
func (c *Config) normalizeAdditionalParams() (map[string]string, error) {
	reserved := make(map[string]string)
	if c.AdditionalParams == nil {
		return reserved, nil
	}

	canonicalized := make(map[string]string, len(c.AdditionalParams))
	for key, value := range c.AdditionalParams {
		canonical, isReserved := reservedAliases[strings.ToLower(key)]
		if !isReserved {
			canonicalized[key] = value
			continue
		}
		if _, duplicate := reserved[canonical]; duplicate {
			return nil, errs.Precondition(errs.ConfigInvalid, codeAdditionalParamDuplicate,
				fmt.Errorf("additional parameter %q is configured more than once", canonical))
		}
		reserved[canonical] = value
		canonicalized[canonical] = value
	}
	c.AdditionalParams = canonicalized
	return reserved, nil
}

// resolveAuthMechanism picks the effective auth mechanism from, in order: the typed
// auth_mechanism field, the legacy additional_params.authMechanism escape hatch, and the
// use_iam toggle (equivalent to selecting MONGODB-AWS). It does not mutate c; the caller
// assigns the result.
func (c *Config) resolveAuthMechanism(reserved map[string]string) (string, authPolicy, error) {
	mechanism := c.AuthMechanism
	if mechanism == "" {
		mechanism = reserved["authMechanism"]
	}
	mechanism = strings.ToUpper(strings.TrimSpace(mechanism))

	switch {
	case c.UseIAM && mechanism != "" && mechanism != AuthMechanismAWS:
		return "", authPolicy{}, errs.Precondition(errs.ConfigInvalid, codeAuthMechanismConflict,
			fmt.Errorf("auth_mechanism cannot be set when use_iam is enabled; IAM authentication uses MONGODB-AWS"))
	case c.UseIAM:
		mechanism = AuthMechanismAWS
	case mechanism == AuthMechanismGSSAPI:
		// Not supported: GSSAPI (Kerberos) sees low adoption among OLake's MongoDB users.
		// It also requires CGO_ENABLED=1 and a mongo-driver build tag OLake does not set,
		// but adoption is the reason it is not planned, not the build constraint.
		return "", authPolicy{}, errs.Precondition(errs.ConfigInvalid, codeAuthMechanismUnsupported,
			fmt.Errorf("GSSAPI is not supported by the OLake MongoDB connector"))
	}

	policy, known := mechanismPolicies[mechanism]
	if !known {
		return "", authPolicy{}, errs.Precondition(errs.ConfigInvalid, codeAuthMechanismUnsupported,
			fmt.Errorf("unsupported auth_mechanism %q", mechanism))
	}
	return mechanism, policy, nil
}

// enforceAuthPolicy checks the username, password, TLS, and certificate requirements for the
// already-resolved mechanism (c.AuthMechanism).
func (c *Config) enforceAuthPolicy(policy authPolicy, tlsEnabled, hasClientCert bool) error {
	mechanism := c.AuthMechanism
	// MONGODB-AWS reads credentials from the environment, so anything configured here is
	// dropped from the URI — say so rather than failing later with an opaque auth error.
	if policy.SkipUserinfo && (c.Username != "" || c.Password != "") {
		logger.Warnf("mongodb: username and password are ignored for %s; credentials come from the environment", mechanism)
	}
	switch {
	case policy.RequireUsername && c.Username == "":
		return errs.Precondition(errs.ConfigInvalid, codeAuthUsernameRequired, fmt.Errorf("username is required"))
	case policy.Password == passwordRequired && c.Password == "":
		if mechanism == "" {
			return errs.Precondition(errs.ConfigInvalid, codeAuthPasswordRequired, fmt.Errorf("password is required"))
		}
		return errs.Precondition(errs.ConfigInvalid, codeAuthPasswordRequired, fmt.Errorf("password is required for %s", mechanism))
	case policy.Password == passwordForbidden && c.Password != "":
		return errs.Precondition(errs.ConfigInvalid, codeAuthPasswordForbidden, fmt.Errorf("password must be empty for %s", mechanism))
	case policy.TLS >= tlsRequired && !tlsEnabled:
		return errs.Precondition(errs.ConfigInvalid, codeAuthTLSRequired, fmt.Errorf("TLS is required for %s authentication", mechanism))
	case policy.TLS == tlsClientCert && !hasClientCert:
		return errs.Precondition(errs.ConfigInvalid, codeAuthClientCertRequired, fmt.Errorf("a client certificate is required for MONGODB-X509"))
	}
	return nil
}

// validateOIDCProperties checks additional_params.authMechanismProperties against what
// MONGODB-OIDC requires for the Azure/GCP URI flows OLake supports.
func validateOIDCProperties(raw string) error {
	if raw == "" {
		return errs.Precondition(errs.ConfigInvalid, codeOIDCPropertiesInvalid,
			fmt.Errorf("MONGODB-OIDC requires additional_params.authMechanismProperties"))
	}

	properties := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(pair, ":")
		if !ok || strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return errs.Precondition(errs.ConfigInvalid, codeOIDCPropertiesInvalid,
				fmt.Errorf("invalid MONGODB-OIDC authMechanismProperties entry %q", pair))
		}
		properties[strings.ToUpper(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}

	environment := strings.ToLower(properties["ENVIRONMENT"])
	if environment != "azure" && environment != "gcp" {
		return errs.Precondition(errs.ConfigInvalid, codeOIDCPropertiesInvalid,
			fmt.Errorf("MONGODB-OIDC ENVIRONMENT must be azure or gcp"))
	}
	if properties["TOKEN_RESOURCE"] == "" {
		return errs.Precondition(errs.ConfigInvalid, codeOIDCPropertiesInvalid,
			fmt.Errorf("MONGODB-OIDC TOKEN_RESOURCE is required for %s", environment))
	}
	return nil
}
