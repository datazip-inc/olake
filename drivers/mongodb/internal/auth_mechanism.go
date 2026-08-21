package driver

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
	Supported         bool // exposed in spec.json enum (GSSAPI is false)
	ExternalAuthDB    bool // force authSource=$external
	RequireUsername   bool
	ForbidPassword    bool // no password in config or URI user:pass
	RequireTLS        bool
	RequireClientCert bool
	SkipUserinfo      bool // IAM: credentials from environment
}

// mechanismPolicies is the single source of truth for per-mechanism auth rules.
var mechanismPolicies = map[string]authPolicy{
	"": {
		RequireUsername: true,
	},
	AuthMechanismSCRAMSHA1: {
		Supported:       true,
		RequireUsername: true,
	},
	AuthMechanismSCRAMSHA256: {
		Supported:       true,
		RequireUsername: true,
	},
	AuthMechanismPLAIN: {
		Supported:       true,
		ExternalAuthDB:  true,
		RequireUsername: true,
		RequireTLS:      true,
	},
	AuthMechanismX509: {
		Supported:         true,
		ExternalAuthDB:    true,
		ForbidPassword:    true,
		RequireTLS:        true,
		RequireClientCert: true,
	},
	AuthMechanismOIDC: {
		Supported:      true,
		ExternalAuthDB: true,
		ForbidPassword: true,
	},
	AuthMechanismAWS: {
		ExternalAuthDB: true,
		SkipUserinfo:   true,
	},
	AuthMechanismGSSAPI: {
		Supported: false,
	},
}

// SupportedAuthMechanisms lists values exposed in spec.json auth_mechanism enum (excluding default "").
// Order matches spec.json; entries must have Supported=true in mechanismPolicies.
var SupportedAuthMechanisms = []string{
	AuthMechanismSCRAMSHA1,
	AuthMechanismSCRAMSHA256,
	AuthMechanismPLAIN,
	AuthMechanismX509,
	AuthMechanismOIDC,
}

// tlsFileParams are connection string file paths that conflict with inline SSL PEMs.
var tlsFileParams = []string{
	"tlsCAFile",
	"tlsCertificateKeyFile",
}

func authPolicyFor(mechanism string) (authPolicy, bool) {
	policy, ok := mechanismPolicies[mechanism]
	return policy, ok
}
