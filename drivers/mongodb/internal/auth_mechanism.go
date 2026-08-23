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
