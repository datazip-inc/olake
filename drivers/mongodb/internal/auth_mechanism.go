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
)

// externalAuthMechanisms lists MongoDB auth mechanisms that require authSource="$external".
var externalAuthMechanisms = []string{
	AuthMechanismPLAIN,
	AuthMechanismX509,
	AuthMechanismOIDC,
	AuthMechanismAWS,
}

// passwordlessAuthMechanisms lists mechanisms that do not use a password in the connection URI.
var passwordlessAuthMechanisms = []string{
	AuthMechanismX509,
	AuthMechanismOIDC,
}

// SupportedAuthMechanisms lists values exposed in spec.json auth_mechanism enum (excluding default "").
// GSSAPI is intentionally excluded due to very low market adoption
var SupportedAuthMechanisms = []string{
	AuthMechanismSCRAMSHA1,
	AuthMechanismSCRAMSHA256,
	AuthMechanismPLAIN,
	AuthMechanismX509,
	AuthMechanismOIDC,
}
