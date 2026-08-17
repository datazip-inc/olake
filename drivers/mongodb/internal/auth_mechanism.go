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
// This slice is independent of downstream logic and should include all such mechanisms.
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
// GSSAPI is intentionally excluded due to very low market adoption, not because OLake cannot build
// with Kerberos support (see drivers.mk for CGO/GSSAPI build configuration).
var SupportedAuthMechanisms = []string{
	AuthMechanismSCRAMSHA1,
	AuthMechanismSCRAMSHA256,
	AuthMechanismPLAIN,
	AuthMechanismX509,
	AuthMechanismOIDC,
}
