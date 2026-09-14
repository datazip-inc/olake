package driver

import (
	"strings"
	"testing"

	"github.com/datazip-inc/olake/constants"
	"github.com/datazip-inc/olake/utils"
	"github.com/datazip-inc/olake/utils/testutils"
)

// baseConfig returns a valid SCRAM-style config for tests to override.
func baseConfig() *Config {
	return &Config{
		Hosts:    []string{"mongo.example.com:27017"},
		Database: "testdb",
		Username: "user",
		Password: "pass",
		AuthDB:   "admin",
	}
}

func validateAndURI(t *testing.T, config *Config) string {
	t.Helper()
	if err := config.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	return config.URI()
}

func additionalParamsContains(params map[string]string, name string) bool {
	for key := range params {
		if strings.EqualFold(key, name) {
			return true
		}
	}
	return false
}

// TestConfig_ValidateThenURI exercises the full Validate() → URI() pipeline, matching how
// Setup() actually calls them (mon.go). Every case asserts the exact final URI.
func TestConfig_ValidateThenURI(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	tests := []struct {
		name    string
		config  *Config
		wantURI string
	}{
		{
			name: "strips tls file params when ssl enabled",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				Database: "testdb",
				AuthDB:   "admin",
				Username: "user",
				Password: "pass",
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyCA,
					ServerCA: certs.CACert,
				},
				AdditionalParams: map[string]string{
					"tls":                   "true",
					"tlsCAFile":             "/certs/root-ca.crt",
					"tlsCertificateKeyFile": "/certs/client.pem",
					"connectTimeoutMS":      "5000",
				},
			},
			wantURI: "mongodb://user:pass@mongo.example.com:27017/?authSource=admin&connectTimeoutMS=5000&tls=true",
		},
		{
			name: "preserves tls file params without ssl",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				Database: "testdb",
				AuthDB:   "admin",
				Username: "user",
				Password: "pass",
				AdditionalParams: map[string]string{
					"tls":       "true",
					"tlsCAFile": "/certs/root-ca.crt",
				},
			},
			wantURI: "mongodb://user:pass@mongo.example.com:27017/?authSource=admin&tls=true&tlsCAFile=%2Fcerts%2Froot-ca.crt",
		},
		{
			name: "adds tls when ssl enabled",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				Database: "testdb",
				AuthDB:   "admin",
				Username: "user",
				Password: "pass",
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyCA,
					ServerCA: certs.CACert,
				},
			},
			wantURI: "mongodb://user:pass@mongo.example.com:27017/?authSource=admin&tls=true",
		},
		{
			name: "explicit SCRAM-SHA-1",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthDB:        "admin",
				Username:      "user",
				Password:      "pass",
				AuthMechanism: AuthMechanismSCRAMSHA1,
			},
			wantURI: "mongodb://user:pass@localhost:27017/?authMechanism=SCRAM-SHA-1&authSource=admin",
		},
		{
			name: "explicit SCRAM-SHA-256",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthDB:        "admin",
				Username:      "user",
				Password:      "pass",
				AuthMechanism: AuthMechanismSCRAMSHA256,
			},
			wantURI: "mongodb://user:pass@localhost:27017/?authMechanism=SCRAM-SHA-256&authSource=admin",
		},
		{
			name: "PLAIN uses external auth source",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthDB:        "admin",
				Username:      "user",
				Password:      "pass",
				AuthMechanism: AuthMechanismPLAIN,
				AdditionalParams: map[string]string{
					"tls": "true",
				},
			},
			wantURI: "mongodb://user:pass@localhost:27017/?authMechanism=PLAIN&authSource=%24external&tls=true",
		},
		{
			name: "MONGODB-X509 without password via auth_mechanism",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				Username:      "CN=user",
				AuthMechanism: AuthMechanismX509,
				AdditionalParams: map[string]string{
					"tls":                   "true",
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			wantURI: "mongodb://CN=user@localhost:27017/?authMechanism=MONGODB-X509&authSource=%24external&tls=true&tlsCertificateKeyFile=%2Fcerts%2Fclient.pem",
		},
		{
			name: "MONGODB-X509 without username has no userinfo",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthMechanism: AuthMechanismX509,
				AdditionalParams: map[string]string{
					"tls":                   "true",
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			wantURI: "mongodb://localhost:27017/?authMechanism=MONGODB-X509&authSource=%24external&tls=true&tlsCertificateKeyFile=%2Fcerts%2Fclient.pem",
		},
		{
			name: "MONGODB-OIDC without password via auth_mechanism",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				Username:      "oidc-user",
				AuthMechanism: AuthMechanismOIDC,
				AdditionalParams: map[string]string{
					"authMechanismProperties": "ENVIRONMENT:gcp,TOKEN_RESOURCE:https://example.com",
				},
			},
			wantURI: "mongodb://oidc-user@localhost:27017/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT%3Agcp%2CTOKEN_RESOURCE%3Ahttps%3A%2F%2Fexample.com&authSource=%24external",
		},
		{
			name: "IAM authentication via use_iam",
			config: &Config{
				Hosts:    []string{"cluster.mongodb.net"},
				Database: "testdb",
				UseIAM:   true,
			},
			wantURI: "mongodb://cluster.mongodb.net/?authMechanism=MONGODB-AWS&authSource=%24external",
		},
		{
			// New: decision to add MONGODB-AWS to the auth_mechanism dropdown (PR #1045)
			// alongside the existing use_iam toggle — both must resolve identically.
			name: "IAM authentication via auth_mechanism dropdown",
			config: &Config{
				Hosts:         []string{"cluster.mongodb.net"},
				Database:      "testdb",
				AuthMechanism: AuthMechanismAWS,
			},
			wantURI: "mongodb://cluster.mongodb.net/?authMechanism=MONGODB-AWS&authSource=%24external",
		},
		{
			// The legacy additional_params.authMechanism escape hatch must also accept AWS
			// now that it is a supported mechanism, not just an error redirecting to use_iam.
			name: "IAM authentication via legacy additional_params",
			config: &Config{
				Hosts:    []string{"cluster.mongodb.net"},
				Database: "testdb",
				AdditionalParams: map[string]string{
					"authMechanism": AuthMechanismAWS,
				},
			},
			wantURI: "mongodb://cluster.mongodb.net/?authMechanism=MONGODB-AWS&authSource=%24external",
		},
		{
			name: "typed authMechanism overwrites additional_params",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthDB:        "admin",
				Username:      "user",
				Password:      "pass",
				AuthMechanism: AuthMechanismSCRAMSHA256,
				AdditionalParams: map[string]string{
					"authMechanism": AuthMechanismSCRAMSHA1,
				},
			},
			wantURI: "mongodb://user:pass@localhost:27017/?authMechanism=SCRAM-SHA-256&authSource=admin",
		},
		{
			// Mechanism matching folds case, so a legacy config that spelled the mechanism in
			// lowercase still resolves to the canonical constant.
			name: "mechanism resolution folds case",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Database: "testdb",
				Username: "CN=user",
				AdditionalParams: map[string]string{
					"authMechanism":         "mongodb-x509",
					"tls":                   "true",
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			wantURI: "mongodb://CN=user@localhost:27017/?authMechanism=MONGODB-X509&authSource=%24external&tls=true&tlsCertificateKeyFile=%2Fcerts%2Fclient.pem",
		},
		{
			// Restored precedence (PR #1045): additional_params.authSource is the legacy way
			// to name the auth database and wins over the typed authdb field, matching
			// pre-refactor behavior for configs saved before auth_mechanism existed.
			name: "additional_params.authSource overrides authdb",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthDB:        "admin",
				Username:      "user",
				Password:      "pass",
				AuthMechanism: AuthMechanismSCRAMSHA256,
				AdditionalParams: map[string]string{
					"authSource": "other",
				},
			},
			wantURI: "mongodb://user:pass@localhost:27017/?authMechanism=SCRAM-SHA-256&authSource=other",
		},
		{
			// A mechanism that forces $external overrides authSource too, not just authdb.
			name: "external mechanism beats additional_params.authSource",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				Username:      "CN=user",
				AuthDB:        "admin",
				AuthMechanism: AuthMechanismX509,
				AdditionalParams: map[string]string{
					"authSource":            "other",
					"tls":                   "true",
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			wantURI: "mongodb://CN=user@localhost:27017/?authMechanism=MONGODB-X509&authSource=%24external&tls=true&tlsCertificateKeyFile=%2Fcerts%2Fclient.pem",
		},
		{
			name: "replica set with default read preference",
			config: &Config{
				Hosts:      []string{"localhost:27017"},
				Database:   "testdb",
				AuthDB:     "admin",
				Username:   "user",
				Password:   "pass",
				ReplicaSet: "rs0",
			},
			wantURI: "mongodb://user:pass@localhost:27017/?authSource=admin&readPreference=secondaryPreferred&replicaSet=rs0",
		},
		{
			name: "replica set preserves explicit read preference",
			config: &Config{
				Hosts:          []string{"localhost:27017"},
				Database:       "testdb",
				AuthDB:         "admin",
				Username:       "user",
				Password:       "pass",
				ReplicaSet:     "rs0",
				ReadPreference: "primary",
			},
			wantURI: "mongodb://user:pass@localhost:27017/?authSource=admin&readPreference=primary&replicaSet=rs0",
		},
		{
			name: "multi-host join",
			config: &Config{
				Hosts:    []string{"mongo1.internal:27017", "mongo2.internal:27017"},
				Database: "testdb",
				AuthDB:   "admin",
				Username: "user",
				Password: "pass",
			},
			wantURI: "mongodb://user:pass@mongo1.internal:27017,mongo2.internal:27017/?authSource=admin",
		},
		{
			name: "password with special characters is url-encoded",
			config: &Config{
				Hosts:    []string{"localhost:27017"},
				Database: "testdb",
				AuthDB:   "admin",
				Username: "user",
				Password: "p@ss:word/!",
			},
			wantURI: "mongodb://user:p%40ss%3Aword%2F%21@localhost:27017/?authSource=admin",
		},
		{
			name: "SRV connection",
			config: &Config{
				Hosts:    []string{"cluster.mongodb.net"},
				Database: "testdb",
				AuthDB:   "admin",
				Username: "user",
				Password: "pass",
				Srv:      true,
			},
			wantURI: "mongodb+srv://user:pass@cluster.mongodb.net/?authSource=admin",
		},
		{
			// Credentials are configured but MONGODB-AWS takes them from the environment,
			// so they must not reach the URI (a warning is logged instead).
			name: "MONGODB-AWS ignores configured credentials",
			config: &Config{
				Hosts:         []string{"cluster.mongodb.net"},
				Database:      "testdb",
				AuthMechanism: AuthMechanismAWS,
				Username:      "AKIAEXAMPLE",
				Password:      "secret",
			},
			wantURI: "mongodb://cluster.mongodb.net/?authMechanism=MONGODB-AWS&authSource=%24external",
		},
		{
			// Legacy path: mechanism only in additional_params.
			name: "legacy X509 mechanism",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				Database: "analytics",
				Username: "CN=olake-client,OU=Data,O=Acme",
				AdditionalParams: map[string]string{
					"authMechanism":         AuthMechanismX509,
					"tls":                   "true",
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			wantURI: "mongodb://CN=olake-client,OU=Data,O=Acme@mongo.internal:27017/?authMechanism=MONGODB-X509&authSource=%24external&tls=true&tlsCertificateKeyFile=%2Fcerts%2Fclient.pem",
		},
		{
			// Legacy path: mechanism only in additional_params.
			name: "legacy OIDC mechanism",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				Database: "analytics",
				Username: "olake-oidc-client",
				AdditionalParams: map[string]string{
					"authMechanism":           AuthMechanismOIDC,
					"authMechanismProperties": "ENVIRONMENT:gcp,TOKEN_RESOURCE:https://example.com",
				},
			},
			wantURI: "mongodb://olake-oidc-client@mongo.internal:27017/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT%3Agcp%2CTOKEN_RESOURCE%3Ahttps%3A%2F%2Fexample.com&authSource=%24external",
		},
	}

	// The latent bug this refactor fixes — inline verify-ca (CA only, no inline client cert)
	// plus a passthrough tlsCertificateKeyFile used to pass validation and then dial with no
	// client certificate, because that file param was stripped from the URI right after being
	// counted toward hasClientCert — is a Validate() rejection, not a URI() case; see
	// TestConfig_Validate/x509_with_inline_verify-ca_and_only_a_passthrough_cert_file_is_rejected.

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateAndURI(t, tt.config)
			if got != tt.wantURI {
				t.Fatalf("URI() = %q, want %q", got, tt.wantURI)
			}
			if additionalParamsContains(tt.config.AdditionalParams, "authMechanism") {
				t.Fatalf("authMechanism was not removed from additional_params")
			}
			if additionalParamsContains(tt.config.AdditionalParams, "authSource") {
				t.Fatalf("authSource was not removed from additional_params")
			}
		})
	}
}

func TestConfig_URI_DoesNotMutate(t *testing.T) {
	c := &Config{
		Hosts:    []string{"cluster.mongodb.net"},
		AuthDB:   "admin",
		Username: "user",
		Password: "pass",
		UseIAM:   true,
	}
	_ = c.URI()
	if c.AuthMechanism != "" {
		t.Fatalf("URI() mutated AuthMechanism to %q", c.AuthMechanism)
	}
	if c.AuthDB != "admin" {
		t.Fatalf("URI() mutated AuthDB to %q", c.AuthDB)
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name              string
		config            *Config
		expectErr         bool
		wantErrContains   string
		wantMaxThreads    int
		wantRetryCount    int
		wantSSLMode       string
		wantAuthDB        string
		wantAuthMechanism string
	}{
		{
			name: "empty hosts",
			config: &Config{
				Hosts:    []string{},
				Database: "testdb",
				Username: "user",
				Password: "pass",
				AuthDB:   "admin",
			},
			expectErr:       true,
			wantErrContains: "hosts is required",
		},
		{
			name: "missing database",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				Username: "user",
				Password: "pass",
				AuthDB:   "admin",
			},
			expectErr:       true,
			wantErrContains: "database is required",
		},
		{
			name: "missing username",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				Database: "testdb",
				AuthDB:   "admin",
			},
			expectErr:       true,
			wantErrContains: "username is required",
		},
		{
			name: "missing authdb",
			config: &Config{
				Hosts:    []string{"mongo.example.com:27017"},
				Database: "testdb",
				Username: "user",
			},
			expectErr:       true,
			wantErrContains: "authdb is required",
		},
		{
			// The negotiated default is SCRAM, which cannot authenticate without a password,
			// so it carries the same rules as an explicitly selected SCRAM mechanism.
			name: "negotiated default without password is rejected",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				Database: "testdb",
				AuthDB:   "admin",
				Username: "appuser",
			},
			expectErr:       true,
			wantErrContains: "password is required",
		},
		{
			name: "unsupported auth mechanism GSSAPI",
			config: func() *Config {
				c := baseConfig()
				c.AuthMechanism = AuthMechanismGSSAPI
				return c
			}(),
			expectErr:       true,
			wantErrContains: "GSSAPI is not supported",
		},
		{
			name: "GSSAPI in additional_params is rejected",
			config: func() *Config {
				c := baseConfig()
				c.AdditionalParams = map[string]string{
					"authMechanism": AuthMechanismGSSAPI,
				}
				return c
			}(),
			expectErr:       true,
			wantErrContains: "GSSAPI is not supported",
		},
		{
			name: "unknown auth mechanism",
			config: func() *Config {
				c := baseConfig()
				c.AuthMechanism = "NOT-A-MECHANISM"
				return c
			}(),
			expectErr:       true,
			wantErrContains: "unsupported auth_mechanism",
		},
		{
			name: "verify-ca requires server ca",
			config: func() *Config {
				c := baseConfig()
				c.SSLConfiguration = &utils.SSLConfig{
					Mode: utils.SSLModeVerifyCA,
				}
				return c
			}(),
			expectErr:       true,
			wantErrContains: "ssl.server_ca",
		},
		{
			name:           "sets defaults",
			config:         baseConfig(),
			expectErr:      false,
			wantMaxThreads: constants.DefaultThreadCount,
			wantRetryCount: constants.DefaultRetryCount,
			wantSSLMode:    utils.SSLModeDisable,
		},
		// Legacy path: mechanism only in additional_params; the schema-side counterpart
		// (spec.json) no longer requires a password here either — see PR #1045.
		{
			name: "x509 without password via additional_params passes validate",
			config: &Config{
				Hosts:    []string{"mongo.internal:27017"},
				Database: "analytics",
				AuthDB:   "$external",
				Username: "CN=olake-client,OU=Data,O=Acme",
				AdditionalParams: map[string]string{
					"authMechanism":         AuthMechanismX509,
					"tls":                   "true",
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			expectErr:         false,
			wantAuthDB:        "$external",
			wantAuthMechanism: AuthMechanismX509,
		},
		{
			name: "x509 via auth_mechanism without password passes validate and sets external authdb",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthDB:        "admin",
				Username:      "CN=user",
				AuthMechanism: AuthMechanismX509,
				AdditionalParams: map[string]string{
					"tls":                   "true",
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			expectErr:         false,
			wantAuthDB:        "$external",
			wantAuthMechanism: AuthMechanismX509,
		},
		{
			name: "x509 without username passes validate",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthMechanism: AuthMechanismX509,
				AdditionalParams: map[string]string{
					"tls":                   "true",
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			expectErr:         false,
			wantAuthDB:        "$external",
			wantAuthMechanism: AuthMechanismX509,
		},
		{
			name: "x509 with inline client certs and empty ssl mode passes validate",
			config: func() *Config {
				certs := testutils.GenerateTestCerts()
				return &Config{
					Hosts:         []string{"localhost:27017"},
					Database:      "testdb",
					AuthMechanism: AuthMechanismX509,
					SSLConfiguration: &utils.SSLConfig{
						ClientCert: certs.ClientCert,
						ClientKey:  certs.ClientKey,
					},
				}
			}(),
			expectErr:         false,
			wantAuthDB:        "$external",
			wantAuthMechanism: AuthMechanismX509,
		},
		{
			// The latent bug this refactor fixes: a passthrough tlsCertificateKeyFile does
			// not count toward "has a client cert" once inline SSL is active, because that
			// exact param is about to be stripped from the URI — it would otherwise pass
			// validation and then dial with no client certificate at all.
			name: "x509 with inline verify-ca and only a passthrough cert file is rejected",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthMechanism: AuthMechanismX509,
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyCA,
					ServerCA: testutils.GenerateTestCerts().CACert,
				},
				AdditionalParams: map[string]string{
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			expectErr:       true,
			wantErrContains: "a client certificate is required for MONGODB-X509",
		},
		{
			name: "x509 with password is rejected",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				Username:      "CN=user",
				Password:      "secret",
				AuthMechanism: AuthMechanismX509,
				AdditionalParams: map[string]string{
					"tls":                   "true",
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			expectErr:       true,
			wantErrContains: "password must be empty for MONGODB-X509",
		},
		{
			name: "PLAIN via auth_mechanism sets external authdb",
			config: func() *Config {
				c := baseConfig()
				c.AuthMechanism = AuthMechanismPLAIN
				c.AdditionalParams = map[string]string{"tls": "true"}
				return c
			}(),
			expectErr:         false,
			wantAuthDB:        "$external",
			wantAuthMechanism: AuthMechanismPLAIN,
		},
		{
			name: "PLAIN without TLS is rejected",
			config: func() *Config {
				c := baseConfig()
				c.AuthMechanism = AuthMechanismPLAIN
				return c
			}(),
			expectErr:       true,
			wantErrContains: "TLS is required for PLAIN",
		},
		{
			name: "explicit SCRAM without password is rejected",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthDB:        "admin",
				Username:      "user",
				AuthMechanism: AuthMechanismSCRAMSHA256,
			},
			expectErr:       true,
			wantErrContains: "password is required for SCRAM-SHA-256",
		},
		{
			name: "PLAIN without password is rejected",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				Username:      "user",
				AuthMechanism: AuthMechanismPLAIN,
				AdditionalParams: map[string]string{
					"tls": "true",
				},
			},
			expectErr:       true,
			wantErrContains: "password is required for PLAIN",
		},
		{
			name: "typed mechanism wins over additional_params",
			config: func() *Config {
				c := baseConfig()
				c.AuthMechanism = AuthMechanismSCRAMSHA256
				c.AdditionalParams = map[string]string{
					"authMechanism": AuthMechanismSCRAMSHA1,
				}
				return c
			}(),
			expectErr:         false,
			wantAuthMechanism: AuthMechanismSCRAMSHA256,
		},
		{
			name: "inline SSL rejects additional_params tls=false",
			config: func() *Config {
				c := baseConfig()
				c.SSLConfiguration = &utils.SSLConfig{Mode: utils.SSLModeRequire}
				c.AdditionalParams = map[string]string{"tls": "false"}
				return c
			}(),
			expectErr:       true,
			wantErrContains: "tls/ssl=false conflicts",
		},
		{
			name: "additional_params.tls enables TLS without inline ssl",
			config: func() *Config {
				c := baseConfig()
				c.AuthMechanism = AuthMechanismPLAIN
				c.AdditionalParams = map[string]string{"tls": "true"}
				return c
			}(),
			expectErr:         false,
			wantAuthMechanism: AuthMechanismPLAIN,
		},
		{
			name: "additional_params.tls must be a boolean",
			config: func() *Config {
				c := baseConfig()
				c.AdditionalParams = map[string]string{"tls": "sometimes"}
				return c
			}(),
			expectErr:       true,
			wantErrContains: "tls/ssl must be true or false",
		},
		{
			name: "x509 without TLS is rejected",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthMechanism: AuthMechanismX509,
				AdditionalParams: map[string]string{
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			expectErr:       true,
			wantErrContains: "TLS is required for MONGODB-X509",
		},
		{
			name: "SRV X509 with explicit tls false is rejected",
			config: &Config{
				Hosts:         []string{"cluster.example.com"},
				Database:      "testdb",
				Srv:           true,
				AuthMechanism: AuthMechanismX509,
				AdditionalParams: map[string]string{
					"tls":                   "false",
					"tlsCertificateKeyFile": "/certs/client.pem",
				},
			},
			expectErr:       true,
			wantErrContains: "TLS is required for MONGODB-X509",
		},
		{
			name: "SRV PLAIN with explicit ssl false is rejected",
			config: &Config{
				Hosts:         []string{"cluster.example.com"},
				Database:      "testdb",
				Srv:           true,
				Username:      "user",
				Password:      "pass",
				AuthMechanism: AuthMechanismPLAIN,
				AdditionalParams: map[string]string{
					"ssl": "false",
				},
			},
			expectErr:       true,
			wantErrContains: "TLS is required for PLAIN",
		},
		{
			name: "OIDC without mechanism properties is rejected",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthMechanism: AuthMechanismOIDC,
			},
			expectErr:       true,
			wantErrContains: "authMechanismProperties",
		},
		{
			name: "OIDC GCP configuration is accepted",
			config: &Config{
				Hosts:         []string{"cluster.mongodb.net"},
				Database:      "testdb",
				AuthMechanism: AuthMechanismOIDC,
				AdditionalParams: map[string]string{
					"authMechanismProperties": "ENVIRONMENT:gcp,TOKEN_RESOURCE:https://example.com",
				},
			},
			expectErr:         false,
			wantAuthDB:        "$external",
			wantAuthMechanism: AuthMechanismOIDC,
		},
		{
			name: "OIDC Azure without token resource is rejected",
			config: &Config{
				Hosts:         []string{"cluster.mongodb.net"},
				Database:      "testdb",
				AuthMechanism: AuthMechanismOIDC,
				AdditionalParams: map[string]string{
					"authMechanismProperties": "ENVIRONMENT:azure",
				},
			},
			expectErr:       true,
			wantErrContains: "TOKEN_RESOURCE is required for azure",
		},
		{
			name: "OIDC unsupported environment is rejected",
			config: &Config{
				Hosts:         []string{"cluster.mongodb.net"},
				Database:      "testdb",
				AuthMechanism: AuthMechanismOIDC,
				AdditionalParams: map[string]string{
					"authMechanismProperties": "ENVIRONMENT:k8s",
				},
			},
			expectErr:       true,
			wantErrContains: "ENVIRONMENT must be azure or gcp",
		},
		{
			name: "OIDC with password is rejected",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				Username:      "oidc-user",
				Password:      "secret",
				AuthMechanism: AuthMechanismOIDC,
			},
			expectErr:       true,
			wantErrContains: "password must be empty for MONGODB-OIDC",
		},
		{
			name: "valid IAM config applies AWS defaults",
			config: &Config{
				Hosts:    []string{"cluster.mongodb.net"},
				Database: "testdb",
				UseIAM:   true,
			},
			expectErr:         false,
			wantAuthDB:        "$external",
			wantAuthMechanism: AuthMechanismAWS,
		},
		{
			// use_iam and auth_mechanism=MONGODB-AWS naming the same mechanism is not a
			// conflict — only a DIFFERENT mechanism alongside use_iam is.
			name: "use_iam and auth_mechanism MONGODB-AWS together is accepted",
			config: &Config{
				Hosts:         []string{"cluster.mongodb.net"},
				Database:      "testdb",
				UseIAM:        true,
				AuthMechanism: AuthMechanismAWS,
			},
			expectErr:         false,
			wantAuthDB:        "$external",
			wantAuthMechanism: AuthMechanismAWS,
		},
		{
			name: "conflicting IAM and auth mechanism",
			config: &Config{
				Hosts:         []string{"cluster.mongodb.net"},
				Database:      "testdb",
				UseIAM:        true,
				AuthMechanism: AuthMechanismSCRAMSHA256,
			},
			expectErr:       true,
			wantErrContains: "auth_mechanism cannot be set when use_iam is enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				if tt.wantErrContains != "" && !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Fatalf("error = %q, want substring %q", err.Error(), tt.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error but got: %v", err)
			}
			if tt.wantMaxThreads != 0 && tt.config.MaxThreads != tt.wantMaxThreads {
				t.Fatalf("MaxThreads = %d, want %d", tt.config.MaxThreads, tt.wantMaxThreads)
			}
			if tt.wantRetryCount != 0 && tt.config.RetryCount != tt.wantRetryCount {
				t.Fatalf("RetryCount = %d, want %d", tt.config.RetryCount, tt.wantRetryCount)
			}
			if tt.wantSSLMode != "" {
				if tt.config.SSLConfiguration == nil {
					t.Fatalf("SSLConfiguration is nil, want mode %q", tt.wantSSLMode)
				}
				if tt.config.SSLConfiguration.Mode != tt.wantSSLMode {
					t.Fatalf("SSL mode = %q, want %q", tt.config.SSLConfiguration.Mode, tt.wantSSLMode)
				}
			}
			if tt.wantAuthDB != "" && tt.config.AuthDB != tt.wantAuthDB {
				t.Fatalf("AuthDB = %q, want %q", tt.config.AuthDB, tt.wantAuthDB)
			}
			if tt.wantAuthMechanism != "" && tt.config.AuthMechanism != tt.wantAuthMechanism {
				t.Fatalf("AuthMechanism = %q, want %q", tt.config.AuthMechanism, tt.wantAuthMechanism)
			}
			if additionalParamsContains(tt.config.AdditionalParams, "authMechanism") {
				t.Fatalf("additional_params authMechanism was not removed")
			}
		})
	}
}

func TestConfig_ValidateRejectsDuplicateReservedAdditionalParams(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
		option string
	}{
		{
			name: "auth mechanism casing",
			params: map[string]string{
				"authMechanism": AuthMechanismSCRAMSHA256,
				"AUTHMECHANISM": AuthMechanismX509,
			},
			option: "authMechanism",
		},
		{
			name: "auth source casing",
			params: map[string]string{
				"authSource": "admin",
				"AUTHSOURCE": "other",
			},
			option: "authSource",
		},
		{
			name: "OIDC properties casing",
			params: map[string]string{
				"authMechanismProperties": "ENVIRONMENT:gcp,TOKEN_RESOURCE:one",
				"AUTHMECHANISMPROPERTIES": "ENVIRONMENT:gcp,TOKEN_RESOURCE:two",
			},
			option: "authMechanismProperties",
		},
		{
			name: "tls and ssl aliases",
			params: map[string]string{
				"tls": "true",
				"ssl": "true",
			},
			option: "tls",
		},
		{
			name: "CA file aliases",
			params: map[string]string{
				"tlsCAFile":                   "/certs/one.pem",
				"sslCertificateAuthorityFile": "/certs/two.pem",
			},
			option: "tlsCAFile",
		},
		{
			name: "combined client certificate file aliases",
			params: map[string]string{
				"tlsCertificateKeyFile":       "/certs/one.pem",
				"sslClientCertificateKeyFile": "/certs/two.pem",
			},
			option: "tlsCertificateKeyFile",
		},
		{
			name: "separate certificate file casing",
			params: map[string]string{
				"tlsCertificateFile": "/certs/one.pem",
				"TLSCERTIFICATEFILE": "/certs/two.pem",
			},
			option: "tlsCertificateFile",
		},
		{
			name: "separate private key file casing",
			params: map[string]string{
				"tlsPrivateKeyFile": "/certs/one.pem",
				"TLSPRIVATEKEYFILE": "/certs/two.pem",
			},
			option: "tlsPrivateKeyFile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := baseConfig()
			config.AdditionalParams = tt.params

			err := config.Validate()
			if err == nil {
				t.Fatalf("Validate() expected duplicate %q error, got nil", tt.option)
			}
			want := "additional parameter \"" + tt.option + "\" is configured more than once"
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate() error = %q, want substring %q", err, want)
			}
		})
	}
}

// TestConfig_ValidateIdempotent covers the invariant that Validate() may run more than once on
// the same *Config (Setup() calls it exactly once today, but nothing prevents a caller from
// calling it again) without changing the resolved auth state on the second pass.
func TestConfig_ValidateIdempotent(t *testing.T) {
	tests := []struct {
		name              string
		config            *Config
		wantAuthDB        string
		wantAuthMechanism string
	}{
		{
			name: "IAM",
			config: &Config{
				Hosts:    []string{"cluster.mongodb.net"},
				Database: "testdb",
				UseIAM:   true,
			},
			wantAuthDB:        "$external",
			wantAuthMechanism: AuthMechanismAWS,
		},
		{
			name: "authSource resolved via additional_params",
			config: &Config{
				Hosts:         []string{"localhost:27017"},
				Database:      "testdb",
				AuthDB:        "admin",
				Username:      "user",
				Password:      "pass",
				AuthMechanism: AuthMechanismSCRAMSHA256,
				AdditionalParams: map[string]string{
					"authSource": "other",
				},
			},
			wantAuthDB:        "other",
			wantAuthMechanism: AuthMechanismSCRAMSHA256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.config.Validate(); err != nil {
				t.Fatalf("first Validate() error = %v", err)
			}
			if err := tt.config.Validate(); err != nil {
				t.Fatalf("second Validate() error = %v", err)
			}
			if tt.config.AuthMechanism != tt.wantAuthMechanism {
				t.Fatalf("AuthMechanism = %q, want %q", tt.config.AuthMechanism, tt.wantAuthMechanism)
			}
			if tt.config.AuthDB != tt.wantAuthDB {
				t.Fatalf("AuthDB = %q, want %q", tt.config.AuthDB, tt.wantAuthDB)
			}
		})
	}
}

// TestConfig_buildTLSConfig covers only mongodb's own choice of host argument (see config.go);
// general BuildTLSConfig behavior — modes, client certs, CA handling — lives in
// utils/ssl_test.go next to the code it tests.
func TestConfig_buildTLSConfig(t *testing.T) {
	certs := testutils.GenerateTestCerts()

	tests := []struct {
		name   string
		config *Config
		want   bool // true: expect a non-nil tls.Config with ServerName == ""
	}{
		{
			name:   "no ssl returns nil",
			config: &Config{Hosts: []string{"mongo.example.com:27017"}},
			want:   false,
		},
		{
			name: "verify-full leaves ServerName empty for multi host",
			config: &Config{
				Hosts: []string{
					"mongo1.internal:27017",
					"mongo2.internal:27017",
					"mongo3.internal:27017",
				},
				SSLConfiguration: &utils.SSLConfig{
					Mode:     utils.SSLModeVerifyFull,
					ServerCA: certs.CACert,
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.config.buildTLSConfig()
			if err != nil {
				t.Fatalf("buildTLSConfig() error = %v", err)
			}
			if !tt.want {
				if got != nil {
					t.Fatalf("expected nil tls config, got %#v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected tls config, got nil")
			}
			if got.ServerName != "" {
				t.Fatalf("ServerName = %q, want empty — mongodb passes \"\" so the driver fills it per dialled host", got.ServerName)
			}
		})
	}
}
