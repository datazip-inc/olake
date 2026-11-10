package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/datazip-inc/olake/utils/errs"
	"github.com/stretchr/testify/require"
)

func TestSSLCertSource(t *testing.T) {
	const (
		pemContent = "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----\n"
		otherPEM   = "-----BEGIN CERTIFICATE-----\nb3RoZXI=\n-----END CERTIFICATE-----\n"
	)

	writeCA := func(t *testing.T) string {
		t.Helper()
		p := filepath.Join(t.TempDir(), "ca.pem")
		require.NoError(t, os.WriteFile(p, []byte(pemContent), 0o600))
		return p
	}

	tests := []struct {
		name     string
		cfg      func(t *testing.T) *SSLConfig
		run      func(sc *SSLConfig) error // defaults to Validate
		wantCode string                    // empty means no error
		wantCA   string                    // expected sc.ServerCA after run
	}{
		{
			name: "file reads ok",
			cfg: func(t *testing.T) *SSLConfig {
				return &SSLConfig{Mode: SSLModeVerifyCA, CertSource: SSLCertSourcePath, ServerCAPath: writeCA(t)}
			},
			wantCA: pemContent,
		},
		{
			name: "missing file",
			cfg: func(t *testing.T) *SSLConfig {
				return &SSLConfig{
					Mode:         SSLModeVerifyCA,
					CertSource:   SSLCertSourcePath,
					ServerCAPath: filepath.Join(t.TempDir(), "missing.pem"),
				}
			},
			wantCode: codeSSLFileUnreadable,
		},
		{
			name: "inline and path both set with different content",
			cfg: func(t *testing.T) *SSLConfig {
				return &SSLConfig{
					Mode:         SSLModeVerifyCA,
					CertSource:   SSLCertSourcePath,
					ServerCA:     otherPEM,
					ServerCAPath: writeCA(t),
				}
			},
			wantCode: codeSSLCertSourceConflict,
			wantCA:   otherPEM, // inline value left untouched
		},
		{
			name: "Validate called twice",
			cfg: func(t *testing.T) *SSLConfig {
				return &SSLConfig{Mode: SSLModeVerifyCA, CertSource: SSLCertSourcePath, ServerCAPath: writeCA(t)}
			},
			run: func(sc *SSLConfig) error {
				if err := sc.Validate(); err != nil {
					return err
				}
				return sc.Validate() // inline now equals file content, must not conflict
			},
			wantCA: pemContent,
		},
		{
			name: "require mode with bad path passes",
			cfg: func(t *testing.T) *SSLConfig {
				return &SSLConfig{
					Mode:         SSLModeRequire,
					CertSource:   SSLCertSourcePath,
					ServerCAPath: filepath.Join(t.TempDir(), "missing.pem"),
				}
			},
		},
		{
			name: "invalid cert_source",
			cfg: func(t *testing.T) *SSLConfig {
				return &SSLConfig{Mode: SSLModeVerifyCA, CertSource: "bogus", ServerCAPath: writeCA(t)}
			},
			wantCode: codeSSLCertSourceInvalid,
		},
		{
			name: "invalid cert_source via ValidateCertSource",
			cfg: func(_ *testing.T) *SSLConfig {
				return &SSLConfig{CertSource: "bogus"}
			},
			run:      func(sc *SSLConfig) error { return sc.ValidateCertSource() },
			wantCode: codeSSLCertSourceInvalid,
		},
		{
			name: "LoadCertFiles missing file",
			cfg: func(t *testing.T) *SSLConfig {
				return &SSLConfig{CertSource: SSLCertSourcePath, ClientKeyPath: filepath.Join(t.TempDir(), "nope.key")}
			},
			run:      func(sc *SSLConfig) error { return sc.LoadCertFiles() },
			wantCode: codeSSLFileUnreadable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := tt.cfg(t)
			run := tt.run
			if run == nil {
				run = func(sc *SSLConfig) error { return sc.Validate() }
			}
			err := run(sc)
			if tt.wantCode == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, tt.wantCode, errs.From(err).Code)
			}
			require.Equal(t, tt.wantCA, sc.ServerCA)
		})
	}
}
