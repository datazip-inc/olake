package driver

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/datazip-inc/olake/utils"
	"github.com/jmoiron/sqlx"
)

const (
	testCACert = `-----BEGIN CERTIFICATE-----
MIIDmzCCAoOgAwIBAgIUDYhrajPJ3W6fLFBY6mUTq+gr0yowDQYJKoZIhvcNAQEL
BQAwXTELMAkGA1UEBhMCVVMxDTALBgNVBAgMBFRlc3QxDTALBgNVBAcMBFRlc3Qx
DjAMBgNVBAoMBU9sYWtlMQ0wCwYDVQQLDARUZXN0MREwDwYDVQQDDAhNeVNRTF9D
QTAeFw0yNTExMDIxNTE3NDJaFw0zNTEwMzExNTE3NDJaMF0xCzAJBgNVBAYTAlVT
MQ0wCwYDVQQIDARUZXN0MQ0wCwYDVQQHDARUZXN0MQ4wDAYDVQQKDAVPbGFrZTEN
MAsGA1UECwwEVGVzdDERMA8GA1UEAwwITXlTUUxfQ0EwggEiMA0GCSqGSIb3DQEB
AQUAA4IBDwAwggEKAoIBAQDLMBbDiFC01KLmbrrdCIbZGAEtcVL/fXsPxixMmqjR
tMuCKTE+ZPmQSHHHlo2tbpYscgCMrvQwT2woqJsVsjw1OnNwY9ukGpBcc7B3YXPO
VnjTVRR/WuGtfUg97hbi3VejLzNYEa6b8FdSc11AJcJHMnEihzc7e9KTdYZ5erQL
f8UnvMwELRAybaKz8JUG+4Sk3rzioFb3HWMYSaCYYjTH5XnICNhfZOQefDYh8HRc
aqFre2W5356Em27xvZx+wMkQIdrewyBsBEbwuDBRQLgXzx3uFMsjLgSN6H593mt9
WipqUkoQHxvfRmD1fgepy/COV5LZ2eCmJ7miul/CsN7FAgMBAAGjUzBRMB0GA1Ud
DgQWBBTMDF36UrjVBC0wZdHy0pOAKQjRgTAfBgNVHSMEGDAWgBTMDF36UrjVBC0w
ZdHy0pOAKQjRgTAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBM
AXYEDhsCV/LQ/n8K+wBl5KgPqbAdl2C4emLsPGomBrNB6ilI/Sfva091jG9HPdVT
85KUK4h+VIXa3gG18Y/YqRfSogY3uzm3jguQiDYlf5HS2jBCcufUGbcB9AuTd0+F
lGUliiB7mTvD15cfhRUQ5fWKugvsde+riv3vleVd5TeiMBtKweOvrTO90q1sjHKk
LRMJHuOtQPq08KLqebaJH5DD0N/qek5iaIefWPAlD+g4K4ndm+9NUDkXB1px8j92
3k+58GI93iOoScLaMNqgfZ3TDU7sDeqly8dUp0KTQoAHrZwNdWkn3aUpdo2MubOB
XSf/VI4Mn+FYtcl9TcG1
-----END CERTIFICATE-----`

	testClientCert = `-----BEGIN CERTIFICATE-----
MIIDdDCCAlygAwIBAgIBAjANBgkqhkiG9w0BAQsFADBdMQswCQYDVQQGEwJVUzEN
MAsGA1UECAwEVGVzdDENMAsGA1UEBwwEVGVzdDEOMAwGA1UECgwFT2xha2UxDTAL
BgNVBAsMBFRlc3QxETAPBgNVBAMMCE15U1FMX0NBMB4XDTI1MTEwMjE1MTc1N1oX
DTM1MTAzMTE1MTc1N1owWjELMAkGA1UEBhMCVVMxDTALBgNVBAgMBFRlc3QxDTAL
BgNVBAcMBFRlc3QxDjAMBgNVBAoMBU9sYWtlMQ0wCwYDVQQLDARUZXN0MQ4wDAYD
VQQDDAVteXNxbDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANh6dScE
4YcjvCB69ddD1CN0EmzUpFvZLJ9vgL+6PSzcbrjWJd3dQo5/roFcjm5eX5taySBc
qm0qBWOLuFLK/5wreNkCWZf2Cq4oGZCqj4ieBVHnqAU6QZmYdp8cvNHZ4iYywdV9
+Aa9Oy04ypaMh6lLYfoEEVv8RcY5KELk7ev/W2mUY7c4Z+5AyyvgUPNRbYS4f5Ht
IdLHBoYopaTKO3D4JXVC1nxiQdSeigrTAwGS0ZyRB33XmLnhyo9Aui5x3mkjIr0C
JOk85ONxUIwRPIZM3mNohWtJnZ5xr/0JVSmVjisRMcOz/qeoRyziysKHgq7YdK6E
WnDpgTc6Jdhr8NECAwEAAaNCMEAwHQYDVR0OBBYEFIBMJYY8+TUnzbAbi2WYCvAu
GrrNMB8GA1UdIwQYMBaAFMwMXfpSuNUELTBl0fLSk4ApCNGBMA0GCSqGSIb3DQEB
CwUAA4IBAQCFXwk3Qg3iSUs1Y/WhgwEFpC42Hdtk16QrXvo63M5lsOvY1Rhz0l+x
TRODwekXtiwM/cfhwLJ1IQiswDAJdL2c3QEAXRGQyvPLHXOqHfDLpQ4MyqSyxAEb
N4le7l1WCICEErvnLFHluPBdel13BERNqL+ahZPB9T2UmoNqfwIq22nCWkig+BKp
y0kojvsfXNm+HZg4BZOWMxNkpXUGYgytfSBiadFqXPkbwoQ8xFxUlSRS1ed9qVJt
QPc3VSjSRmOH69i9TsiIIfxf+KTHq86QVSwLv10graCNUICMEHue/5/DeWfHctSc
AF01IuE93adjB1Uzo1MAf0vVF06sJNoe
-----END CERTIFICATE-----`

	testClientKey = `-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDYenUnBOGHI7wg
evXXQ9QjdBJs1KRb2Syfb4C/uj0s3G641iXd3UKOf66BXI5uXl+bWskgXKptKgVj
i7hSyv+cK3jZAlmX9gquKBmQqo+IngVR56gFOkGZmHafHLzR2eImMsHVffgGvTst
OMqWjIepS2H6BBFb/EXGOShC5O3r/1tplGO3OGfuQMsr4FDzUW2EuH+R7SHSxwaG
KKWkyjtw+CV1QtZ8YkHUnooK0wMBktGckQd915i54cqPQLoucd5pIyK9AiTpPOTj
cVCMETyGTN5jaIVrSZ2eca/9CVUplY4rETHDs/6nqEcs4srCh4Ku2HSuhFpw6YE3
OiXYa/DRAgMBAAECggEAA0yEuM4281XUyfOjdXFWvmeRaytwrlURoN85IFdq7vSC
JShfbMhC1PaES43NYv8vvznl8wcEMm1kOH0hThmwahNEqXjMH97abD3b6U4JO+vi
RJWwuxTOVR1Orgif72693aknf1JiQRSbuYn7TYDhTqa4bxSBpYB+0EgSapFpG+tm
ZTTTEY/y674DM5g7LX37oxYQKxvNrupxiLUWf5FHn37UfUlPu4b3Twnnbz0+DYI2
bqVUYJTahf0/MqEPiGlOvZP+i5A86uSYAzDCZUpLDiv3R/3QVZyt6ApIbdFYLahk
XcWoxRMogWtcG1QhCmyHzXz1S2LSqCLS6cQU/FqdRQKBgQDxDFgQxw+unhe5nPm1
fTEZ2NFJ6TjU3p6N75pL5TekAju4AuCb7mUWQ7lg+BusRo9XHSNXteetztDzwQKd
xDyHOYGdSZde+x7kVUAY0OSILc2B2pF+HoYxir/OPcUvE5DOr85AfzIvUyjQ1yLy
LHEdv0jqvMyZf/1hipvsjdZ3hwKBgQDl5/akR06ADDarIIgUe7luraC6Gcoa99xV
8egDYV+bOatnX/qhT5hUBzpu+k0bXMyjnrmgpMwmruyAkHAEYcpWxqPxcT0rcJFW
jevSZSCtopuf7Gr+XZq3Sj6YqtrC/umbhENaUi8o3Fm4XWD7j8oF8U5zQyn0sY8Z
oKUilgK65wKBgQCpDstFGRe6lE53c8z+qMsjIZnHiLa/NVNmoMFKsXyQnrBbokj9
k/l54A3IILrn3KTzqA/9mCrhD8gk7R10oQkCniZ1tgNgLifAZLoLrZZanCUiCbU5
Cxd676EOeOwu1D4fd9XkDlGYN27M6dYsm1bKUjpFyByHG+kN3DT3d5MPSQKBgQCX
w74+x4t8X5Oe94Sc8OeBtkAJWYjesIvUeDHOOXMhRrptLSCHZ8GIhpT+OWC5FVNJ
Fzg2YVManhIk9DPd0Kf/DHWgpj3Y9SAb4OexJWyi8lqFAU3HyAafw/T6Vp6+ZgPT
00Wa0/GpqXvYhlvE9DBKJ/a/g7CohKWQJ0mbQSdgNQKBgQC4kYY3LkGRZF2jTUEs
JYiUJx9FwC+2TXLz7J6v6yVXHTQjuMjXoslaqwzmCIpfwzfJQBAlQxhpmsREEh1d
2l4PgdKeUc3ntU5F5Zv2wnES9IpP7OQTBSq3TZ3ptl33f1kdRdSy1dy+d08MLS2b
26lK6T8exUUhjR7wTL654Yjy8g==
-----END PRIVATE KEY-----`
)

func TestIntegration_BasicConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := &Config{
		Host:     "localhost",
		Port:     3306,
		Username: "mysql",
		Password: "secret1234",
		Database: "olake_mysql_test",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sqlx.ConnectContext(ctx, "mysql", config.URI())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	var result int
	if err := db.GetContext(ctx, &result, "SELECT 1"); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected 1, got %d", result)
	}

	t.Log("✓ Basic connection successful")
}

func TestIntegration_SSLDisable(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := &Config{
		Host:     "localhost",
		Port:     3306,
		Username: "mysql",
		Password: "secret1234",
		Database: "olake_mysql_test",
		SSLConfiguration: &utils.SSLConfig{
			Mode: utils.SSLModeDisable,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := config.URI()
	if !strings.Contains(uri, "tls=false") {
		t.Errorf("Expected tls=false in URI: %s", uri)
	}

	db, err := sqlx.ConnectContext(ctx, "mysql", uri)
	if err != nil {
		t.Fatalf("Failed to connect with SSL disabled: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	t.Log("✓ SSL disable mode successful")
}

func TestIntegration_SSLRequire(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := &Config{
		Host:     "localhost",
		Port:     3307,
		Username: "mysql",
		Password: "secret1234",
		Database: "olake_mysql_test",
		SSLConfiguration: &utils.SSLConfig{
			Mode: utils.SSLModeRequire,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := config.URI()
	if !strings.Contains(uri, "tls=skip-verify") {
		t.Errorf("Expected tls=skip-verify in URI: %s", uri)
	}

	db, err := sqlx.ConnectContext(ctx, "mysql", uri)
	if err != nil {
		t.Skipf("SSL MySQL not available on port 3307: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	t.Log("✓ SSL require mode successful")
}

func TestIntegration_SSLVerifyCA(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := &Config{
		Host:     "localhost",
		Port:     3307,
		Username: "mysql",
		Password: "secret1234",
		Database: "olake_mysql_test",
		SSLConfiguration: &utils.SSLConfig{
			Mode:       utils.SSLModeVerifyCA,
			ServerCA:   testCACert,
			ClientCert: testClientCert,
			ClientKey:  testClientKey,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := config.URI()

	db, err := sqlx.ConnectContext(ctx, "mysql", uri)
	if err != nil {
		t.Skipf("SSL MySQL not available on port 3307: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	var result string
	if err := db.GetContext(ctx, &result, "SELECT VERSION()"); err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	t.Logf("✓ SSL verify-ca mode successful (MySQL version: %s)", result)
}

func TestIntegration_JDBCParams(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := &Config{
		Host:     "localhost",
		Port:     3306,
		Username: "mysql",
		Password: "secret1234",
		Database: "olake_mysql_test",
		JDBCURLParams: map[string]string{
			"charset":   "utf8mb4",
			"parseTime": "true",
			"timeout":   "10s",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := config.URI()
	if !strings.Contains(uri, "charset=utf8mb4") {
		t.Errorf("Expected charset in URI: %s", uri)
	}
	if !strings.Contains(uri, "parseTime=true") {
		t.Errorf("Expected parseTime in URI: %s", uri)
	}

	db, err := sqlx.ConnectContext(ctx, "mysql", uri)
	if err != nil {
		t.Fatalf("Failed to connect with JDBC params: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	t.Log("✓ JDBC parameters successful")
}

func TestIntegration_CombinedSSLAndJDBC(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := &Config{
		Host:     "localhost",
		Port:     3307,
		Username: "mysql",
		Password: "secret1234",
		Database: "olake_mysql_test",
		JDBCURLParams: map[string]string{
			"charset":   "utf8mb4",
			"parseTime": "true",
		},
		SSLConfiguration: &utils.SSLConfig{
			Mode: utils.SSLModeRequire,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	uri := config.URI()

	db, err := sqlx.ConnectContext(ctx, "mysql", uri)
	if err != nil {
		t.Skipf("SSL MySQL not available on port 3307: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	t.Log("✓ Combined SSL + JDBC params successful")
}

func TestIntegration_CertificatesAsTextStrings(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	config := &Config{
		Host:     "localhost",
		Port:     3307,
		Username: "mysql",
		Password: "secret1234",
		Database: "olake_mysql_test",
		SSLConfiguration: &utils.SSLConfig{
			Mode:       utils.SSLModeVerifyCA,
			ServerCA:   testCACert,
			ClientCert: testClientCert,
			ClientKey:  testClientKey,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	db, err := sqlx.ConnectContext(ctx, "mysql", config.URI())
	if err != nil {
		t.Skipf("SSL MySQL not available: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Connection failed: %v", err)
	}

	t.Log("✓ Certificates passed as text strings in config (not file paths)")
}
