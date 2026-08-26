package utils

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/datazip-inc/olake/utils/errs"
	"golang.org/x/crypto/ssh"
)

// Codes for conditions this package detects itself, where the error alone cannot say the address
// that failed was the bastion and not the database.
const (
	codeSSHConfigInvalid = "ssh.config_invalid"
	codeSSHKeyInvalid    = "ssh.private_key_invalid"
	codeSSHDialFailed    = "ssh.dial_failed"
	codeSSHHostNotFound  = "ssh.host_not_found"
	codeSSHDialTimeout   = "ssh.dial_timeout"
	codeSSHUnreachable   = "ssh.unreachable"
)

type SSHConfig struct {
	Host       string `json:"host,omitempty"`
	Port       int    `json:"port,omitempty"`
	Username   string `json:"username,omitempty"`
	PrivateKey string `json:"private_key,omitempty"`
	Passphrase string `json:"passphrase,omitempty"`
	Password   string `json:"password,omitempty"`
}

func (c *SSHConfig) Validate() error {
	if c.Host == "" {
		return errors.New("ssh host is required")
	}

	if c.Port <= 0 || c.Port > 65535 {
		return errors.New("invalid ssh port number: must be between 1 and 65535")
	}

	if c.Username == "" {
		return errors.New("ssh username is required")
	}

	if c.PrivateKey == "" && c.Password == "" {
		return errors.New("private key or password is required")
	}

	return nil
}

func (c *SSHConfig) SetupSSHConnection() (*ssh.Client, error) {
	err := c.Validate()
	if err != nil {
		return nil, errs.Precondition(errs.ConfigInvalid, codeSSHConfigInvalid,
			fmt.Errorf("failed to validate ssh config: %w", err))
	}
	var authMethods []ssh.AuthMethod

	if c.Password != "" {
		authMethods = append(authMethods, ssh.Password(c.Password))
	}

	if c.PrivateKey != "" {
		signer, err := ParsePrivateKey(c.PrivateKey, c.Passphrase)
		if err != nil {
			return nil, errs.Precondition(errs.SSHTunnelFailed, codeSSHKeyInvalid,
				fmt.Errorf("failed to parse SSH private key: %w", err))
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	sshCfg := &ssh.ClientConfig{
		User: c.Username,
		Auth: authMethods,
		// Allows everyone to connect to the server without verifying the host key
		// TODO: Add proper host key verification
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // #nosec G106
		Timeout:         30 * time.Second,
	}

	bastionAddr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	sshClient, err := ssh.Dial("tcp", bastionAddr, sshCfg)
	if err != nil {
		// Splits ssh_tunnel_failed by what the dial error hit.
		code := codeSSHDialFailed
		switch errs.Standard(err).Category {
		case errs.DNSResolutionFailed:
			code = codeSSHHostNotFound
		case errs.Timeout:
			code = codeSSHDialTimeout
		case errs.NetworkUnreachable:
			code = codeSSHUnreachable
		}
		return nil, errs.Precondition(errs.SSHTunnelFailed, code,
			fmt.Errorf("ssh dial bastion: %w", err))
	}

	return sshClient, nil
}

// ParsePrivateKey parses a private key from a PEM string
func ParsePrivateKey(pemText, passphrase string) (ssh.Signer, error) {
	if passphrase != "" {
		return ssh.ParsePrivateKeyWithPassphrase([]byte(pemText), []byte(passphrase))
	}

	signer, err := ssh.ParsePrivateKey([]byte(pemText))
	if err == nil {
		return signer, nil
	}
	if _, ok := err.(*ssh.PassphraseMissingError); ok {
		return nil, fmt.Errorf("SSH private key appears encrypted, enter the passphrase")
	}
	return nil, err
}

// NoDeadlineConn wraps a net.Conn to suppress "deadline not supported" errors from the crypto/ssh package.
type NoDeadlineConn struct {
	net.Conn
}

func (c *NoDeadlineConn) SetDeadline(_ time.Time) error {
	return nil // Ignore deadline setting
}

func (c *NoDeadlineConn) SetReadDeadline(_ time.Time) error {
	return nil // Ignore read deadline setting
}

func (c *NoDeadlineConn) SetWriteDeadline(_ time.Time) error {
	return nil // Ignore write deadline setting
}

// The crypto/ssh package does not support deadline methods.
//   - Required for: mongodb and oracle drivers, which internally set default deadlines or call these methods unconditionally.
//   - Not Required for: mysql driver (only calls SetDeadline if a timeout is explicitly configured) or postgres driver (uses context for timeouts).
func ConnWithCustomDeadlineSupport(conn net.Conn) (net.Conn, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}
	return &NoDeadlineConn{Conn: conn}, nil
}
