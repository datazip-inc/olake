package utils

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

type SSHConfig struct {
	Host                    string `json:"host,omitempty"`
	Port                    int    `json:"port,omitempty"`
	Username                string `json:"username,omitempty"`
	PrivateKey              string `json:"private_key,omitempty"`
	Passphrase              string `json:"passphrase,omitempty"`
	Password                string `json:"password,omitempty"`
	HostKeyVerificationMode string `json:"host_key_verification_mode,omitempty"`
	KnownHostsFilePath      string `json:"known_hosts_file_path,omitempty"`
}

const (
	StrictHostKeyVerification   = "strict"
	InsecureHostKeyVerification = "insecure"
)

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

	if c.HostKeyVerificationMode == StrictHostKeyVerification {
		if c.KnownHostsFilePath == "" {
			return errors.New("known_hosts file path is required for strict verification")
		}
	}

	if c.HostKeyVerificationMode == "" {
		c.HostKeyVerificationMode = InsecureHostKeyVerification
	}

	return nil
}

func (c *SSHConfig) getHostKeyCallback() (ssh.HostKeyCallback, error) {
	strictStrategy := func() (ssh.HostKeyCallback, error) {
		// need an absolute path to the known_hosts file
		if err := CheckIfFilesExists(c.KnownHostsFilePath); err != nil {
			return nil, fmt.Errorf("known_hosts file validation failed: %w", err)
		}

		callback, err := knownhosts.New(c.KnownHostsFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to load known_hosts file: %w", err)
		}

		return callback, nil
	}

	insecureStrategy := func() (ssh.HostKeyCallback, error) {
		return ssh.InsecureIgnoreHostKey(), nil // #nosec G106
	}

	switch c.HostKeyVerificationMode {
	case InsecureHostKeyVerification:
		return insecureStrategy()
	case StrictHostKeyVerification:
		return strictStrategy()
	default:
		return nil, fmt.Errorf("unknown host key verification strategy: %s", c.HostKeyVerificationMode)
	}
}

func (c *SSHConfig) SetupSSHConnection() (*ssh.Client, error) {
	err := c.Validate()
	if err != nil {
		return nil, fmt.Errorf("failed to validate ssh config: %s", err)
	}
	var authMethods []ssh.AuthMethod

	if c.Password != "" {
		authMethods = append(authMethods, ssh.Password(c.Password))
	}

	if c.PrivateKey != "" {
		signer, err := ParsePrivateKey(c.PrivateKey, c.Passphrase)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SSH private key: %s", err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	hostKeyCallback, err := c.getHostKeyCallback()
	if err != nil {
		return nil, fmt.Errorf("failed to get host key callback: %s", err)
	}

	sshCfg := &ssh.ClientConfig{
		User:            c.Username,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
		Timeout:         30 * time.Second,
	}

	bastionAddr := net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	sshClient, err := ssh.Dial("tcp", bastionAddr, sshCfg)
	if err != nil {
		return nil, fmt.Errorf("ssh dial bastion: %s", err)
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
