package utils

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"golang.org/x/crypto/ssh"
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

// NoDeadlineConn is a net.Conn that ignores deadlines
// The package crypto/ssh does not support deadlines, and some DB drivers(for GoLang) like that of MongoDB and Oracle internally call these deadline methods. So we wrap the connection in this custom connection that has those methods.
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

func ConnWithCustomDeadlineSupport(conn net.Conn) (net.Conn, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection is nil")
	}
	return &NoDeadlineConn{Conn: conn}, nil
}
