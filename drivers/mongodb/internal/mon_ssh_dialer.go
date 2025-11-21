package driver

import (
	"context"
	"fmt"
	"net"
	"time"

	"golang.org/x/crypto/ssh"
)

type MongoSSHDialer struct {
	sshClient *ssh.Client
}

// deadlineConn wraps an SSH connection to provide deadline support
// that the MongoDB driver requires, even though SSH connections don't natively support deadlines
type deadlineConn struct {
	net.Conn
	readDeadline  time.Time
	writeDeadline time.Time
}

func (c *deadlineConn) SetDeadline(t time.Time) error {
	c.readDeadline = t
	c.writeDeadline = t
	// SSH connections don't support deadlines, so we just store them
	// The MongoDB driver will use context timeouts for actual timeout handling
	return nil
}

func (c *deadlineConn) SetReadDeadline(t time.Time) error {
	c.readDeadline = t
	return nil
}

func (c *deadlineConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline = t
	return nil
}

func (d *MongoSSHDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if d.sshClient == nil {
		return nil, fmt.Errorf("SSH client is not initialized")
	}
	conn, err := d.sshClient.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &deadlineConn{Conn: conn}, nil
}
