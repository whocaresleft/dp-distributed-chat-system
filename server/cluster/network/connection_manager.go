/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package network

import (
	"errors"
	"fmt"
	"net"
	"server/cluster/node"
	"strings"
	"syscall"
	"time"

	zmq "github.com/pebbe/zmq4"
)

// Prepends the prefix `tcp://` to address
func getFullAddress(address string) string {
	return fmt.Sprintf("tcp://%s", address)
}

// Removes the prexif `tcp://` from fullAddress
func removePrefix(fullAddress string) string {
	return strings.Split(fullAddress, "//")[1]
}

// A Connection manager is the node's component that handles communication with other nodes in the system.
// It handles election messages, heartbeat (post election) messages and request forwarding
type ConnectionManager struct {
	ctx    *zmq.Context
	socket *zmq.Socket
	poller *zmq.Poller
}

// Creates a connection manager setting the identity with the given config.
func NewConnectionManager(id node.NodeId) (*ConnectionManager, error) {

	context, err := zmq.NewContext()
	if err != nil {
		return nil, err
	}

	r, err := context.NewSocket(zmq.Type(zmq.ROUTER))
	if err != nil {
		return nil, fmt.Errorf("Error during the creation of the router ZMQ4 socket for node %d", id)
	}

	if err = r.SetIdentity(id.Identifier()); err != nil {
		r.Close()
		return nil, fmt.Errorf("Could not set identity for router socket on node %d", id)
	}

	r.SetRouterHandover(true)
	r.SetRcvtimeo(-1)

	p := zmq.NewPoller()
	p.Add(r, zmq.POLLIN)

	return &ConnectionManager{
		context,
		r,
		p,
	}, nil
}

// StartMonitoring creates a monitor socket for this manager's socket, listening for
// events suck as connect, disconnect and handshake. Calling a callback when an handshake is completed
func (c *ConnectionManager) StartMonitoring(handshakeCallback func(string)) {

	monitorAddress := fmt.Sprintf("inproc://monitor-socket-%s", c.GetIdentity())
	c.socket.Monitor(monitorAddress, zmq.EVENT_ACCEPTED|zmq.EVENT_CONNECTED|zmq.EVENT_DISCONNECTED|zmq.EVENT_CLOSED|zmq.EVENT_CONNECT_RETRIED|zmq.EVENT_HANDSHAKE_SUCCEEDED)
	monitorSocket, _ := c.ctx.NewSocket(zmq.Type(zmq.PAIR))
	monitorSocket.Connect(monitorAddress)

	go func() {
		defer monitorSocket.Close()
		for {
			event, addr, _, err := monitorSocket.RecvEvent(0)
			if err != nil {
				break
			}
			switch event {
			case zmq.EVENT_HANDSHAKE_SUCCEEDED:
				handshakeCallback(addr)
			}
		}
	}()
}

// Bind binds the socket on the given port
func (c *ConnectionManager) Bind(port uint16) error {
	if err := c.socket.Bind(fmt.Sprintf("tcp://*:%d", port)); err != nil {
		return fmt.Errorf("Could not bind the socket on port %d", port)
	}
	return nil
}

// GetOutboundIP Returns the IP address used on this node, probably the local one
func GetOutboundIP() string {
	conn, _ := net.Dial("udp", "8.8.8.8:80")
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

// GetIdentity Returns the identity of the socket
func (c *ConnectionManager) GetIdentity() string {
	i, _ := c.socket.GetIdentity()
	return i
}

// ConnectTo Connects to address, enstablishing a connection
func (c *ConnectionManager) ConnectTo(address string) error {
	fullAddr := getFullAddress(address)

	err := c.socket.Connect(fullAddr)
	if err != nil {
		return fmt.Errorf("Could not connect to %s", address)
	}
	return nil
}

// DisconnectFrom Closes the connection with address
func (c *ConnectionManager) DisconnectFrom(address string) error {
	fullAddr := getFullAddress(address)

	err := c.socket.Disconnect(fullAddr)
	if err != nil {
		return fmt.Errorf("Could not disconnect from %s", address)
	}
	return nil
}

// SwitchAddress Replaces the connection with old with one with new.
// When successful error is nil
func (c *ConnectionManager) SwitchAddress(old, new string) error {
	if err := c.ConnectTo(new); err != nil {
		return fmt.Errorf("Error during address switch: %s", err.Error())
	}
	if err := c.DisconnectFrom(old); err != nil {
		_ = c.DisconnectFrom(new)
		return fmt.Errorf("Error during address switch: %s", err.Error())
	}
	return nil
}

// SendTo sends payload to the connected peer, identified by id.
// When successful error is nil
func (c *ConnectionManager) SendTo(id string, payload []byte) error {
	_, err := c.socket.SendMessage(id, "", payload)
	if err != nil {
		return fmt.Errorf("Error during send message to %s", id)
	}
	return nil
}

// Recv receives a [][]byte message on the socket, and returns it.
// When successful in receiving, error is nil.
// When socket times out, error is ErrRecvNotReady
func (c *ConnectionManager) Recv() ([][]byte, error) {
	msg, err := c.socket.RecvMessageBytes(zmq.DONTWAIT)
	if err != nil {
		if isRecvNotReadyError(err) {
			return nil, ErrRecvNotReady
		}
		return nil, fmt.Errorf("Recv network error: %v", err)
	}
	return msg, nil
}

// Poll waits for an amount of time, timeout, for an event to occur.
// The registered event is POLLIN (messages incoming).
// nil is returned when a message is ready to be recv'd
// ErrRecvNotReady when the poll times out, but IT IS NOT BAD, IT IS A GOOD ERROR
// If there is any other error, a serious problem (network scope) occurred
func (c *ConnectionManager) Poll(timeout time.Duration) error {
	sockets, err := c.poller.Poll(timeout)
	if err != nil {
		return fmt.Errorf("Polling error: %v", err)
	}
	if len(sockets) == 0 {
		return ErrRecvNotReady
	}
	return nil
}

// Destroy closes the current socket
func (c *ConnectionManager) Destroy() {
	c.socket.Close()
	c.ctx.Term()
}

// Error used for when the poll and recv failed, but not maliciously (they only timed out)
var ErrRecvNotReady = errors.New("No data is avaiable to recv() on the socket")

// isRecvNotReadyError tells wheter the error err is ErrRecvNotReady
func isRecvNotReadyError(err error) bool {
	var errno zmq.Errno
	if errors.As(err, &errno) {
		return errno == zmq.AsErrno(syscall.EAGAIN)
	}
	return false
}
