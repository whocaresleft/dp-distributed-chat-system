/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package connection

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"server/cluster/node"
	"strconv"
	"strings"
	"syscall"
	"time"

	zmq "github.com/pebbe/zmq4"
)

func getFullAddress(address string) string {
	return fmt.Sprintf("tcp://%s", address)
}
func removePrefix(fullAddress string) string {
	return strings.Split(fullAddress, "//")[1]
}

func Identifier(id node.NodeId) string {
	return fmt.Sprintf("node-%d", id)
}
func ExtractIdentifier(frame []byte) (node.NodeId, error) {
	numBytes := bytes.TrimPrefix(frame, []byte("node-"))
	id, err := strconv.Atoi(string(numBytes))
	return node.NodeId(id), err
}

// A Connection manager is the node's component that handles communication with other nodes in the system.
// It handles election messages, heartbeat (post election) messages and request forwarding
type ConnectionManager struct {
	socket *zmq.Socket
	poller *zmq.Poller
}

// Creates a connection manager setting the identity with the given config.
func NewConnectionManager(node node.NodeConfig) (*ConnectionManager, error) {
	nodeId, nodePort := node.GetId(), node.GetPort()

	r, err := zmq.NewSocket(zmq.Type(zmq.ROUTER))
	if err != nil {
		return nil, fmt.Errorf("Error during the creation of the router ZMQ4 socket for node %d", nodeId)
	}

	if err = r.SetIdentity(Identifier(nodeId)); err != nil {
		r.Close()
		return nil, fmt.Errorf("Could not set identity for router socket on node %d", nodeId)
	}

	if err = r.Bind(fmt.Sprintf("tcp://*:%d", nodePort)); err != nil {
		return nil, fmt.Errorf("Could not bind the socket on port %d for node %d", nodePort, nodeId)
	}

	r.SetRouterHandover(true)
	r.SetRcvtimeo(-1)

	p := zmq.NewPoller()
	p.Add(r, zmq.POLLIN)

	return &ConnectionManager{
		r,
		p,
	}, nil
}

func (c *ConnectionManager) StartMonitoring(connectCallback, disconnectCallback func(string)) {
	monitorAddress := fmt.Sprintf("inproc://monitor-socket-%s", c.GetIdentity())
	c.socket.Monitor(monitorAddress, zmq.EVENT_ACCEPTED|zmq.EVENT_CONNECTED|zmq.EVENT_DISCONNECTED|zmq.EVENT_CLOSED|zmq.EVENT_CONNECT_RETRIED|zmq.EVENT_HANDSHAKE_SUCCEEDED)
	monitorSocket, _ := zmq.NewSocket(zmq.Type(zmq.PAIR))
	monitorSocket.Connect(monitorAddress)

	go func() {
		defer monitorSocket.Close()
		for {
			event, addr, _, err := monitorSocket.RecvEvent(0)
			if err != nil {
				break
			}
			switch event {
			case zmq.EVENT_CONNECTED, zmq.EVENT_ACCEPTED, zmq.EVENT_HANDSHAKE_SUCCEEDED:
				connectCallback(removePrefix(addr))
			case zmq.EVENT_DISCONNECTED, zmq.EVENT_CLOSED:
				disconnectCallback(removePrefix(addr))
			}
		}
	}()
}

func GetOutboundIP() string {
	conn, _ := net.Dial("udp", "8.8.8.8:80")
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

func (c *ConnectionManager) GetIdentity() string {
	i, _ := c.socket.GetIdentity()
	return i
}

func (c *ConnectionManager) ConnectTo(address string) error {
	fullAddr := getFullAddress(address)

	err := c.socket.Connect(fullAddr)
	if err != nil {
		return fmt.Errorf("Could not connect to %s", address)
	}
	return nil
}

func (c *ConnectionManager) DisconnectFrom(address string) error {
	fullAddr := getFullAddress(address)

	err := c.socket.Disconnect(fullAddr)
	if err != nil {
		return fmt.Errorf("Could not disconnect from %s", address)
	}
	return nil
}

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

func (c *ConnectionManager) SendTo(id string, payload []byte) error {
	_, err := c.socket.SendMessage(id, "", payload)
	if err != nil {
		return fmt.Errorf("Error during send message to %s", id)
	}
	return nil
}

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

func (c *ConnectionManager) Destroy() {
	c.socket.Close()
}

var ErrRecvNotReady = errors.New("No data is avaiable to recv() on the socket")

func isRecvNotReadyError(err error) bool {
	var errno zmq.Errno
	if errors.As(err, &errno) {
		return errno == zmq.AsErrno(syscall.EAGAIN)
	}
	return false
}
