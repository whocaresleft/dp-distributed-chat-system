/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package connection

import (
	"fmt"
	"server/cluster/node"

	zmq "github.com/pebbe/zmq4"
)

// A Connection manager is the node's component that handles communication with other nodes in the system.
// It handles election messages, heartbeat (post election) messages and request forwarding
type ConnectionManager struct {
	electionSocket *zmq.Socket
}

func getFullAddress(address string) string {
	return fmt.Sprintf("tcp://%s", address)
}
func Identifier(id node.NodeId) string {
	return fmt.Sprintf("node-%d", id)
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
		return nil, fmt.Errorf("Could not bind the socket on port %d for node %d", nodePort)
	}

	r.SetRouterHandover(true)

	return &ConnectionManager{r}, nil
}

func (c *ConnectionManager) ConnectTo(address string) error {
	fullAddr := getFullAddress(address)

	err := c.electionSocket.Connect(fullAddr)
	if err != nil {
		return fmt.Errorf("Could not connect to %s", address)
	}
	return nil
}

func (c *ConnectionManager) DisconnectFrom(address string) error {
	fullAddr := getFullAddress(address)

	err := c.electionSocket.Disconnect(fullAddr)
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
	_, err := c.electionSocket.SendMessage(id, "", payload)
	if err != nil {
		return fmt.Errorf("Error during send message to %s", id)
	}
	return nil
}

func (c *ConnectionManager) Recv() (string, []string, error) {
	strings, err := c.electionSocket.RecvMessage(0)

	return strings[0], strings[1:], err
}

func (c *ConnectionManager) Destroy() {
	c.electionSocket.Close()
}
