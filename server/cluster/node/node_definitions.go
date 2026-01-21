/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package node

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"time"
)

type NodeId uint64

func (n NodeId) Identifier() string {
	return fmt.Sprintf("node-%d", n)
}
func ExtractIdentifier(identifier []byte) (NodeId, error) {
	numBytes := bytes.TrimPrefix(identifier, []byte("node-"))
	id, err := strconv.Atoi(string(numBytes))
	return NodeId(id), err
}

var MinimumValidPort int = 46000
var MaximumValidPort int = 65535

// Checks if the given port is valid with respect with the minimum and maximum numbers allowed
func IsPortValid(port int) error {
	if port < MinimumValidPort || port > MaximumValidPort {
		return fmt.Errorf("The port %d is outside valid range: [%d, %d]", port, MinimumValidPort, MaximumValidPort)
	}
	return nil
}

type Address struct {
	Host string `json:"host"`
	Port uint16 `json:"port"`
}

func NewAddress(host string, port uint16) Address {
	return Address{host, port}
}

func (a Address) Clone() Address {
	return Address{
		a.Host,
		a.Port,
	}
}

func ValidateAddress(address Address) error {
	if net.ParseIP(address.Host) == nil {
		return fmt.Errorf("The Host (%s) is not valid", address.Host)
	}
	if err := IsPortValid(int(address.Port)); err != nil {
		return err
	}
	return nil
}

func FullAddr(host string, port uint16) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func (a Address) FullAddr() string {
	return FullAddr(a.Host, a.Port)
}

type Connection interface {
	GetIdentity() string
	Bind(uint16) error
	ConnectTo(string) error
	DisconnectFrom(string) error
	SendTo(string, []byte) error
	Recv() ([][]byte, error)
	Poll(time.Duration) error
}

type HostFinder interface {
	GetHost(id NodeId) (string, error)
}

type RouteFinder interface {
	GetLeader() NodeId
	GetUpstreamPort() uint16
	GetPersistenceNextHop() (NodeId, bool)
	GetInputNextHop(target NodeId) (NodeId, bool)
}

// The Role of a node is an enum, it specifies what are the responsibilities of a particular node in the system
type NodeRoleFlags uint8

const (
	RoleFlags_NONE        NodeRoleFlags = 0b00000000 // Nodes that neither handle database operations, nor they expose an HTTP server; they act as routers inside the system's network, by forwarding requests to the correct nodes.
	RoleFlags_LEADER      NodeRoleFlags = 0b00000001 // The node that won the leader election, it distributes the other roles amongst the remaining nodes. It also handles both read and write database operations.
	RoleFlags_PERSISTENCE NodeRoleFlags = 0b00000010 // Nodes that handle read-only operations on the database (for consistency). Write operations are forwarded to the leader to handle.
	RoleFlags_INPUT       NodeRoleFlags = 0b00000100 // Nodes that expose endpoints that clients can connect to (they host an HTTP server). They gather client requests and forward them to persistence nodes.
)

func (r NodeRoleFlags) String() string {
	var s = "{"

	if r == RoleFlags_NONE {
		return fmt.Sprintf("%sNONE}", s)
	}

	if r&RoleFlags_PERSISTENCE > 0 {
		s = fmt.Sprintf("%sPersistence (R", s)
	}

	if r&RoleFlags_LEADER > 0 {
		s = fmt.Sprintf("%sW)", s)
	} else {
		s = fmt.Sprintf("%s)", s)
	}

	if r&RoleFlags_INPUT > 0 {
		s = fmt.Sprintf("%s Input", s)
	}
	return fmt.Sprintf("%s}", s)
}
