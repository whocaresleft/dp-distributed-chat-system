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

// NodeId is just uint64. Howerver it made it easier to have consistency (and not write uint or int64 by mistake)
type NodeId uint64

// Identifier returns a string that expresses this node's identity, using its id.
// Returns node-<id>
func (n NodeId) Identifier() string {
	return fmt.Sprintf("node-%d", n)
}

// ExtractIdentifier is the opposite of identifier, it extracts the node id from a string that is `node-<id>`.
// When succesful error is nil
func ExtractIdentifier(identifier []byte) (NodeId, error) {
	numBytes := bytes.TrimPrefix(identifier, []byte("node-"))
	id, err := strconv.Atoi(string(numBytes))
	return NodeId(id), err
}

const MinimumValidPort int = 46000 // Lower bound, useful to make sure we use ephimeral ports
const MaximumValidPort int = 65535 // Port numbers are 16 bits, better to not go over

// IsPortValid checks if the given port is valid with respect with the minimum and maximum numbers allowed
func IsPortValid(port int) error {
	if port < MinimumValidPort || port > MaximumValidPort {
		return fmt.Errorf("The port %d is outside valid range: [%d, %d]", port, MinimumValidPort, MaximumValidPort)
	}
	return nil
}

// Address is a wrapper around a couple (Host, Port)
type Address struct {
	Host string `json:"host"`
	Port uint16 `json:"port"`
}

// NewAddress combines host and port into an Address
func NewAddress(host string, port uint16) Address {
	return Address{host, port}
}

// Clone returns a copy of the Address a
func (a Address) Clone() Address {
	return Address{
		a.Host,
		a.Port,
	}
}

// ValidateAddress checks if the given address is valid or not, checking both hostname and port.
// Returns nil when successful
func ValidateAddress(address Address) error {
	if net.ParseIP(address.Host) == nil {
		return fmt.Errorf("The Host (%s) is not valid", address.Host)
	}
	if err := IsPortValid(int(address.Port)); err != nil {
		return err
	}
	return nil
}

// FullAddr combines the given host and port into the following string: `host:port`
func FullAddr(host string, port uint16) string {
	return fmt.Sprintf("%s:%d", host, port)
}

// FullAddr combines the host and port inside the given Address into a string `host:port`
func (a Address) FullAddr() string {
	return FullAddr(a.Host, a.Port)
}

// Connection is used by the data plane to be able to connect to other nodes, in the data plane.
// A connection offers such support.
type Connection interface {
	GetIdentity() string
	Bind(uint16) error
	ConnectTo(string) error
	DisconnectFrom(string) error
	SendTo(string, []byte) error
	Recv() ([][]byte, error)
	Poll(time.Duration) error
}

// HostFinder is used by the data plane to be able to retreive the Hostname of its "Data Plane Neighbors".
type HostFinder interface {
	GetHost(id NodeId) (string, error)
}

// NodeRoleFlags is an enum, it specifies what are the responsibilities of a particular node in the system
type NodeRoleFlags uint8

const (
	RoleFlags_NONE        NodeRoleFlags = 0b00000000 // Nodes that neither handle database operations, nor they expose an HTTP server; they act as routers inside the system's network, by forwarding requests to the correct nodes.
	RoleFlags_LEADER      NodeRoleFlags = 0b00000001 // The node that won the leader election, it distributes the other roles amongst the remaining nodes. It also handles both read and write database operations.
	RoleFlags_PERSISTENCE NodeRoleFlags = 0b00000010 // Nodes that handle read-only operations on the database (for consistency). Write operations are forwarded to the leader to handle.
	RoleFlags_INPUT       NodeRoleFlags = 0b00000100 // Nodes that expose endpoints that clients can connect to (they host an HTTP server). They gather client requests and forward them to persistence nodes.
)

// String returns the readable role flags
func (r NodeRoleFlags) String() string {
	var s = "{"

	if r == RoleFlags_NONE {
		return fmt.Sprintf("%sNONE}", s)
	}

	if r&RoleFlags_PERSISTENCE > 0 {
		s = fmt.Sprintf("%sPersistence (R", s)
		if r&RoleFlags_LEADER > 0 {
			s = fmt.Sprintf("%sW", s)
		}
		s = fmt.Sprintf("%s)", s)
	}

	if r&RoleFlags_INPUT > 0 {
		s = fmt.Sprintf("%sInput", s)
	}
	return fmt.Sprintf("%s}", s)
}
