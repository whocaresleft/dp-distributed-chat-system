/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package node

import (
	"encoding/json"
	"strings"
)

// The Role of a node is an enum, it specifies what are the responsibilities of a particular node in the system
type NodeRoleFlags uint8

const (
	Leader      NodeRoleFlags = 0b00000001 // The node that won the leader election, it distributes the other roles amongst the remaining nodes. It also handles both read and write database operations.
	Persistence NodeRoleFlags = 0b00000010 // Nodes that handle read-only operations on the database (for consistency). Write operations are forwarded to the leader to handle.
	Input       NodeRoleFlags = 0b00000100 // Nodes that expose endpoints that clients can connect to (they host an HTTP server). They gather client requests and forward them to persistence nodes.
	Forwarder   NodeRoleFlags = 0b00001000 // Nodes that neither handle database operations, nor they expose an HTTP server; they act as routers inside the system's network, by forwarding requests to the correct nodes.
)

// Maps each NodeRole enum variant to a string with it's name as content. Makes JSON marshalling easier, thanks to O(1) lookup.
var roleToName = map[NodeRoleFlags]string{
	Leader:      "leader",
	Persistence: "persistence",
	Input:       "input",
	Forwarder:   "forwarder",
}

// Reverse map of the previous one. Makes JSON un-marshalling easier, thanks to O(1) lookup
var nameToRole = map[string]NodeRoleFlags{
	"leader":      Leader,
	"persistence": Persistence,
	"input":       Input,
	"forwarder":   Forwarder,
}

// Encodes the NodeRole variant, role, into a JSON field
func (role *NodeRoleFlags) MarshalJSON() ([]byte, error) {
	return json.Marshal(roleToName[*role])
}

// Decodes the JSON field (in the byte array) into a NodeRole
func (role *NodeRoleFlags) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	*role = nameToRole[strings.ToLower(s)]
	return nil
}
