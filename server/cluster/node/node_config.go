/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package node

import "fmt"

// A NodeConfig contains the minimum required information for a node to enter the cluster
type NodeConfig struct {
	Id   NodeId `json:"id"`   // Required to participate in the election process
	Port uint16 `json:"port"` // Port that this node exposes to connect to other cluster's nodes
}

// Creates a new node config struct, ensuring that the given port number falls inside the ephemeral range (49152-65535), to avoid any collision (for example in production, to be safe)
func NewNodeConfig(id NodeId, port int) (*NodeConfig, error) {
	if err := IsPortValid(port); err != nil {
		return nil, err
	}
	fmt.Printf("Created node config for node %d on port %d\n", id, port)
	return &NodeConfig{id, uint16(port)}, nil
}

// Retrieves the node's id
func (n *NodeConfig) GetId() NodeId {
	return n.Id
}

// Retrieves the nodes' port
func (n *NodeConfig) GetPort() uint16 {
	return n.Port
}
