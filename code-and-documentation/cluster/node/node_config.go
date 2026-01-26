/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package node

import "fmt"

// NodeConfig contains the minimum required information for a node to enter the cluster
type NodeConfig struct {
	id          NodeId // Required to participate in the election process
	controlPort uint16 // Port that this node exposes to connect to other cluster's nodes for the control plane
	dataPort    uint16 // Port that this node exposes to connect to other cluster's nodes for the data plane
}

// NewNodeConfig creates a new node config struct, ensuring that the given port number falls inside the ephemeral range (49152-65535), to avoid any collision (for example in production, to be safe)
func NewNodeConfig(id NodeId, controlPort, dataPort int) (*NodeConfig, error) {
	if err := IsPortValid(controlPort); err != nil {
		return nil, err
	}
	if err := IsPortValid(dataPort); err != nil {
		return nil, err
	}
	if controlPort == dataPort {
		return nil, fmt.Errorf("Cannot use the same port for data and control plane")
	}
	return &NodeConfig{id, uint16(controlPort), uint16(dataPort)}, nil
}

// GetId retrieves the node's id
func (n *NodeConfig) GetId() NodeId {
	return n.id
}

// GetControlPort retrieves the nodes' port used for control plane
func (n *NodeConfig) GetControlPort() uint16 {
	return n.controlPort
}

// GetDataPort retrieves the nodes' port used for data plane
func (n *NodeConfig) GetDataPort() uint16 {
	return n.dataPort
}
