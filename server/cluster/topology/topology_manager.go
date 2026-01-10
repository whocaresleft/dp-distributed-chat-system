/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package topology

import (
	"fmt"
	"net"
	"server/cluster/connection"
	"server/cluster/node"
	"strconv"
)

type Status bool

const (
	On  Status = true
	Off Status = false
)

// A TopologyManager is a component of a node that handles its local view inside the cluster's network.
type TopologyManager struct {
	id                int
	neighbors         map[node.NodeId]string        // Map of neighboring nodes, maps node ids to strings formatted as `<ip-address>:<port-number>`
	neighborsStatus   map[node.NodeId]Status        // Map of the status of neighbors, for each neighbor it reports either 'On' or 'Off'
	connectionManager *connection.ConnectionManager // Handles the connections between this node and the neighbors
}

// Checks if the given string is a valid address, that is, formatted as `<ip-address>:<port-number>`.
func validateAddressString(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("Address string is mal-formatted: {%s}", address)
	}

	if net.ParseIP(host) == nil {
		return fmt.Errorf("The Host (%s) is not valid", host)
	}

	intPort, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("The port, %s, is not a valid number", port)
	}

	return node.IsPortValid(intPort)
}

// Creates a topology manager with an empty map.
func NewTopologyManager(config node.NodeConfig) (*TopologyManager, error) {

	connMan, err := connection.NewConnectionManager(config)
	if err != nil {
		return nil, err
	}

	t := &TopologyManager{
		int(config.Id),
		make(map[node.NodeId]string),
		make(map[node.NodeId]Status),
		connMan,
	}

	//t.connectionManager.StartMonitoring(t.markOn, t.markOff)
	t.connectionManager.StartMonitoring(func(string) {}, func(string) {})

	return t, nil
}

// Adds the pair (node_id, address) to the neighbors.
// This only works if the neighbor is not already present and the address must be properly formatted as `<ip-address>:<port-number>`.
func (t *TopologyManager) Add(neighbor node.NodeId, address string) error {
	if t.Exists(neighbor) {
		return fmt.Errorf("The ID %d corresponds to an already present neighbor, to replace use .Replace()", neighbor)
	}

	if err := validateAddressString(address); err != nil {
		return err
	}

	if err := t.connectionManager.ConnectTo(address); err != nil {
		return err
	}

	t.LogicalAdd(neighbor, address)
	return nil
}

func (t *TopologyManager) LogicalAdd(neighbor node.NodeId, address string) {
	t.neighbors[neighbor] = address
	t.neighborsStatus[neighbor] = Off
}

// Replaces the address of the given node id with the new one.
// This only works if the neighbor is already present and the address must be properly formatted as `<ip-address>:<port-number>`.
func (t *TopologyManager) replace(neighbor node.NodeId, address string) error {
	if !t.Exists(neighbor) {
		return fmt.Errorf("The ID %d does not correspond to any neighbor, to add it use .Add()", neighbor)
	}

	if err := validateAddressString(address); err != nil {
		return err
	}

	if err := t.connectionManager.SwitchAddress(t.neighbors[neighbor], address); err != nil {
		return err
	}

	t.neighbors[neighbor] = address
	t.neighborsStatus[neighbor] = Off
	return nil
}

// Removes the address of the neighbor with given id.
// It return an error when the node was not present.
func (t *TopologyManager) Remove(neighbor node.NodeId) error {
	if !t.Exists(neighbor) {
		return fmt.Errorf("The ID %d does not correspond to any neighbor, can't proceed to deletion", neighbor)
	}

	if err := t.connectionManager.DisconnectFrom(t.neighbors[neighbor]); err != nil {
		return err
	}

	delete(t.neighbors, neighbor)
	delete(t.neighborsStatus, neighbor)
	return nil
}

// Returns the address of the neighbor with given id.
// It return (address, nil) if the neighbor is present, and (nil, error) otherwise.
func (t *TopologyManager) Get(neighbor node.NodeId) (string, error) {
	node, ok := t.neighbors[neighbor]
	if !ok {
		return "", fmt.Errorf("The ID %d does not correspond to any neighbor", neighbor)
	}
	return node, nil
}

// Returns the status of the neighbor with given id.
// It returns (On/Off, nil) if the neighbor is present, and (Off, error) otherwise.
func (t *TopologyManager) GetStatus(neighbor node.NodeId) (Status, error) {
	status, ok := t.neighborsStatus[neighbor]
	if !ok {
		return Off, fmt.Errorf("The ID %d does not correspond to any neighbor", neighbor)
	}
	return status, nil
}

// Returns the ID of the neighbor with the given address.
// It returns (id, nil) if the neighbor is present, and (0, error) otherwise.
func (t *TopologyManager) getByAddress(neighborAddr string) (node.NodeId, error) {
	for k, v := range t.neighbors {
		if v == neighborAddr {
			return k, nil
		}
	}
	return 0, fmt.Errorf("There is no neighbor with address %s", neighborAddr)
}

func (t *TopologyManager) markOff(neighborAddr string) {
	fmt.Printf("%s went off\n", neighborAddr)
	id, err := t.getByAddress(neighborAddr)
	if err != nil {
		return
	}
	if t.neighborsStatus[id] == On {
		t.neighborsStatus[id] = Off
	}
}
func (t *TopologyManager) markOn(neighborAddr string) {
	fmt.Printf("%s came on\n", neighborAddr)
	id, err := t.getByAddress(neighborAddr)
	if err != nil {
		return
	}
	if t.neighborsStatus[id] == Off {
		t.neighborsStatus[id] = On
	}
}

func GetOutboundIP() string {
	return connection.GetOutboundIP()
}

func Identifier(id node.NodeId) string {
	return connection.Identifier(id)
}

func ExtractIdentifier(address string) (node.NodeId, error) {
	return connection.ExtractIdentifier([]byte(address))
}

// Checks whether there exists a neighbor with the given id.
func (t *TopologyManager) Exists(neighbor node.NodeId) bool {
	_, ok := t.neighbors[neighbor]
	return ok
}

// Returns the number of neighbors.
func (t *TopologyManager) Length() int {
	return len(t.neighbors)
}

// Returns true when there is at least one neighbor.
// It internally calls .Length() and checks if it's greater than zero.
func (t *TopologyManager) HasNeighbors() bool {
	return t.Length() > 0
}

// Returns true when there are no neighbors.
// It internally calls .Length() and checks if it's equal to zero.
func (t *TopologyManager) IsDisconnected() bool {
	return t.Length() == 0
}

// Creates a list with the IDs of all neighbors
func (t *TopologyManager) NeighborList() []node.NodeId {
	list := make([]node.NodeId, t.Length())

	idx := 0
	for id, _ := range t.neighbors {
		list[idx] = id
		idx++
	}
	return list
}

func (t *TopologyManager) SendTo(neighbor node.NodeId, payload []byte) error {
	if !t.Exists(neighbor) {
		return fmt.Errorf("Node %d is not a neighbor", neighbor)
	}
	return t.connectionManager.SendTo(connection.Identifier(neighbor), payload)
}

func (t *TopologyManager) Recv() (sender node.NodeId, contents [][]byte, err error) {
	payload, err := t.connectionManager.Recv()
	if err != nil {
		return 0, [][]byte{}, err
	}
	sender, err = connection.ExtractIdentifier(payload[0])
	if err != nil {
		return 0, [][]byte{}, err
	}
	return sender, payload[2:], err
}

func (t *TopologyManager) Destroy() {
	t.connectionManager.Destroy()
}
