/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package topology

import (
	"errors"
	"fmt"
	"server/cluster/network"
	"server/cluster/node"
	"sync"
	"time"
)

const timeout = 10 * time.Second

// A TopologyManager is a component of a node that handles its local view inside the cluster's network.
type TopologyManager struct {
	neighbors         map[node.NodeId]node.Address // Map of neighboring nodes, maps node ids to strings formatted as `<ip-address>:<port-number>`
	neighborsLastSeen map[node.NodeId]time.Time
	connMan           *network.ConnectionManager // Handles the connections between this node and the neighbors

	randomHeartbeats map[node.NodeId]uint8
	AckJoinPending   map[node.NodeId]node.Address
	AckPending       map[node.NodeId]node.Address

	neighborMutex sync.RWMutex
}

// Creates a topology manager with an empty map.
func NewTopologyManager(id node.NodeId, port uint16) (*TopologyManager, error) {

	connMan, err := network.NewConnectionManager(id)
	if err != nil {
		return nil, err
	}

	if err := connMan.Bind(port); err != nil {
		return nil, err
	}

	t := &TopologyManager{
		make(map[node.NodeId]node.Address),
		make(map[node.NodeId]time.Time),
		connMan,
		make(map[node.NodeId]uint8),
		make(map[node.NodeId]node.Address),
		make(map[node.NodeId]node.Address),
		sync.RWMutex{},
	}

	return t, nil
}

// Adds the pair (node_id, address) to the neighbors.
// This only works if the neighbor is not already present and the address must be properly formatted as `<ip-address>:<port-number>`.
func (t *TopologyManager) Add(neighbor node.NodeId, address node.Address) error {
	if _, ok := t.neighbors[neighbor]; ok {
		return fmt.Errorf("The ID %d corresponds to an already present neighbor, to replace use .Replace()", neighbor)
	}

	if err := node.ValidateAddress(address); err != nil {
		return err
	}

	if err := t.connMan.ConnectTo(address.FullAddr()); err != nil {
		return err
	}

	t.neighborMutex.Lock()
	t.AckJoinPending[neighbor] = address
	t.neighborMutex.Unlock()
	return nil
}

func (t *TopologyManager) IncreaseRandomHeartbeat(nonNeighbor node.NodeId) (uint8, error) {
	t.neighborMutex.RLock()

	if _, ok := t.neighbors[nonNeighbor]; ok {
		return 0, fmt.Errorf("%d is a neighbor, the heartbeat is justified", nonNeighbor)
	}
	t.neighborMutex.RUnlock()

	if _, ok := t.randomHeartbeats[nonNeighbor]; !ok {
		t.randomHeartbeats[nonNeighbor] = 0
	}
	t.randomHeartbeats[nonNeighbor]++
	return t.randomHeartbeats[nonNeighbor], nil
}

func (t *TopologyManager) ResetRandomHeartbeats(nonNeighbor node.NodeId) error {
	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	if _, ok := t.neighbors[nonNeighbor]; ok {
		return fmt.Errorf("%d is a neighbor, the heartbeats are normal", nonNeighbor)
	}
	t.randomHeartbeats[nonNeighbor] = 0
	return nil
}

func (t *TopologyManager) GetRandomHeartbeatNumber(nonNeighbor node.NodeId) (uint8, error) {
	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	if _, ok := t.neighbors[nonNeighbor]; ok {
		return 0, fmt.Errorf("%d is a neighbor, the heartbeats are normal", nonNeighbor)
	}
	h, ok := t.randomHeartbeats[nonNeighbor]
	if !ok {
		return 0, fmt.Errorf("%d has not send any random heartbeat", nonNeighbor)
	}
	return h, nil
}
func (t *TopologyManager) RemoveReAckJoinPending(oldNeighbor node.NodeId) {
	delete(t.AckJoinPending, oldNeighbor)
}
func (t *TopologyManager) SetReAckJoinPending(oldNeighbor node.NodeId) {
	t.AckJoinPending[oldNeighbor] = t.neighbors[oldNeighbor]
}
func (t *TopologyManager) SetAckJoinPending(neighbor node.NodeId, address node.Address) {
	t.AckJoinPending[neighbor] = address
}

func (t *TopologyManager) SetReAckPending(oldNeighbor node.NodeId) {
	t.AckPending[oldNeighbor] = t.neighbors[oldNeighbor]
}

func (t *TopologyManager) SetAckPending(neighbor node.NodeId, address node.Address) {
	t.AckPending[neighbor] = address
}

func (t *TopologyManager) MarkAck(neighbor node.NodeId) {

	t.neighborMutex.Lock()
	addr, ok := t.AckPending[neighbor]
	if !ok {
		return
	}
	delete(t.AckPending, neighbor)
	t.neighborMutex.Unlock()

	t.LogicalAdd(neighbor, addr)
}
func (t *TopologyManager) MarkReAck(neighbor node.NodeId) {

	_, ok := t.AckPending[neighbor]
	if !ok {
		return
	}
	delete(t.AckPending, neighbor)

	t.UpdateLastSeen(neighbor, time.Now())
}

func (t *TopologyManager) MarkAckJoin(neighbor node.NodeId) {

	addr, ok := t.AckJoinPending[neighbor]
	if !ok {
		return
	}
	delete(t.AckJoinPending, neighbor)

	t.LogicalAdd(neighbor, addr)
}

func (t *TopologyManager) MarkReAckJoin(neighbor node.NodeId, address node.Address) {

	_, ok := t.AckJoinPending[neighbor]
	if !ok {
		return
	}
	delete(t.AckJoinPending, neighbor)

	t.LogicalAdd(neighbor, address)
	t.UpdateLastSeen(neighbor, time.Now())
}

func (t *TopologyManager) LogicalAdd(neighbor node.NodeId, address node.Address) {
	t.neighborMutex.Lock()
	defer t.neighborMutex.Unlock()

	t.neighbors[neighbor] = address
	t.neighborsLastSeen[neighbor] = time.Time{}
	delete(t.AckJoinPending, neighbor) //jic
	delete(t.AckPending, neighbor)
}

// Replaces the address of the given node id with the new one.
// This only works if the neighbor is already present and the address must be properly formatted as `<ip-address>:<port-number>`.
func (t *TopologyManager) replace(neighbor node.NodeId, address node.Address) error {
	if !t.Exists(neighbor) {
		return fmt.Errorf("The ID %d does not correspond to any neighbor, to add it use .Add()", neighbor)
	}

	if err := node.ValidateAddress(address); err != nil {
		return err
	}

	if err := t.connMan.SwitchAddress(t.neighbors[neighbor].FullAddr(), address.FullAddr()); err != nil {
		return err
	}

	t.LogicalAdd(neighbor, address)
	return nil
}

// Removes the address of the neighbor with given id.
// It return an error when the node was not present.
func (t *TopologyManager) Remove(neighbor node.NodeId) error {
	if !t.Exists(neighbor) {
		return fmt.Errorf("The ID %d does not correspond to any neighbor, can't proceed to deletion", neighbor)
	}

	if err := t.connMan.DisconnectFrom(t.neighbors[neighbor].FullAddr()); err != nil {
		return err
	}

	t.neighborMutex.Lock()
	defer t.neighborMutex.Unlock()

	delete(t.neighbors, neighbor)
	delete(t.neighborsLastSeen, neighbor)
	return nil
}

// Returns the address of the neighbor with given id.
// It return (address, nil) if the neighbor is present, and (nil, error) otherwise.
func (t *TopologyManager) Get(neighbor node.NodeId) (node.Address, error) {
	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	addr, ok := t.neighbors[neighbor]
	if !ok {
		return node.Address{}, fmt.Errorf("The ID %d does not correspond to any neighbor", neighbor)
	}
	return addr, nil
}

func (t *TopologyManager) GetHost(neighbor node.NodeId) (string, error) {
	t.neighborMutex.RLock()
	defer t.neighborMutex.Unlock()

	addr, err := t.Get(neighbor)
	if err != nil {
		return "", nil
	}
	return addr.Host, nil
}

func (t *TopologyManager) GetLastSeen(neighbor node.NodeId) (time.Time, error) {
	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	lastSeen, ok := t.neighborsLastSeen[neighbor]
	if !ok {
		return time.Time{}, fmt.Errorf("The ID %d does not correspond to any neighbor", neighbor)
	}
	return lastSeen, nil
}

// Returns the ID of the neighbor with the given address.
// It returns (id, nil) if the neighbor is present, and (0, error) otherwise.
func (t *TopologyManager) getByAddress(neighborAddr node.Address) (node.NodeId, error) {
	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	for k, v := range t.neighbors {
		if v == neighborAddr {
			return k, nil
		}
	}
	return 0, fmt.Errorf("There is no neighbor with address %v", neighborAddr)
}

func (t *TopologyManager) UpdateLastSeen(neighbor node.NodeId, lastSeen time.Time) error {
	t.neighborMutex.Lock()
	defer t.neighborMutex.Unlock()

	_, ok := t.neighborsLastSeen[neighbor]
	if !ok {
		return fmt.Errorf("The ID %d does not correspont to any neighbor", neighbor)
	}
	t.neighborsLastSeen[neighbor] = lastSeen
	return nil
}

func (t *TopologyManager) IsAlive(neighbor node.NodeId) bool {

	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	lastSeen, ok := t.neighborsLastSeen[neighbor]
	if !ok || lastSeen.IsZero() {
		return false
	}

	return time.Since(lastSeen) < timeout
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

func (t *TopologyManager) OnNeighborList() []node.NodeId {
	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	list := make([]node.NodeId, 0)

	for id := range t.neighbors {
		if t.IsAlive(id) {
			list = append(list, id)
		}
	}
	return list
}

// Creates a list with the IDs of all neighbors
func (t *TopologyManager) NeighborList() []node.NodeId {
	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	list := make([]node.NodeId, t.Length())

	idx := 0
	for id := range t.neighbors {
		list[idx] = id
		idx++
	}
	return list
}

func (t *TopologyManager) isPending(neighbor node.NodeId) bool {
	_, aj := t.AckJoinPending[neighbor]
	_, a := t.AckPending[neighbor]
	_, ok := t.randomHeartbeats[neighbor]
	return aj || a || ok
}

func (t *TopologyManager) SendTo(neighbor node.NodeId, payload []byte) error {
	if !(t.Exists(neighbor) || t.isPending(neighbor)) { // If it's neither a neighbor, nor a pending neighbor
		return fmt.Errorf("Node %d is not a neighbor", neighbor)
	}
	return t.connMan.SendTo(neighbor.Identifier(), payload)
}

func (t *TopologyManager) Recv() (sender node.NodeId, contents [][]byte, err error) {
	payload, err := t.connMan.Recv()
	if err != nil {
		return 0, [][]byte{}, err
	}
	sender, err = node.ExtractIdentifier(payload[0])
	if err != nil {
		return 0, [][]byte{}, err
	}
	return sender, payload[2:], err
}

func (t *TopologyManager) Destroy() {
	t.connMan.Destroy()
}

func IsRecvNotReadyError(err error) bool {
	return errors.Is(err, network.ErrRecvNotReady)
}

func (t *TopologyManager) Poll(timeout time.Duration) error {
	return t.connMan.Poll(timeout)
}
