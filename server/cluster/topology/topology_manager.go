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

// TopologyManager is responsible for handling the local view of the node inside the cluster's network.
type TopologyManager struct {
	neighbors         map[node.NodeId]node.Address // Map of neighboring nodes, maps node ids to an Address {host, port}
	neighborsLastSeen map[node.NodeId]time.Time    // Traces the last time this node received a message from each neighbor
	connMan           *network.ConnectionManager   // ConnectionManager that handles the 'under the hood' network management

	randomHeartbeats map[node.NodeId]uint8        // Traces each time a non-neighbor node sends an heartbeat (to proc a REJOIN attempt)
	AckJoinPending   map[node.NodeId]node.Address // Map used when a node sends a JOIN to another, and puts said node in a 'Join Ack' waiting list
	AckPending       map[node.NodeId]node.Address // Map used when a node received a JOIN from a node, sends a JOIN-ACK and puts the node in a 'Ack' waiting list

	neighborMutex sync.RWMutex
}

// NewTopologyManager creates a new, empty. topology manager
func NewTopologyManager(id node.NodeId, port uint16) (*TopologyManager, error) {

	connMan, err := network.NewConnectionManager(id)
	if err != nil {
		return nil, err
	}

	// For the control plane, the connection is eager, the socket is opened as soon as possible.
	// This is because we want to make the node talk to each other so they start and finish an election.
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

// Add adds the pair (neighbor, address) to the neighbors.
// This only works if neighbor is not already present and address must be valid.
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

// Get returns the address of the neighbor with given id.
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

// GetHost returns the hostname of neighbor.
// When successful, error is nil
func (t *TopologyManager) GetHost(neighbor node.NodeId) (string, error) {
	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	addr, ok := t.neighbors[neighbor]
	if !ok {
		return "", nil
	}
	return addr.Host, nil
}

// Remove removes the address of the neighbor with given id.
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

// replace replaces the address of the given node id with the new one.
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

// LogicalAdd adds the pair (neighbor, address) to neighbors
func (t *TopologyManager) LogicalAdd(neighbor node.NodeId, address node.Address) {
	t.neighborMutex.Lock()
	defer t.neighborMutex.Unlock()

	t.logicalAddUnsafe(neighbor, address)
}

// logicalAddUnsafe performs the actual logic of Logically adding neighbor with its address to neighbors, without lock guard
func (t *TopologyManager) logicalAddUnsafe(neighbor node.NodeId, address node.Address) {
	t.neighbors[neighbor] = address
	t.neighborsLastSeen[neighbor] = time.Time{}
	delete(t.AckJoinPending, neighbor) //jic
	delete(t.AckPending, neighbor)
}

// RemoveReAckJoinPending removes oldNeighbor from the join-ack pending list
func (t *TopologyManager) RemoveReAckJoinPending(oldNeighbor node.NodeId) {
	delete(t.AckJoinPending, oldNeighbor)
}

// SetAckJoinPending sets oldNeighbor in the ACK-JOIN waiting list (we are waiting its acj+join)
func (t *TopologyManager) SetAckJoinPending(neighbor node.NodeId, address node.Address) {
	t.AckJoinPending[neighbor] = address
}

// SetReAckJoinPending sets oldNeighbor in the RE+ACK-JOIN waiting list (we are waiting its re+acj+join)
func (t *TopologyManager) SetReAckJoinPending(oldNeighbor node.NodeId) {
	t.AckJoinPending[oldNeighbor] = t.neighbors[oldNeighbor]
}

// SetAckPending sets oldNeighbor in the ACK waiting list (we are waiting its acj)
func (t *TopologyManager) SetAckPending(neighbor node.NodeId, address node.Address) {
	t.AckPending[neighbor] = address
}

// SetReAckPending sets oldNeighbor in the RE+ACK waiting list (we are waiting its re+acj)
func (t *TopologyManager) SetReAckPending(oldNeighbor node.NodeId) {
	t.AckPending[oldNeighbor] = t.neighbors[oldNeighbor]
}

// MarkAck confirms that we received neighbor's ACK, and adds it to the neighbor list
func (t *TopologyManager) MarkAck(neighbor node.NodeId) {

	addr, ok := t.AckPending[neighbor]
	if !ok {
		return
	}
	delete(t.AckPending, neighbor)

	t.LogicalAdd(neighbor, addr)
}

// MarkReAck confirms that we received neighbor's RE+ACK, and update its last seen time
func (t *TopologyManager) MarkReAck(neighbor node.NodeId) {

	_, ok := t.AckPending[neighbor]
	if !ok {
		return
	}
	delete(t.AckPending, neighbor)

	t.UpdateLastSeen(neighbor, time.Now())
}

// MarkAckJoin confirms that we received neighbor's JOIN-ACK, and adds it to the neighbor list
func (t *TopologyManager) MarkAckJoin(neighbor node.NodeId) {

	addr, ok := t.AckJoinPending[neighbor]
	if !ok {
		return
	}
	delete(t.AckJoinPending, neighbor)

	t.LogicalAdd(neighbor, addr)
}

// MarkReAckJoin confirms that we received neighbor's RE+JOIN-ACK, and adds it to the neighbor list, updating its last seen time
func (t *TopologyManager) MarkReAckJoin(neighbor node.NodeId, address node.Address) {

	_, ok := t.AckJoinPending[neighbor]
	if !ok {
		return
	}
	delete(t.AckJoinPending, neighbor)

	t.LogicalAdd(neighbor, address)
	t.UpdateLastSeen(neighbor, time.Now())
}

// IncreaseRandomHeartbeat increases by one the number of heartbeats sent be nonNeighbor, returing the current count.
// When successful, error is nil
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

// GetRandomHeartbeatNumber returns the number of heartbeats sent be nonNeighbor
// When successful, error is nil
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

// ResetRandomHeartbeats resets to 0 the number of random heartbeats sent by nonNeighbor
func (t *TopologyManager) ResetRandomHeartbeats(nonNeighbor node.NodeId) error {
	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	if _, ok := t.neighbors[nonNeighbor]; ok {
		return fmt.Errorf("%d is a neighbor, the heartbeats are normal", nonNeighbor)
	}
	t.randomHeartbeats[nonNeighbor] = 0
	return nil
}

// GetLastSeen returns the last time neighbor was seen, that is, the time we last received a message from him.
// When successful, error is nil
func (t *TopologyManager) GetLastSeen(neighbor node.NodeId) (time.Time, error) {
	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	lastSeen, ok := t.neighborsLastSeen[neighbor]
	if !ok {
		return time.Time{}, fmt.Errorf("The ID %d does not correspond to any neighbor", neighbor)
	}
	return lastSeen, nil
}

// UpdateLastSeen updates the last seen time for neighbor, setting it to the time of the last message
// that we received from him. When successful, error is nil
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

// IsAlive tells whether neighbor is ON or OFF. This is determined by the last time
// a message from himwas received (and consequently last time seen was updated).
func (t *TopologyManager) IsAlive(neighbor node.NodeId) bool {

	t.neighborMutex.RLock()
	defer t.neighborMutex.RUnlock()

	lastSeen, ok := t.neighborsLastSeen[neighbor]
	if !ok || lastSeen.IsZero() {
		return false
	}

	return time.Since(lastSeen) < timeout
}

// getByAddress returns the ID of the neighbor with the given address.
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

// Exists checks whether there exists a neighbor with the given id.
func (t *TopologyManager) Exists(neighbor node.NodeId) bool {
	_, ok := t.neighbors[neighbor]
	return ok
}

// Length returns the number of neighbors.
func (t *TopologyManager) Length() int {
	return len(t.neighbors)
}

// HasNeighbors returns true when there is at least one neighbor.
// It internally calls .Length() and checks if it's greater than zero.
func (t *TopologyManager) HasNeighbors() bool {
	return t.Length() > 0
}

// IsDisconnected returns true when there are no neighbors.
// It internally calls .Length() and checks if it's equal to zero.
func (t *TopologyManager) IsDisconnected() bool {
	return t.Length() == 0
}

// OnNeighborList creates a list with the IDs of all neighbors that are ON.
// That is, they have sent a message withing the timeout period
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

// NeighborList creates a list with the IDs of all neighbors
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

// isPending tell whether the current node is "pending".
// This is because nodes don't accept messages from non-neighbors, BUT, they make an exception for:
//
//	JOIN messages and messages from pending nodes (because they are almost-neighbors)
func (t *TopologyManager) isPending(neighbor node.NodeId) bool {
	_, aj := t.AckJoinPending[neighbor]
	_, a := t.AckPending[neighbor]
	_, ok := t.randomHeartbeats[neighbor]
	return aj || a || ok
}

// SendTo sends payload to the neighbor.
// When successful error is nil
func (t *TopologyManager) SendTo(neighbor node.NodeId, payload []byte) error {
	if !(t.Exists(neighbor) || t.isPending(neighbor)) { // If it's neither a neighbor, nor a pending neighbor
		return fmt.Errorf("Node %d is not a neighbor", neighbor)
	}
	return t.connMan.SendTo(neighbor.Identifier(), payload)
}

// Recv receives a [][]byte on the socket, parses it and returns it as a tuple (node-id, [][]byte, error).
// The parsing is done with the knowledge that the underlying sockets use the following identity: node-<id> and the first trambe (contents[0]) is such identity.
// When successful in receiving, error is nil.
// When socket times out, error is ErrRecvNotReady
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

// Poll waits for an amount of time, timeout, for an event to occur.
// The registered event is POLLIN (messages incoming).
// nil is returned when a message is ready to be recv'd
// ErrRecvNotReady when the poll times out, but IT IS NOT BAD, IT IS A GOOD ERROR
// If there is any other error, a serious problem (network scope) occurred
func (t *TopologyManager) Poll(timeout time.Duration) error {
	return t.connMan.Poll(timeout)
}

// IsRecvNotReadyError tells wheter the error err is ErrRecvNotReady
func IsRecvNotReadyError(err error) bool {
	return errors.Is(err, network.ErrRecvNotReady)
}

// Destroy closes the manager
func (t *TopologyManager) Destroy() {
	t.connMan.Destroy()
}
