/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package control

import (
	"server/cluster/election"
	"server/cluster/network"
	"server/cluster/node"
	"sync"
)

// RuntimeContext stores information about a node's job after the election process.
// It stores the latest election, to remember the ID and leader, while also carrying the role flags (stating whether it's an input or persistence node)
// It also has a routing table, to determine next hops in the SPT
type RuntimeContext struct {
	mutex sync.RWMutex

	lastElection *election.ElectionResult // LeaderID and ElectionID of last election
	roleFlags    node.NodeRoleFlags       // Role of the node, specifies what are the responsibilities of a particular node in the system.
	routing      *network.RoutingTable    // Routing table for upstream and downstream next hops
}

//=========================================================//
// The following functions are basically wrappers of the   //
// ones given by the fields of the RuntimeContext, the     //
// only difference is that they are mutex guarded          //
//=========================================================//

// NewRuntimeContext returns a new runtime context, with empty flags
func NewRuntimeContext() *RuntimeContext {
	return &RuntimeContext{sync.RWMutex{}, nil, node.RoleFlags_NONE, nil}
}

// SetLastElection sets er as the last election result
func (r *RuntimeContext) SetLastElection(er *election.ElectionResult) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.lastElection = er
}

// GetLastElection returns the last election result er
func (r *RuntimeContext) GetLastElection() (er *election.ElectionResult) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.lastElection
}

// SetRoles sets roleFlags as the role flags
func (r *RuntimeContext) SetRoles(roleFlags node.NodeRoleFlags) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.roleFlags = roleFlags
}

// GetRoles returns the role flags roleFlags
func (r *RuntimeContext) GetRoles() (roleFlags node.NodeRoleFlags) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.roleFlags
}

// SetRouting sets rt as the routint table
func (r *RuntimeContext) SetRouting(rt *network.RoutingTable) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.routing = rt
}

// GetRouring returns the routing table rt
func (r *RuntimeContext) GetRouting() (rt *network.RoutingTable) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.routing
}

// Clone creates a deep copy of the RuntimeContext r
func (r *RuntimeContext) Clone() *RuntimeContext {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	electionRes := election.NewElectionResult(
		r.lastElection.GetLeaderID(),
		r.lastElection.GetElectionID(),
	)

	roleFlags := r.roleFlags

	table := r.routing.Clone()

	return &RuntimeContext{
		sync.RWMutex{},
		electionRes,
		roleFlags,
		table,
	}
}
