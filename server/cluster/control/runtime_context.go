package control

import (
	"server/cluster/election"
	"server/cluster/network"
	"server/cluster/node"
	"sync"
)

type RuntimeContext struct {
	mutex sync.RWMutex

	lastElection *election.ElectionResult
	roleFlags    node.NodeRoleFlags // Role of the node, specifies what are the responsibilities of a particular node in the system.
	routing      *network.RoutingTable
}

func NewRuntimeContext() *RuntimeContext {
	return &RuntimeContext{sync.RWMutex{}, nil, node.RoleFlags_NONE, nil}
}

func (r *RuntimeContext) SetLastElection(e *election.ElectionResult) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.lastElection = e
}

func (r *RuntimeContext) GetLastElection() *election.ElectionResult {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.lastElection
}

func (r *RuntimeContext) SetRoles(roleFlags node.NodeRoleFlags) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.roleFlags = roleFlags
}

// Returns the node's role.
func (r *RuntimeContext) GetRoles() node.NodeRoleFlags {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.roleFlags
}

func (r *RuntimeContext) SetRouting(rt *network.RoutingTable) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.routing = rt
}

func (r *RuntimeContext) GetRouting() *network.RoutingTable {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.routing
}

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
