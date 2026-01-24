package network

import (
	"server/cluster/node"
)

// RoutingTable is a struct used on the data plane,
// calculated by the control plane during the SPT construction.
// It enables the data plane to correctly forward messages towards their destination
type RoutingTable struct {
	upstreamPort       uint16                      // Port used to communicate with the parent in the SPT on the data plane
	upstreamNextHop    node.NodeId                 // Next hop towards the root of the SPT
	upstreamNextHopSet bool                        // Is there any node between me and root?
	downstreamNextHop  map[node.NodeId]node.NodeId // Next hops for downstream. Each input node has a children in the SPT it can be reached with
}

// NewRoutingTable creates and returns a new, empty, routing table
func NewRoutingTable() *RoutingTable {
	return &RoutingTable{
		upstreamPort:       0,
		upstreamNextHop:    0,
		upstreamNextHopSet: false,
		downstreamNextHop:  make(map[node.NodeId]node.NodeId),
	}
}

// SetUpstreamPort sets the port used to communicate with upstream
func (r *RoutingTable) SetUpstreamPort(port uint16) {
	r.upstreamPort = port
}

// GetUpstreamPort returns the port used to communicate with upstream
func (r *RoutingTable) GetUpstreamPort() (port uint16) {
	return r.upstreamPort
}

// SetUpstreamNextHop sets nextHop as the next hop for upstream, towards root
func (r *RoutingTable) SetUpstreamNextHop(nextHop node.NodeId) {
	r.upstreamNextHopSet = true
	r.upstreamNextHop = nextHop
}

// GetUpstreamNextHop returns the next hop, nextHop, towards the root of the tree.
// ok is true if next hop is present
func (r *RoutingTable) GetUpstreamNextHop() (nextHop node.NodeId, ok bool) {
	return r.upstreamNextHop, r.upstreamNextHopSet
}

// SetDownstreamNextHop sets nextHop as the next hop towards downstreamNode
func (r *RoutingTable) SetDownstreamNextHop(downstreamNode, nextHop node.NodeId) {
	r.downstreamNextHop[downstreamNode] = nextHop
}

// GetDownstreamNextHop returns the next hop, nextHop, for the given destination downstreamNode.
// ok is true if the next hop was present
func (r *RoutingTable) GetDownstreamNextHop(downstreamNode node.NodeId) (nextHop node.NodeId, ok bool) {
	next, ok := r.downstreamNextHop[downstreamNode]
	return next, ok
}

// GetDownstreamsMap returns the <destination> => <nextHop> map
func (r *RoutingTable) GetDownstreamsMap() map[node.NodeId]node.NodeId {
	return r.downstreamNextHop
}

// GetDownstreamHops returns only the nextHops, ignoring the corresponding destination
func (r *RoutingTable) GetDownstreamHops() map[node.NodeId]struct{} {
	nextHops := make(map[node.NodeId]struct{})
	for _, v := range r.downstreamNextHop {
		nextHops[v] = struct{}{}
	}
	return nextHops
}

// Clone creates a deep copy of the routing table r
func (r *RoutingTable) Clone() *RoutingTable {
	newMap := make(map[node.NodeId]node.NodeId, len(r.downstreamNextHop))

	for k, v := range r.downstreamNextHop {
		newMap[k] = v
	}

	return &RoutingTable{
		r.upstreamPort,
		r.upstreamNextHop,
		r.upstreamNextHopSet,
		newMap,
	}
}
