package network

import (
	"server/cluster/node"
)

type RoutingTable struct {
	upstreamPort       uint16
	upstreamNextHop    node.NodeId
	upstreamNextHopSet bool
	downstreamNextHop  map[node.NodeId]node.NodeId
}

func NewRoutingTable() *RoutingTable {
	return &RoutingTable{
		upstreamPort:       0,
		upstreamNextHop:    0,
		upstreamNextHopSet: false,
		downstreamNextHop:  make(map[node.NodeId]node.NodeId),
	}
}

func (r *RoutingTable) SetUpstreamPort(port uint16) {
	r.upstreamPort = port
}

func (r *RoutingTable) GetUpstreamPort() (port uint16) {
	return r.upstreamPort
}

func (r *RoutingTable) SetUpstreamNextHop(nextHop node.NodeId) {
	r.upstreamNextHopSet = true
	r.upstreamNextHop = nextHop
}

func (r *RoutingTable) GetUpstreamNextHop() (nextHop node.NodeId, ok bool) {
	return r.upstreamNextHop, r.upstreamNextHopSet
}

func (r *RoutingTable) SetDownstreamNextHop(downstreamNode, nextHop node.NodeId) {
	r.downstreamNextHop[downstreamNode] = nextHop
}

func (r *RoutingTable) GetDownstreamNextHop(downstreamNode node.NodeId) (nextHop node.NodeId, ok bool) {
	next, ok := r.downstreamNextHop[downstreamNode]
	return next, ok
}

func (r *RoutingTable) GetDownstreamsMap() map[node.NodeId]node.NodeId {
	return r.downstreamNextHop
}

func (r *RoutingTable) GetDownstreamHops() map[node.NodeId]struct{} {
	nextHops := make(map[node.NodeId]struct{})
	for _, v := range r.downstreamNextHop {
		nextHops[v] = struct{}{}
	}
	return nextHops
}

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
