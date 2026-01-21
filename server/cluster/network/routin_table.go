package network

import (
	"server/cluster/node"
)

type RoutingTable struct {
	upstreamPort       uint16
	upstreamNextHop    node.NodeId
	upstreamNextHopSet bool
	downstreamsNextHop map[node.NodeId]node.NodeId
}

func NewRoutingTable() *RoutingTable {
	return &RoutingTable{
		upstreamPort:       0,
		upstreamNextHop:    0,
		upstreamNextHopSet: false,
		downstreamsNextHop: make(map[node.NodeId]node.NodeId),
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
	r.downstreamsNextHop[downstreamNode] = nextHop
}

func (r *RoutingTable) GetDownstreamNextHop(downstreamNode node.NodeId) (nextHop node.NodeId, ok bool) {
	next, ok := r.downstreamsNextHop[downstreamNode]
	return next, ok
}

func (r *RoutingTable) GetDownstreamsMap() map[node.NodeId]node.NodeId {
	return r.downstreamsNextHop
}

func (r *RoutingTable) Clone() *RoutingTable {
	newMap := make(map[node.NodeId]node.NodeId, len(r.downstreamsNextHop))

	for k, v := range r.downstreamsNextHop {
		newMap[k] = v
	}

	return &RoutingTable{
		r.upstreamPort,
		r.upstreamNextHop,
		r.upstreamNextHopSet,
		newMap,
	}
}
