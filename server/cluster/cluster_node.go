/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package cluster

import (
	"fmt"
	"server/cluster/election"
	"server/cluster/node"
	"server/cluster/topology"
)

// A Cluster Node represents a single node in the distributed system. It holds different components of the node together
type ClusterNode struct {
	config          *node.NodeConfig
	topologyMan     *topology.TopologyManager
	electionCtx     *election.ElectionContext
	postElectionCtx *election.PostElectionContext
	treeMan         *topology.TreeManager
}

func NewClusterNode(id node.NodeId, port uint16) (*ClusterNode, error) {
	config, err := node.NewNodeConfig(id, int(port))
	if err != nil {
		return nil, err
	}

	tp, err := topology.NewTopologyManager(*config)
	if err != nil {
		return nil, err
	}

	return &ClusterNode{
		config,
		tp,
		nil,
		nil,
		topology.NewTreeManager(),
	}, nil
}

func (n *ClusterNode) getId() node.NodeId {
	return n.config.GetId()
}
func (n *ClusterNode) getPort() uint16 {
	return n.config.GetPort()
}

func (n *ClusterNode) AddNeighbor(id node.NodeId, address string) error {
	if n.getId() == id {
		return fmt.Errorf("Cannot set the node as its own neighbor")
	}
	return n.topologyMan.Add(id, address)
}

func (n *ClusterNode) replaceNeighbor(id node.NodeId, newAddress string) error {
	if n.getId() == id {
		return fmt.Errorf("Cannot set the node as its own neighbor")
	}
	return n.topologyMan.Replace(id, newAddress)
}

func (n *ClusterNode) removeNeighbor(id node.NodeId) error {
	return n.topologyMan.Remove(id)
}

func (n *ClusterNode) getNeighbor(id node.NodeId) (string, error) {
	return n.topologyMan.Get(id)
}

func (n *ClusterNode) isNeighborsWith(id node.NodeId) bool {
	return n.topologyMan.Exists(id)
}

func (n *ClusterNode) hasNeighbors() bool {
	return n.topologyMan.HasNeighbors()
}

func (n *ClusterNode) neighborList() []node.NodeId {
	return n.topologyMan.NeighborList()
}

func (n *ClusterNode) setTreeParent(parentId node.NodeId) error {
	if n.getId() == parentId {
		return fmt.Errorf("Cannot set the node as its own parent")
	}
	n.treeMan.SetParent(&parentId)
	return nil
}

func (n *ClusterNode) getTreeParent() (node.NodeId, error) {
	return n.treeMan.GetParent()
}

func (n *ClusterNode) isTreeRoot() bool {
	return n.treeMan.IsRoot()
}

func (n *ClusterNode) isTreeLeaf() bool {
	return n.treeMan.IsLeaf()
}

func (n *ClusterNode) treeChildrenLength() int {
	return n.treeMan.ChildrenLength()
}

func (n *ClusterNode) existsTreeChild(childId node.NodeId) bool {
	return n.treeMan.ExistsChild(childId)
}

func (n *ClusterNode) addTreeChild(childId node.NodeId) error {
	if n.getId() == childId {
		return fmt.Errorf("Cannot set the node as its own child")
	}
	return n.treeMan.AddChild(childId)
}

func (n *ClusterNode) removeTreeChild(childId node.NodeId) error {
	return n.treeMan.Remove(childId)
}

func (n *ClusterNode) Destroy() {
	n.config = nil
	n.electionCtx = nil
	n.postElectionCtx = nil
	n.treeMan = nil
	n.topologyMan.Destroy()
}

func (n *ClusterNode) ElectionSetup() {
	n.electionCtx = election.NewElectionContext()

	// We can skip the ID exchange thanks to the topology creation

	myId := n.config.GetId()
	var neighborId node.NodeId
	for _, neighborId = range n.topologyMan.NeighborList() {
		if myId < neighborId {
			n.electionCtx.Add(neighborId, election.Outgoing)
			fmt.Printf("%d, For neighbor %d the direction was Outgoing", myId, neighborId)
		} else {
			n.electionCtx.Add(neighborId, election.Incoming)
			fmt.Printf("%d, For neighbor %d the direction was Incoming", myId, neighborId)
		}

	}
}

func (n *ClusterNode) handleElection() {

	type ElectionResult bool
	const (
		lost ElectionResult = false
		won
	)
	var electionResult ElectionResult

	n.electionCtx.SwitchToState(election.Idle)

	for !n.electionCtx.IsDone() {

		switch n.electionCtx.GetState() {

		case election.Idle:
			_, msg, _ := n.topologyMan.Recv()
			if msg[0] == "start" {
				n.ElectionSetup()
				n.electionCtx.SwitchToState(election.YoDown)
			}

		case election.YoDown:
			switch n.electionCtx.GetStatus() {
			case election.Source:
				if n.electionCtx.OutNodesCount() == 0 {
					electionResult = won
					n.electionCtx.SetDone()
					continue
				}

				for _, outNode := range n.electionCtx.OutNodes() {
					n.topologyMan.SendTo(outNode, []byte{byte(n.getId())})
				}
				n.electionCtx.SwitchToState(election.YoUp)
			}

		case election.YoUp:
			switch n.electionCtx.GetStatus() {
			case election.Source:
				// A source WAITS until it receives the vote from all out-neighbors
				winning := 0
				outNodes := n.electionCtx.OutNodes()
				for _, node := range outNodes {
					_, vote, _ := n.topologyMan.Recv()
					if vote[0] == "YES" {
						winning++
					} else { // "NO"
						n.electionCtx.InvertOrientation(node)
					}
				}
				n.electionCtx.UpdateStatus()
				n.electionCtx.SwitchToState(election.YoDown)
			}
		}

	}

	fmt.Printf("Election result: %d", electionResult)
}
