/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package cluster

import (
	"encoding/json"
	"fmt"
	"server/cluster/connection"
	"server/cluster/election"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"server/cluster/topology"
	"strconv"
)

// A Cluster Node represents a single node in the distributed system. It holds different components of the node together
type ClusterNode struct {
	config          *node.NodeConfig
	topologyMan     *topology.TopologyManager
	electionCtx     *election.ElectionContext
	postElectionCtx *election.PostElectionContext
	treeMan         *topology.TreeManager

	messageDispatcher map[protocol.MessageType](chan *protocol.Message)
}

func (c *ClusterNode) GT() *topology.TopologyManager { return c.topologyMan }

func NewClusterNode(id node.NodeId, port uint16) (*ClusterNode, error) {
	config, err := node.NewNodeConfig(id, int(port))
	if err != nil {
		return nil, err
	}

	tp, err := topology.NewTopologyManager(*config)
	if err != nil {
		return nil, err
	}

	electionInbox := make(chan *protocol.Message, 100)

	dispatcher := make(map[protocol.MessageType]chan *protocol.Message)
	dispatcher[protocol.ElectionJoin] = electionInbox
	dispatcher[protocol.ElectionStart] = electionInbox
	dispatcher[protocol.ElectionProposal] = electionInbox
	dispatcher[protocol.ElectionVote] = electionInbox
	dispatcher[protocol.ElectionLeader] = electionInbox

	fmt.Printf("Created dispatcher and assigned election messages\n", id)

	return &ClusterNode{
		config,
		tp,
		election.NewElectionContext(),
		nil,
		topology.NewTreeManager(),
		dispatcher,
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

func (n *ClusterNode) removeNeighbor(id node.NodeId) error {
	return n.topologyMan.Remove(id)
}

func (n *ClusterNode) getNeighborAddress(id node.NodeId) (string, error) {
	return n.topologyMan.Get(id)
}

func (n *ClusterNode) IsNeighborsWith(id node.NodeId) bool {
	return n.topologyMan.Exists(id)
}

func (n *ClusterNode) SendToNeighbor(id node.NodeId, message *protocol.Message) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	n.topologyMan.SendTo(id, payload)
	return nil
}

func (n *ClusterNode) recv() (id node.NodeId, msg *protocol.Message, err error) {
	id, payload, err := n.topologyMan.Recv()
	if err != nil {
		return 0, nil, err
	}

	msg = new(protocol.Message)
	err = json.Unmarshal(payload[0], msg)

	if err != nil {
		// Discard message?
		return 0, nil, err
	}

	return id, msg, nil
}

func (n *ClusterNode) RunDispatcher() {
	for {
		id, msg, err := n.recv()
		if err != nil {
			// Skip message
			continue
		}
		if !n.IsNeighborsWith(id) && msg.Type != protocol.ElectionJoin {
			continue
		}

		channel, ok := n.messageDispatcher[msg.Type]

		if ok && channel != nil {
			n.messageDispatcher[msg.Type] <- msg
		}
	}
}

func (n *ClusterNode) RecvElectionMessage() *protocol.Message {
	return <-n.messageDispatcher[protocol.ElectionJoin]
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
	n.electionCtx.Reset()

	// We can skip the ID exchange thanks to the topology creation

	myId := n.config.GetId()
	var neighborId node.NodeId
	for _, neighborId = range n.topologyMan.NeighborList() {
		if myId < neighborId {
			n.electionCtx.Add(neighborId, election.Outgoing)
		} else {
			n.electionCtx.Add(neighborId, election.Incoming)
		}
	}
	n.electionCtx.FirstRound()
}

func (n *ClusterNode) getElectionState() election.ElectionState {
	return n.electionCtx.GetState()
}
func (n *ClusterNode) getElectionStatus() election.ElectionStatus {
	return n.electionCtx.GetStatus()
}

func (n *ClusterNode) ElectionHandle() {

	n.electionCtx.SwitchToState(election.Idle)

	for {

		message := n.RecvElectionMessage()
		switch n.getElectionState() {

		case election.Idle:
			n.handleIdleState(message)

		case election.WaitingYoDown:
			n.handleWaitingYoDown(message)

		case election.WaitingYoUp:
			n.handleWaitingYoUp(message)
		}
	}
}

func (n *ClusterNode) handleIdleState(message *protocol.Message) {
	switch message.Type {
	case protocol.ElectionJoin: // A new neighbor has joined the topology
		if err := n.handleEnteringNeighbor(message); err != nil {
			return
		}
		// The neighbor was added, time to start the election

		n.ElectionSetup()
		for _, neighbor := range n.neighborList() {
			n.SendToNeighbor(neighbor, n.newStartMessage(neighbor))
		}
		n.electionCtx.SetStartReceived()
		n.enterYoDown()

	case protocol.ElectionStart: // A node sent START because a neighbor received JOIN or START
		if n.electionCtx.HasReceivedStart() {
			return
		}
		sender, _ := connection.ExtractIdentifier([]byte(message.Sender))

		n.ElectionSetup()
		for _, neighbor := range n.neighborList() {
			if neighbor == sender {
				continue
			}
			n.SendToNeighbor(neighbor, n.newStartMessage(neighbor))
		}
		n.electionCtx.SetStartReceived()
		n.enterYoDown()

	case protocol.ElectionLeader:
		if n.electionCtx.HasReceivedLeader() {
			return
		}
		sender, _ := connection.ExtractIdentifier([]byte(message.Sender))
		leaderId, _ := strconv.ParseUint(message.Body[0], 10, 64)
		n.endElection(node.Follower, node.NodeId(leaderId))
		for _, neighbor := range n.neighborList() {
			if neighbor == sender {
				continue
			}
			n.SendToNeighbor(neighbor, n.newLeaderMessage(neighbor, node.NodeId(leaderId)))
		}
	default:
		fmt.Printf("TO BE HANDLED")
	}
}

func (n *ClusterNode) handleWaitingYoDown(message *protocol.Message) {
	switch message.Type {
	case protocol.ElectionProposal:
		sender, _ := connection.ExtractIdentifier([]byte(message.Sender))
		proposed, _ := strconv.Atoi(message.Body[0])

		currentRound := n.electionCtx.CurrentRound()
		if message.Round < currentRound {
			return
		} else if message.Round > currentRound {
			n.electionCtx.StashFutureProposal(sender, node.NodeId(proposed), message.Round)
			return
		}

		switch n.electionCtx.GetStatus() {

		case election.InternalNode: // We need to gather all proposal from in neighbors, since we already received a message, update the round context
			n.electionCtx.StoreProposal(sender, node.NodeId(proposed))

			if !n.electionCtx.ReceivedAllProposals() {
				return
			}

			smallestId := n.electionCtx.GetSmallestId()

			// Forward proposal to out neighbors
			for _, outNode := range n.electionCtx.OutNodes() {
				n.SendToNeighbor(outNode, n.newProposalMessage(outNode, smallestId))
			}

		case election.Sink:
			n.electionCtx.StoreProposal(sender, node.NodeId(proposed))

			if !n.electionCtx.ReceivedAllProposals() {
				return
			}

		case election.Source:
			// Nothing, sources don't wait on YoDown
		}
		n.enterYoUp()
	default:
		fmt.Printf("TO BE HANDLED")
	}
}

func (n *ClusterNode) handleWaitingYoUp(message *protocol.Message) {
	switch message.Type {
	case protocol.ElectionVote:
		sender, _ := connection.ExtractIdentifier([]byte(message.Sender))
		vote := (message.Body[0] == "YES")
		pruneChild, _ := strconv.ParseBool(message.Body[1])

		currentRound := n.electionCtx.CurrentRound()
		if message.Round < currentRound {
			return
		} else if message.Round > currentRound {
			n.electionCtx.StashFutureVote(sender, vote, message.Round)
			return
		}

		switch n.electionCtx.GetStatus() {

		case election.InternalNode: // We need to gather all votes from out neighbors, since we already received a message, update the round context
			n.electionCtx.StoreVote(sender, vote)
			if pruneChild {
				n.electionCtx.PruneThisRound(sender)
			}

			if !n.electionCtx.ReceivecAllVotes() {
				return
			}

			// Check if every in node sent the same ID
			pruneMe := true
			reference := n.electionCtx.GetSmallestId()
			for _, proposed := range n.electionCtx.GetAllProposals() {
				if proposed != reference {
					pruneMe = false
					break
				}
			}

			inNodes := n.electionCtx.InNodes()

			voteValidator, _ := n.electionCtx.DetermineVote(inNodes[0]) // Did this parent propose the winning ID?
			n.SendToNeighbor(inNodes[0], n.newVoteMessage(inNodes[0], vote && voteValidator, false))
			for _, inNode := range inNodes[1:] {
				voteValidator, _ = n.electionCtx.DetermineVote(inNode) // Did this parent propose the winning ID?
				n.SendToNeighbor(inNode, n.newVoteMessage(inNode, vote && voteValidator, pruneMe))
				if pruneMe {
					n.electionCtx.PruneThisRound(inNode)
				}
			}

		case election.Source:
			n.electionCtx.StoreVote(sender, vote)
			if pruneChild {
				n.electionCtx.PruneThisRound(sender)
			}

			if !n.electionCtx.ReceivecAllVotes() {
				return
			}

		case election.Sink:
			// Nothing, sinks don't wait on YoUP
		}
		n.electionCtx.NextRound()
		n.enterYoDown()
	default:
		fmt.Printf("TO BE HANDLED")
	}
}

func (n *ClusterNode) enterYoDown() {

	switch n.getElectionStatus() {

	case election.Leader:
		for _, neighbor := range n.neighborList() {
			n.SendToNeighbor(neighbor, n.newLeaderMessage(neighbor, n.getId()))
		}
		n.endElection(node.Leader, n.getId())
		n.electionCtx.SwitchToState(election.Idle)
	case election.Lost:
		n.electionCtx.SwitchToState(election.Idle)

	case election.Source:
		for _, outNode := range n.electionCtx.OutNodes() {
			n.SendToNeighbor(outNode, n.newProposalMessage(outNode, n.getId()))
		}
		n.electionCtx.SwitchToState(election.WaitingYoUp)

	case election.InternalNode:
		n.electionCtx.SwitchToState(election.WaitingYoDown)
	case election.Sink:
		n.electionCtx.SwitchToState(election.WaitingYoDown)
	}
}
func (n *ClusterNode) enterYoUp() {

	switch n.getElectionStatus() {
	case election.Sink:

		// If im here i got all proposals, i need to check for pruning
		pruneMe := false
		if n.electionCtx.InNodesCount() == 1 {
			pruneMe = true

			inNode := n.electionCtx.InNodes()[0] // 1 element only anyways
			vote, _ := n.electionCtx.DetermineVote(inNode)
			n.SendToNeighbor(inNode, n.newVoteMessage(inNode, vote, pruneMe))
			if pruneMe {
				n.electionCtx.PruneThisRound(inNode)
			}
		} else {
			pruneMe = true
			reference := n.electionCtx.GetSmallestId()
			for _, proposed := range n.electionCtx.GetAllProposals() {
				if proposed != reference {
					pruneMe = false
					break
				}
			}

			inNodes := n.electionCtx.InNodes()
			vote, _ := n.electionCtx.DetermineVote(inNodes[0])
			n.SendToNeighbor(inNodes[0], n.newVoteMessage(inNodes[0], vote, false))

			for _, inNode := range inNodes[1:] {
				vote, _ := n.electionCtx.DetermineVote(inNode)
				n.SendToNeighbor(inNode, n.newVoteMessage(inNode, vote, pruneMe))
				if pruneMe {
					n.electionCtx.PruneThisRound(inNode)
				}
			}
		}

		n.electionCtx.NextRound()
		n.electionCtx.SwitchToState(election.WaitingYoDown)

	case election.InternalNode:
		n.electionCtx.SwitchToState(election.WaitingYoUp)

	case election.Source:
		// Shouldn't be possible but anyways it would be
		n.electionCtx.SwitchToState(election.WaitingYoUp)
	}
}

func (n *ClusterNode) handleEnteringNeighbor(msg *protocol.Message) error {
	sourceId, err := connection.ExtractIdentifier([]byte(msg.Sender))
	if err != nil {
		// Malformatted, skip
		return err
	}
	return n.AddNeighbor(sourceId, msg.Body[0])
}

func (n *ClusterNode) newStartMessage(neighbor node.NodeId) *protocol.Message {
	return &protocol.Message{
		Sender:      connection.Identifier(n.getId()),
		Destination: connection.Identifier(neighbor),
		Type:        protocol.ElectionStart,
		Body:        []string{},
		Round:       0,
	}
}
func (n *ClusterNode) newProposalMessage(neighbor, proposal node.NodeId) *protocol.Message {
	return &protocol.Message{
		Sender:      connection.Identifier(n.getId()),
		Destination: connection.Identifier(neighbor),
		Type:        protocol.ElectionProposal,
		Body:        []string{fmt.Sprintf("%d", proposal)},
		Round:       n.electionCtx.CurrentRound(),
	}
}
func (n *ClusterNode) newLeaderMessage(neighbor node.NodeId, leaderId node.NodeId) *protocol.Message {
	return &protocol.Message{
		Sender:      connection.Identifier(n.getId()),
		Destination: connection.Identifier(neighbor),
		Type:        protocol.ElectionLeader,
		Body:        []string{fmt.Sprintf("%d", leaderId)},
		Round:       n.electionCtx.CurrentRound(),
	}
}
func (n *ClusterNode) newVoteMessage(neighbor node.NodeId, vote, prune bool) *protocol.Message {
	// During this round, has this in neighbor voted for this id?

	var body []string = make([]string, 1)
	if vote {
		body[0] = "YES"
	} else {
		body[0] = "NO"
	}
	body = append(body, strconv.FormatBool(prune))
	return &protocol.Message{
		Sender:      connection.Identifier(n.getId()),
		Destination: connection.Identifier(neighbor),
		Type:        protocol.ElectionVote,
		Body:        body,
		Round:       n.electionCtx.CurrentRound(),
	}
}

func (n *ClusterNode) endElection(role node.NodeRole, leaderId node.NodeId) {
	n.electionCtx.Reset()
	n.electionCtx.SetLeaderReceived()
	n.postElectionCtx = election.NewPostElectionContext(role, leaderId)
}
