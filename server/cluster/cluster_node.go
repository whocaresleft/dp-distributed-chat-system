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
	"net"
	"server/cluster/connection"
	"server/cluster/election"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"server/cluster/topology"
	"strconv"
	"time"
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

	fmt.Printf("Created dispatcher and assigned election messages for node %d\n", id)

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
	err := n.topologyMan.Add(id, address)
	go func() {
		time.Sleep(3 * time.Second)
		n.SendToNeighbor(id, n.newJoinMessage(id))
	}()
	return err
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
	fmt.Printf("Sent to %d, %v\n", id, *message)
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
	n.electionCtx.UpdateStatus()
	n.electionCtx.FirstRound()
}

func (n *ClusterNode) getElectionState() election.ElectionState {
	return n.electionCtx.GetState()
}
func (n *ClusterNode) getElectionStatus() election.ElectionStatus {
	return n.electionCtx.GetStatus()
}

func (n *ClusterNode) ElectionHandle() {
	fmt.Printf("%d: Election routine stard\n", n.getId())
	n.electionCtx.SwitchToState(election.Idle)

	for {

		fmt.Printf("%d: Waiting for election message\n", n.getId())
		message := n.RecvElectionMessage()

		fmt.Printf("%d: Received %v on round %d. Also my state is %v\n", n.getId(), *message, n.electionCtx.CurrentRound(), n.getElectionState())
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
		fmt.Printf("%d: Election context set up perfectly\n", n.getId())
		n.electionCtx.SetStartReceived()
		for _, neighbor := range n.neighborList() {
			n.SendToNeighbor(neighbor, n.newStartMessage(neighbor))
		}
		fmt.Printf("%d: Mandato start ai vicini, comincio YODOWN\n", n.getId())
		n.enterYoDown()

	case protocol.ElectionStart: // A node sent START because a neighbor received JOIN or START
		if n.electionCtx.HasReceivedStart() {
			return
		}
		sender, _ := connection.ExtractIdentifier([]byte(message.Sender))

		n.electionCtx.SetStartReceived()
		n.ElectionSetup()
		fmt.Printf("%d: Election context set up perfectly\n", n.getId())
		for _, neighbor := range n.neighborList() {
			if neighbor == sender {
				continue
			}
			n.SendToNeighbor(neighbor, n.newStartMessage(neighbor))
		}
		fmt.Printf("%d: I too have received START, i forwarded it to my neighbors, but sender, and i start yo down\n", n.getId())
		n.enterYoDown()

	case protocol.ElectionLeader:
		n.handleLeaderMessage(message)
	default:
		fmt.Printf("TO BE HANDLED")
	}
}

func (n *ClusterNode) handleLeaderMessage(message *protocol.Message) {
	if n.electionCtx.HasReceivedLeader() {
		return
	}
	sender, _ := connection.ExtractIdentifier([]byte(message.Sender))
	leaderId, _ := strconv.ParseUint(message.Body[0], 10, 64)
	n.endElection(node.Follower, node.NodeId(leaderId))
	fmt.Printf("I ended the election: Leader is: %d", leaderId)
	for _, neighbor := range n.neighborList() {
		if neighbor == sender {
			continue
		}
		n.SendToNeighbor(neighbor, n.newLeaderMessage(neighbor, node.NodeId(leaderId)))
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
		fmt.Printf("%d: I have received the proposal: %d", n.getId(), proposed)

		switch n.electionCtx.GetStatus() {

		case election.InternalNode: // We need to gather all proposal from in neighbors, since we already received a message, update the round context
			n.electionCtx.StoreProposal(sender, node.NodeId(proposed))

			if !n.electionCtx.ReceivedAllProposals() {
				return
			}
			fmt.Printf("%d: I have received ALL!", n.getId())

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
			fmt.Printf("%d: I have received ALL!", n.getId())

		case election.Source:
			// Nothing, sources don't wait on YoDown
		}

		fmt.Printf("My yo down is done! I can enter yo up\n")
		n.enterYoUp()
	case protocol.ElectionLeader:
		fmt.Printf("YO\n")
		n.handleLeaderMessage(message)

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

		fmt.Printf("%d: I have received the vote: %d, %v, %v", n.getId(), sender, vote, pruneChild)

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

			if !n.electionCtx.ReceivedAllVotes() {
				return
			}

			fmt.Printf("%d, I have received all votes\n", n.getId())

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

			if !n.electionCtx.ReceivedAllVotes() {
				return
			}

			fmt.Printf("%d, I have received all votes\n", n.getId())

		case election.Sink:
			// Nothing, sinks don't wait on YoUP
		}

		fmt.Printf("%d: My roudn is done\n", n.getId())
		n.electionCtx.NextRound()
		n.enterYoDown()
	default:
		fmt.Printf("TO BE HANDLED")
	}
}

func (n *ClusterNode) enterYoDown() {

	fmt.Printf("%d: Started yo down, also i am a ", n.getId())
	switch n.getElectionStatus() {

	case election.Leader:
		fmt.Printf("leader\n")
		for _, neighbor := range n.neighborList() {
			n.SendToNeighbor(neighbor, n.newLeaderMessage(neighbor, n.getId()))
		}
		n.endElection(node.Leader, n.getId())
		n.electionCtx.SwitchToState(election.Idle)
	case election.Lost:
		fmt.Printf("Loser\n")
		n.electionCtx.SwitchToState(election.Idle)

	case election.Source:
		fmt.Printf("source\n")
		for _, outNode := range n.electionCtx.OutNodes() {
			n.SendToNeighbor(outNode, n.newProposalMessage(outNode, n.getId()))
		}
		fmt.Printf("%d: I have completed my yo down: Election proposals sent. Waiting for YO UP\n", n.getId())
		n.electionCtx.SwitchToState(election.WaitingYoUp)

	case election.InternalNode:
		fmt.Printf("internal node. I just switch to waiting\n")
		n.electionCtx.SwitchToState(election.WaitingYoDown)
	case election.Sink:
		fmt.Printf("sink. I just switch to waiting\n")
		n.electionCtx.SwitchToState(election.WaitingYoDown)
	}
}
func (n *ClusterNode) enterYoUp() {

	fmt.Printf("%d: Started yo up, also i am a ", n.getId())
	switch n.getElectionStatus() {
	case election.Sink:
		fmt.Printf("Sink\n")
		// If im here i got all proposals, i need to check for pruning
		pruneMe := false
		if n.electionCtx.InNodesCount() == 1 {
			fmt.Printf("%d: I have a single parent, i need to be pruned\n", n.getId())
			pruneMe = true

			inNode := n.electionCtx.InNodes()[0] // 1 element only anyways
			vote, _ := n.electionCtx.DetermineVote(inNode)
			fmt.Printf("%d: My vote is %v\n", n.getId(), vote)
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
			fmt.Printf("%d: I have multiple parents but they all gave me the same, i need to be pruned from all but one of them\n", n.getId())

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

		fmt.Printf("%d: My round is done\n", n.getId())
		n.electionCtx.NextRound()
		n.electionCtx.SwitchToState(election.WaitingYoDown)

	case election.InternalNode:
		fmt.Printf("Internal node. I have nothing to do, i go to WAIT\n")
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
	fmt.Printf("%d: A neighbor connected to me (%d %s), i have to add it!\n", n.getId(), sourceId, msg.Body[0])
	n.aknowledgeNeighborExistance(sourceId, msg.Body[0])
	return nil
}

func (n *ClusterNode) aknowledgeNeighborExistance(id node.NodeId, address string) {
	n.topologyMan.LogicalAdd(id, address)
}

func getOutboundIP() string {
	conn, _ := net.Dial("udp", "8.8.8.8:80")
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}

func (n *ClusterNode) newJoinMessage(neighbor node.NodeId) *protocol.Message {
	return &protocol.Message{
		Sender:      connection.Identifier(n.getId()),
		Destination: connection.Identifier(neighbor),
		Type:        protocol.ElectionJoin,
		Body:        []string{fmt.Sprintf("%s:%d", getOutboundIP(), n.getPort())},
		Round:       0,
	}
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
