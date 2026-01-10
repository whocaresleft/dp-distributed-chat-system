/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"server/cluster/election"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"server/cluster/topology"
	"strconv"
	"sync"
	"time"
)

type outMessage struct {
	Neighbor node.NodeId
	Message  protocol.Message
}

// A Cluster Node represents a single node in the distributed system. It holds different components of the node together
type ClusterNode struct {
	ctx    context.Context
	cancel context.CancelFunc

	logicalClock uint64
	clockMutex   sync.Mutex

	config          *node.NodeConfig
	topologyMan     *topology.TopologyManager
	electionCtx     *election.ElectionContext
	postElectionCtx *election.PostElectionContext
	treeMan         *topology.TreeManager

	topologyInbox chan *protocol.JoinMessage
	electionInbox chan *election.ElectionMessage
	outputChannel chan outMessage
	logger        *log.Logger
}

// Creates a new cluster node with the given ID and on the given port
// It returns a pointer to said node if no problems arise. Otherwise, the pointer is nil and an appropriate error is returned
func NewClusterNode(id node.NodeId, port uint16) (*ClusterNode, error) {
	logOut, _ := os.OpenFile(fmt.Sprintf("%d.log", id), os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
	logger := log.New(logOut, fmt.Sprintf("[[Node %d]]: ", id), log.Ldate|log.Ltime)

	logger.Printf("Logger created. Starting node creation...")
	logger.Printf("Creating configuration component")

	config, err := node.NewNodeConfig(id, int(port))
	if err != nil {
		return nil, err
	}

	logger.Printf("Configuration component correctly created: Id{%d}, Port{%d}", id, port)
	logger.Printf("Creating topology component")

	tp, err := topology.NewTopologyManager(*config)
	if err != nil {
		return nil, err
	}

	logger.Printf("Topology component correctly created")

	electionInbox := make(chan *election.ElectionMessage, 500)
	joinInbox := make(chan *protocol.JoinMessage, 500)
	outChan := make(chan outMessage, 500)

	ctx, cancel := context.WithCancel(context.Background())

	logger.Printf("Created context")
	logger.Printf("Node is all set")

	return &ClusterNode{
		ctx,
		cancel,
		0,
		sync.Mutex{},
		config,
		tp,
		election.NewElectionContext(config.GetId()),
		nil,
		topology.NewTreeManager(),
		joinInbox,
		electionInbox,
		outChan,
		logger,
	}, nil
}

// Logs the given string. Wrap around logger.Printf
func (n *ClusterNode) logf(format string, a ...any) {
	n.logger.Printf(fmt.Sprintf("{%d}. %s", n.logicalClock, format), a...)
}

// Increments this node's logical clock and returns its value
func (n *ClusterNode) incrementClock() uint64 {
	n.clockMutex.Lock()
	defer n.clockMutex.Unlock()

	n.logicalClock++
	return n.logicalClock
}

// Updates this node's logical clock based on the received one
func (n *ClusterNode) updateClock(received uint64) {
	n.clockMutex.Lock()
	defer n.clockMutex.Unlock()

	if received > n.logicalClock {
		n.logicalClock = received
	}
	n.logicalClock++
}

// Dispatches incoming messages based on message type.
// This is supposed to be run as a goroutine.
// It waits for incoming messages and dispatches them to the correct input channel.
// If no channel meets the criteria, the message is dropped.
func (n *ClusterNode) RunInputDispatcher() {

	n.logf("Started input dispatcher. Awaiting messages...")

	for {
		id, msg, err := n.recv()
		if err != nil {
			// Skip message
			n.logf("Message received with an error: %v", err)
			continue
		}

		n.logf("Message received: %d, %v", id, msg.String())

		header := msg.GetHeader()

		// If timestamp < mine ignore?

		switch header.Type {
		case protocol.Join:
			if !n.IsNeighborsWith(id) {
				continue
			}
			if m, ok := msg.(*protocol.JoinMessage); ok {
				n.topologyInbox <- m
			}

		case protocol.Election:
			if m, ok := msg.(*election.ElectionMessage); ok {
				n.electionInbox <- m
			}
		}
	}
}

// This function is supposed to be run as a goroutine.
// It waits on an output channel for messages, and each time one is received, it sends it to the destination neighbor.
func (n *ClusterNode) RunOutputDispatcher() {

	n.logf("Started output dispatcher. Awaiting messages to send...")

	for {
		select {
		case out := <-n.outputChannel:
			err := n.sendToNeighbor(out.Neighbor, out.Message)

			n.logf("Message sent to %d: %v", out.Neighbor, out.Message.String())

			if err != nil {
				n.logf("An error occurred after sendToNeighbor(): %v", err)
			}
		case <-n.ctx.Done():
		}
	}
}

func (n *ClusterNode) RecvJoinMessage() *protocol.JoinMessage {
	return <-n.topologyInbox
}

// Retrieves an election message, that is, reading from the input channel designated for election messages.
// It returns a pointer to the message
func (n *ClusterNode) RecvElectionMessage() *election.ElectionMessage {
	return <-n.electionInbox
}

// It sends an election message to the given node (based on ID). It sends the message onto the output channel.
func (n *ClusterNode) SendElectionMessage(neighbor node.NodeId, msg protocol.Message) {
	n.outputChannel <- outMessage{neighbor, msg}
}

func (n *ClusterNode) JoinHandle() {
	n.logf("Started join handle")
	for {

		n.logf("Awaiting join message...")
		message := n.RecvJoinMessage()
		n.logf("Join message received: %s", message.String())

		if err := n.handleEnteringNeighbor(message); err != nil {
			n.logf("False positive, the message was mal-formatted, ignoring it.")
			return
		}

		n.logf("We need to start a new election ASAP")
		//n.setElectionStartReceived()

		// The neighbor was added, time to start the election
		n.logf("Preparing election context...")
		electionID := n.ElectionSetup()
		n.logf("Setup the election context")

		for _, neighbor := range n.neighborList() {
			n.SendElectionMessage(neighbor, n.newStartMessage(neighbor, electionID))
			n.logf("Sent START message to %d", neighbor)
		}

		n.logf("Sent START to all my neighbors, waiting confirm or earlier election proposal")
		n.switchToElectionState(election.Idle)
	}
}

// FSM that handles the election process.
// This function is supposed to be run as a goroutine, after the node's creation.
// It starts in Idle state and waits for a message, then processes it based on the state.
func (n *ClusterNode) ElectionHandle() {

	n.logf("Started election handle")

	n.switchToElectionState(election.Idle)

	for {

		n.logf("Awaiting election message...")
		message := n.RecvElectionMessage()
		n.logf("Election message received: %s", message.String())
		n.logf("Current state is %v", n.getElectionState())

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

func (n *ClusterNode) handleIdleState(message *election.ElectionMessage) {

	switch message.MessageType {

	case election.Start: // A node sent START because a neighbor received JOIN or START
		n.handleStartMessage(message)

	case election.Leader:
		n.logf("Someone sent me a LEADER message")
		n.handleLeaderMessage(message)
	default:
		n.logf("Received %v type during Idle State, ignoring?", message.MessageType)
	}
}

func (n *ClusterNode) handleWaitingYoDown(message *election.ElectionMessage) {

	h := message.GetHeader()
	switch message.MessageType {
	case election.Proposal:
		sender, _ := extractConnectionIdentifier(h.Sender)
		proposed, _ := strconv.Atoi(message.Body[0])

		receivedElectionId := message.ElectionId
		currentElectionId := n.electionCtx.GetId()

		if currentElectionId != receivedElectionId { // Is this vote for the current election
			if election.IsStrongerThan(currentElectionId, receivedElectionId) {
				n.logf("Proposal message received for a weaker, older, election %v, ignoring.", receivedElectionId)
				return
			}
			if election.IsStrongerThan(receivedElectionId, currentElectionId) {
				n.logf("Proposal message received for a stronger, newer, election %v, catching up.", receivedElectionId)
				n.ElectionSetupWithID(receivedElectionId)
			}
		}

		currentRound := n.currentElectionRound()
		if message.Round < currentRound {
			return
		} else if message.Round > currentRound {
			n.stashFutureElectionProposal(sender, node.NodeId(proposed), message.Round)
			return
		}

		switch n.getElectionStatus() {

		case election.InternalNode: // We need to gather all proposal from in neighbors, since we already received a message, update the round context
			n.electionCtx.StoreProposal(sender, node.NodeId(proposed))

			if !n.electionCtx.ReceivedAllProposals() {
				return
			}

			smallestId := n.electionCtx.GetSmallestId()

			// Forward proposal to out neighbors
			for _, outNode := range n.electionCtx.OutNodes() {
				n.SendElectionMessage(outNode, n.newProposalMessage(outNode, smallestId))
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
	case election.Leader:
		n.handleLeaderMessage(message)

	case election.Start:
		n.handleStartMessage(message)

	default:
		n.logf("Received %v type during WaitingYoDown State, ignoring?", message.MessageType)
	}
}

func (n *ClusterNode) handleWaitingYoUp(message *election.ElectionMessage) {
	h := message.GetHeader()

	switch message.MessageType {

	case election.Vote:
		sender, _ := extractConnectionIdentifier(h.Sender)
		vote, _ := strconv.ParseBool(message.Body[0])
		pruneChild, _ := strconv.ParseBool(message.Body[1])

		receivedElectionId := message.ElectionId
		currentElectionId := n.electionCtx.GetId()

		if currentElectionId != receivedElectionId { // Is this vote for the current election
			if election.IsStrongerThan(currentElectionId, receivedElectionId) {
				n.logf("Proposal message received for a weaker, older, election %v, ignoring.", receivedElectionId)
				return
			}
			if election.IsStrongerThan(receivedElectionId, currentElectionId) {
				n.logf("Proposal message received for a stronger, newer, election %v, catching up.", receivedElectionId)
				n.ElectionSetupWithID(receivedElectionId)
			}
		}

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
			n.SendElectionMessage(inNodes[0], n.newVoteMessage(inNodes[0], vote && voteValidator, false))
			for _, inNode := range inNodes[1:] {
				voteValidator, _ = n.electionCtx.DetermineVote(inNode) // Did this parent propose the winning ID?
				n.SendElectionMessage(inNode, n.newVoteMessage(inNode, vote && voteValidator, pruneMe))
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

		case election.Sink:
			// Nothing, sinks don't wait on YoUP
		}

		n.electionCtx.NextRound()
		n.enterYoDown()

	case election.Start:
		n.handleStartMessage(message)
	default:
		n.logf("Received %v type during WaitingYoUp State, ignoring?", message.MessageType)
	}
}

func (n *ClusterNode) enterYoDown() {

	switch n.getElectionStatus() {

	case election.Winner:
		for _, neighbor := range n.neighborList() {
			n.SendElectionMessage(neighbor, n.newLeaderMessage(neighbor, n.getId()))
		}
		n.endElection(node.Leader, n.getId())
		n.switchToElectionState(election.Idle)
	case election.Loser:
		n.switchToElectionState(election.Idle)

	case election.Source:
		for _, outNode := range n.electionCtx.OutNodes() {
			n.SendElectionMessage(outNode, n.newProposalMessage(outNode, n.getId()))
		}
		n.switchToElectionState(election.WaitingYoUp)

	case election.InternalNode:
		n.switchToElectionState(election.WaitingYoDown)
	case election.Sink:
		n.switchToElectionState(election.WaitingYoDown)
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
			n.SendElectionMessage(inNode, n.newVoteMessage(inNode, vote, pruneMe))
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
			n.SendElectionMessage(inNodes[0], n.newVoteMessage(inNodes[0], vote, false))

			for _, inNode := range inNodes[1:] {
				vote, _ := n.electionCtx.DetermineVote(inNode)
				n.SendElectionMessage(inNode, n.newVoteMessage(inNode, vote, pruneMe))
				if pruneMe {
					n.electionCtx.PruneThisRound(inNode)
				}
			}
		}

		n.electionCtx.NextRound()
		n.switchToElectionState(election.WaitingYoDown)

	case election.InternalNode:
		n.switchToElectionState(election.WaitingYoUp)

	case election.Source:
		// Shouldn't be possible but anyways it would be
		n.switchToElectionState(election.WaitingYoUp)
	}
}

func (n *ClusterNode) ElectionSetup() election.ElectionId {
	id := n.generateElectionId()
	n.ElectionSetupWithID(id)
	return id
}

func (n *ClusterNode) ConfirmElectionId() {
	n.electionCtx.SetId(n.electionCtx.GetIdProposal())
}

func (n *ClusterNode) ElectionSetupWithID(id election.ElectionId) {
	n.logf("Resetting the election context...")
	n.electionCtx.Reset(n.incrementClock())
	n.electionCtx.SetId(id)

	// We can skip the ID exchange thanks to the topology creation

	n.logf("Orienting the nodes with current neighbors")
	myId := n.getId()
	var neighborId node.NodeId
	for _, neighborId = range n.topologyMan.NeighborList() {
		if myId < neighborId {
			n.electionCtx.Add(neighborId, election.Outgoing)
			n.logf("%d: Outgoing", neighborId)
		} else {
			n.electionCtx.Add(neighborId, election.Incoming)
			n.logf("%d: Incoming", neighborId)
		}
	}

	n.electionCtx.UpdateStatus()
	n.logf("Calculating election %v status: I am %v", id, n.getElectionStatus())
	n.electionCtx.FirstRound()
}

func (n *ClusterNode) handleEnteringNeighbor(msg *protocol.JoinMessage) error {
	sourceId, err := extractConnectionIdentifier(msg.GetHeader().Sender)
	if err != nil {
		// Mal-formatted, skip
		return err
	}
	n.acknowledgeNeighborExistence(sourceId, msg.Address)
	return nil
}

func (n *ClusterNode) handleStartMessage(message *election.ElectionMessage) error {
	h := message.GetHeader()
	sender, _ := extractConnectionIdentifier(h.Sender)
	electionId := election.ElectionId(message.Body[0])
	myId := n.electionCtx.GetIdProposal()

	if electionId == myId {
		if !n.hasReceivedElectionStart() {
			n.logf("%d sent me my own start back. Considering it as (ACK)", sender)
			n.setElectionStartReceived()
		}
		return nil
	}
	if !election.IsStrongerThan(myId, electionId) {
		n.logf("%d sent me a weaker START message: received{%s}, mine{%s}. Ignoring it.", sender, electionId, myId)
		return nil
	}

	n.logf("%d sent me a stronger START message: received{%s}, mine{%s}. Switching to this one", sender, electionId, myId)
	n.setElectionStartReceived()

	n.logf("Preparing election context for election %v...", electionId)
	n.ElectionSetupWithID(electionId)
	n.logf("Setup the election context")

	for _, neighbor := range n.neighborList() {
		n.SendElectionMessage(neighbor, n.newStartMessage(neighbor, electionId))
		n.logf("Sent START message to %d", neighbor)
	}
	n.enterYoDown()
	return nil
}

func (n *ClusterNode) handleLeaderMessage(message *election.ElectionMessage) error {
	if n.electionCtx.HasReceivedLeader() {
		n.logf("I have already received the leader news, ignoring...")
		return nil
	}
	sender, err := extractConnectionIdentifier(message.GetHeader().Sender)
	if err != nil {
		return err
	}

	leaderId, err := strconv.ParseUint(message.Body[0], 10, 64)
	if err != nil {
		return err
	}

	n.logf("Closing my election context...")
	n.endElection(node.Follower, node.NodeId(leaderId))
	for _, neighbor := range n.neighborList() {
		if neighbor == sender {
			continue
		}
		n.SendElectionMessage(neighbor, n.newLeaderMessage(neighbor, node.NodeId(leaderId)))
		n.logf("Sent new Leader (%d) message to %d", leaderId, neighbor)
	}

	return nil
}

func (n *ClusterNode) NewMessageHeader(neighbor node.NodeId, mType protocol.MessageType) *protocol.MessageHeader {
	return protocol.NewMessageHeader(
		n.connectionIdentifier(),
		connectionIdentifier(neighbor),
		mType,
	)
}

func (n *ClusterNode) newJoinMessage(neighbor node.NodeId) protocol.Message {
	return protocol.NewJoinMessage(
		n.NewMessageHeader(neighbor, protocol.Join),
		fmt.Sprintf("%s:%d", getOutboundIP(), n.getPort()),
	)
}

func (n *ClusterNode) newStartMessage(neighbor node.NodeId, proposedElectionId election.ElectionId) protocol.Message {
	return election.NewElectionMessage(
		n.NewMessageHeader(neighbor, protocol.Election),
		election.Start,
		election.InvalidId, // Not yet set,
		[]string{string(proposedElectionId)},
		0,
	)
}
func (n *ClusterNode) newProposalMessage(neighbor, proposal node.NodeId) protocol.Message {
	return election.NewElectionMessage(
		n.NewMessageHeader(neighbor, protocol.Election),
		election.Proposal,
		n.electionCtx.GetId(),
		[]string{fmt.Sprintf("%d", proposal)},
		n.electionCtx.CurrentRound(),
	)
}
func (n *ClusterNode) newVoteMessage(neighbor node.NodeId, vote, prune bool) protocol.Message {
	// During this round, has this in neighbor voted for this id?
	return election.NewElectionMessage(
		n.NewMessageHeader(neighbor, protocol.Election),
		election.Vote,
		n.electionCtx.GetId(),
		[]string{
			strconv.FormatBool(vote),  // Vote
			strconv.FormatBool(prune), // Prune
		},
		n.electionCtx.CurrentRound(),
	)
}
func (n *ClusterNode) newLeaderMessage(neighbor node.NodeId, leaderId node.NodeId) protocol.Message {
	return election.NewElectionMessage(
		n.NewMessageHeader(neighbor, protocol.Election),
		election.Leader,
		n.electionCtx.GetId(),
		[]string{fmt.Sprintf("%d", leaderId)},
		n.electionCtx.CurrentRound(),
	)
}

func (n *ClusterNode) endElection(role node.NodeRole, leaderId node.NodeId) {
	n.electionCtx.Clear()
	n.electionCtx.SetLeaderReceived()
	n.postElectionCtx = election.NewPostElectionContext(role, leaderId)
}

func (n *ClusterNode) timestampMessage(message protocol.Message) {
	h := message.GetHeader()
	h.MarkTimestamp(n.incrementClock())
}

// Function used to destroy the node.
// Deallocating used resources (mainly sockets).
func (n *ClusterNode) destroy() {
	n.config = nil
	n.electionCtx = nil
	n.postElectionCtx = nil
	n.treeMan = nil
	n.topologyMan.Destroy()
	n.cancel()
}

//============================================================================//
//  Wrappers for NodeConfig component                                         //
//============================================================================//

// Returns the ID of the node.
func (n *ClusterNode) getId() node.NodeId {
	return n.config.GetId()
}

// Returns the port of the node.
func (n *ClusterNode) getPort() uint16 {
	return n.config.GetPort()
}

//============================================================================//
//  Wrappers for TopologyManager component                                    //
//============================================================================//

// Adds the node with given ID as a neighbor in the topology. The address must be in formatted as `<ip-address>:<port>`.
func (n *ClusterNode) AddNeighbor(id node.NodeId, address string) error {
	if n.getId() == id {
		return fmt.Errorf("Cannot set the node as its own neighbor")
	}
	err := n.topologyMan.Add(id, address)
	go func() {
		time.Sleep(3 * time.Second)
		n.SendElectionMessage(id, n.newJoinMessage(id))
	}()
	return err
}

// Acknowledges the presence of a neighbor.
// It adds a neighbor just logically to the topology, without sending a JOIN message to it.
// Useful to have make the topology consistent after a neighbor sent a JOIN, without sending one right after.
func (n *ClusterNode) acknowledgeNeighborExistence(id node.NodeId, address string) {
	n.topologyMan.LogicalAdd(id, address)
}

// Removes the neighbor with the given ID from the topology.
func (n *ClusterNode) removeNeighbor(id node.NodeId) error {
	return n.topologyMan.Remove(id)
}

// Retrieves the IP address of the neighbor with given ID.
func (n *ClusterNode) getNeighborAddress(id node.NodeId) (string, error) {
	return n.topologyMan.Get(id)
}

// Returns true if this node is neighbors with node that has id ID.
// This is true after some time in two situations:
//   - This node previously called AddNeighbor() passing the ID of the other node;
//   - The other node called AddNeighbor() passing this node's ID (if the IP was correct).
func (n *ClusterNode) IsNeighborsWith(id node.NodeId) bool {
	return n.topologyMan.Exists(id)
}

// Returns true when the node has at least one neighbor.
func (n *ClusterNode) hasNeighbors() bool {
	return n.topologyMan.HasNeighbors()
}

// Returns a slice containing the IDs of all the neighboring nodes.
func (n *ClusterNode) neighborList() []node.NodeId {
	return n.topologyMan.NeighborList()
}

// Sends a message to the neighbor node with given ID.
// Returns an error if the message is mal-formatted.
func (n *ClusterNode) sendToNeighbor(id node.NodeId, message protocol.Message) error {

	n.timestampMessage(message)

	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	n.topologyMan.SendTo(id, payload)
	return nil
}

// Retrieves a message from the topology.
// Returns an error if the message was mal-formatted or if there was a network error.
// Otherwise it returns the ID of the sender node and a pointer to the message
func (n *ClusterNode) recv() (id node.NodeId, msg protocol.Message, err error) {
	id, payload, err := n.topologyMan.Recv()
	if err != nil {
		return 0, nil, err
	}

	var header protocol.MessageHeader
	if err := json.Unmarshal(payload[0], &header); err != nil {
		return 0, nil, err
	}

	n.updateClock(msg.GetHeader().TimeStamp)

	switch header.Type {
	case protocol.Join:

		msg = &protocol.JoinMessage{}
	case protocol.Election:

		msg = &election.ElectionMessage{}

	default:

		return 0, nil, fmt.Errorf("Unknown message type: %v", header.Type)
	}

	if err := json.Unmarshal(payload[0], msg); err != nil {
		return 0, nil, err
	}
	return id, msg, nil
}

// Returns the identifier associated with the given ID.
// It returns `node-<d>` where <d> is the ID.
func connectionIdentifier(id node.NodeId) string {
	return topology.Identifier(id)
}

// Returns the identifier of this node.
// It's equivalent to call connectionIdentifier(n.getId())
func (n *ClusterNode) connectionIdentifier() string {
	return connectionIdentifier(n.getId())
}

func extractConnectionIdentifier(address string) (node.NodeId, error) {
	return topology.ExtractIdentifier(address)
}

// Returns the local IP used by this node.
func getOutboundIP() string {
	return topology.GetOutboundIP()
}

//============================================================================//
//  Wrappers for TreeManager component                                        //
//============================================================================//

// Adds the node with given ID as a child in the SPT.
// Returns an error if the ID corresponds to this node or a non neighbor.
func (n *ClusterNode) addTreeChild(childId node.NodeId) error {
	if n.getId() == childId {
		return fmt.Errorf("Cannot set the node as its own child")
	}
	if !n.IsNeighborsWith(childId) {
		return fmt.Errorf("Cannot have a non neighboring node as child")
	}
	return n.treeMan.AddChild(childId)
}

// Removes the node with given ID from the children in the SPT.
// Returns an error if the child was not present.
func (n *ClusterNode) removeTreeChild(childId node.NodeId) error {
	return n.treeMan.Remove(childId)
}

// Returns true when the node with given ID is a child in the SPT.
func (n *ClusterNode) existsTreeChild(childId node.NodeId) bool {
	return n.treeMan.ExistsChild(childId)
}

// Returns the number of children in the SPT.
func (n *ClusterNode) treeChildrenLength() int {
	return n.treeMan.ChildrenLength()
}

// Sets the node with given ID as the parent in the SPT.
// Returns an error if the ID corresponds to this node or a non neighbor.
func (n *ClusterNode) setTreeParent(parentId node.NodeId) error {
	if n.getId() == parentId {
		return fmt.Errorf("Cannot set the node as its own parent")
	}
	if !n.IsNeighborsWith(parentId) {
		return fmt.Errorf("Cannot have a non neighboring node as parent")
	}
	n.treeMan.SetParent(&parentId)
	return nil
}

// Returns the ID of the parent node, or nil if the node is root.
func (n *ClusterNode) getTreeParent() (node.NodeId, error) {
	return n.treeMan.GetParent()
}

// Returns true when this node is root in the SPT.
func (n *ClusterNode) isTreeRoot() bool {
	return n.treeMan.IsRoot()
}

// Returns true when this node is a leaf in the SPT.
func (n *ClusterNode) isTreeLeaf() bool {
	return n.treeMan.IsLeaf()
}

//============================================================================//
//  Wrappers for ElectionContext (and PostElectionContext) component(s)       //
//============================================================================//

// Returns the state of this node during the election (Idle, Waiting for YoDown or Waiting for YoUp).
func (n *ClusterNode) getElectionState() election.ElectionState {
	return n.electionCtx.GetState()
}

// Returns the status of this node during the current round of the election (Source, Internal Node, Sink, Leader or Lost).
func (n *ClusterNode) getElectionStatus() election.ElectionStatus {
	return n.electionCtx.GetStatus()
}

// Switches to the given state.
func (n *ClusterNode) switchToElectionState(state election.ElectionState) {
	n.electionCtx.SwitchToState(state)
}

// Marks the "START" message as received
func (n *ClusterNode) setElectionStartReceived() {
	n.electionCtx.SetStartReceived()
}

func (n *ClusterNode) hasReceivedElectionStart() bool {
	return n.electionCtx.HasReceivedStart()
}

// Returns the current round number for this election
func (n *ClusterNode) currentElectionRound() uint {
	return n.electionCtx.CurrentRound()
}

func (n *ClusterNode) stashFutureElectionProposal(sender node.NodeId, proposed node.NodeId, roundEpoch uint) {
	n.electionCtx.StashFutureProposal(sender, proposed, roundEpoch)
}

func (n *ClusterNode) generateElectionId() election.ElectionId {
	return election.GenerateId(n.incrementClock(), n.getId())
}
func (n *ClusterNode) setElectionId(id election.ElectionId) {
	n.electionCtx.SetId(id)
}
