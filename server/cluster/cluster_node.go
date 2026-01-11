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
	"server/cluster/election"
	election_definitions "server/cluster/election/definitions"
	"server/cluster/nlog"
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

func registerLogfiles(n *nlog.NodeLogger) error {
	if err := n.AddLogger("main"); err != nil {
		return err
	}
	if err := n.AddLogger("join"); err != nil {
		return err
	}
	if err := n.AddLogger("election"); err != nil {
		return err
	}
	if err := n.AddLogger("heartbeat"); err != nil {
		return err
	}
	return nil
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
	electionInbox chan *election_definitions.ElectionMessage
	outputChannel chan outMessage

	logger *nlog.NodeLogger
}

// Creates a new cluster node with the given ID and on the given port
// It returns a pointer to said node if no problems arise. Otherwise, the pointer is nil and an appropriate error is returned
func NewClusterNode(id node.NodeId, port uint16, logging bool) (*ClusterNode, error) {

	//logger := log.New(logOut, fmt.Sprintf("[[Node %d]]: ", id), log.Ldate|log.Ltime)

	config, err := node.NewNodeConfig(id, int(port))
	if err != nil {
		return nil, err
	}

	logger, err := nlog.NewNodeLogger(config.GetId(), logging)
	if err != nil {
		return nil, err
	}

	if err := registerLogfiles(logger); err != nil {
		return nil, err
	}

	logger.Logf("main", "Configuration component correctly created: Id{%d}, Port{%d}", id, port)
	logger.Logf("main", "Creating topology component")

	tp, err := topology.NewTopologyManager(*config)
	if err != nil {
		return nil, err
	}

	logger.Logf("main", "Topology component correctly created")

	electionInbox := make(chan *election_definitions.ElectionMessage, 500)
	joinInbox := make(chan *protocol.JoinMessage, 500)
	outChan := make(chan outMessage, 500)

	ctx, cancel := context.WithCancel(context.Background())

	logger.Logf("main", "Created context")
	logger.Logf("main", "Node is all set")

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

func (n *ClusterNode) EnableLogging() {
	n.logger.EnableLogging()
}

func (n *ClusterNode) DisableLogging() {
	n.logger.DisableLogging()
}

// Logs the given string. Wrap around logger.Printf
func (n *ClusterNode) logf(filename, format string, a ...any) {
	n.logger.Logf(filename, fmt.Sprintf("{%d}. %s", n.logicalClock, format), a...)
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

	n.logf("main", "Started input dispatcher. Awaiting messages...")

	for {
		id, msg, err := n.recv()
		if err != nil {
			// Skip message
			n.logf("main", "Message received on dispatcher with an error: %v", err)
			continue
		}

		header := msg.GetHeader()
		n.logf("main", "Message received on dispatcher: %d, %v", id, header.String())

		n.logf("heartbeat", "Message used as keepalive for %d", id)
		n.markAlive(id)

		switch header.Type {
		case protocol.Join:
			if n.IsNeighborsWith(id) {
				continue
			}
			if m, ok := msg.(*protocol.JoinMessage); ok {
				n.topologyInbox <- m
			}

		case protocol.Election:
			if m, ok := msg.(*election_definitions.ElectionMessage); ok {
				n.electionInbox <- m
			}
		}
	}
}

// This function is supposed to be run as a goroutine.
// It waits on an output channel for messages, and each time one is received, it sends it to the destination neighbor.
func (n *ClusterNode) RunOutputDispatcher() {

	n.logf("main", "Started output dispatcher. Awaiting messages to send...")

	for {
		select {
		case out := <-n.outputChannel:
			err := n.sendToNeighbor(out.Neighbor, out.Message)

			n.logf("main", "Message sent to %d: %v", out.Neighbor, out.Message.String())

			if err != nil {
				n.logf("main", "An error occurred after sendToNeighbor(): %v", err)
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
func (n *ClusterNode) RecvElectionMessage() *election_definitions.ElectionMessage {
	return <-n.electionInbox
}

// It sends an election message to the given node (based on ID). It sends the message onto the output channel.
func (n *ClusterNode) SendElectionMessage(neighbor node.NodeId, msg protocol.Message) {
	n.outputChannel <- outMessage{neighbor, msg}
}

// It sends an heartbeat message to the given node (based on ID). It sends the message onto the output channel.
func (n *ClusterNode) SendHeartbeat(neighbor node.NodeId) {
	n.outputChannel <- outMessage{
		neighbor,
		topology.NewHeartbeatMessage(
			protocol.NewMessageHeader(
				n.connectionIdentifier(),
				connectionIdentifier(neighbor),
				protocol.Heartbeat,
			),
		),
	}
}

func (n *ClusterNode) HeartbeatHandle() {
	ticker := time.NewTicker(2 * time.Second)
	for range ticker.C {
		for _, neighbor := range n.neighborList() {
			n.SendHeartbeat(neighbor)
		}
	}
}

func (n *ClusterNode) JoinHandle() {
	n.logf("join", "Started join handle")
	for {

		n.logf("join", "Awaiting join message...")
		message := n.RecvJoinMessage()
		n.logf("join", "Join message received: %s", message.String())

		if err := n.handleEnteringNeighbor(message); err != nil {
			n.logf("join", "False positive, the message was mal-formatted, ignoring it.")
			return
		}

		n.logf("join", "We need to start a new election ASAP")
		//n.setElectionStartReceived()

		// The neighbor was added, time to start the election
		n.startElection()

		//n.logf("election", "Sent START to all my neighbors, waiting confirm or earlier election proposal")
		//n.setElectionStartReceived()
		//n.enterYoDown()
	}
}

func (n *ClusterNode) startElection() {
	n.logf("election", "Preparing election context...")
	electionID := n.ElectionSetup()
	n.logf("election", "Setup the election context")

	for _, neighbor := range n.neighborList() {
		n.SendElectionMessage(neighbor, n.NewStartMessage(neighbor, electionID))
		n.logf("election", "Sent START message to %d", neighbor)
	}

	n.logf("election", "Sent START to all my neighbors, waiting confirm or earlier election proposal")
	n.setElectionStartReceived()
	n.enterYoDown()
}

// FSM that handles the election process.
// This function is supposed to be run as a goroutine, after the node's creation.
// It starts in Idle state and waits for a message, then processes it based on the state.
func (n *ClusterNode) ElectionHandle() {

	n.logf("election", "Started election handle")

	n.switchToElectionState(election_definitions.Idle)

	for {
		n.logf("election", "Awaiting election message (or timeout)...")
		state := n.getElectionState()

		select {
		case message := <-n.electionInbox:
			n.logf("election", "Election message received: %s", message.String())
			n.logf("election", "Current state is %v", state.String())

			switch state {

			case election_definitions.Idle:
				n.handleIdleState(message)

			case election_definitions.WaitingYoDown:
				n.handleWaitingYoDown(message)

			case election_definitions.WaitingYoUp:
				n.handleWaitingYoUp(message)
			}

		case <-time.After(15 * time.Second):
			n.logf("election", "Timeout occurred... Current state is %v", state.String())

			switch state {
			case election_definitions.WaitingYoDown:
				n.handleYoDownTimeout()
			case election_definitions.WaitingYoUp:
				n.handleYoUpTimeout()
			case election_definitions.Idle:
				// Nothing
			}
		}

	}
}

func (n *ClusterNode) handleYoDownTimeout() {

	for _, node := range n.electionCtx.InNodes() {
		_, err := n.electionCtx.RetrieveProposal(node)
		if err != nil {
			if !n.isAlive(node) {
				n.logf("election", "InNode %d is OFF. Restarting", node)
				n.startElection()
				return
			}
		}
	}
}

func (n *ClusterNode) handleYoUpTimeout() {

	for _, node := range n.electionCtx.OutNodes() {
		_, err := n.electionCtx.RetrieveVote(node)
		if err != nil {
			if !n.isAlive(node) {
				n.logf("election", "InNode %d is OFF. Restarting", node)
				n.startElection()
				return
			}
		}
	}
}

func (n *ClusterNode) handleIdleState(message *election_definitions.ElectionMessage) {

	switch message.MessageType {

	case election_definitions.Start: // A node sent START because a neighbor received JOIN or START

		if n.postElectionCtx != nil {

			electionId := n.postElectionCtx.GetElectionId()
			receivedId := election_definitions.ElectionId(message.Body[0]) // A start message has the electionId proposal in the body

			cmp := electionId.Compare(receivedId)

			if cmp > 0 {
				n.logf("election", "The start message is for an older, or this, election, ignoring")
				return
			}

			// Either the first election or a newer and stronger one, follow it
			n.handleStartMessage(message)
		}

	case election_definitions.Leader:
		if n.postElectionCtx != nil {
			electionId := n.postElectionCtx.GetElectionId()
			receivedId := message.ElectionId

			cmp := electionId.Compare(receivedId)
			if cmp > 0 {
				n.logf("election", "The leader message is for an older, or this, election, ignoring")
				return
			}
		}
		// Either the first election of a newer one, forwarding it
		n.handleLeaderMessage(message)

	case election_definitions.Proposal:
		if n.postElectionCtx != nil {
			cmp := n.postElectionCtx.GetElectionId().Compare(message.ElectionId)
			if cmp > 0 {
				n.logf("election", "Received a proposal for an older election, ignoring")
				return
			}
		}
		// Either the first election or someone sent me a proposal for the current/newer election, try  to start another
		n.startElection()

	case election_definitions.Vote:
		if n.postElectionCtx != nil {
			cmp := n.postElectionCtx.GetElectionId().Compare(message.ElectionId)
			if cmp > 0 {
				n.logf("election", "Received a vote for an older election, ignoring")
				return
			}
		}
		// Either the first election or someone sent me a proposal for the current/newer election, try  to start another
		n.startElection()
	}
}

func (n *ClusterNode) handleWaitingYoDown(message *election_definitions.ElectionMessage) {

	h := message.GetHeader()
	switch message.MessageType {
	case election_definitions.Proposal:
		sender, _ := extractConnectionIdentifier(h.Sender)
		proposed, _ := strconv.Atoi(message.Body[0])

		receivedElectionId := message.ElectionId
		currentElectionId := n.electionCtx.GetId()

		cmp := receivedElectionId.Compare(currentElectionId)

		if cmp < 0 {
			n.logf("election", "Proposal message received by %d for a weaker, older, election %v, ignoring.", sender, receivedElectionId)
			return
		}
		if cmp > 0 {
			n.logf("election", "Proposal message received by %d for a stronger, newer, election %v, catching up.", sender, receivedElectionId)
			n.ElectionSetupWithID(receivedElectionId)

			for _, neighbor := range n.neighborList() {
				if neighbor == sender {
					n.logf("election", "Ignoring %d as it is the sender", neighbor)
					continue
				}
				n.SendElectionMessage(neighbor, n.NewStartMessage(neighbor, receivedElectionId))
				n.logf("election", "Sent START message to %d", neighbor)
			}
			n.enterYoDown()
			return
		}

		currentRound := n.currentElectionRound()
		if message.Round < currentRound {
			n.logf("election", "This message is for an older round (%d < %d), ignoring it", message.Round, currentRound)
			return
		} else if message.Round > currentRound {
			n.logf("election", "This message is for a future round (%d > %d), stashing it", message.Round, currentRound)
			n.stashFutureElectionProposal(sender, node.NodeId(proposed), message.Round)
			return
		}

		switch n.getElectionStatus() {

		case election_definitions.InternalNode: // We need to gather all proposal from in neighbors, since we already received a message, update the round context
			n.electionCtx.StoreProposal(sender, node.NodeId(proposed))
			n.logf("election", "Stored the proposal of %d by %d, waiting for %d more proposals...", proposed, sender, n.electionCtx.GetAwaitedProposals())

			if !n.electionCtx.ReceivedAllProposals() {
				return
			}

			smallestId := n.electionCtx.GetSmallestId()
			n.logf("election", "Got all proposals, calculating the smallest ID... {%d}", smallestId)

			// Forward proposal to out neighbors
			for _, outNode := range n.electionCtx.OutNodes() {
				n.SendElectionMessage(outNode, n.newProposalMessage(outNode, smallestId))
				n.logf("election", "Forwarding proposal to %d", outNode)
			}

		case election_definitions.Sink:
			n.electionCtx.StoreProposal(sender, node.NodeId(proposed))
			n.logf("election", "Stored the proposal of %d by %d, waiting for %d more proposals...", proposed, sender, n.electionCtx.GetAwaitedProposals())

			if !n.electionCtx.ReceivedAllProposals() {
				return
			}
			n.logf("election", "Got all proposals, calculating the smallest ID... {%d}", n.electionCtx.GetSmallestId())

		case election_definitions.Source:
			// Nothing, sources don't wait on YoDown
		}

		n.enterYoUp()
	case election_definitions.Leader:
		n.handleLeaderMessage(message)

	case election_definitions.Start:
		n.handleStartMessage(message)

	default:
		n.logf("election", "Received %v type during WaitingYoDown State, ignoring?", message.MessageType)
	}
}

func (n *ClusterNode) handleWaitingYoUp(message *election_definitions.ElectionMessage) {
	h := message.GetHeader()

	switch message.MessageType {

	case election_definitions.Vote:
		sender, _ := extractConnectionIdentifier(h.Sender)
		vote, _ := strconv.ParseBool(message.Body[0])
		pruneChild, _ := strconv.ParseBool(message.Body[1])

		receivedElectionId := message.ElectionId
		currentElectionId := n.electionCtx.GetId()

		cmp := receivedElectionId.Compare(currentElectionId)

		if cmp < 0 {
			n.logf("election", "Proposal message received by %d for a weaker, older, election %v, ignoring.", sender, receivedElectionId)
			return
		}
		if cmp > 0 {
			n.logf("election", "Proposal message received by %d for a stronger, newer, election %v, catching up.", sender, receivedElectionId)
			n.ElectionSetupWithID(receivedElectionId)

			for _, neighbor := range n.neighborList() {
				if neighbor == sender {
					n.logf("election", "Ignoring %d as it is the sender", neighbor)
					continue
				}
				n.SendElectionMessage(neighbor, n.NewStartMessage(neighbor, receivedElectionId))
				n.logf("election", "Sent START message to %d", neighbor)
			}
			n.enterYoDown()
			return
		}

		currentRound := n.electionCtx.CurrentRound()
		if message.Round < currentRound {
			n.logf("election", "This message is for an older round (%d < %d), ignoring it", message.Round, currentRound)
			return
		} else if message.Round > currentRound {
			n.logf("election", "This message is for a future round (%d > %d), stashing it", message.Round, currentRound)
			n.electionCtx.StashFutureVote(sender, vote, message.Round)
			return
		}

		switch n.electionCtx.GetStatus() {

		case election_definitions.InternalNode: // We need to gather all votes from out neighbors, since we already received a message, update the round context
			n.logf("election", "Stored the vote of %v by %d, waiting for %d more proposals...  Pruning asked: %v", vote, sender, n.electionCtx.GetAwaitedProposals(), pruneChild)
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
			n.logf("election", "Got all votes, also am I gonna prune some of my parents? %v", pruneMe)

			inNodes := n.electionCtx.InNodes()

			voteValidator, _ := n.electionCtx.DetermineVote(inNodes[0]) // Did this parent propose the winning ID?
			n.SendElectionMessage(inNodes[0], n.newVoteMessage(inNodes[0], vote && voteValidator, false))
			n.logf("election", "Sent vote message to %d", inNodes[0])
			for _, inNode := range inNodes[1:] {
				voteValidator, _ = n.electionCtx.DetermineVote(inNode) // Did this parent propose the winning ID?
				n.SendElectionMessage(inNode, n.newVoteMessage(inNode, vote && voteValidator, pruneMe))
				n.logf("election", "Sent vote message to %d", inNode)
				if pruneMe {
					n.electionCtx.PruneThisRound(inNode)
				}
			}
			n.nextElectionRound()
			n.enterYoDown()

		case election_definitions.Source:
			n.logf("election", "Stored the vote of %v by %d, waiting for %d more proposals...  Pruning asked: %v", vote, sender, n.electionCtx.GetAwaitedProposals(), pruneChild)
			n.electionCtx.StoreVote(sender, vote)
			if pruneChild {
				n.electionCtx.PruneThisRound(sender)
			}

			if !n.electionCtx.ReceivedAllVotes() {
				return
			}
			n.logf("election", "Got all votes")
			n.nextElectionRound()
			n.enterYoDown()

		case election_definitions.Sink:
			// Nothing, sinks don't wait on YoUP
		}

	case election_definitions.Start:
		n.handleStartMessage(message)
	default:
		n.logf("election", "Received %v type during WaitingYoUp State, ignoring?", message.MessageType)
	}
}

func (n *ClusterNode) enterYoDown() {

	n.logf("election", "Yo down started for round %d", n.currentElectionRound())
	switch n.getElectionStatus() {

	case election_definitions.Winner:
		n.logf("election", "I won the election, sending my ID to others")
		for _, neighbor := range n.neighborList() {
			n.SendElectionMessage(neighbor, n.newLeaderMessage(neighbor, n.getId()))
			n.logf("election", "Sent leader message to %d", neighbor)
		}
		n.endElection(node.Leader, n.getId())
		n.switchToElectionState(election_definitions.Idle)
	case election_definitions.Loser:
		n.logf("election", "I lost the election")
		n.switchToElectionState(election_definitions.Idle)

	case election_definitions.Source:
		n.logf("election", "Source: Sending my id to out nodes...")
		for _, outNode := range n.electionCtx.OutNodes() {
			n.SendElectionMessage(outNode, n.newProposalMessage(outNode, n.getId()))
			n.logf("election", "Sent to %d", outNode)
		}
		n.switchToElectionState(election_definitions.WaitingYoUp)

	case election_definitions.InternalNode:
		n.logf("election", "Internal node: waiting for inlinks to send proposals")
		n.switchToElectionState(election_definitions.WaitingYoDown)
	case election_definitions.Sink:
		n.logf("election", "Sink: waiting for inlinks to send proposals")
		n.switchToElectionState(election_definitions.WaitingYoDown)
	}
}
func (n *ClusterNode) enterYoUp() {

	n.logf("election", "Yo up started for round %d", n.currentElectionRound())
	switch n.getElectionStatus() {
	case election_definitions.Sink:
		n.logf("election", "SINK: Got all the proposals, need to send votes back")
		// If im here i got all proposals, i need to check for pruning
		pruneMe := false
		if n.electionCtx.InNodesCount() == 1 {
			n.logf("election", "I have a single parent, need pruning")
			pruneMe = true

			inNode := n.electionCtx.InNodes()[0] // 1 element only anyways
			vote, _ := n.electionCtx.DetermineVote(inNode)
			n.SendElectionMessage(inNode, n.newVoteMessage(inNode, vote, pruneMe))
			n.logf("election", "Sent vote %v to %d", vote, inNode)
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
			n.logf("election", "I have multiple parents, do i need pruning? %v", pruneMe)

			inNodes := n.electionCtx.InNodes()
			vote, _ := n.electionCtx.DetermineVote(inNodes[0])
			n.SendElectionMessage(inNodes[0], n.newVoteMessage(inNodes[0], vote, false))
			n.logf("election", "Sent vote %v to %d", vote, inNodes[0])

			for _, inNode := range inNodes[1:] {
				vote, _ := n.electionCtx.DetermineVote(inNode)
				n.SendElectionMessage(inNode, n.newVoteMessage(inNode, vote, pruneMe))
				n.logf("election", "Sent vote %v to %d", vote, inNode)
				if pruneMe {
					n.electionCtx.PruneThisRound(inNode)
				}
			}
		}

		n.nextElectionRound()
		n.switchToElectionState(election_definitions.WaitingYoDown)

	case election_definitions.InternalNode:
		n.logf("election", "Internal node: waiting for outlinks to send votes")
		n.switchToElectionState(election_definitions.WaitingYoUp)

	case election_definitions.Source:
		// Shouldn't be possible but anyways it would be
		n.logf("election", "Source: waiting for outlinks to send votes")
		n.switchToElectionState(election_definitions.WaitingYoUp)
	}
}

func (n *ClusterNode) ElectionSetup() election_definitions.ElectionId {
	id := n.generateElectionId()
	n.ElectionSetupWithID(id)
	return id
}

func (n *ClusterNode) ElectionSetupWithID(id election_definitions.ElectionId) {
	n.logf("election", "Resetting the election context...")
	n.electionCtx.Reset(n.incrementClock())
	n.setElectionId(id)

	// We can skip the ID exchange thanks to the topology creation

	n.logf("election", "Orienting the nodes with current neighbors")
	myId := n.getId()
	var neighborId node.NodeId
	for _, neighborId = range n.topologyMan.NeighborList() {
		if n.isAlive(neighborId) {
			if myId < neighborId {
				n.electionCtx.Add(neighborId, election_definitions.Outgoing)
				n.logf("election", "%d: Outgoing", neighborId)
			} else {
				n.electionCtx.Add(neighborId, election_definitions.Incoming)
				n.logf("election", "%d: Incoming", neighborId)
			}
		} else {
			n.logf("election", "%d: Off", neighborId)
		}
	}

	n.electionCtx.UpdateStatus()
	n.logf("election", "Calculating election %v status: I am %v", id, n.getElectionStatus().String())

	n.electionCtx.FirstRound()
}

func (n *ClusterNode) handleEnteringNeighbor(msg *protocol.JoinMessage) error {
	sourceId, err := extractConnectionIdentifier(msg.GetHeader().Sender)
	if err != nil {
		// Mal-formatted, skip
		return err
	}
	n.acknowledgeNeighborExistence(sourceId, msg.Address)
	n.markAlive(sourceId)
	return nil
}

func (n *ClusterNode) handleStartMessage(message *election_definitions.ElectionMessage) error {
	h := message.GetHeader()
	sender, _ := extractConnectionIdentifier(h.Sender)
	electionId := election_definitions.ElectionId(message.Body[0])

	localBestId := n.electionCtx.GetId()
	myIdProposal := n.electionCtx.GetIdProposal()

	if myIdProposal.Compare(localBestId) > 0 {
		localBestId = myIdProposal
	}

	cmp := electionId.Compare(localBestId)
	if cmp == 0 {
		if !n.hasReceivedElectionStart() {
			n.logf("election", "%d sent me my own start back. Considering it as (ACK)", sender)
			n.setElectionStartReceived()
		}
		return nil
	}
	if cmp < 0 {
		n.logf("election", "%d sent me a weaker START message: received{%s}, mine{%s}. Ignoring it.", sender, electionId, localBestId)
		return nil
	}

	n.logf("election", "%d sent me a stronger START message: received{%s}, mine{%s}. Switching to this one", sender, electionId, localBestId)
	n.setElectionStartReceived()

	n.logf("election", "Preparing election context for election {%s}...", electionId)
	n.ElectionSetupWithID(electionId)
	n.logf("election", "Finished setup for the election context")

	for _, neighbor := range n.neighborList() {
		if neighbor == sender {
			n.logf("election", "Ignoring %d as it is the sender", neighbor)
			continue
		}
		n.SendElectionMessage(neighbor, n.NewStartMessage(neighbor, electionId))
		n.logf("election", "Sent START message to %d", neighbor)
	}
	n.enterYoDown()
	return nil
}

func (n *ClusterNode) handleLeaderMessage(message *election_definitions.ElectionMessage) error {
	n.logf("election", "Someone sent me a LEADER message")
	if n.electionCtx.HasReceivedLeader() {
		n.logf("election", "I have already received the leader news, ignoring...")
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

	if n.getId() < node.NodeId(leaderId) {
		n.logf("election", "%d said it was leader, but I am with a stronger ID...", leaderId)
		n.startElection()
		return nil
	}

	n.logf("election", "%d said it was leader. closing my election context...")
	n.endElection(node.Follower, node.NodeId(leaderId))
	for _, neighbor := range n.neighborList() {
		if neighbor == sender {
			continue
		}
		n.SendElectionMessage(neighbor, n.newLeaderMessage(neighbor, node.NodeId(leaderId)))
		n.logf("election", "Sent new Leader (%d) message to %d", leaderId, neighbor)
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

func (n *ClusterNode) NewStartMessage(neighbor node.NodeId, proposedElectionId election_definitions.ElectionId) protocol.Message {
	return election_definitions.NewElectionMessage(
		n.NewMessageHeader(neighbor, protocol.Election),
		election_definitions.Start,
		election_definitions.InvalidId, // Not yet set,
		[]string{string(proposedElectionId)},
		0,
	)
}
func (n *ClusterNode) newProposalMessage(neighbor, proposal node.NodeId) protocol.Message {
	return election_definitions.NewElectionMessage(
		n.NewMessageHeader(neighbor, protocol.Election),
		election_definitions.Proposal,
		n.electionCtx.GetId(),
		[]string{fmt.Sprintf("%d", proposal)},
		n.electionCtx.CurrentRound(),
	)
}
func (n *ClusterNode) newVoteMessage(neighbor node.NodeId, vote, prune bool) protocol.Message {
	// During this round, has this in neighbor voted for this id?
	return election_definitions.NewElectionMessage(
		n.NewMessageHeader(neighbor, protocol.Election),
		election_definitions.Vote,
		n.electionCtx.GetId(),
		[]string{
			strconv.FormatBool(vote),  // Vote
			strconv.FormatBool(prune), // Prune
		},
		n.electionCtx.CurrentRound(),
	)
}
func (n *ClusterNode) newLeaderMessage(neighbor node.NodeId, leaderId node.NodeId) protocol.Message {
	return election_definitions.NewElectionMessage(
		n.NewMessageHeader(neighbor, protocol.Election),
		election_definitions.Leader,
		n.electionCtx.GetId(),
		[]string{fmt.Sprintf("%d", leaderId)},
		n.electionCtx.CurrentRound(),
	)
}

func (n *ClusterNode) endElection(role node.NodeRole, leaderId node.NodeId) {
	n.electionCtx.Clear()
	n.electionCtx.SetLeaderReceived()
	n.postElectionCtx = election.NewPostElectionContext(role, leaderId, n.electionCtx.GetId())
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

func (n *ClusterNode) markAlive(neighbor node.NodeId) {
	n.topologyMan.UpdateLastSeen(neighbor, time.Now())
}

func (n *ClusterNode) isAlive(neighbor node.NodeId) bool {
	return n.topologyMan.IsAlive(neighbor)
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

	var headerWrapper struct {
		Header protocol.MessageHeader `json:"header"`
	}
	if err := json.Unmarshal(payload[0], &headerWrapper); err != nil {
		return 0, nil, err
	}

	header := headerWrapper.Header
	n.updateClock(header.TimeStamp)

	switch header.Type {
	case protocol.Join:
		msg = &protocol.JoinMessage{}

	case protocol.Election:
		msg = &election_definitions.ElectionMessage{}

	case protocol.Heartbeat:
		msg = &topology.HeartbeatMessage{}

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
func (n *ClusterNode) getElectionState() election_definitions.ElectionState {
	return n.electionCtx.GetState()
}

// Returns the status of this node during the current round of the election (Source, Internal Node, Sink, Leader or Lost).
func (n *ClusterNode) getElectionStatus() election_definitions.ElectionStatus {
	return n.electionCtx.GetStatus()
}

// Switches to the given state.
func (n *ClusterNode) switchToElectionState(state election_definitions.ElectionState) {
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

func (n *ClusterNode) generateElectionId() election_definitions.ElectionId {
	return election.GenerateId(n.incrementClock(), n.getId())
}
func (n *ClusterNode) setElectionId(id election_definitions.ElectionId) {
	n.electionCtx.SetId(id)
}

func (n *ClusterNode) nextElectionRound() {
	n.logf("election", "Preparing for next round...")
	n.electionCtx.NextRound()
	n.logf("election", "Context prepared, current round is %v and I am %s", n.currentElectionRound(), n.getElectionStatus().String())
}
