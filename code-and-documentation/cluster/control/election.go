/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package control

import (
	"fmt"
	"server/cluster/election"
	"server/cluster/network"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"server/cluster/topology"
	"strconv"
)

//===================================================//
//                                                   //
// Control plane - Election, tree and timeout logic  //
//                                                   //
//   It is still part of the control plane manager   //
//                                                   //
//     Different file for better readability         //
//                                                   //
//===================================================//

// startElection starts a new election, it prepares the node for e new election, and then
// sends a "START" to each ON neighbor. Then it instantly performs the YO-DOWN on the first round.
func (c *ControlPlaneManager) startElection() {
	c.logfElection("Preparing election context...")
	electionID := c.ElectionSetup()
	c.logfElection("Setup the election context")

	for _, neighbor := range c.NeighborList() {
		if c.IsAlive(neighbor) {
			c.SendMessageTo(neighbor, c.newStartMessage(neighbor, electionID))
			c.logfElection("Sent START message to %d", neighbor)
		} else {
			c.logfElection("NOT Sent START message to %d, Skipping", neighbor)
		}
	}

	c.logfElection("Sent START to all my ON neighbors, waiting confirm or earlier election proposal")
	c.electionCtx.SetStartReceived()
	c.enterYoDown() // Performs yo down
}

// endElection ends the current election, it creates a new runtime context, saving the election result there.
func (c *ControlPlaneManager) endElection(leaderId node.NodeId, electionId election.ElectionId) {

	newRuntime := NewRuntimeContext()
	newRuntime.SetLastElection(election.NewElectionResult(leaderId, electionId))
	newRuntime.SetRouting(network.NewRoutingTable())

	c.electionCtx.SetId(electionId)

	c.electionCtx.Clear()
	c.electionCtx.SetLeaderReceived()

	c.updateRuntime(newRuntime)
}

// ElectionSetup sets the node up to propose an election. It generates an election ID and returns it
func (c *ControlPlaneManager) ElectionSetup() election.ElectionId {
	id := c.GenerateElectionId()
	c.ElectionSetupWithID(id)
	return id
}

// ElectionsSetupWithId sets the node up based on an given election ID
func (c *ControlPlaneManager) ElectionSetupWithID(id election.ElectionId) {
	c.logfElection("Resetting the election context...")
	c.electionCtx.Reset(c.IncrementClock())
	c.SetElectionId(id)

	c.treeMan.Reset() // After this election we need to build a new spanning tree

	// We can skip the ID exchange thanks to the topology creation
	// Orienting links based on id difference
	c.logfElection("Orienting the nodes with current neighbors")
	myId := c.GetId()
	var neighborId node.NodeId
	for _, neighborId = range c.topologyMan.NeighborList() { // Iterating over every on neighbor
		if c.IsAlive(neighborId) {
			if myId < neighborId {
				c.electionCtx.Add(neighborId, election.Outgoing)
				c.logfElection("%d: Outgoing", neighborId)
			} else {
				c.electionCtx.Add(neighborId, election.Incoming)
				c.logfElection("%d: Incoming", neighborId)
			}
		} else {
			c.logfElection("%d: Off", neighborId)
		}
	}

	c.electionCtx.UpdateStatus() // Calculate election status (source, sink, internal, loser, winner)
	c.logfElection("Calculating election %v status: I am %v", id, c.GetElectionStatus().String())

	c.electionCtx.FirstRound()
}

// enterYoDown performs the Yo- phase of the Yo-Yo algorithm.
// Bases on the node's role, the following happens:
//   - Source: They send their ID to each outgoing link
//   - Internal node: Nothing, they have to wait for all sources to send their ID
//   - Sink: Nothing, they have to wait for all the internal nodes to send their ID
//
// Also:
//   - Winner: Since he won th election, he starts the shout to broadcast it (and start building the SPT)
//   - Loser: He lost the election, knows it, goes back to idle and waits
func (c *ControlPlaneManager) enterYoDown() {

	c.logfElection("Yo down started for round %d", c.GetCurrentRoundNumber())
	switch c.GetElectionStatus() {

	case election.Winner:
		c.logfElection("I won the election, sending my ID to others")
		c.startShout()
		c.SwitchToElectionState(election.Idle)
	case election.Loser:
		c.logfElection("I lost the election")
		c.SwitchToElectionState(election.Idle)

	case election.Source:
		c.logfElection("Source: Sending my id to out nodes...")
		for _, outNode := range c.electionCtx.OutNodes() {
			c.SendMessageTo(outNode, c.newProposalMessage(outNode, c.GetId()))
			c.logfElection("Sent to %d", outNode)
		}
		c.SwitchToElectionState(election.WaitingYoUp)

	case election.InternalNode:
		c.logfElection("Internal node: waiting for inlinks to send proposals")
		c.SwitchToElectionState(election.WaitingYoDown)
	case election.Sink:
		c.logfElection("Sink: waiting for inlinks to send proposals")
		c.SwitchToElectionState(election.WaitingYoDown)
	}
}

// enterYoUp performs the -Yo phase of the Yo-Yo algorithm.
// Bases on the node's role, the following happens:
//   - Source: Nothing, they have to wait for all internal nodes to send their votes
//   - Internal node: Nothing, they have to wait for all sinks to send their votes
//   - Sink: They send the votes based on the proposals, to thei incoming links
func (c *ControlPlaneManager) enterYoUp() {

	c.logfElection("Yo up started for round %d", c.GetCurrentRoundNumber())
	switch c.GetElectionStatus() {
	case election.Sink:
		c.logfElection("SINK: Got all the proposals, need to send votes back")
		// If im here i got all proposals, i need to check for pruning
		pruneMe := false
		if c.electionCtx.InNodesCount() == 1 { // Pruning condition 1 for sinks: single parent, their vote is useless
			c.logfElection("I have a single parent, need pruning")
			pruneMe = true

			inNode := c.electionCtx.InNodes()[0] // 1 element only anyways
			vote, _ := c.electionCtx.DetermineVote(inNode)
			c.SendMessageTo(inNode, c.newVoteMessage(inNode, vote, pruneMe))
			c.logfElection("Sent vote %v to %d", vote, inNode)
			if pruneMe {
				c.electionCtx.PruneThisRound(inNode)
			}
		} else {

			// Pruning condition 2 for sinks: all parents send the same ID, they all come from the same source/internal, prune all but one
			pruneMe = true
			reference := c.electionCtx.GetSmallestId()
			for _, proposed := range c.electionCtx.GetAllProposals() {
				if proposed != reference {
					pruneMe = false
					break
				}
			}
			c.logfElection("I have multiple parents, do i need pruning? %v", pruneMe)

			// First we send the prune = false message, either way, even if pruneMe is true, at least one needs to remain
			inNodes := c.electionCtx.InNodes()
			vote, _ := c.electionCtx.DetermineVote(inNodes[0])
			c.SendMessageTo(inNodes[0], c.newVoteMessage(inNodes[0], vote, false))
			c.logfElection("Sent vote %v to %d", vote, inNodes[0])

			// For the remaining innodes, we send pruneMe, either true or false
			for _, inNode := range inNodes[1:] {
				vote, _ := c.electionCtx.DetermineVote(inNode)
				c.SendMessageTo(inNode, c.newVoteMessage(inNode, vote, pruneMe))
				c.logfElection("Sent vote %v to %d", vote, inNode)
				if pruneMe {
					c.electionCtx.PruneThisRound(inNode)
				}
			}
		}

		c.NextElectionRound()
		c.SwitchToElectionState(election.WaitingYoDown)

	case election.InternalNode:
		c.logfElection("Internal node: waiting for outlinks to send votes")
		c.SwitchToElectionState(election.WaitingYoUp)

	case election.Source:
		// Shouldn't be possible but anyways it would be
		c.logfElection("Source: waiting for outlinks to send votes")
		c.SwitchToElectionState(election.WaitingYoUp)
	}
}

// handleIdleState handles the reception of election messages during the Idle phase of the algorithm
func (c *ControlPlaneManager) handleIdleState(message *protocol.ElectionMessage) {

	lastResult := c.runtimeCtx.Load().GetLastElection()
	switch message.MessageType {

	case protocol.Start: // A node sent START because a neighbor received JOIN or START

		if lastResult != nil { // It must be a new neighbor if an election already concluded

			electionId := lastResult.GetElectionID()
			receivedId := election.ElectionId(message.Body[0]) // A start message has the electionId proposal in the body

			cmp := electionId.Compare(receivedId)

			if cmp == 0 { // It is for the current election, try to notify it of current leader and ask it to be my tree child
				senderId, _ := ExtractIdentifier(message.Header.Sender)
				c.treeMan.SwitchToState(topology.TreeActive)
				c.SendCurrentLeader(senderId)
				return
			}
			if cmp > 0 {
				c.logfElection("The start message is for an older, or this, election, ignoring")
				return
			}

		}
		// Either the first election or a newer and stronger one, follow it
		c.handleStartMessage(message)

	case protocol.Leader:
		if lastResult != nil {
			electionId := lastResult.GetElectionID()
			receivedId := message.ElectionId

			cmp := electionId.Compare(receivedId)
			if cmp > 0 {
				c.logfElection("The leader message is for an older, or this, election, ignoring")
				return
			}
		}
		// Either the first election of a newer one, forwarding it
		c.handleLeaderMessage(message)

	case protocol.Proposal, protocol.Vote:
		if lastResult != nil {
			cmp := lastResult.GetElectionID().Compare(message.ElectionId)
			if cmp > 0 {
				c.logfElection("Received a proposal/vote for an older election, ignoring")
				return
			}
			if cmp == 0 { // This node is still voting/proposing for this election, we need to notify about the conclusion
				senderId, _ := ExtractIdentifier(message.Header.Sender)
				c.treeMan.SwitchToState(topology.TreeActive)
				c.SendCurrentLeader(senderId)
				return
			}
		}
		// Either the first election or someone sent me a proposal for the current/newer election
		// We could be a 'slower' node and didn't receive the start yet, but we can try to get ahead and still partecipate
		cmp := message.ElectionId.Compare(c.electionCtx.GetId())
		if cmp >= 0 {
			if cmp > 0 && !c.HasReceivedElectionStart() {
				c.ElectionSetupWithID(message.ElectionId)
				c.handleWaitingYoDown(message)
				c.enterYoDown()
			}
			if message.MessageType == protocol.Proposal {
				c.handleWaitingYoDown(message)
			} else {
				c.handleWaitingYoUp(message)
			}
		}
		//c.startElection()
	}
}

// handleWaitingYoDown manages the waiting phase of Sinks and Internal Nodes during Yo-
// Generally the compare the election id with the current one. An older is ignored, a new one makes us switch to that one and
// a proposal for the current one has to be handled carefully:
// If the round is older  than the current, we ignore, if it's greater we stash it and if it's the correct one, we process it
func (c *ControlPlaneManager) handleWaitingYoDown(message *protocol.ElectionMessage) {

	h := message.GetHeader()
	switch message.MessageType {
	case protocol.Proposal:
		sender, _ := ExtractIdentifier(h.Sender)
		proposed, _ := strconv.Atoi(message.Body[0])

		receivedElectionId := message.ElectionId
		currentElectionId := c.electionCtx.GetId()

		cmp := receivedElectionId.Compare(currentElectionId)

		if cmp < 0 {
			c.logfElection("Proposal message received by %d for a weaker, older, election %v, ignoring.", sender, receivedElectionId)
			return
		}
		if cmp > 0 {
			c.logfElection("Proposal message received by %d for a stronger, newer, election %v, catching up.", sender, receivedElectionId)
			c.ElectionSetupWithID(receivedElectionId)

			for _, neighbor := range c.NeighborList() {
				if neighbor == sender {
					c.logfElection("Ignoring %d as it is the sender", neighbor)
					continue
				}
				c.SendMessageTo(neighbor, c.newStartMessage(neighbor, receivedElectionId))
				c.logfElection("Sent START message to %d", neighbor)
			}
			c.enterYoDown()
			return
		}

		currentRound := c.GetCurrentRoundNumber()
		if message.Round < currentRound {
			c.logfElection("This message is for an older round (%d < %d), ignoring it", message.Round, currentRound)
			return
		} else if message.Round > currentRound {
			c.logfElection("This message is for a future round (%d > %d), stashing it", message.Round, currentRound)
			c.StashFutureElectionProposal(sender, node.NodeId(proposed), message.Round)
			return
		}

		switch c.GetElectionStatus() {

		case election.InternalNode: // We need to gather all proposal from in neighbors, since we already received a message, update the round context
			c.electionCtx.StoreProposal(sender, node.NodeId(proposed))
			c.logfElection("Stored the proposal of %d by %d, waiting for %d more proposals...", proposed, sender, c.electionCtx.GetAwaitedProposals())

			if !c.electionCtx.ReceivedAllProposals() {
				return
			}

			smallestId := c.electionCtx.GetSmallestId()
			c.logfElection("Got all proposals, calculating the smallest ID... {%d}", smallestId)

			// Forward proposal to out neighbors
			for _, outNode := range c.electionCtx.OutNodes() {
				c.SendMessageTo(outNode, c.newProposalMessage(outNode, smallestId))
				c.logfElection("Forwarding proposal to %d", outNode)
			}

		case election.Sink:
			c.electionCtx.StoreProposal(sender, node.NodeId(proposed))
			c.logfElection("Stored the proposal of %d by %d, waiting for %d more votes...", proposed, sender, c.electionCtx.GetAwaitedProposals())

			if !c.electionCtx.ReceivedAllProposals() {
				return
			}
			c.logfElection("Got all proposals, calculating the smallest ID... {%d}", c.electionCtx.GetSmallestId())

		case election.Source:
			// Nothing, sources don't wait on YoDown
		}

		c.enterYoUp()
	case protocol.Leader:
		c.handleLeaderMessage(message)

	case protocol.Start:
		c.handleStartMessage(message)

	default:
		c.logfElection("Received %v type during WaitingYoDown State, ignoring?", message.MessageType)
	}
}

// handleWaitingYoUp manages the waiting phase of Sources and Internal Nodes during -Yo
// Generally the compare the election id with the current one. An older is ignored, a new one makes us switch to that one and
// a vote for the current one has to be handled carefully:
// If the round is older  than the current, we ignore, if it's greater we stash it and if it's the correct one, we process it
func (c *ControlPlaneManager) handleWaitingYoUp(message *protocol.ElectionMessage) {
	h := message.GetHeader()

	switch message.MessageType {

	case protocol.Vote:
		sender, _ := ExtractIdentifier(h.Sender)
		vote, _ := strconv.ParseBool(message.Body[0])
		pruneChild, _ := strconv.ParseBool(message.Body[1])

		receivedElectionId := message.ElectionId
		currentElectionId := c.electionCtx.GetId()

		cmp := receivedElectionId.Compare(currentElectionId)

		if cmp < 0 {
			c.logfElection("Proposal message received by %d for a weaker, older, election %v, ignoring.", sender, receivedElectionId)
			return
		}
		if cmp > 0 {
			c.logfElection("Proposal message received by %d for a stronger, newer, election %v, catching up.", sender, receivedElectionId)
			c.ElectionSetupWithID(receivedElectionId)

			for _, neighbor := range c.NeighborList() {
				if neighbor == sender {
					c.logfElection("Ignoring %d as it is the sender", neighbor)
					continue
				}
				c.SendMessageTo(neighbor, c.newStartMessage(neighbor, receivedElectionId))
				c.logfElection("Sent START message to %d", neighbor)
			}
			c.enterYoDown()
			return
		}

		currentRound := c.electionCtx.CurrentRound()
		if message.Round < currentRound {
			c.logfElection("This message is for an older round (%d < %d), ignoring it", message.Round, currentRound)
			return
		} else if message.Round > currentRound {
			c.logfElection("This message is for a future round (%d > %d), stashing it", message.Round, currentRound)
			c.StashFutureElectionVote(sender, vote, message.Round)
			return
		}

		switch c.electionCtx.GetStatus() {

		case election.InternalNode: // We need to gather all votes from out neighbors, since we already received a message, update the round context
			c.logfElection("Stored the vote of %v by %d, waiting for %d more vote...  Pruning asked: %v", vote, sender, c.electionCtx.GetAwaitedVotes(), pruneChild)
			c.electionCtx.StoreVote(sender, vote)
			if pruneChild {
				c.electionCtx.PruneThisRound(sender)
			}

			if !c.electionCtx.ReceivedAllVotes() {
				return
			}

			// Check if every in node sent the same ID
			pruneMe := true
			reference := c.electionCtx.GetSmallestId()
			for _, proposed := range c.electionCtx.GetAllProposals() {
				if proposed != reference {
					pruneMe = false
					break
				}
			}
			c.logfElection("Got all votes, also am I gonna prune some of my parents? %v", pruneMe)

			inNodes := c.electionCtx.InNodes()

			voteValidator, _ := c.electionCtx.DetermineVote(inNodes[0]) // Did this parent propose the winning ID?
			c.SendMessageTo(inNodes[0], c.newVoteMessage(inNodes[0], vote && voteValidator, false))
			c.logfElection("Sent vote message to %d", inNodes[0])
			for _, inNode := range inNodes[1:] {
				voteValidator, _ = c.electionCtx.DetermineVote(inNode) // Did this parent propose the winning ID?
				c.SendMessageTo(inNode, c.newVoteMessage(inNode, vote && voteValidator, pruneMe))
				c.logfElection("Sent vote message to %d", inNode)
				if pruneMe {
					c.electionCtx.PruneThisRound(inNode)
				}
			}
			c.NextElectionRound()
			c.enterYoDown()

		case election.Source:
			c.logfElection("Stored the vote of %v by %d, waiting for %d more votes...  Pruning asked: %v", vote, sender, c.electionCtx.GetAwaitedVotes(), pruneChild)
			c.electionCtx.StoreVote(sender, vote)
			if pruneChild {
				c.electionCtx.PruneThisRound(sender)
			}

			if !c.electionCtx.ReceivedAllVotes() {
				return
			}
			c.logfElection("Got all votes")
			c.NextElectionRound()
			c.enterYoDown()

		case election.Sink:
			// Nothing, sinks don't wait on YoUP
		}

	case protocol.Start:
		c.handleStartMessage(message)

	case protocol.Leader:
		c.handleLeaderMessage(message)
	default:
		c.logfElection("Received %v type during WaitingYoUp State, ignoring?", message.MessageType)
	}
}

// handleStartMessage manages "START" messages in such a way that prevents the system to continuosly start one election after the other
func (c *ControlPlaneManager) handleStartMessage(message *protocol.ElectionMessage) error {
	h := message.GetHeader()
	sender, _ := ExtractIdentifier(h.Sender)
	electionId := election.ElectionId(message.Body[0])

	localBestId := c.electionCtx.GetId()
	myIdProposal := c.electionCtx.GetIdProposal()

	// We follow the stronger election
	if myIdProposal.Compare(localBestId) > 0 {
		localBestId = myIdProposal
	}

	cmp := electionId.Compare(localBestId)
	if cmp == 0 { // My own start means that everyone forwarded it without a stronger one popping out, meaning it's the strongest
		if !c.HasReceivedElectionStart() {
			c.logfElection("%d sent me my own start back. Considering it as (ACK)", sender)
			c.SetElectionStartReceived()
		}
		return nil
	}
	if cmp < 0 {
		c.logfElection("%d sent me a weaker START message: received{%s}, mine{%s}. Ignoring it.", sender, electionId, localBestId)
		return nil
	}

	// When we get a stronger election, we switch to that
	c.logfElection("%d sent me a stronger START message: received{%s}, mine{%s}. Switching to this one", sender, electionId, localBestId)
	c.SetElectionStartReceived()

	c.logfElection("Preparing election context for election {%s}...", electionId)
	c.ElectionSetupWithID(electionId)
	c.logfElection("Finished setup for the election context")

	// Notify the others with the stronger election
	for _, neighbor := range c.NeighborList() {
		if neighbor == sender {
			c.logfElection("Ignoring %d as it is the sender", neighbor)
			continue
		}
		c.SendMessageTo(neighbor, c.newStartMessage(neighbor, electionId))
		c.logfElection("Sent START message to %d", neighbor)
	}
	c.enterYoDown()
	return nil
}

// handleLeaderMessage handles the behaviour of the node when receiving a Leader message.
// Ideally we need to check if it's for the correct election and, if so, partecipate in the shout
func (c *ControlPlaneManager) handleLeaderMessage(message *protocol.ElectionMessage) error {

	lastResult := c.runtimeCtx.Load().GetLastElection()
	if lastResult != nil {

		// This could be a message that got stuck on a slow network route
		if message.ElectionId.Compare(lastResult.GetElectionID()) < 0 {
			c.logfElection("Someone, %s, sent me a LEADER message for an older election: %s", message.Header.Sender, message.ElectionId)
			return nil
		}
	}
	// Last result is nil, this is the first leader message

	currentId := c.electionCtx.GetId()
	receivedId := message.ElectionId
	sender, err := ExtractIdentifier(message.GetHeader().Sender)
	if err != nil {
		return err
	}

	cmp := currentId.Compare(receivedId)
	switch {
	case cmp > 0:
		c.logfElection("Received a leader message, by %d, for an older election (mine: %s, received: %s) Ignoring.", sender, currentId, receivedId)
		return nil
	case cmp == 0:

		// A message for the correct election is considered also a valid message for the shout, so we check the SPT constructing state
		c.logfTree("%d My state is %v.", sender, c.treeMan.GetState().String())

		switch c.treeMan.GetState() {
		case topology.TreeActive: // Waiting for responses, I already have a parent

			switch message.Body[1] { // It's either Q or A
			case "Q": // Q
				c.logfTree("Received Q leader message from %d. Sending NO", sender)
				leaderId, err := strconv.ParseUint(message.Body[0], 10, 64)
				if err != nil {
					return err
				}
				c.SendMessageTo(sender, c.newLeaderResponseMessage(sender, node.NodeId(leaderId), message.ElectionId, false))
			case "A": // Y/N
				yes, err := strconv.ParseBool(message.Body[2])
				if err != nil {
					return err
				}
				neighborLen := c.topologyMan.Length()

				// Response to Q, and positive
				if yes {
					c.treeMan.AddTreeNeighbor(sender)
					c.logfTree("Received YES from %d, adding to tree neighbors. Current counter{%d}", sender, c.treeMan.GetTreeNeighborCount())
				} else { // Negative
					c.treeMan.AcknowledgeNo(sender)
					c.logfTree("Received NO from %d, just increasing counter {%d}", sender, c.treeMan.GetTreeNeighborCount())
				}
				c.logfTree("Awaiting %d responses... %v", (c.treeMan.GetTreeNeighborCount())-neighborLen, c.NeighborList())
				if c.treeMan.GetTreeNeighborCount() == neighborLen {
					c.becomeTreeDone()
				}
			default:
				return fmt.Errorf("Unkwown flag set in %v", message)
			}
		case topology.TreeDone: // Even if done, some messages could arrive
			if message.Body[1] == "Q" { // Q
				c.logfTree("Received Q leader message while DONE from %d. My tree is already set, sending NO", sender)
				leaderId, err := strconv.ParseUint(message.Body[0], 10, 64)
				if err != nil {
					return err
				}
				c.SendMessageTo(sender, c.newLeaderResponseMessage(sender, node.NodeId(leaderId), message.ElectionId, false))
			}
		}
		if c.electionCtx.HasReceivedLeader() {
			c.logfElection("Received a leader message, by %d, for the current election (mine: %s, received: %s) Confirming.", sender, currentId, receivedId)
			return nil
		}
	case cmp < 0:
		c.logfElection("Received a leader message, by %d, for a newer election (mine: %s, received: %s) Accepting this one.", sender, currentId, receivedId)
	}

	// Each time a stronger election arrives, we discard the previous one and we re-try to endElection with the new one, we don't stop until a stable situation, that is, the SPT constructed

	leaderId, err := strconv.ParseUint(message.Body[0], 10, 64)
	if err != nil {
		return err
	}

	c.endElection(node.NodeId(leaderId), message.ElectionId)
	rootHopCount, _ := strconv.ParseUint(message.Body[2], 10, 64)

	c.logfElection("%d said it was leader. closing my election context... with %v", leaderId, c.runtimeCtx.Load().lastElection.String())

	// After end election, we are in TreeIdle, so we need to senq Q's to ALL, also, the sender that gave us the leader message is our parent
	c.logfTree("Setting up spanning tree process, im IDLE and received a message")
	c.treeMan.SetRoot(false)
	c.treeMan.SetParent(sender, rootHopCount+1)
	c.treeMan.AddTreeNeighbor(sender)
	parent, children, hasParent := c.treeMan.GetTreeNeighbors()
	c.logfTree("Root{%v}, Parent{sender:%v}, TreeChildren{%v}", !hasParent, parent, children)

	c.SendMessageTo(sender, c.newLeaderResponseMessage(sender, node.NodeId(leaderId), message.ElectionId, true))
	counter := c.treeMan.GetTreeNeighborCount()

	c.logfTree("Sent yes to %d. Counter = %d", sender, counter)
	if counter == c.topologyMan.Length() { // if 1 neighbor
		c.becomeTreeDone()
	} else {
		c.logfTree("Other neighbors, sending Q's")
		for _, neighbor := range c.NeighborList() {
			if neighbor == sender {
				continue
			}

			if c.IsAlive(neighbor) {

				msg := c.newLeaderAnnouncementMessage(neighbor, node.NodeId(leaderId), message.ElectionId, c.treeMan.GetRootHopCount())
				c.SendMessageTo(neighbor, msg)

				c.logfTree("Sent Q to %d", neighbor)
				c.logfElection("Sent new Leader (%d) message to %d. %v", leaderId, neighbor, msg)
			} else {

				c.logfTree("NOT Sent Q to %d. Skipping offline", neighbor)
				c.logfElection("NOT Sent new Leader (%d) message to %d. Skipping offline", leaderId, neighbor)
			}
		}
		c.logfTree("Awaiting responses... Becoming ACTIVE")
		c.treeMan.SwitchToState(topology.TreeActive)
	}

	return nil
}

// handleYoDownTimeout handles a timeout during the Yo- phase.
// Since Sources don't wait on Yo-, the node that registered the timeout is either internal or sink.
// They wait for proposals on their inlinks, so we need to check the status of each in link.
func (c *ControlPlaneManager) handleYoDownTimeout() {

	for _, node := range c.electionCtx.InNodes() {
		_, err := c.electionCtx.RetrieveProposal(node)
		if err != nil {

			// Not proposed
			if !c.IsAlive(node) {

				// And is off
				c.logfElection("InNode %d is OFF. Restarting", node)
				c.startElection()
				return
			}
			c.electionCtx.IncreaseTimeout(node)
			faulty, _ := c.electionCtx.IsFaulty(node)
			if faulty {
				c.logfElection("%d is ON, but won't give election messages... sending JOIN to wake him up", node)
				c.startElection()
				return
			}
		}
	}
}

// handleYoUpTimeout handles a timeout during the -Yo phase.
// Since Sinks don't wait on -Yo, the node that registered the timeout is either internal or source.
// They wait for votes on their outlinks, so we need to check the status of each out link.
func (c *ControlPlaneManager) handleYoUpTimeout() {

	for _, node := range c.electionCtx.OutNodes() {
		_, err := c.electionCtx.RetrieveVote(node)
		if err != nil {

			// Not voted
			if !c.IsAlive(node) {

				// And is off
				c.logfElection("InNode %d is OFF. Restarting", node)
				c.startElection()
				return
			}
			c.electionCtx.IncreaseTimeout(node)
			faulty, _ := c.electionCtx.IsFaulty(node)
			if faulty {
				c.logfElection("%d is ON, but won't give election messages... sending JOIN to wake him up", node)
				c.startElection()
				return
			}
		}
	}
}

// handleIdleTimeout manages what happens when, inside the ElectionHandle, a 15 second timeout occurs without receiving messages.
// It is normal to not have traffic sometimes, but we can use this occasion to check if the neighbors are still alive. Same goes for the
// leader and tree neighbors (this is because Leader messages were user to build the SPT).
func (c *ControlPlaneManager) handleIdleTimeout() {

	lastElection := c.runtimeCtx.Load().GetLastElection()
	if lastElection != nil {

		leaderId := lastElection.GetLeaderID()
		if c.GetId() != leaderId { // Check leader status only if not leader
			c.logfHeartbeat("Am i neighbors with %d? %v", leaderId, c.IsNeighborsWith(leaderId))
			if c.IsNeighborsWith(leaderId) && !c.IsAlive(leaderId) {

				// Check the progress on leader timeouts, since the If condition was Neighbors with Leader and !Alive (= OFF)
				leaderTimeouts := c.IncreaseLeaderTimeouts()
				switch {
				case leaderTimeouts < election.RejoinLeaderTimeout:
					c.logfElection("Leader has not responded for %d time now. %d more and i try to ReJoin", leaderTimeouts, election.RejoinLeaderTimeout-leaderTimeouts)
				case leaderTimeouts == election.RejoinLeaderTimeout:
					c.logfElection("Leader shutoff? Probably forgot about me. Try probing with REJOIN")
					c.SendMessageTo(leaderId, c.newReJoinMessage(leaderId))
					c.topologyMan.SetReAckJoinPending(leaderId)
				case leaderTimeouts > election.RejoinLeaderTimeout && leaderTimeouts < election.StartoverLeaderTimeout:
					c.logfElection("Rejoin didn't work. In %d we start over...", election.StartoverLeaderTimeout-leaderTimeouts)
				default:
					c.logfElection("Leader is considered OFF. New election MUST start")
					c.ResetLeaderTimeouts()
					c.startElection()
				}
			}
		}

		// Check SPT state
		switch c.treeMan.GetState() {
		case topology.TreeDone: // Tree was all set, monitoring neighbors
			parent, children, hasParent := c.GetTreeNeighbors()

			// What about children?
			for child := range children {

				// Increase timeout for OFF children
				if !c.IsAlive(child) {
					childTimeouts := c.treeMan.IncreaseTimeout(child)
					switch {
					case childTimeouts < topology.TolerableTreeNeighborTimeouts:
						c.logfTree("Child(%d) has been OFF for some time (%d timeouts)", child, childTimeouts)
					case childTimeouts == topology.TolerableTreeNeighborTimeouts:
						c.logfTree("Child(%d) has been OFF for some time (%d timeouts). Removing", child, childTimeouts)
						c.RemoveTreeNeighbor(child) // Or better child death handling
					}
				}
			}

			// What about parent now?
			if hasParent && !c.IsAlive(parent) { // Non root, the only one to handle parent's deaths

				parentTimeouts := c.treeMan.IncreaseTimeout(parent)
				switch {
				case parentTimeouts <= topology.TolerableTreeNeighborTimeouts:
					c.logfTree("Parent(%d) has been OFF for some time (%d timeouts)", parent, parentTimeouts)
				case parentTimeouts > topology.TolerableTreeNeighborTimeouts:
					c.logfTree("Parent(%d) has been OFF for some time (%d timeouts). Removing parent", parent, parentTimeouts)
					c.RemoveTreeNeighbor(parent)
					c.RemoveTreeParent()

					// Sending an HELP NO PARENT request to all topology neighbors that are ON and not children in the tree
					neighbors := c.NeighborList()
					for _, neighbor := range neighbors {
						if _, ok := children[neighbor]; ok || !c.IsAlive(neighbor) {
							continue
						}
						c.logfTree("Asking %d if he is still attached to the Tree", neighbor)
						c.SendMessageTo(neighbor, c.newTreeHelpReq(neighbor))
					}
				}
			}

		case topology.TreeActive: // Waiting for response on some tree neighbor
			neighbors := c.NeighborList()
			for _, neighbor := range neighbors {
				if !c.treeMan.HasAnswered(neighbor) && !c.IsAlive(neighbor) { // Check which one has not answered and is off, and increase its timeouts
					childTimeouts := c.treeMan.IncreaseTimeout(neighbor)
					c.logfTree("Neighbor(%d) did not answer the SHOUT for (%d) timeouts and is off...", neighbor, childTimeouts)
					if childTimeouts == 5 {
						c.logfTree("Neighbor(%d) did not answer the SHOUT and has been off for too much time. Considering NO", neighbor)
						c.treeMan.AcknowledgeNo(neighbor)
						if c.treeMan.GetTreeNeighborCount() == len(neighbors) {
							c.becomeTreeDone()
						}
					}
				}
			}
		}
	} else { // If the last election is nil it means we did not participate in any election yes, just checking for neighbors.
		onNeighbors := 0
		for _, node := range c.topologyMan.NeighborList() {
			if c.IsAlive(node) {
				onNeighbors++
			}
		}
		if onNeighbors > 0 {
			return
		}

		// If I am with no neighbors on, for this much time, without any election, I probably am alone, so I start an election by miself to instanly become leader
		c.electionCtx.IncreaseTimeout(c.GetId()) // My own timeouts, to understand how many times i had a timeout by myself
		if t, _ := c.electionCtx.GetTimeout(c.GetId()); t == 5 {
			c.electionCtx.ResetTimeouts(c.GetId())
			c.startElection()
		}
	}
}
