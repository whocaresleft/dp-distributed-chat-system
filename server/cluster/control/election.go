package control

import (
	"fmt"
	"server/cluster/election"
	election_definitions "server/cluster/election/definitions"
	"server/cluster/network"
	"server/cluster/node"
	"server/cluster/topology"
	"strconv"
)

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
	c.enterYoDown()
}

func (c *ControlPlaneManager) endElection(leaderId node.NodeId, electionId election_definitions.ElectionId) {

	newRuntime := NewRuntimeContext()
	newRuntime.SetLastElection(election.NewElectionResult(leaderId, electionId))
	newRuntime.SetRouting(network.NewRoutingTable())

	c.electionCtx.SetId(electionId)

	c.electionCtx.Clear()
	c.electionCtx.SetLeaderReceived()

	c.updateRuntime(newRuntime)
}
func (c *ControlPlaneManager) enterYoDown() {

	c.logfElection("Yo down started for round %d", c.GetCurrentRoundNumber())
	switch c.GetElectionStatus() {

	case election_definitions.Winner:
		c.logfElection("I won the election, sending my ID to others")
		c.startShout()
		c.SwitchToElectionState(election_definitions.Idle)
	case election_definitions.Loser:
		c.logfElection("I lost the election")
		c.SwitchToElectionState(election_definitions.Idle)

	case election_definitions.Source:
		c.logfElection("Source: Sending my id to out nodes...")
		for _, outNode := range c.electionCtx.OutNodes() {
			c.SendMessageTo(outNode, c.newProposalMessage(outNode, c.GetId()))
			c.logfElection("Sent to %d", outNode)
		}
		c.SwitchToElectionState(election_definitions.WaitingYoUp)

	case election_definitions.InternalNode:
		c.logfElection("Internal node: waiting for inlinks to send proposals")
		c.SwitchToElectionState(election_definitions.WaitingYoDown)
	case election_definitions.Sink:
		c.logfElection("Sink: waiting for inlinks to send proposals")
		c.SwitchToElectionState(election_definitions.WaitingYoDown)
	}
}
func (c *ControlPlaneManager) enterYoUp() {

	c.logfElection("Yo up started for round %d", c.GetCurrentRoundNumber())
	switch c.GetElectionStatus() {
	case election_definitions.Sink:
		c.logfElection("SINK: Got all the proposals, need to send votes back")
		// If im here i got all proposals, i need to check for pruning
		pruneMe := false
		if c.electionCtx.InNodesCount() == 1 {
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
			pruneMe = true
			reference := c.electionCtx.GetSmallestId()
			for _, proposed := range c.electionCtx.GetAllProposals() {
				if proposed != reference {
					pruneMe = false
					break
				}
			}
			c.logfElection("I have multiple parents, do i need pruning? %v", pruneMe)

			inNodes := c.electionCtx.InNodes()
			vote, _ := c.electionCtx.DetermineVote(inNodes[0])
			c.SendMessageTo(inNodes[0], c.newVoteMessage(inNodes[0], vote, false))
			c.logfElection("Sent vote %v to %d", vote, inNodes[0])

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
		c.SwitchToElectionState(election_definitions.WaitingYoDown)

	case election_definitions.InternalNode:
		c.logfElection("Internal node: waiting for outlinks to send votes")
		c.SwitchToElectionState(election_definitions.WaitingYoUp)

	case election_definitions.Source:
		// Shouldn't be possible but anyways it would be
		c.logfElection("Source: waiting for outlinks to send votes")
		c.SwitchToElectionState(election_definitions.WaitingYoUp)
	}
}

func (c *ControlPlaneManager) ElectionSetup() election_definitions.ElectionId {
	id := c.GenerateElectionId()
	c.ElectionSetupWithID(id)
	return id
}

func (c *ControlPlaneManager) ElectionSetupWithID(id election_definitions.ElectionId) {
	c.logfElection("Resetting the election context...")
	c.electionCtx.Reset(c.IncrementClock())
	c.SetElectionId(id)

	c.treeMan.Reset() // After this election we need to build a new spanning tree

	// We can skip the ID exchange thanks to the topology creation

	c.logfElection("Orienting the nodes with current neighbors")
	myId := c.GetId()
	var neighborId node.NodeId
	for _, neighborId = range c.topologyMan.NeighborList() {
		if c.IsAlive(neighborId) {
			if myId < neighborId {
				c.electionCtx.Add(neighborId, election_definitions.Outgoing)
				c.logfElection("%d: Outgoing", neighborId)
			} else {
				c.electionCtx.Add(neighborId, election_definitions.Incoming)
				c.logfElection("%d: Incoming", neighborId)
			}
		} else {
			c.logfElection("%d: Off", neighborId)
		}
	}

	c.electionCtx.UpdateStatus()
	c.logfElection("Calculating election %v status: I am %v", id, c.GetElectionStatus().String())

	c.electionCtx.FirstRound()
}

func (c *ControlPlaneManager) handleIdleState(message *election_definitions.ElectionMessage) {

	lastResult := c.runtimeCtx.Load().GetLastElection()
	switch message.MessageType {

	case election_definitions.Start: // A node sent START because a neighbor received JOIN or START

		if lastResult != nil {

			electionId := lastResult.GetElectionID()
			receivedId := election_definitions.ElectionId(message.Body[0]) // A start message has the electionId proposal in the body

			cmp := electionId.Compare(receivedId)

			if cmp == 0 {
				senderId, _ := ExtractIdentifier(message.Header.Sender)
				c.treeMan.SwitchToState(topology.TreeActive) // forse
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

	case election_definitions.Leader:
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

	case election_definitions.Proposal, election_definitions.Vote:
		if lastResult != nil {
			cmp := lastResult.GetElectionID().Compare(message.ElectionId)
			if cmp > 0 {
				c.logfElection("Received a proposal/vote for an older election, ignoring")
				return
			}
			if cmp == 0 {
				senderId, _ := ExtractIdentifier(message.Header.Sender)
				c.treeMan.SwitchToState(topology.TreeActive) // forse
				c.SendCurrentLeader(senderId)
				return
			}
		}
		// Either the first election or someone sent me a proposal for the current/newer election, try  to start another
		cmp := message.ElectionId.Compare(c.electionCtx.GetId())
		if cmp >= 0 {
			if cmp > 0 && !c.HasReceivedElectionStart() {
				c.ElectionSetupWithID(message.ElectionId)
				c.handleWaitingYoDown(message)
				c.enterYoDown()
			}
			if message.MessageType == election_definitions.Proposal {
				c.handleWaitingYoDown(message)
			} else {
				c.handleWaitingYoUp(message)
			}
		}
		//c.startElection()
	}
}

func (c *ControlPlaneManager) handleWaitingYoDown(message *election_definitions.ElectionMessage) {

	h := message.GetHeader()
	switch message.MessageType {
	case election_definitions.Proposal:
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
			c.electionCtx.StashFutureProposal(sender, node.NodeId(proposed), message.Round)
			return
		}

		switch c.GetElectionStatus() {

		case election_definitions.InternalNode: // We need to gather all proposal from in neighbors, since we already received a message, update the round context
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

		case election_definitions.Sink:
			c.electionCtx.StoreProposal(sender, node.NodeId(proposed))
			c.logfElection("Stored the proposal of %d by %d, waiting for %d more votes...", proposed, sender, c.electionCtx.GetAwaitedProposals())

			if !c.electionCtx.ReceivedAllProposals() {
				return
			}
			c.logfElection("Got all proposals, calculating the smallest ID... {%d}", c.electionCtx.GetSmallestId())

		case election_definitions.Source:
			// Nothing, sources don't wait on YoDown
		}

		c.enterYoUp()
	case election_definitions.Leader:
		c.handleLeaderMessage(message)

	case election_definitions.Start:
		c.handleStartMessage(message)

	default:
		c.logfElection("Received %v type during WaitingYoDown State, ignoring?", message.MessageType)
	}
}

func (c *ControlPlaneManager) handleWaitingYoUp(message *election_definitions.ElectionMessage) {
	h := message.GetHeader()

	switch message.MessageType {

	case election_definitions.Vote:
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
			c.electionCtx.StashFutureVote(sender, vote, message.Round)
			return
		}

		switch c.electionCtx.GetStatus() {

		case election_definitions.InternalNode: // We need to gather all votes from out neighbors, since we already received a message, update the round context
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

		case election_definitions.Source:
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

		case election_definitions.Sink:
			// Nothing, sinks don't wait on YoUP
		}

	case election_definitions.Start:
		c.handleStartMessage(message)

	case election_definitions.Leader:
		c.handleLeaderMessage(message)
	default:
		c.logfElection("Received %v type during WaitingYoUp State, ignoring?", message.MessageType)
	}
}

func (c *ControlPlaneManager) handleStartMessage(message *election_definitions.ElectionMessage) error {
	h := message.GetHeader()
	sender, _ := ExtractIdentifier(h.Sender)
	electionId := election_definitions.ElectionId(message.Body[0])

	localBestId := c.electionCtx.GetId()
	myIdProposal := c.electionCtx.GetIdProposal()

	if myIdProposal.Compare(localBestId) > 0 {
		localBestId = myIdProposal
	}

	cmp := electionId.Compare(localBestId)
	if cmp == 0 {
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

	c.logfElection("%d sent me a stronger START message: received{%s}, mine{%s}. Switching to this one", sender, electionId, localBestId)
	c.SetElectionStartReceived()

	c.logfElection("Preparing election context for election {%s}...", electionId)
	c.ElectionSetupWithID(electionId)
	c.logfElection("Finished setup for the election context")

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

func (c *ControlPlaneManager) handleLeaderMessage(message *election_definitions.ElectionMessage) error {

	lastResult := c.runtimeCtx.Load().GetLastElection()
	if lastResult != nil {
		if message.ElectionId.Compare(lastResult.GetElectionID()) < 0 {
			c.logfElection("Someone, %s, sent me a LEADER message for an older election: %s", message.Header.Sender, message.ElectionId)
			return nil
		}
	}

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
		c.logfTree("%d My state is %v.", sender, c.treeMan.GetState().String())

		switch c.treeMan.GetState() {
		case topology.TreeActive:

			switch message.Body[1] {
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
				if yes {
					c.treeMan.AddTreeNeighbor(sender)
					c.logfTree("Received YES from %d, adding to tree neighbors. Current counter{%d}", sender, c.treeMan.GetCounter())
				} else {
					c.treeMan.AcknowledgeNo(sender)
					c.logfTree("Received NO from %d, just increasing counter {%d}", sender, c.treeMan.GetCounter())
				}
				c.logfTree("Awaiting %d responses... %v", (c.treeMan.GetCounter())-neighborLen, c.NeighborList())
				if c.treeMan.GetCounter() == neighborLen {
					c.becomeTreeDone()
				}
			default:
				return fmt.Errorf("Unkwown flag set in %v", message)
			}
		case topology.TreeDone:
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

	leaderId, err := strconv.ParseUint(message.Body[0], 10, 64)
	if err != nil {
		return err
	}

	c.endElection(node.NodeId(leaderId), message.ElectionId)
	rootHopCount, _ := strconv.ParseUint(message.Body[2], 10, 64)

	c.logfElection("%d said it was leader. closing my election context... with %v", leaderId, c.runtimeCtx.Load().lastElection.String())

	c.logfTree("Setting up spanning tree process, im IDLE and received a message")
	c.treeMan.SetRoot(false)
	c.treeMan.SetParent(sender, rootHopCount+1)
	c.treeMan.AddTreeNeighbor(sender)
	parent, children, hasParent := c.treeMan.GetTreeNeighbors()
	c.logfTree("Root{%v}, Parent{sender:%v}, TreeChildren{%v}", !hasParent, parent, children)

	c.SendMessageTo(sender, c.newLeaderResponseMessage(sender, node.NodeId(leaderId), message.ElectionId, true))
	counter := c.treeMan.GetCounter()

	c.logfTree("Sent yes to %d. Counter = %d", sender, counter)
	if counter == c.topologyMan.Length() { // if 1 neighbor
		c.becomeTreeDone()
	} else {
		c.logfTree("Other neighbors, sending Q's")
		for _, neighbor := range c.NeighborList() {
			if neighbor == sender { // || neighbor == node.NodeId(leaderId) {
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
func (c *ControlPlaneManager) handleYoDownTimeout() {

	for _, node := range c.electionCtx.InNodes() {
		_, err := c.electionCtx.RetrieveProposal(node)
		if err != nil { // Not proposed, i could add a map [node]counter. Counting (or not counting ..ADFBHS) the timeouts, if you dont vote but you already game me 50 heartbeats... dude wake up
			if !c.IsAlive(node) {
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

func (c *ControlPlaneManager) handleYoUpTimeout() {

	for _, node := range c.electionCtx.OutNodes() {
		_, err := c.electionCtx.RetrieveVote(node)
		if err != nil {
			if !c.IsAlive(node) {
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

func (c *ControlPlaneManager) handleIdleTimeout() {

	lastElection := c.runtimeCtx.Load().GetLastElection()
	if lastElection != nil {
		leaderId := lastElection.GetLeaderID()
		if c.GetId() != leaderId { // Check leader status only if not leader
			c.logfHeartbeat("Am i neighbors with %d? %v", leaderId, c.IsNeighborsWith(leaderId))
			if c.IsNeighborsWith(leaderId) && !c.IsAlive(leaderId) {

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

		switch c.treeMan.GetState() {
		case topology.TreeDone: // Tree was all set, monitoring neighbors
			parent, children, hasParent := c.GetTreeNeighbors()

			// What about children?
			for child := range children {
				if !c.IsAlive(child) {
					childTimeouts := c.treeMan.IncreaseTimeout(child)
					switch {
					case childTimeouts < topology.TolerableTreeNeighborTimeouts:
						c.logfTree("Child(%d) has been OFF for some time (%d timeouts)", child, childTimeouts)
					case childTimeouts == topology.TolerableTreeNeighborTimeouts:
						c.logfTree("Child(%d) has been OFF for some time (%d timeouts). Removing", child, childTimeouts)
						c.RemoveTreeNeighbor(child) // Or better child death handling
						if c.GetId() == leaderId && c.treeMan.GetCounter() == 0 {
							c.becomeTreeDone()
						}
					}
				}
			}

			if hasParent && !c.IsAlive(parent) { // Non root, the only one to handle parent's deaths

				parentTimeouts := c.treeMan.IncreaseTimeout(parent)
				switch {
				case parentTimeouts <= topology.TolerableTreeNeighborTimeouts:
					c.logfTree("Parent(%d) has been OFF for some time (%d timeouts)", parent, parentTimeouts)
				case parentTimeouts > topology.TolerableTreeNeighborTimeouts:
					c.logfTree("Parent(%d) has been OFF for some time (%d timeouts). Removing parent", parent, parentTimeouts)
					c.RemoveTreeNeighbor(parent)
					c.RemoveTreeParent()

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

		case topology.TreeActive:
			neighbors := c.NeighborList()
			for _, neighbor := range neighbors {
				if !c.treeMan.HasAnswered(neighbor) && !c.IsAlive(neighbor) {
					childTimeouts := c.treeMan.IncreaseTimeout(neighbor)
					c.logfTree("Neighbor(%d) did not answer the SHOUT for (%d) timeouts and is off...", neighbor, childTimeouts)
					if childTimeouts == 5 {
						c.logfTree("Neighbor(%d) did not answer the SHOUT and has been off for too much time. Considering NO", neighbor)
						c.treeMan.AcknowledgeNo(neighbor)
						if c.treeMan.GetCounter() == len(neighbors) {
							c.becomeTreeDone()
						}
					}
				}
			}
		}
	} else { // postCtx is nil, we are idle, so no election has been performed yet
		onNeighbors := 0
		for _, node := range c.topologyMan.NeighborList() {
			if c.IsAlive(node) {
				onNeighbors++
			}
		}
		if onNeighbors > 0 {
			return
		}
		c.electionCtx.IncreaseTimeout(c.GetId()) // My own timeouts, to understand how many times i had a timeout by myself
		if t, _ := c.electionCtx.GetTimeout(c.GetId()); t == 5 {
			c.electionCtx.ResetTimeouts(c.GetId())
			c.startElection()
		}
	}
}
