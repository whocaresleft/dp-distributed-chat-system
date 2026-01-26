/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package control

import (
	"context"
	"encoding/json"
	"fmt"
	"server/cluster/election"
	"server/cluster/network"
	"server/cluster/nlog"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"server/cluster/topology"
	"strconv"
	"sync/atomic"
	"time"
)

// DataPlaneEnv is a small used as DTO, to give the Data Plane the configuration it needs to start
type DataPlaneEnv struct {
	CanWrite bool                            // This node's data plane W permissions
	CanRead  bool                            // This node's data plane R permissions
	MakeBind bool                            // Does this node have to Bind his data plane socket? Or is connect just enough?
	Runtime  *atomic.Pointer[RuntimeContext] // Last election, routing table and roleflags
}

// ControlPlaneManager is responsible for almost everything in the cluster node. It handles the connection between
// neighbors (join, join-acks, acks), as well as the communication with neighbors (topology manager). Most importantly, it handles
// the election process, as well as the followup SPT construction. Lastly it calculates the appropriate configuration for this node's
// Data Plane. This manager starts almost immediatly, because it needs to have the data plane started as soon (and stable) as possible.
type ControlPlaneManager struct {
	config *node.NodeConfig   // Config of this node (tuple id, control port, data port)
	clock  *node.LogicalClock // logical clock for message synchronization

	runtimeCtx     atomic.Pointer[RuntimeContext] // Runtime context, apart from storing the election, it gets populated so we can give it to the data plane
	leaderTimeouts uint8                          // Timeouts of the current leader, to be able to determine when to start a new election

	mainLogger nlog.Logger // General subsystem logger

	bufferedNeighborBatch map[node.NodeId]node.Address   // Map of (id-address) of nodes, retreived from the bootstrap, stored just to add them later (when the topology manager is ready)
	topologyMan           *topology.TopologyManager      // Used for the local network knowledge of the node
	topologyInbox         chan *protocol.TopologyMessage // Stores topology messages
	topologyLogger        nlog.Logger                    // Subsystem logger for the join, join-ack and ack (also re+) messages

	electionCtx    *election.ElectionContext      // Used to manage the election
	electionInbox  chan *protocol.ElectionMessage // Stores election messages
	electionLogger nlog.Logger                    // Subsystem logger for the election process

	treeMan    *topology.TreeManager      // Manages the SPT construction
	treeInbox  chan *protocol.TreeMessage // Stores tree messages
	treeLogger nlog.Logger                // Subsystem logger for the SPT construction

	heartbeatLogger nlog.Logger // Subsystem logger for the heartbeats (Registers each heartbeat)

	dataEnvChan   chan DataPlaneEnv // Channel used to send the data plane env
	dataPlaneEnv  DataPlaneEnv
	outputChannel chan protocol.OutMessage // Messages that are to be sent are buffered hwere
}

//============================================================================//
// These functions are helpers used to set the required components of the     //
// ControlPlaneManager. They are all required to start the manager correctly  //
//============================================================================//

// NewControlPlaneManager creates and returns an emtpy control plane env
func NewControlPlaneManager(cfg *node.NodeConfig) *ControlPlaneManager {

	c := &ControlPlaneManager{
		config:                cfg,
		bufferedNeighborBatch: nil,
		leaderTimeouts:        0,
		runtimeCtx:            atomic.Pointer[RuntimeContext]{},
		topologyInbox:         make(chan *protocol.TopologyMessage, 500),
		electionInbox:         make(chan *protocol.ElectionMessage, 500),
		treeInbox:             make(chan *protocol.TreeMessage, 500),
		outputChannel:         make(chan protocol.OutMessage, 500),
	}

	c.runtimeCtx.Store(NewRuntimeContext())

	c.dataPlaneEnv = DataPlaneEnv{
		false, false, false, &c.runtimeCtx,
	}

	return c
}

// SetClock injects a clock
func (c *ControlPlaneManager) SetClock(clock *node.LogicalClock) { c.clock = clock }

// SetMainLogger injects the general logger
func (c *ControlPlaneManager) SetMainLogger(m nlog.Logger) { c.mainLogger = m }

// SetTopologyEnv injects the topology manager and logger
func (c *ControlPlaneManager) SetTopologyEnv(topologyMan *topology.TopologyManager, topologyLogger nlog.Logger) {
	c.topologyMan = topologyMan
	c.topologyLogger = topologyLogger

}

// SetElectionEnv injects the election context and logger
func (c *ControlPlaneManager) SetElectionEnv(electionCtx *election.ElectionContext, electionLogger nlog.Logger) {
	c.electionCtx = electionCtx
	c.electionLogger = electionLogger
}

// SetTreeEnv injects the tree manager and logger
func (c *ControlPlaneManager) SetTreeEnv(treeMan *topology.TreeManager, treeLogger nlog.Logger) {
	c.treeMan = treeMan
	c.treeLogger = treeLogger
}

// SetHeartbeatEnv injects the heartbeat logger
func (c *ControlPlaneManager) SetHeartbeatEnv(heartbeatLogger nlog.Logger) {
	c.heartbeatLogger = heartbeatLogger
}

// SetDataEnvChannel injects a channel to be used to send a data plane env once calculated
func (c *ControlPlaneManager) SetDataEnvChannel(ch chan DataPlaneEnv) {
	c.dataEnvChan = ch
}

// updateRuntime updates the current runtime context, by replacing it with the new one
func (c *ControlPlaneManager) updateRuntime(newCtx *RuntimeContext) {
	c.runtimeCtx.Store(newCtx)
}

// RegisterNeighborBatch buffers the neighbor batch, so that connections can be enstablished in a later moment
func (c *ControlPlaneManager) RegisterNeighborBatch(batch map[node.NodeId]node.Address) {
	c.bufferedNeighborBatch = batch
}

// sendDataEnv sends the calculated dataPlaneEnd into the injected channel
func (c *ControlPlaneManager) sendDataEnv() {
	c.dataEnvChan <- c.dataPlaneEnv
}

// IsReady tells whether this manager is ready to start, that is, all required components are set
func (c *ControlPlaneManager) IsReady() bool {
	return c.clock != nil &&
		c.mainLogger != nil &&
		c.electionCtx != nil &&
		c.electionLogger != nil &&
		c.topologyLogger != nil &&
		c.topologyMan != nil &&
		c.treeMan != nil &&
		c.treeLogger != nil &&
		c.heartbeatLogger != nil &&
		c.dataEnvChan != nil
}

//============================================================================//
//                                                                            //
//	        Handlers for various events (election, topology, tree).           //
//                                                                            //
//        THESE SHOULD BE RUN AS GOROUTINES, THEY ARE MOSTLY BLOCKING         //
//                                                                            //
//============================================================================//

// RunInputDispatcher dispatches incoming messages based on message scope.
// It waits for incoming messages and dispatches them to the correct input channel.
// If no channel meets the criteria, the message is dropped.
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) RunInputDispatcher(ctx context.Context) {

	c.logf("Started input dispatcher. Awaiting messages...")

	for {
		select {
		case <-ctx.Done():
			c.logf("Input dispatcher: Stop signal received")
			return
		default:
		}

		// We listen to the topology manager for 0.5 seconds. If there is an event in this period, we can recv.
		if err := c.Poll(500 * time.Millisecond); err != nil {
			if IsRecvNotReadyError(err) {
				continue
			}
			c.logf("Polling error %v:", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// Thanks to the topology manager we get (id, msg, err)
		id, msg, err := c.Recv()
		if err != nil {
			if IsRecvNotReadyError(err) {
				c.logf("Recv not ready: %v", err)
				continue
			}
			c.logf("Message received on dispatcher with an error: %v", err)
			continue
		}

		header := msg.GetHeader()
		c.logf("Message received on dispatcher: %d, %v", id, header.String()) // I placed most of these logs for debug, but it's quite satisfying watching the whole network traffic exchange at message level

		c.MarkAlive(id) // Every message can be used for keepalive, not strictly heartbeats. Those can help if there is NO other traffic
		c.logfHeartbeat("Message used as keepalive for %d", id)

		switch header.Scope {
		case protocol.Topology:
			if m, ok := msg.(*protocol.TopologyMessage); ok {
				c.topologyInbox <- m
			}

		case protocol.Election:
			if m, ok := msg.(*protocol.ElectionMessage); ok {
				c.electionInbox <- m
			}

		case protocol.Tree:
			if m, ok := msg.(*protocol.TreeMessage); ok {
				c.treeInbox <- m
			}

		case protocol.Heartbeat:

			// If a node continues to receive heartbeats from a non-neighbor, it means that they were previously neighbors
			// (hence the sending of heartbeats), and this one probably reconnected and did not remember the other.
			if !c.IsNeighborsWith(id) {
				c.logf("heartbeat", "I don't know %d, but he keeps sending heartbeats", id)
				heartbeats, _ := c.topologyMan.IncreaseRandomHeartbeat(id)
				if heartbeats >= 5 { // Not acting instantly because it could be an error or a 'troll' (perhaphs)
					c.logfHeartbeat("%d is persistent... Maybe it's an old neighbor, trying to REJOIN", id) // Optimistically trusting a stranger here
					c.topologyMan.ResetRandomHeartbeats(id)
					c.topologyMan.SetReAckJoinPending(id)
					c.SendMessageTo(id, c.newReJoinMessage(id))
				}
			}
		}
	}
}

// RunOutputDispatcher waits on an output channel for messages, and each time one is received, it sends it to the destination neighbor.
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) RunOutputDispatcher(ctx context.Context) {

	c.logf("Started output dispatcher. Awaiting messages to send...")

	for {
		select {
		case <-ctx.Done():
			c.logf("Output dispatcher: Stop signal received")
			return
		case out := <-c.outputChannel:
			err := c.SendToNeighbor(out.DestId, out.Message)

			c.logf("Message sent to %d: %v", out.DestId, out.Message.String())

			if err != nil {
				c.logf("An error occurred after sendToNeighbor(): %v", err)
			}
		}
	}
}

// JoinHandle handles incoming topology messages, both JOIN and REJOIN variants
// It helps in the construction and maintainance of the topology, by handling incoming neighbor
// messages, both for first time entries and neighbors rejoining.
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) JoinHandle(ctx context.Context) {
	c.logfTopology("Started join handle")

	time.Sleep(100 * time.Millisecond) // Empirical

	for {
		c.logfTopology("Awaiting join message...")
		select {
		case <-ctx.Done():
			c.logf("Join: Stop signal received")
			c.logfTopology("Stop signal received")
			return
		case message := <-c.topologyInbox:

			c.logfTopology("Join message received: %s", message.String())

			rejoin := message.Flags&protocol.Jflags_REJOIN > 0 // Rejoin is used with join and ack to perform a re-handshake (like two old friends getting back to eachother)

			if rejoin {
				c.handleRejoin(message)
			} else {
				c.handleFirstTimeJoin(message)
			}
		}
	}
}

// ElectionHandle is a FSM that handles the election process.
// It processes any incoming election message in order to complete an election.
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) ElectionHandle(ctx context.Context) {

	c.logfElection("Started election handle")

	c.SwitchToElectionState(election.Idle)
	time.Sleep(3 * time.Second) // Empirical

	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()

	for {
		timer.Reset(15 * time.Second)
		c.logfElection("Awaiting election message...")
		state := c.GetElectionState()

		select {
		case <-ctx.Done():
			c.logf("Election: Stop signal received")
			c.logfElection("Stop signal received")
			return

		case message := <-c.electionInbox:

			senderId, _ := ExtractIdentifier(message.GetHeader().Sender)
			destId, _ := ExtractIdentifier(message.GetHeader().Destination)

			// Check if the destination is correct
			if c.GetId() != destId {
				c.logfElection("I Received someone else's message? (%d): %v", destId, message.String())
				continue
			}

			// 'Conservative behaviour', the only stranger messages we accept are in topology, because they could ruin the election
			if !c.IsNeighborsWith(senderId) {
				c.logfElection("Election message received by a STRANGER (%d): %s", senderId, message.String())
				continue
			}

			c.logfElection("Election message received: %s. Current state{%v}", message.String(), state.String())

			switch state {

			case election.Idle:
				c.handleIdleState(message)

			case election.WaitingYoDown:
				c.handleWaitingYoDown(message)

			case election.WaitingYoUp:
				c.handleWaitingYoUp(message)
			}

		// Election timeout
		case <-timer.C:
			c.logfElection("Timeout occurred... Current state is %v", state.String())

			switch state {
			case election.WaitingYoDown:
				c.handleYoDownTimeout()
			case election.WaitingYoUp:
				c.handleYoUpTimeout()
			case election.Idle:
				c.handleIdleTimeout()
			}
		}
	}
}

// TreeHandle processes any incoming tree message in order to try and keep it synchronized and autoheal it,
// for example by handling OFF children and parents.
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) TreeHandle(ctx context.Context) {
	c.logfTree("Started tree handle")

	time.Sleep(3 * time.Second) // Empirical

	for {
		c.logfTree("Awaiting tree message...")
		select {
		case <-ctx.Done():
			c.logf("Tree: Stop signal received")
			c.logfTree("Stop signal received")
			return
		case message := <-c.treeInbox:

			sender, _ := ExtractIdentifier(message.Header.Sender)
			c.logfTree("Message from %d received: %s", sender, message.String())

			// Propagate epoch amongst children
			flags := message.Flags

			parent, children, hasParent := c.GetTreeNeighbors()

			switch flags {
			case protocol.TFlags_EPOCH:

				// An epoch message is just the parent forwarding it's tree-epoch counter. Updating and forwarding as well
				if hasParent && parent == sender {
					c.treeMan.SetEpoch(message.Epoch)
					for child := range children {
						c.logfTree("Forwarded message to (%d)", child)
						c.SendMessageTo(child, c.copyMessage(child, message))
					}
				}

			case protocol.TFlags_NOPARENTREQ: // Someone asked me if I'm still attached to the tree
				if c.treeMan.HasParent() { // I could help
					if message.Epoch >= c.treeMan.GetEpoch() { // I am the one behind, can't help
						c.SendMessageTo(sender, c.newTreeHelpRepNegative(sender))
					} else {
						c.treeMan.RemoveTreeNeighbor(sender)
						c.treeMan.SwitchToState(topology.TreeActive)
						c.SendMessageTo(sender, c.newTreeHelpRepPositive(sender)) // True also sends Q
					}
				} else { // I can't help
					c.SendMessageTo(sender, c.newTreeHelpRepNegative(sender))
				}
			case protocol.TFlags_NOPARENTREP | protocol.TFlags_Q: // Asked me to be his child
				if !c.treeMan.HasParent() { // I still don't have a parent, accept instantly
					c.logfTree("%d said he can help! Setting him as my parent. Sending YES", sender)
					rootHops, _ := strconv.ParseUint(message.Body[0], 10, 64)
					upstreamPort, _ := strconv.ParseUint(message.Body[1], 10, 64)
					c.treeMan.SetParent(sender, rootHops+1)
					c.treeMan.SetEpoch(message.Epoch)
					runtime := c.runtimeCtx.Load().Clone()
					router := runtime.GetRouting()
					router.SetUpstreamPort(uint16(upstreamPort))
					c.updateRuntime(runtime)
					c.SendMessageTo(sender, c.newTreeNoParentQAnswer(sender, true))
					go func() { // Wait some time before starting
						time.Sleep(7 * time.Second)
						for child := range c.treeMan.GetChildren() {
							c.SendMessageTo(sender, c.newTreeInputNextHopMessage(sender, child))
						}
					}()
					_, children, _ := c.treeMan.GetTreeNeighbors()
					for child := range children {
						c.logfTree("Forwarding new epoch (%d) to %d", message.Epoch, child)
						c.SendMessageTo(child, c.newTreeEpochMessage(child, message.Epoch))
					}
				} else { // I already have a parent now
					c.logfTree("%d said he can help! However he is late, I already have a new parent. Sending NO", sender)
					c.SendMessageTo(sender, c.newTreeNoParentQAnswer(sender, false))
				}
			case protocol.TFlags_NOPARENTREP: // Denied (REP + Q means yes I can help)
				c.logfTree("%d said he can't help... ", sender)
			case protocol.TFlags_A:
				if c.treeMan.GetState() == topology.TreeActive { // If I'm back to active, I helped a node, sent Q and I'm waiting for A(Y/N)
					answer, _ := strconv.ParseBool(message.Body[0])
					if answer { // Yes
						c.AddTreeNeighbor(sender)
						c.logfTree("%d Responded with YES, he is my new neighbor", sender)
					} else { // No
						c.treeMan.AcknowledgeNo(sender)
						c.logfTree("%d Responded with No, he is not my neighbor", sender)
					}
					if c.treeMan.GetTreeNeighborCount() == len(c.topologyMan.NeighborList()) { // Either way (A yes -> new child counter+1, A no -> counter+1), check if the state can go back to done
						c.becomeTreeDone()
					}
				} else {
					c.logfTree("Shouldn't have gotten this")
				}
			case protocol.TFLags_INPUTNEXTHOP: // A child sent a new input node that is reachable through it

				inputNode, err := strconv.ParseUint(message.Body[0], 10, 64)
				if err != nil {
					continue
				}

				runtime := c.runtimeCtx.Load().Clone()
				router := runtime.GetRouting()

				router.SetDownstreamNextHop(node.NodeId(inputNode), sender)
				c.logfTree("(%d) told me that (%d) is reachable through itself. Current map{%v}", sender, inputNode, router.GetDownstreamsMap())
				c.updateRuntime(runtime)

				if hasParent {
					c.logfTree("Forwarding (%d) next input hop to parent (%d)", inputNode, parent)
					c.SendMessageTo(parent, c.newTreeInputNextHopMessage(parent, node.NodeId(inputNode)))
				}

			case protocol.TFLags_PARENTPORT: // A parente sent the port he binded on the data plane
				parentPort, err := strconv.ParseUint(message.Body[0], 10, 64)
				if err != nil {
					continue
				}

				c.logfTree("Parent (%d) said he listens on port (%d) for data plane", sender, parentPort)
				runtime := c.runtimeCtx.Load().Clone()
				router := runtime.GetRouting()
				router.SetUpstreamPort(uint16(parentPort))
				router.SetUpstreamNextHop(parent)
				c.updateRuntime(runtime)
				c.forwardPortToChildren()
			case protocol.TFlags_Q:
				c.logfTree("I lost hope")
			default:
				c.logfTree("I lost hope")
			}
		}
	}
}

// HeartbeatHandle sends an heartbeat message to each neighbor on a fixed period.
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) HeartbeatHandle(ctx context.Context) {
	c.logfHeartbeat("Heartbeat handle has started...")

	time.Sleep(100 * time.Millisecond) // Empirical

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logf("Heartbeat: Stop signal received")
			c.logfHeartbeat("Stop signal received")
			return
		case <-ticker.C:
			for _, neighbor := range c.NeighborList() {
				c.SendMessageTo(neighbor, c.newHeartbeatMessage(neighbor))
			}
		}
	}
}

// Run starts all the required functionality goroutines of the control plane, that are:
//   - The Input dispatcher
//   - The Output dispatcher
//   - The Join Handle
//   - The Election Handle
//   - The Tree Handle
//   - The Heartbeat Handle
//
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) Run(ctx context.Context) {
	go c.RunInputDispatcher(ctx)
	go c.RunOutputDispatcher(ctx)
	go c.JoinHandle(ctx)
	go c.ElectionHandle(ctx)
	go c.TreeHandle(ctx)
	go c.HeartbeatHandle(ctx)

	// Since the topology manager is now created and the handlers are ready, the node can start sending JOINS to its neighbors
	if batch := c.bufferedNeighborBatch; batch != nil {
		for id, address := range batch {
			time.Sleep(100 * time.Microsecond)
			c.AddNeighbor(id, address)
		}
	}
}

//============================================================================//
// Actual control plane manager logic                                         //
//============================================================================//

//=====================================//
// Topology logic subsection           //
//=====================================//

// handleFirstTimeJoin is responsible to process the messages that make up a three-way handshake (join, join-ack, ack. I already used join a lot before realizing i could mimic tcp and call it syn as well)
// Generally:
//
//   - N sends JOIN to M and marks M as join-ack-pending
//
//   - M receives JOIN, sends a JOIN-ACK to N and marks N as ack-pending
//
//   - N receives JOIN-ACK, crosses M from join-ack-pending, sends an ACK to M
//
//   - M receives ACK, crosses N from ack-pending
//
//   - After the join-ack, N knew M was his neighbor and after ack, M knew N was his neighbor
func (c *ControlPlaneManager) handleFirstTimeJoin(msg *protocol.TopologyMessage) error {
	sourceId, err := ExtractIdentifier(msg.GetHeader().Sender)
	if err != nil {
		c.logfTopology("Malformatted message %v", err)
		return err
	}

	if msg.Flags&protocol.Jflags_JOIN > 0 {

		if msg.Flags&protocol.Jflags_ACK > 0 {
			// This is a JOIN-ACK message, confirm neighbor and send ACK

			c.logfTopology("%d sent a JOIN-ACK message: %v", sourceId, msg)

			c.topologyMan.MarkAckJoin(sourceId)
			c.MarkAlive(sourceId)
			c.logfTopology("%d is ON from JOIN-ACK.", sourceId)
			c.SendMessageTo(sourceId, c.newAckMessage(sourceId))

			return nil
		}

		// This is a JOIN message, mark as possible neighbor, send ack-join and wait for ack (only mark it as pending)

		c.logfTopology("%d sent a JOIN message: %v", sourceId, msg)
		c.topologyMan.SetAckPending(sourceId, msg.Address)

		c.SendMessageTo(sourceId, c.newJoinAckMessage(sourceId))
		return nil
	}

	if msg.Flags&protocol.Jflags_ACK > 0 {
		// This is an ACK message, confirm neighbor

		c.logfTopology("%d sent a ACK message: %v", sourceId, msg)

		c.topologyMan.MarkAck(sourceId)
		c.MarkAlive(sourceId)
		c.logfTopology("%d is ON from ACK.", sourceId)

		// If we are not dealing with an election, we send the current leader
		lastElection := c.runtimeCtx.Load().GetLastElection()
		if c.GetElectionState() == election.Idle && lastElection != nil {
			c.logfElection("Sending %d information about last election: %v", sourceId, lastElection)

			c.logfTree("Since I have a new neighbor (%d), I send a Q and go back to being active to wait for its approval", sourceId)
			c.treeMan.SwitchToState(topology.TreeActive)
			c.SendCurrentLeader(sourceId)
			return nil
		}

		// Otherwise, send start
		c.startElection()
		return nil
	}
	return nil
}

// handleRejoin is responsible to process the messages that make up a three-way handshake with a rejoin flag ON (rejoin +[join, join-ack, ack])
// Generally:
//
//   - N sends RE+JOIN to M and marks M as re+join-ack-pending
//
//   - M receives RE+JOIN, sends a RE+JOIN-ACK to N and marks N as re+ack-pending
//
//   - N receives RE+JOIN-ACK, crosses M from re+join-ack-pending, sends an RE+ACK to M
//
//   - M receives RE+ACK, crosses N from re+ack-pending
//
//   - After the join-ack, N knew M was his neighbor and after ack, M knew N was his neighbor
func (c *ControlPlaneManager) handleRejoin(msg *protocol.TopologyMessage) error {
	sourceId, err := ExtractIdentifier(msg.GetHeader().Sender)
	if err != nil {
		c.logfTopology("Malformatted message %v", err)
		return err
	}

	if msg.Flags&protocol.Jflags_JOIN > 0 {

		if msg.Flags&protocol.Jflags_ACK > 0 {
			// This is a RE+JOIN-ACK message, confirm neighbor and send RE+ACK

			c.logfTopology("%d sent a JOIN-ACK + REJOIN message: %v", sourceId, msg)

			c.topologyMan.MarkReAckJoin(sourceId, msg.Address)
			c.logfTopology("%d is ON from RE-JOIN-ACK.", sourceId)
			c.SendMessageTo(sourceId, c.newReAckMessage(sourceId))
			return nil
		}

		// This is a RE+JOIN message, mark as possible neighbor, send re+ack-join and wait for re+ack (only mark it as pending)

		c.logfTopology("%d sent a JOIN + REJOIN message: %v", sourceId, msg)
		// The neighbor might be turning on again => send ack + rejoin
		c.topologyMan.SetReAckPending(sourceId)
		c.SendMessageTo(sourceId, c.newReJoinAckMessage(sourceId))
		return nil
	}

	if msg.Flags&protocol.Jflags_ACK > 0 {
		// This is an RE+ACK message, confirm neighbor

		c.logfTopology("%d sent a ACK + REJOIN message: %v", sourceId, msg)
		c.topologyMan.MarkReAck(sourceId)
		c.logfTopology("%d is ON from RE-ACK.", sourceId)

		// If we are not dealing with an election, we send the current leader
		lastResult := c.runtimeCtx.Load().GetLastElection()
		if c.GetElectionState() == election.Idle && lastResult != nil {
			c.logfElection("Sending %d information about last election: %v", sourceId, lastResult)

			c.logfTree("Since I have a new neighbor, i go back to being active to wait for its approval")
			c.treeMan.SwitchToState(topology.TreeActive)
			c.SendCurrentLeader(sourceId)
			return nil
		}
		return nil
	}
	return nil
}

// SendCurrentLeader sends a "LEADER" message to neighbor, informing it that there is already a leader (no need for a new election).
// Together with the "LEADER", a tree_Q is sent, to also ask it to be a child of this node in the SPT.
func (c *ControlPlaneManager) SendCurrentLeader(neighbor node.NodeId) {
	lastElection := c.runtimeCtx.Load().GetLastElection()
	electionMsg := c.newLeaderAnnouncementMessage(neighbor, lastElection.GetLeaderID(), lastElection.GetElectionID(), c.treeMan.GetRootHopCount())
	if electionMsg.ElectionId == election.InvalidId {
		c.logf("The electionId in post election context is invalid")
		return
	}
	c.SendMessageTo(neighbor, electionMsg)
}

// IncreaseLeaderTimeouts increases the leader timeouts by 1 and returns the new value
func (c *ControlPlaneManager) IncreaseLeaderTimeouts() uint8 {
	c.leaderTimeouts++
	return c.leaderTimeouts
}

// GetLeaderTimeouts returns the amounts of timeouts accumulated by the leader
func (c *ControlPlaneManager) GetLeaderTimeouts() uint8 {
	return c.leaderTimeouts
}

// ResetLeaderTimeouts brings back leader timeouts to 0
func (c *ControlPlaneManager) ResetLeaderTimeouts() {
	c.leaderTimeouts = 0
}

//============================================================================//
//	Wrappers for plane config component                                       //
//============================================================================//

// GetId returns the ID of this node
func (c *ControlPlaneManager) GetId() node.NodeId {
	return c.config.GetId()
}

// GetControlPort returns the port for the control plane for this node
func (c *ControlPlaneManager) GetControlPort() uint16 {
	return c.config.GetControlPort()
}

// GetDataPort returns the port for the data plane for this node
func (c *ControlPlaneManager) GetDataPort() uint16 {
	return c.config.GetDataPort()
}

//============================================================================//
//	Wrappers for logical clock component                                      //
//============================================================================//

// UpdateClock updates the logical clock based on the internal and given value, following Lampert's rule
func (c *ControlPlaneManager) UpdateClock(received uint64) {
	c.clock.UpdateClock(received)
}

// IncrementClock increments by 1 the logical clock, returing the updated value
func (c *ControlPlaneManager) IncrementClock() uint64 {
	return c.clock.IncrementClock()
}

// Snapshot returns the current value of the clock
func (c *ControlPlaneManager) Snapshot() uint64 {
	return c.clock.Snapshot()
}

//============================================================================//
//	Logging wrappers                                                          //
//============================================================================//

// logf logs the given formatting string, with args, on the main logger.
func (c *ControlPlaneManager) logf(format string, v ...any) {
	c.mainLogger.Logf(format, v...)
}

// logfTopology logs on the topology logger.
func (c *ControlPlaneManager) logfTopology(format string, v ...any) {
	c.topologyLogger.Logf(format, v...)
}

// logfElection logs on the election logger.
func (c *ControlPlaneManager) logfElection(format string, v ...any) {
	c.electionLogger.Logf(format, v...)
}

// logfTree logs on the tree logger.
func (c *ControlPlaneManager) logfTree(format string, v ...any) {
	c.treeLogger.Logf(format, v...)
}

// logfHeartbeat logs on the heartbeat logger.
func (c *ControlPlaneManager) logfHeartbeat(format string, v ...any) {
	c.heartbeatLogger.Logf(format, v...)
}

// SendMessageTo it sends an election message to the given node (based on ID). It sends the message onto the output channel.
func (c *ControlPlaneManager) SendMessageTo(neighbor node.NodeId, msg protocol.Message) {
	c.outputChannel <- protocol.OutMessage{DestId: neighbor, Message: msg}
}

//============================================================================//
//  Utility wrappers                                                          //
//============================================================================//

// newMessageHeader creates a preconfigured message header for the given neighbor (destination) and type.
// The timestamp field is empty, it will be automatically marked when Sent (for accuracy)
func (c *ControlPlaneManager) newMessageHeader(neighbor node.NodeId, mType protocol.MessageScope) *protocol.MessageHeader {
	return protocol.NewMessageHeader(
		c.StringIdentifier(),
		neighbor.Identifier(),
		mType,
	)
}

// newTopologyMessage creates a preconfigured message for the Topology Type
func (c *ControlPlaneManager) newTopologyMessage(neighbor node.NodeId, flags protocol.Jflags) *protocol.TopologyMessage {
	addr := node.NewAddress(network.GetOutboundIP(), c.GetControlPort())
	return protocol.NewTopologyMessage(
		c.newMessageHeader(neighbor, protocol.Topology),
		addr,
		flags,
	)
}

// newJoinMessage JOIN | Usage: sent the first time we connect to this neighbor (spontanously).
func (c *ControlPlaneManager) newJoinMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_JOIN)
}

// newReJoinMessage RE+JOIN | Usage: sent to reconnect to a neighbor (e.g. after some timeouts).
func (c *ControlPlaneManager) newReJoinMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_JOIN|protocol.Jflags_REJOIN)
}

// newJoinAckMessage JOIN-ACK | Usage: sent to respond to a JOIN message.
func (c *ControlPlaneManager) newJoinAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_JOIN|protocol.Jflags_ACK)
}

// newReJoinAckMessage RE+JOIN-ACK | Usage: sent to respond to a RE+JOIN message.
func (c *ControlPlaneManager) newReJoinAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_JOIN|protocol.Jflags_ACK|protocol.Jflags_REJOIN)
}

// newAckMessage ACK | Usage: sent to respond to a JOIN-ACK message.
func (c *ControlPlaneManager) newAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_ACK)
}

// newReAckMessage RE+ACK | Usage: sent to respond to a RE+JOIN-ACK message.
func (c *ControlPlaneManager) newReAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_ACK|protocol.Jflags_REJOIN)
}

// newElectionMessage creates a preconfigured message for the Election Type
func (c *ControlPlaneManager) newElectionMessage(neighbor node.NodeId, typ protocol.ElectionMessageType, electionId election.ElectionId, roundEpoch uint, body []string) *protocol.ElectionMessage {
	return protocol.NewElectionMessage(
		c.newMessageHeader(neighbor, protocol.Election),
		typ,
		electionId,
		body,
		roundEpoch,
	)
}

// newStartMessage returns a preconfigured election message to use to try to start a new election
func (c *ControlPlaneManager) newStartMessage(neighbor node.NodeId, proposedElectionId election.ElectionId) *protocol.ElectionMessage {
	return c.newElectionMessage(
		neighbor,
		protocol.Start,
		election.InvalidId,
		0,
		[]string{string(proposedElectionId)},
	)
}

// newProposalMessage returns a preconfigured election message to send during the YO-DOWN phase (the proposal)
func (c *ControlPlaneManager) newProposalMessage(neighbor, proposal node.NodeId) *protocol.ElectionMessage {
	return c.newElectionMessage(
		neighbor,
		protocol.Proposal,
		c.GetElectionId(),
		c.GetCurrentRoundNumber(),
		[]string{fmt.Sprintf("%d", proposal)},
	)
}

// newVoteMessage returns a preconfigured election message to send during the YO-UP phase (the vote)
func (c *ControlPlaneManager) newVoteMessage(neighbor node.NodeId, vote, prune bool) *protocol.ElectionMessage {
	return c.newElectionMessage(
		neighbor,
		protocol.Vote,
		c.GetElectionId(),
		c.GetCurrentRoundNumber(),
		[]string{
			strconv.FormatBool(vote),
			strconv.FormatBool(prune),
		},
	)
}

// newLeaderAnnouncementMessage returns a preconfigured election message to send during the SHOUT to forward the leader ID, it also creates a SPT in the process (this is the Q message).
func (c *ControlPlaneManager) newLeaderAnnouncementMessage(neighbor, leader node.NodeId, electionId election.ElectionId, hopCount uint64) *protocol.ElectionMessage {
	return c.newElectionMessage(
		neighbor,
		protocol.Leader,
		electionId,
		c.GetCurrentRoundNumber(),
		[]string{
			strconv.FormatUint(uint64(leader), 10),
			"Q",
			strconv.FormatUint(uint64(hopCount), 10),
		},
	)
}

// newLeaderResponseMessage returns a preconfigured election message to send during the SHOUT to forward the leader ID, it also creates a SPT in the process (this is the Answer message).
func (c *ControlPlaneManager) newLeaderResponseMessage(neighbor, leader node.NodeId, electionId election.ElectionId, answer bool) *protocol.ElectionMessage {
	return c.newElectionMessage(
		neighbor,
		protocol.Leader,
		electionId,
		c.GetCurrentRoundNumber(),
		[]string{
			strconv.FormatUint(uint64(leader), 10),
			"A",
			strconv.FormatBool(answer),
		},
	)
}

// newTreeMessage creates a preconfigured message for the Tree Type
func (c *ControlPlaneManager) newTreeMessage(id node.NodeId, epoch uint64, flags protocol.TreeFlags, body []string) *protocol.TreeMessage {
	return protocol.NewTreeMessage(
		c.newMessageHeader(id, protocol.Tree),
		epoch,
		flags,
		body,
	)
}

// newTreeInputNextHopMessage creates a preconfigured message for the Tree Type to be used when sending a next hop to the SPT parent.
func (c *ControlPlaneManager) newTreeInputNextHopMessage(tParent node.NodeId, inputNode node.NodeId) *protocol.TreeMessage {
	return c.newTreeMessage(
		tParent,
		c.treeMan.GetEpoch(),
		protocol.TFLags_INPUTNEXTHOP,
		[]string{
			strconv.FormatUint(uint64(inputNode), 10), // The ID of the input node
		},
	)
}

// newTreeParentPortMessage creates a preconfigured message for the Tree Type to be used when sending the data plane port to the SPT children.
func (c *ControlPlaneManager) newTreeParentPortMessage(tChild node.NodeId, dataPort uint16) *protocol.TreeMessage {
	return c.newTreeMessage(
		tChild,
		c.treeMan.GetEpoch(),
		protocol.TFLags_PARENTPORT,
		[]string{
			strconv.FormatUint(uint64(dataPort), 10), // Port the parent (n) uses on the data plane
		},
	)
}

// newTreeEpochMessage creates a preconfigured message for the Tree Type to be used when sending the epoch counter message.
func (c *ControlPlaneManager) newTreeEpochMessage(tChild node.NodeId, epoch uint64) *protocol.TreeMessage {
	return c.newTreeMessage(tChild, epoch, protocol.TFlags_EPOCH, []string{})
}

// newTreeHelpReq creates a preconfigured message for the Tree Type to be used when a node becomes parent-less
func (c *ControlPlaneManager) newTreeHelpReq(nonTreeNeighbor node.NodeId) *protocol.TreeMessage {
	return c.newTreeMessage(nonTreeNeighbor, c.treeMan.GetEpoch(), protocol.TFlags_NOPARENTREQ, []string{})
}

// newTreeHelpRepPositive creates a preconfigured message for the Tree Type that can be used to respond to a NOPARENTREQ, by asking that node to become a child of this
func (c *ControlPlaneManager) newTreeHelpRepPositive(nonTreeNeighbor node.NodeId) *protocol.TreeMessage {
	return c.newTreeMessage(
		nonTreeNeighbor,
		c.treeMan.GetEpoch(),
		protocol.TFlags_NOPARENTREP|protocol.TFlags_Q,
		[]string{
			strconv.FormatUint(c.treeMan.GetRootHopCount(), 10),
			strconv.FormatUint(uint64(c.GetDataPort()), 10),
		},
	)
}

// newTreeHelpRepPositive creates a preconfigured message for the Tree Type that can be used to respond to a NOPARENTREQ, by informing that node that this one cannot help
func (c *ControlPlaneManager) newTreeHelpRepNegative(nonTreeNeighbor node.NodeId) *protocol.TreeMessage {
	return c.newTreeMessage(
		nonTreeNeighbor,
		c.treeMan.GetEpoch(),
		protocol.TFlags_NOPARENTREP,
		[]string{strconv.FormatUint(c.treeMan.GetRootHopCount(), 10)},
	)
}

// newTreeNoParentQAnswer creates a preconfigured message for the Tree Type that can be used to respond to a NOPARENTREP, by confirmg or not the Q sent by the nodes who offeres help (can only accept one)
func (c *ControlPlaneManager) newTreeNoParentQAnswer(neighbor node.NodeId, answer bool) *protocol.TreeMessage {

	body := []string{strconv.FormatBool(answer)}

	return c.newTreeMessage(
		neighbor,
		c.treeMan.GetEpoch(),
		protocol.TFlags_A,
		body,
	)
}

// Creates a preconfigured message for the Heartbeat Type
func (c *ControlPlaneManager) newHeartbeatMessage(neighbor node.NodeId) *protocol.HeartbeatMessage {
	return protocol.NewHeartbeatMessage(c.newMessageHeader(neighbor, protocol.Heartbeat))
}

// copyMessage creates and returns a message that has the same body as the original one, but has the header changed in the following manner:
//
//	The source is 'this' node (.GetId())
//	The destination is newDestination
//	The type is the same as the original
//
// This should be used when we want to forward a message without modifying anything inside, just the header (for correct routing)
func (c *ControlPlaneManager) copyMessage(newDestination node.NodeId, original protocol.Message) protocol.Message {
	oldHeader := original.GetHeader()
	newHeader := c.newMessageHeader(newDestination, oldHeader.Scope)

	newMessage := original.Clone()
	newMessage.SetHeader(newHeader)
	return newMessage
}

// StringIdentifier returns the identifier of this node.
// It's equivalent to call .GetId().Identifier()
func (c *ControlPlaneManager) StringIdentifier() string {
	return c.GetId().Identifier()
}

// ExtractIdentifier extracts the ID of a node from an identifier string, which has this format: `node-<id>`
func ExtractIdentifier(identifier string) (node.NodeId, error) {
	return node.ExtractIdentifier([]byte(identifier))
}

//============================================================================//
//  Wrappers for TopologyManager component                                    //
//============================================================================//

// AddNeighbor adds the node with given ID as a neighbor in the topology. The address must be in formatted as `<ip-address>:<port>`.
func (c *ControlPlaneManager) AddNeighbor(possibleNeighbor node.NodeId, address node.Address) error {
	if c.GetId() == possibleNeighbor {
		return fmt.Errorf("Cannot set the node as its own neighbor")
	}
	err := c.topologyMan.Add(possibleNeighbor, address)
	go c.ReliableJoin(possibleNeighbor)
	return err
}

// ReliableJoin tries to enstablish a connection with possibleNeighbor, by sending JOIN messages with an exponential backoff.
// Either possibleNeighbor becomes a neighbor (completes the handshake, becomes 'ON') or we don't stop sending messages.
// Even in the worst case, it becomes a message every 16 seconds, it's still optimistic because the neighbor could come on anytime.
func (c *ControlPlaneManager) ReliableJoin(possibleNeighbor node.NodeId) {
	backoff := 500 * time.Millisecond
	maxBackoff := 16 * time.Second

	for {
		if c.IsAlive(possibleNeighbor) {
			c.logf("%d is ON, I can stop with JOINs", possibleNeighbor)
			return
		}

		c.SendMessageTo(possibleNeighbor, c.newJoinMessage(possibleNeighbor))
		c.logf("Sending JOIN to %d...", possibleNeighbor)

		time.Sleep(backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// AcknowledgeNeighborExistence acknowledges the presence of a neighbor.
// It adds a neighbor just logically to the topology, without sending a JOIN message to it.
// Useful to have make the topology consistent after a neighbor sent a JOIN, without sending one right after.
func (c *ControlPlaneManager) AcknowledgeNeighborExistence(id node.NodeId, address node.Address) {
	c.topologyMan.LogicalAdd(id, address)
}

// RemoveNeighbor removes the neighbor with the given ID from the topology.
func (c *ControlPlaneManager) RemoveNeighbor(id node.NodeId) error {
	return c.topologyMan.Remove(id)
}

// GetNeighborAddress retrieves the IP address of the neighbor with given ID.
func (c *ControlPlaneManager) GetNeighborAddress(id node.NodeId) (node.Address, error) {
	return c.topologyMan.Get(id)
}

// IsNeighborsWith returns true if this node is neighbors with node that has id ID.
// This is true after some time in two situations:
//   - This node previously called AddNeighbor() passing the ID of the other node;
//   - The other node called AddNeighbor() passing this node's ID (if the IP was correct).
func (c *ControlPlaneManager) IsNeighborsWith(id node.NodeId) bool {
	return c.topologyMan.Exists(id)
}

// HasNeighbors returns true when the node has at least one neighbor.
func (c *ControlPlaneManager) HasNeighbors() bool {
	return c.topologyMan.HasNeighbors()
}

// NeighborList returns a slice containing the IDs of all the neighboring nodes.
func (c *ControlPlaneManager) NeighborList() []node.NodeId {
	return c.topologyMan.NeighborList()
}

// MarkAlive marks neighbor as ON
func (c *ControlPlaneManager) MarkAlive(neighbor node.NodeId) {
	c.topologyMan.UpdateLastSeen(neighbor, time.Now())
}

// IsAlive tells wheter or not neighbor is ON or OFF
func (c *ControlPlaneManager) IsAlive(neighbor node.NodeId) bool {
	return c.topologyMan.IsAlive(neighbor)
}

// SendToNeighbor sends a message to the neighbor node with given ID.
// Returns an error if the message is mal-formatted.
func (c *ControlPlaneManager) SendToNeighbor(id node.NodeId, message protocol.Message) error {

	message.MarkTimestamp(c.IncrementClock())

	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return c.topologyMan.SendTo(id, payload)
}

// Recv retrieves a message from the topology.
// Returns an error if the message was mal-formatted or if there was a network error.
// Otherwise it returns the ID of the sender node and a pointer to the message
func (c *ControlPlaneManager) Recv() (id node.NodeId, msg protocol.Message, err error) {
	id, payload, err := c.topologyMan.Recv()
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
	c.UpdateClock(header.TimeStamp)

	switch header.Scope {
	case protocol.Topology:
		msg = &protocol.TopologyMessage{}

	case protocol.Election:
		msg = &protocol.ElectionMessage{}

	case protocol.Heartbeat:
		msg = &protocol.HeartbeatMessage{}

	case protocol.Tree:
		msg = &protocol.TreeMessage{}

	default:

		return 0, nil, fmt.Errorf("Unknown message type: %v", header.Scope)
	}

	if err := json.Unmarshal(payload[0], msg); err != nil {
		return 0, nil, err
	}
	return id, msg, nil
}

// Poll waits for an amount of time, timeout, for an event to occur.
// The registered event is POLLIN (messages incoming).
// nil is returned when a message is ready to be recv'd
// ErrRecvNotReady when the poll times out, but IT IS NOT BAD, IT IS A GOOD ERROR
// If there is any other error, a serious problem (network scope) occurred
func (c *ControlPlaneManager) Poll(timeout time.Duration) error {
	return c.topologyMan.Poll(timeout)
}

// IsRecvNotReadyError tells wheter the error err is ErrRecvNotRead
func IsRecvNotReadyError(err error) bool {
	return topology.IsRecvNotReadyError(err)
}

//============================================================================//
//  Wrappers for TreeManager component                                        //
//============================================================================//

// AddTreeNeighbor adds the node with given ID as a child in the SPT.
// Returns an error if the ID corresponds to this node or a non neighbor.
func (c *ControlPlaneManager) AddTreeNeighbor(neighborId node.NodeId) error {
	if c.GetId() == neighborId {
		return fmt.Errorf("Cannot set the node as its own tree neighbor")
	}
	if !c.IsNeighborsWith(neighborId) {
		return fmt.Errorf("Cannot have a non neighboring node as tree neighbor")
	}
	return c.treeMan.AddTreeNeighbor(neighborId)
}

// RemoveTreeNeighbor removes the node with given ID from the neighbors in the SPT.
// Returns an error if the child was not present.
func (c *ControlPlaneManager) RemoveTreeNeighbor(neighborId node.NodeId) {
	c.treeMan.RemoveTreeNeighbor(neighborId)
}

// RemoveTreeParent removes the parent in the SPT structure.
func (c *ControlPlaneManager) RemoveTreeParent() error {
	return c.treeMan.RemoveParent()
}

// GetTreeNeighbors returns the children in the tree SPT, and a parent, with a bool that tells wheter the parent is present or not.
func (c *ControlPlaneManager) GetTreeNeighbors() (parent node.NodeId, children map[node.NodeId]struct{}, hasParent bool) {
	return c.treeMan.GetTreeNeighbors()
}

// GetTreeChildren returns the  children in the SPT.
func (c *ControlPlaneManager) GetTreeChildren() (children map[node.NodeId]struct{}) {
	return c.treeMan.GetChildren()
}

// SetTreeParent sets the node with given ID as the parent in the SPT.
// Returns an error if the ID corresponds to this node or a non neighbor.
func (c *ControlPlaneManager) SetTreeParent(parentId node.NodeId, rootHopCount uint64) error {
	if c.GetId() == parentId {
		return fmt.Errorf("Cannot set the node as its own tree parent")
	}
	if !c.IsNeighborsWith(parentId) {
		return fmt.Errorf("Cannot have a non neighboring node as tree parent")
	}
	c.treeMan.SetParent(parentId, rootHopCount)
	return nil
}

// GetTreeParent returns the ID of the parent node and a bool stating wheter it has a parent or not.
func (c *ControlPlaneManager) GetTreeParent() (parentId node.NodeId, hasParent bool) {
	return c.treeMan.GetParent()
}

// isTreeRoot returns true when this node is root in the SPT.
func (c *ControlPlaneManager) isTreeRoot() bool {
	return c.treeMan.IsRoot()
}

// isTreeLeaf returns true when this node is a leaf in the SPT.
func (c *ControlPlaneManager) isTreeLeaf() bool {
	return c.treeMan.IsLeaf()
}

// ResetTree resets the tree manager
func (c *ControlPlaneManager) ResetTree() {
	c.treeMan.Reset()
}

//============================================================================//
//              Wrappers for ElectionContext component                        //
//============================================================================//

// GetElectionState returns the state of this node during the election (Idle, Waiting for YoDown or Waiting for YoUp).
func (c *ControlPlaneManager) GetElectionState() election.ElectionState {
	return c.electionCtx.GetState()
}

// GetElectionStatus returns the status of this node during the current round of the election (Source, Internal Node, Sink, Leader or Lost).
func (c *ControlPlaneManager) GetElectionStatus() election.ElectionStatus {
	return c.electionCtx.GetStatus()
}

// SwitchToElectionState switches to the given state.
func (c *ControlPlaneManager) SwitchToElectionState(state election.ElectionState) {
	c.electionCtx.SwitchToState(state)
}

// SetElectionStartReceived marks the "START" message as received
func (c *ControlPlaneManager) SetElectionStartReceived() {
	c.electionCtx.SetStartReceived()
}

// HasReceivedElectionStart tells wheter "START" has been received
func (c *ControlPlaneManager) HasReceivedElectionStart() bool {
	return c.electionCtx.HasReceivedStart()
}

// StashFutureElectionProposal stores an election proposal sent for a round greater than the current one
func (c *ControlPlaneManager) StashFutureElectionProposal(sender node.NodeId, proposed node.NodeId, roundEpoch uint) {
	c.electionCtx.StashFutureProposal(sender, proposed, roundEpoch)
}

// StashFutureElectionVote stores an election vote sent for a round greater than the current one
func (c *ControlPlaneManager) StashFutureElectionVote(sender node.NodeId, vote bool, roundEpoch uint) {
	c.electionCtx.StashFutureVote(sender, vote, roundEpoch)
}

// GenerateElectionId generates an election ID based on the clock and id
func (c *ControlPlaneManager) GenerateElectionId() election.ElectionId {
	return election.GenerateId(c.IncrementClock(), c.GetId())
}

// SetElectionId stores the given election ID
func (c *ControlPlaneManager) SetElectionId(id election.ElectionId) {
	c.electionCtx.SetId(id)
}

// GetElectionId returns the accorded election ID
func (c *ControlPlaneManager) GetElectionId() election.ElectionId {
	return c.electionCtx.GetId()
}

// GetCurrentRoundNumber returns the number of the current round for the current election
func (c *ControlPlaneManager) GetCurrentRoundNumber() uint {
	return c.electionCtx.CurrentRound()
}

// NextElectionRound prepares for the next round of the election
func (c *ControlPlaneManager) NextElectionRound() {
	c.logfElection("Preparing for next round...")
	c.electionCtx.NextRound()
	c.logfElection("Context prepared, current round is %v and I am %s", c.GetCurrentRoundNumber(), c.GetElectionStatus().String())
}
