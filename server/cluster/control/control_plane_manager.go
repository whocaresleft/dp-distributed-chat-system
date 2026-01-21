package control

import (
	"context"
	"encoding/json"
	"fmt"
	"server/cluster/election"
	election_definitions "server/cluster/election/definitions"
	"server/cluster/network"
	"server/cluster/nlog"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"server/cluster/topology"
	"strconv"
	"sync/atomic"
	"time"
)

type DataPlaneEnv struct {
	CanWrite bool
	CanRead  bool
	MakeBind bool
	Runtime  *atomic.Pointer[RuntimeContext]
}

type ControlPlaneManager struct {
	config *node.NodeConfig
	clock  *node.LogicalClock

	runtimeCtx     atomic.Pointer[RuntimeContext]
	leaderTimeouts uint8

	mainLogger nlog.Logger

	bufferedNeighborBatch map[node.NodeId]node.Address
	topologyMan           *topology.TopologyManager
	topologyInbox         chan *protocol.TopologyMessage
	topologyLogger        nlog.Logger

	electionCtx    *election.ElectionContext
	electionInbox  chan *election_definitions.ElectionMessage
	electionLogger nlog.Logger

	treeMan    *topology.TreeManager
	treeInbox  chan *protocol.TreeMessage
	treeLogger nlog.Logger

	heartbeatLogger nlog.Logger

	dataEnvChan   chan DataPlaneEnv
	dataPlaneEnv  DataPlaneEnv
	outputChannel chan protocol.OutMessage
}

//============================================================================//
// These functions are helpers used to set the required components of the     //
// ControlPlaneManager. They are all required to start the manager correctly  //
//============================================================================//

func NewControlPlaneManager(cfg *node.NodeConfig) *ControlPlaneManager {

	c := &ControlPlaneManager{
		config:                cfg,
		bufferedNeighborBatch: nil,
		leaderTimeouts:        0,
		runtimeCtx:            atomic.Pointer[RuntimeContext]{},
		topologyInbox:         make(chan *protocol.TopologyMessage, 500),
		electionInbox:         make(chan *election_definitions.ElectionMessage, 500),
		treeInbox:             make(chan *protocol.TreeMessage, 500),
		outputChannel:         make(chan protocol.OutMessage, 500),
	}

	c.runtimeCtx.Store(NewRuntimeContext())

	return c
}

func (c *ControlPlaneManager) SetClock(clock *node.LogicalClock) { c.clock = clock }

func (c *ControlPlaneManager) SetMainLogger(m nlog.Logger) { c.mainLogger = m }

func (c *ControlPlaneManager) SetTopologyEnv(topologyMan *topology.TopologyManager, topologyLogger nlog.Logger) {
	c.topologyMan = topologyMan
	c.topologyLogger = topologyLogger

}

func (c *ControlPlaneManager) SetElectionEnv(electionCtx *election.ElectionContext, electionLogger nlog.Logger) {
	c.electionCtx = electionCtx
	c.electionLogger = electionLogger
}

func (c *ControlPlaneManager) SetTreeEnv(treeMan *topology.TreeManager, treeLogger nlog.Logger) {
	c.treeMan = treeMan
	c.treeLogger = treeLogger
}

func (c *ControlPlaneManager) SetHeartbeatEnv(heartbeatLogger nlog.Logger) {
	c.heartbeatLogger = heartbeatLogger
}

func (c *ControlPlaneManager) SetDataEnvChannel(ch chan DataPlaneEnv) {
	c.dataEnvChan = ch
}

func (c *ControlPlaneManager) updateRuntime(newCtx *RuntimeContext) {
	c.runtimeCtx.Store(newCtx)
}

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

// Dispatches incoming messages based on message type.
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

		if err := c.Poll(500 * time.Millisecond); err != nil {
			if IsRecvNotReadyError(err) {
				continue
			}
			c.logf("main", "Polling error %v:", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

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
		c.logf("Message received on dispatcher: %d, %v", id, header.String())

		c.MarkAlive(id)
		c.logfHeartbeat("Message used as keepalive for %d", id)

		switch header.Type {
		case protocol.Topology:
			if m, ok := msg.(*protocol.TopologyMessage); ok {
				c.topologyInbox <- m
			}

		case protocol.Election:
			if m, ok := msg.(*election_definitions.ElectionMessage); ok {
				c.electionInbox <- m
			}

		case protocol.Tree:
			if m, ok := msg.(*protocol.TreeMessage); ok {
				c.treeInbox <- m
			}

		case protocol.Heartbeat:
			if !c.IsNeighborsWith(id) {
				c.logf("heartbeat", "I don't know %d, but he keeps sending heartbeats", id)
				heartbeats, _ := c.topologyMan.IncreaseRandomHeartbeat(id)
				if heartbeats >= 5 {
					c.logfHeartbeat("%d is persistent... Maybe it's an old neighbor, trying to REJOIN", id)
					c.topologyMan.ResetRandomHeartbeats(id)
					c.topologyMan.SetReAckJoinPending(id)
					c.SendMessageTo(id, c.newReJoinMessage(id))
				}
			}
		}
	}
}

// It waits on an output channel for messages, and each time one is received, it sends it to the destination neighbor.
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

// Handles incoming topology messages, both JOIN and REJOIN variants
// It helps in the construction and maintainance of the topology, by handling incoming neighbor messages, both for first time entries and neighbors rejoining.
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) JoinHandle(ctx context.Context) {
	c.logfTopology("Started join handle")

	time.Sleep(100 * time.Millisecond)

	for {
		c.logfTopology("Awaiting join message...")
		select {
		case <-ctx.Done():
			c.logf("Join: Stop signal received")
			c.logfTopology("Stop signal received")
			return
		case message := <-c.topologyInbox:

			c.logfTopology("Join message received: %s", message.String())

			rejoin := message.Flags&protocol.Jflags_REJOIN > 0

			if rejoin {
				c.handleRejoin(message)
			} else {
				c.handleFirstTimeJoin(message)
			}
		}
	}
}

// FSM that handles the election process.
// It processes any incoming election message in order to complete an election.
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) ElectionHandle(ctx context.Context) {

	c.logfElection("Started election handle")

	c.SwitchToElectionState(election_definitions.Idle)
	time.Sleep(3 * time.Second)

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
			if c.GetId() != destId {
				c.logfElection("I Received someone else's message? (%d): %v", destId, message.String())
				continue
			}
			if !c.IsNeighborsWith(senderId) {
				c.logfElection("Election message received by a STRANGER (%d): %s", senderId, message.String())
				continue
			}

			c.logfElection("Election message received: %s. Current state{%v}", message.String(), state.String())

			switch state {

			case election_definitions.Idle:
				c.handleIdleState(message)

			case election_definitions.WaitingYoDown:
				c.handleWaitingYoDown(message)

			case election_definitions.WaitingYoUp:
				c.handleWaitingYoUp(message)
			}

		case <-timer.C:
			c.logfElection("Timeout occurred... Current state is %v", state.String())

			switch state {
			case election_definitions.WaitingYoDown:
				c.handleYoDownTimeout()
			case election_definitions.WaitingYoUp:
				c.handleYoUpTimeout()
			case election_definitions.Idle:
				c.handleIdleTimeout()
			}
		}
	}
}

// It processes any incoming tree message in order to try and keep it synchronized and autoheal it, for example by handling OFF children and parents.
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) TreeHandle(ctx context.Context) {
	c.logfTree("Started tree handle")

	time.Sleep(3 * time.Second)

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
			case protocol.TFlags_NOPARENTREP | protocol.TFlags_Q: // Asked me to be his child, accept instantly
				if !c.treeMan.HasParent() { // I still don't have a parent
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
			case protocol.TFlags_NOPARENTREP: // Denied
				c.logfTree("%d said he can't help... ", sender)
			case protocol.TFlags_A:
				if c.treeMan.GetState() == topology.TreeActive {
					answer, _ := strconv.ParseBool(message.Body[0])
					if answer {
						c.AddTreeNeighbor(sender)
						c.logfTree("%d Responded with YES, he is my new neighbor", sender)
					} else {
						c.treeMan.AcknowledgeNo(sender)
						c.logfTree("%d Responded with No, he is not my neighbor", sender)
					}
					if c.treeMan.GetCounter() == len(c.topologyMan.NeighborList()) {
						c.becomeTreeDone()
					}
				} else {
					c.logfTree("Shouldn't have gotten this")
				}
			case protocol.TFLags_INPUTNEXTHOP:

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
					c.logfTree("Forwarding (%d) next input hvop to parent (%d)", inputNode, parent)
					c.SendMessageTo(parent, c.newTreeInputNextHopMessage(parent, node.NodeId(inputNode)))
				}
			case protocol.TFLags_PARENTPORT:
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

// Sends an heartbeat message to each neighbor on a fixed period.
// N.B. This is blocking, run as goroutine preferrably.
func (c *ControlPlaneManager) HeartbeatHandle(ctx context.Context) {
	c.logfHeartbeat("Heartbeat handle has started...")

	time.Sleep(100 * time.Millisecond)

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

func (c *ControlPlaneManager) Run(ctx context.Context) {
	go c.RunInputDispatcher(ctx)
	go c.RunOutputDispatcher(ctx)
	go c.JoinHandle(ctx)
	go c.ElectionHandle(ctx)
	go c.TreeHandle(ctx)
	go c.HeartbeatHandle(ctx)

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

func (c *ControlPlaneManager) RegisterNeighborBatch(batch map[node.NodeId]node.Address) {
	c.bufferedNeighborBatch = batch
}

func (c *ControlPlaneManager) handleFirstTimeJoin(msg *protocol.TopologyMessage) error {
	sourceId, err := ExtractIdentifier(msg.GetHeader().Sender)
	if err != nil {
		// Mal-formatted, skiphandleEnteringN
		return err
	}

	if msg.Flags&protocol.Jflags_JOIN > 0 {

		if msg.Flags&protocol.Jflags_ACK > 0 {
			// This is a JOIN-ACK message
			// send ack

			c.logfTopology("%d sent a JOIN-ACK message: %v", sourceId, msg)

			c.topologyMan.MarkAckJoin(sourceId)
			c.MarkAlive(sourceId)
			c.logfTopology("%d is ON from JOIN-ACK. A{%v} AJ{%v}", sourceId, c.topologyMan.AckPending, c.topologyMan.AckJoinPending)
			c.SendMessageTo(sourceId, c.newAckMessage(sourceId))

			return nil
		}

		// This is a JOIN message
		// mark node as possible neighbor, send ack-join, wait for ack

		c.logfTopology("%d sent a JOIN message: %v", sourceId, msg)
		c.topologyMan.SetAckPending(sourceId, msg.Address)

		c.SendMessageTo(sourceId, c.newJoinAckMessage(sourceId))
		return nil
	}

	if msg.Flags&protocol.Jflags_ACK > 0 {
		// This is an ACK message

		c.logfTopology("%d sent a ACK message: %v", sourceId, msg)

		c.topologyMan.MarkAck(sourceId)
		c.MarkAlive(sourceId)
		c.logf("%d is ON from ACK. A{%v} AJ{%v}", sourceId, c.topologyMan.AckPending, c.topologyMan.AckJoinPending)

		// If we are not dealing with an election, we send the current leader
		lastElection := c.runtimeCtx.Load().GetLastElection()
		if c.GetElectionState() == election_definitions.Idle && lastElection != nil {
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

func (c *ControlPlaneManager) handleRejoin(msg *protocol.TopologyMessage) error {
	sourceId, err := ExtractIdentifier(msg.GetHeader().Sender)
	if err != nil {
		// Mal-formatted, skiphandleEnteringN
		return err
	}

	if msg.Flags&protocol.Jflags_JOIN > 0 {

		if msg.Flags&protocol.Jflags_ACK > 0 {
			// This is a REJOIN-ACK message
			// send ack

			c.logfTopology("%d sent a JOIN-ACK + REJOIN message: %v", sourceId, msg)

			c.topologyMan.MarkReAckJoin(sourceId, msg.Address)
			c.logfTopology("%d is ON from RE-JOIN-ACK. A{%v} AJ{%v}", sourceId, c.topologyMan.AckPending, c.topologyMan.AckJoinPending)
			c.SendMessageTo(sourceId, c.newReAckMessage(sourceId))
			return nil
		}

		// This is a REJOIN message
		// mark node as possible neighbor, send ack-join, wait for ack

		c.logfTopology("%d sent a JOIN + REJOIN message: %v", sourceId, msg)
		// The neighbor might be turning on again => send ack + rejoin
		c.topologyMan.SetReAckPending(sourceId)
		c.SendMessageTo(sourceId, c.newReJoinAckMessage(sourceId))
		return nil
	}

	if msg.Flags&protocol.Jflags_ACK > 0 {
		// This is an RE-ACK message

		c.logfTopology("%d sent a ACK + REJOIN message: %v", sourceId, msg)
		c.topologyMan.MarkReAck(sourceId)
		c.logfTopology("%d is ON from RE-ACK. A{%v} AJ{%v}", sourceId, c.topologyMan.AckPending, c.topologyMan.AckJoinPending)

		// If we are not dealing with an election, we send the current leader
		lastResult := c.runtimeCtx.Load().GetLastElection()
		if c.GetElectionState() == election_definitions.Idle && lastResult != nil {
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

func (c *ControlPlaneManager) becomeTreeDone() {

	c.treeMan.SwitchToState(topology.TreeDone)
	roleFlags := c.updateTreeRole()

	c.logfTree("No more neighbors to await, becoming DONE. My roles: %v", roleFlags.String())

	runtime := c.runtimeCtx.Load()
	runtime.SetRoles(roleFlags)
	c.updateRuntime(runtime)

	c.finishUpPostCtx()
}

func (c *ControlPlaneManager) finishUpPostCtx() {
	runtime := c.runtimeCtx.Load()

	if runtime.GetLastElection() == nil {
		c.logf("No election has been hold yet. Cannot finish postCtx")
		return
	}
	roleFlags := runtime.GetRoles()

	c.dataPlaneEnv = DataPlaneEnv{
		CanWrite: false,
		CanRead:  false,
		MakeBind: !c.treeMan.IsLeaf(),
		Runtime:  &c.runtimeCtx,
	}

	if roleFlags&node.RoleFlags_LEADER > 0 {
		c.dataPlaneEnv.CanWrite = true
		c.dataPlaneEnv.CanRead = true
		go c.startTreeEpochCounter()
		c.logfTree("Sending all of my children my port for the data plane...")
		c.forwardPortToChildren()

	}
	if roleFlags&node.RoleFlags_PERSISTENCE > 0 {
		c.dataPlaneEnv.CanWrite = false
		c.dataPlaneEnv.CanRead = true
	}
	if roleFlags&node.RoleFlags_INPUT > 0 { // Start input
		if parent, hasParent := c.GetTreeParent(); hasParent {
			c.logfTree("Telling my parent (%d) I am input node", parent)
			c.SendMessageTo(parent, c.newTreeInputNextHopMessage(parent, c.GetId()))
		}
	}
}

func (c *ControlPlaneManager) forwardPortToChildren() {
	children := c.GetTreeChildren()
	dataPort := c.GetDataPort()
	for child := range children {
		c.logfTree("Sending (%d) the port (%d)", child, dataPort)
		c.SendMessageTo(child, c.newTreeParentPortMessage(child, dataPort))
	}
	c.sendDataEnv() // I could be ready to give the ctx to cluster
}

func (c *ControlPlaneManager) sendDataEnv() {
	c.dataEnvChan <- c.dataPlaneEnv
}

func (c *ControlPlaneManager) updateTreeRole() node.NodeRoleFlags {
	var roleFlags node.NodeRoleFlags = node.RoleFlags_NONE

	treeParent, treeChildren, hasTreeParent := c.GetTreeNeighbors()
	if !hasTreeParent { // I'm root
		roleFlags |= node.RoleFlags_LEADER | node.RoleFlags_PERSISTENCE
		if len(treeChildren) == 0 {
			roleFlags |= node.RoleFlags_INPUT
		}
	} else {
		if len(treeChildren) == 0 { // Only 1 tree neighbor (sender) => Leaf => Input
			roleFlags |= node.RoleFlags_INPUT
		}
		if treeParent == c.runtimeCtx.Load().lastElection.GetLeaderID() { // MaxHops from leader: 1 => Storage
			roleFlags |= node.RoleFlags_PERSISTENCE
		}
	}
	return roleFlags
}

func (c *ControlPlaneManager) SendCurrentLeader(neighbor node.NodeId) {
	lastElection := c.runtimeCtx.Load().GetLastElection()
	electionMsg := c.newLeaderAnnouncementMessage(neighbor, lastElection.GetLeaderID(), lastElection.GetElectionID(), c.treeMan.GetRootHopCount())
	if electionMsg.ElectionId == election_definitions.InvalidId {
		c.logf("The electionId in post election context is invalid")
		return
	}
	c.SendMessageTo(neighbor, electionMsg)
}

func (c *ControlPlaneManager) startShout() {

	c.logfTree("Setting up spanning tree process, im INITIATOR")
	c.treeMan.SetRoot(true)
	c.logfTree("Root{true}, Parent{none}, TreeNeighbors{[]}")

	electionId := c.electionCtx.GetId()
	onNeighbors := 0
	for _, neighbor := range c.NeighborList() {

		if c.IsAlive(neighbor) {
			onNeighbors++
			c.SendMessageTo(neighbor, c.newLeaderAnnouncementMessage(neighbor, c.GetId(), electionId, 0))
			c.logfElection("Sent leader message to %d", neighbor)
			c.logfTree("Sent Q message to %d", neighbor)
		} else {
			c.logfElection("NOT sent leader message to %d", neighbor)
			c.logfTree("NOT sent Q message to %d", neighbor)
		}
	}
	c.endElection(c.GetId(), electionId)

	if onNeighbors > 0 {
		c.logfTree("Awaiting responses... becoming ACTIVE")
		c.treeMan.SwitchToState(topology.TreeActive)
	} else {
		c.logfTree("I have no ON neighbors... I am alone")
		c.becomeTreeDone()
	}
}

func (c *ControlPlaneManager) startTreeEpochCounter() {

	c.logfTree("Started tree epoch counter. Initial value {0}")
	var counter uint64 = 0

	ticker := time.NewTicker(2 * time.Second)

	_, children, _ := c.treeMan.GetTreeNeighbors()
	for range ticker.C {
		for child := range children {
			c.SendMessageTo(child, c.newTreeEpochMessage(child, counter))
			c.logfTree("Sending current counter{%d} to %d", counter, child)
		}
		counter++
	}
}

func (c *ControlPlaneManager) IncreaseLeaderTimeouts() uint8 {
	c.leaderTimeouts++
	return c.leaderTimeouts
}

func (c *ControlPlaneManager) GetLeaderTimeouts() uint8 {
	return c.leaderTimeouts
}

func (c *ControlPlaneManager) ResetLeaderTimeouts() {
	c.leaderTimeouts = 0
}

//============================================================================//
//	Wrappers for plane config component                                       //
//============================================================================//

// Returns the ID of this node
func (c *ControlPlaneManager) GetId() node.NodeId {
	return c.config.GetId()
}

// Returns the port for the control plane for this node
func (c *ControlPlaneManager) GetControlPort() uint16 {
	return c.config.GetControlPort()
}

// Returns the port for the data plane for this node
func (c *ControlPlaneManager) GetDataPort() uint16 {
	return c.config.GetDataPort()
}

//============================================================================//
//	Wrappers for logical clock component                                      //
//============================================================================//

// Updates the logical clock based on the internal and given value, following Lampert's rule
func (c *ControlPlaneManager) UpdateClock(received uint64) {
	c.clock.UpdateClock(received)
}

// Increments by 1 the logical clock, returing the updated value
func (c *ControlPlaneManager) IncrementClock() uint64 {
	return c.clock.IncrementClock()
}

// Returns the current value of the clock
func (c *ControlPlaneManager) Snapshot() uint64 {
	return c.clock.Snapshot()
}

//============================================================================//
//	Logging wrappers                                                          //
//============================================================================//

// Logs the given formatting string, with args, on the main logger.
func (c *ControlPlaneManager) logf(format string, v ...any) {
	c.mainLogger.Logf(format, v...)
}

// Logs on the topology logger.
func (c *ControlPlaneManager) logfTopology(format string, v ...any) {
	c.topologyLogger.Logf(format, v...)
}

// Logs on the election logger.
func (c *ControlPlaneManager) logfElection(format string, v ...any) {
	c.electionLogger.Logf(format, v...)
}

// Logs on the tree logger.
func (c *ControlPlaneManager) logfTree(format string, v ...any) {
	c.treeLogger.Logf(format, v...)
}

// Logs on the heartbeat logger.
func (c *ControlPlaneManager) logfHeartbeat(format string, v ...any) {
	c.heartbeatLogger.Logf(format, v...)
}

// It sends an election message to the given node (based on ID). It sends the message onto the output channel.
func (c *ControlPlaneManager) SendMessageTo(neighbor node.NodeId, msg protocol.Message) {
	c.outputChannel <- protocol.OutMessage{DestId: neighbor, Message: msg}
}

//============================================================================//
//  Utility wrappers                                                          //
//============================================================================//

// Creates a preconfigured message header for the given neighbor (destination) and type.
// The timestamp field is empty, it will be automatically marked when Sent (for accuracy)
func (c *ControlPlaneManager) newMessageHeader(neighbor node.NodeId, mType protocol.MessageType) *protocol.MessageHeader {
	return protocol.NewMessageHeader(
		c.StringIdentifier(),
		neighbor.Identifier(),
		mType,
	)
}

// Creates a preconfigured message for the Topology Type
func (c *ControlPlaneManager) newTopologyMessage(neighbor node.NodeId, flags protocol.Jflags) *protocol.TopologyMessage {
	addr := node.NewAddress(network.GetOutboundIP(), c.GetControlPort())
	return protocol.NewTopologyMessage(
		c.newMessageHeader(neighbor, protocol.Topology),
		addr,
		flags,
	)
}

// JOIN | Usage: sent the first time we connect to this neighbor (spontanously).
func (c *ControlPlaneManager) newJoinMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_JOIN)
}

// RE+JOIN | Usage: sent to reconnect to a neighbor (e.g. after some timeouts).
func (c *ControlPlaneManager) newReJoinMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_JOIN|protocol.Jflags_REJOIN)
}

// JOIN-ACK | Usage: sent to respond to a JOIN message.
func (c *ControlPlaneManager) newJoinAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_JOIN|protocol.Jflags_ACK)
}

// RE+JOIN-ACK | Usage: sent to respond to a RE+JOIN message.
func (c *ControlPlaneManager) newReJoinAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_JOIN|protocol.Jflags_ACK|protocol.Jflags_REJOIN)
}

// ACK | Usage: sent to respond to a JOIN-ACK message.
func (c *ControlPlaneManager) newAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_ACK)
}

// RE+ACK | Usage: sent to respond to a RE+JOIN-ACK message.
func (c *ControlPlaneManager) newReAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
	return c.newTopologyMessage(neighbor, protocol.Jflags_ACK|protocol.Jflags_REJOIN)
}

// Creates a preconfigured message for the Election Type
func (c *ControlPlaneManager) newElectionMessage(neighbor node.NodeId, typ election_definitions.ElectionMessageType, electionId election_definitions.ElectionId, roundEpoch uint, body []string) *election_definitions.ElectionMessage {
	return election_definitions.NewElectionMessage(
		c.newMessageHeader(neighbor, protocol.Election),
		typ,
		electionId,
		body,
		roundEpoch,
	)
}

// Returns a preconfigured election message to use to try to start a new election
func (c *ControlPlaneManager) newStartMessage(neighbor node.NodeId, proposedElectionId election_definitions.ElectionId) *election_definitions.ElectionMessage {
	return c.newElectionMessage(
		neighbor,
		election_definitions.Start,
		election_definitions.InvalidId,
		0,
		[]string{string(proposedElectionId)},
	)
}

// Returns a preconfigured election message to send during the YO-DOWN phase (the proposal)
func (c *ControlPlaneManager) newProposalMessage(neighbor, proposal node.NodeId) *election_definitions.ElectionMessage {
	return c.newElectionMessage(
		neighbor,
		election_definitions.Proposal,
		c.GetElectionId(),
		c.GetCurrentRoundNumber(),
		[]string{fmt.Sprintf("%d", proposal)},
	)
}

// Returns a preconfigured election message to send during the YO-UP phase (the vote)
func (c *ControlPlaneManager) newVoteMessage(neighbor node.NodeId, vote, prune bool) *election_definitions.ElectionMessage {
	return c.newElectionMessage(
		neighbor,
		election_definitions.Vote,
		c.GetElectionId(),
		c.GetCurrentRoundNumber(),
		[]string{
			strconv.FormatBool(vote),
			strconv.FormatBool(prune),
		},
	)
}

// Returns a preconfigured election message to send during the SHOUT to forward the leader ID, it also creates a SPT in the process (this is the Q message).
func (c *ControlPlaneManager) newLeaderAnnouncementMessage(neighbor, leader node.NodeId, electionId election_definitions.ElectionId, hopCount uint64) *election_definitions.ElectionMessage {
	return c.newElectionMessage(
		neighbor,
		election_definitions.Leader,
		electionId,
		c.GetCurrentRoundNumber(),
		[]string{
			strconv.FormatUint(uint64(leader), 10),
			"Q",
			strconv.FormatUint(uint64(hopCount), 10),
		},
	)
}

// Returns a preconfigured election message to send during the SHOUT to forward the leader ID, it also creates a SPT in the process (this is the Answer message).
func (c *ControlPlaneManager) newLeaderResponseMessage(neighbor, leader node.NodeId, electionId election_definitions.ElectionId, answer bool) *election_definitions.ElectionMessage {
	return c.newElectionMessage(
		neighbor,
		election_definitions.Leader,
		electionId,
		c.GetCurrentRoundNumber(),
		[]string{
			strconv.FormatUint(uint64(leader), 10),
			"A",
			strconv.FormatBool(answer),
		},
	)
}

// Creates a preconfigured message for the Tree Type
func (c *ControlPlaneManager) newTreeMessage(id node.NodeId, epoch uint64, flags protocol.TreeFlags, body []string) *protocol.TreeMessage {
	return protocol.NewTreeMessage(
		c.newMessageHeader(id, protocol.Tree),
		epoch,
		flags,
		body,
	)
}
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

func (c *ControlPlaneManager) newTreeEpochMessage(tChild node.NodeId, epoch uint64) *protocol.TreeMessage {
	return c.newTreeMessage(tChild, epoch, protocol.TFlags_EPOCH, []string{})
}

func (c *ControlPlaneManager) newTreeHelpReq(nonTreeNeighbor node.NodeId) *protocol.TreeMessage {
	return c.newTreeMessage(nonTreeNeighbor, c.treeMan.GetEpoch(), protocol.TFlags_NOPARENTREQ, []string{})
}

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

func (c *ControlPlaneManager) newTreeHelpRepNegative(nonTreeNeighbor node.NodeId) *protocol.TreeMessage {
	return c.newTreeMessage(
		nonTreeNeighbor,
		c.treeMan.GetEpoch(),
		protocol.TFlags_NOPARENTREP,
		[]string{strconv.FormatUint(c.treeMan.GetRootHopCount(), 10)},
	)
}

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
func (c *ControlPlaneManager) newHeartbeatMessage(neighbor node.NodeId) *topology.HeartbeatMessage {
	return topology.NewHeartbeatMessage(c.newMessageHeader(neighbor, protocol.Heartbeat))
}

// Creates and returns a message that has the same body as the original one, but has the header changed in the following manner:
//
//	The source is 'this' node (.GetId())
//	The destination is newDestination
//	The type is the same as the original
//
// This should be used when we want to forward a message without modifying anything inside, just the header (for correct routing)
func (c *ControlPlaneManager) copyMessage(newDestination node.NodeId, original protocol.Message) protocol.Message {
	oldHeader := original.GetHeader()
	newHeader := c.newMessageHeader(newDestination, oldHeader.Type)

	newMessage := original.Clone()
	newMessage.SetHeader(newHeader)
	return newMessage
}

// Returns the identifier of this node.
// It's equivalent to call .GetId().Identifier()
func (c *ControlPlaneManager) StringIdentifier() string {
	return c.GetId().Identifier()
}

// Extracts the ID of a node from an identifier string, which has this format: `node-<id>`
func ExtractIdentifier(identifier string) (node.NodeId, error) {
	return node.ExtractIdentifier([]byte(identifier))
}

//============================================================================//
//  Wrappers for TopologyManager component                                    //
//============================================================================//

// Adds the node with given ID as a neighbor in the topology. The address must be in formatted as `<ip-address>:<port>`.
func (c *ControlPlaneManager) AddNeighbor(possibleNeighbor node.NodeId, address node.Address) error {
	if c.GetId() == possibleNeighbor {
		return fmt.Errorf("Cannot set the node as its own neighbor")
	}
	err := c.topologyMan.Add(possibleNeighbor, address)
	go c.ReliableJoin(possibleNeighbor)
	return err
}

func (c *ControlPlaneManager) ReliableJoin(possibleNeighbor node.NodeId) {
	backoff := 500 * time.Millisecond
	maxBackoff := 8 * time.Second

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

// Acknowledges the presence of a neighbor.
// It adds a neighbor just logically to the topology, without sending a JOIN message to it.
// Useful to have make the topology consistent after a neighbor sent a JOIN, without sending one right after.
func (c *ControlPlaneManager) AcknowledgeNeighborExistence(id node.NodeId, address node.Address) {
	c.topologyMan.LogicalAdd(id, address)
}

// Removes the neighbor with the given ID from the topology.
func (c *ControlPlaneManager) RemoveNeighbor(id node.NodeId) error {
	return c.topologyMan.Remove(id)
}

// Retrieves the IP address of the neighbor with given ID.
func (c *ControlPlaneManager) GetNeighborAddress(id node.NodeId) (node.Address, error) {
	return c.topologyMan.Get(id)
}

// Returns true if this node is neighbors with node that has id ID.
// This is true after some time in two situations:
//   - This node previously called AddNeighbor() passing the ID of the other node;
//   - The other node called AddNeighbor() passing this node's ID (if the IP was correct).
func (c *ControlPlaneManager) IsNeighborsWith(id node.NodeId) bool {
	return c.topologyMan.Exists(id)
}

// Returns true when the node has at least one neighbor.
func (c *ControlPlaneManager) HasNeighbors() bool {
	return c.topologyMan.HasNeighbors()
}

// Returns a slice containing the IDs of all the neighboring nodes.
func (c *ControlPlaneManager) NeighborList() []node.NodeId {
	return c.topologyMan.NeighborList()
}

func (c *ControlPlaneManager) MarkAlive(neighbor node.NodeId) {
	c.topologyMan.UpdateLastSeen(neighbor, time.Now())
}

func (c *ControlPlaneManager) IsAlive(neighbor node.NodeId) bool {
	return c.topologyMan.IsAlive(neighbor)
}

// Sends a message to the neighbor node with given ID.
// Returns an error if the message is mal-formatted.
func (c *ControlPlaneManager) SendToNeighbor(id node.NodeId, message protocol.Message) error {

	message.MarkTimestamp(c.IncrementClock())

	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return c.topologyMan.SendTo(id, payload)
}

func (c *ControlPlaneManager) Poll(timeout time.Duration) error {
	return c.topologyMan.Poll(timeout)
}

// Retrieves a message from the topology.
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

	switch header.Type {
	case protocol.Topology:
		msg = &protocol.TopologyMessage{}

	case protocol.Election:
		msg = &election_definitions.ElectionMessage{}

	case protocol.Heartbeat:
		msg = &topology.HeartbeatMessage{}

	case protocol.Tree:
		msg = &protocol.TreeMessage{}

	default:

		return 0, nil, fmt.Errorf("Unknown message type: %v", header.Type)
	}

	if err := json.Unmarshal(payload[0], msg); err != nil {
		return 0, nil, err
	}
	return id, msg, nil
}

func IsRecvNotReadyError(err error) bool {
	return topology.IsRecvNotReadyError(err)
}

//============================================================================//
//  Wrappers for TreeManager component                                        //
//============================================================================//

// Adds the node with given ID as a child in the SPT.
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

// Removes the node with given ID from the children in the SPT.
// Returns an error if the child was not present.
func (c *ControlPlaneManager) RemoveTreeNeighbor(neighborId node.NodeId) {
	c.treeMan.RemoveTreeNeighbor(neighborId)
}

// Removes the parent in the SPT structure.
func (c *ControlPlaneManager) RemoveTreeParent() error {
	return c.treeMan.RemoveParent()
}

// Returns the children in the tree SPT, and a parent, with a bool that tells wheter the parent is present or not.
func (c *ControlPlaneManager) GetTreeNeighbors() (parent node.NodeId, children map[node.NodeId]struct{}, hasParent bool) {
	return c.treeMan.GetTreeNeighbors()
}

// Returns the  children in the SPT.
func (c *ControlPlaneManager) GetTreeChildren() (children map[node.NodeId]struct{}) {
	return c.treeMan.GetChildren()
}

// Sets the node with given ID as the parent in the SPT.
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

// Returns the ID of the parent node and a bool stating wheter it has a parent or not.
func (c *ControlPlaneManager) GetTreeParent() (parentId node.NodeId, hasParent bool) {
	return c.treeMan.GetParent()
}

// Returns true when this node is root in the SPT.
func (c *ControlPlaneManager) isTreeRoot() bool {
	return c.treeMan.IsRoot()
}

// Returns true when this node is a leaf in the SPT.
func (c *ControlPlaneManager) isTreeLeaf() bool {
	return c.treeMan.IsLeaf()
}

// Resets the tree manager
func (c *ControlPlaneManager) ResetTree() {
	c.treeMan.Reset()
}

//============================================================================//
//  Wrappers for ElectionContext (and PostElectionContext) component(s)       //
//============================================================================//

// Returns the state of this node during the election (Idle, Waiting for YoDown or Waiting for YoUp).
func (c *ControlPlaneManager) GetElectionState() election_definitions.ElectionState {
	return c.electionCtx.GetState()
}

// Returns the status of this node during the current round of the election (Source, Internal Node, Sink, Leader or Lost).
func (c *ControlPlaneManager) GetElectionStatus() election_definitions.ElectionStatus {
	return c.electionCtx.GetStatus()
}

// Switches to the given state.
func (c *ControlPlaneManager) SwitchToElectionState(state election_definitions.ElectionState) {
	c.electionCtx.SwitchToState(state)
}

// Marks the "START" message as received
func (c *ControlPlaneManager) SetElectionStartReceived() {
	c.electionCtx.SetStartReceived()
}

func (c *ControlPlaneManager) HasReceivedElectionStart() bool {
	return c.electionCtx.HasReceivedStart()
}
func (c *ControlPlaneManager) StashFutureElectionProposal(sender node.NodeId, proposed node.NodeId, roundEpoch uint) {
	c.electionCtx.StashFutureProposal(sender, proposed, roundEpoch)
}

// Generates an election ID based on the clock and id
func (c *ControlPlaneManager) GenerateElectionId() election_definitions.ElectionId {
	return election.GenerateId(c.IncrementClock(), c.GetId())
}

// Stores the given election ID
func (c *ControlPlaneManager) SetElectionId(id election_definitions.ElectionId) {
	c.electionCtx.SetId(id)
}

// Returns the accorded election ID
func (c *ControlPlaneManager) GetElectionId() election_definitions.ElectionId {
	return c.electionCtx.GetId()
}

// Returns the number of the current round for the current election
func (c *ControlPlaneManager) GetCurrentRoundNumber() uint {
	return c.electionCtx.CurrentRound()
}

// Prepares for the next round of the election
func (c *ControlPlaneManager) NextElectionRound() {
	c.logfElection("Preparing for next round...")
	c.electionCtx.NextRound()
	c.logfElection("Context prepared, current round is %v and I am %s", c.GetCurrentRoundNumber(), c.GetElectionStatus().String())
}
