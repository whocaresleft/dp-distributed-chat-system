/*
 * Copyright (c) 2026 Francesco Biribo'
 *
 * Permission to use, copy, modify, and distribute this software for any purpose with or without fee is hereby granted, provided that the above copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
/*
   package cluster

   import (

   	"context"
   	"encoding/json"
   	"fmt"
   	"log"
   	bootstrap_protocol "server/cluster/bootstrap/protocol"
   	"server/cluster/election"
   	election_definitions "server/cluster/election/definitions"
   	"server/cluster/network"
   	"server/cluster/nlog"
   	"server/cluster/node"
   	"server/cluster/node/protocol"
   	"server/cluster/topology"
   	"server/internal/data"
   	"server/internal/input"
   	"strconv"
   	"sync/atomic"
   	"time"

   	"google.golang.org/grpc"
   	"google.golang.org/grpc/credentials/insecure"

   )

   	var LogTopics = []string{
   		"main",
   		"join",
   		"election",
   		"heartbeat",
   		"tree",
   		"input",
   		"data",
   	}

   	func registerLogfiles(n *nlog.NodeLogger, topics []string) error {
   		for _, topic := range topics {
   			if _, err := n.RegisterSubsystem(topic); err != nil {
   				return err
   			}
   		}
   		return nil
   	}

   // A Cluster Node represents a single node in the distributed system. It holds different components of the node together

   	type ZlusterNode struct {
   		ctx          context.Context
   		cancel       context.CancelFunc
   		logicalClock *node.LogicalClock
   		config       *node.NodeConfig
   		logger       *nlog.NodeLogger

   		topologyMan *topology.TopologyManager
   		electionCtx *election.ElectionContext
   		treeMan     *topology.TreeManager

   		postElectionCtx atomic.Pointer[election.PostElectionContext]

   		topologyInbox  chan *protocol.TopologyMessage
   		electionInbox  chan *election_definitions.ElectionMessage
   		treeInbox      chan *protocol.TreeMessage
   		inputInbox     chan string
   		dataPlaneInbox chan string
   		outputChannel  chan protocol.OutMessage

   		DataPlaneManager *data.DataPlaneManager
   		inputMan         *input.InputManager
   	}

   // Creates a new cluster node with the given ID and on the given port
   // It returns a pointer to said node if no problems arise. Otherwise, the pointer is nil and an appropriate error is returned
   func NewZlusterNode(id node.NodeId, controlPort, dataPort uint16, logging bool) (*ZlusterNode, error) {

   		config, err := node.NewNodeConfig(id, int(controlPort), int(dataPort))
   		if err != nil {
   			return nil, err
   		}

   		logger, err := nlog.NewNodeLogger(config.GetId(), logging)
   		if err != nil {
   			return nil, err
   		}

   		if err := registerLogfiles(logger, LogTopics); err != nil {
   			return nil, err
   		}

   		go logger.Run()

   		logger.Logf("main", "Configuration component correctly created: Id{%d}, Ports{Control: %d, Data: %d}", id, controlPort, dataPort)
   		logger.Logf("main", "Creating topology component")

   		tp, err := topology.NewTopologyManager(id, controlPort)
   		if err != nil {
   			return nil, err
   		}
   		tm := topology.NewTreeManager()

   		conMan, err := network.NewConnectionManager(id)
   		if err != nil {
   			return nil, err
   		}

   		logger.Logf("main", "Topology component correctly created")

   		electionInbox := make(chan *election_definitions.ElectionMessage, 500)
   		topologyInbox := make(chan *protocol.TopologyMessage, 500)
   		treeInbox := make(chan *protocol.TreeMessage, 500)
   		dataPlaneInbox := make(chan string, 500)
   		inputInbox := make(chan string, 500)
   		outChan := make(chan protocol.OutMessage, 500)

   		dm := data.NewDataPlaneManager(conMan)
   		//dm.SetTreeFinder(tm)
   		dm.SetHostFinder(tp)

   		logger.Logf("main", "Created context")
   		logger.Logf("main", "Node is all set")

   		return &ZlusterNode{
   			ctx:              nil,
   			cancel:           nil,
   			logicalClock:     node.NewLogicalClock(),
   			config:           config,
   			topologyMan:      tp,
   			electionCtx:      election.NewElectionContext(config.GetId()),
   			postElectionCtx:  atomic.Pointer[election.PostElectionContext]{},
   			treeMan:          tm,
   			DataPlaneManager: dm,
   			inputMan:         nil,
   			topologyInbox:    topologyInbox,
   			electionInbox:    electionInbox,
   			treeInbox:        treeInbox,
   			dataPlaneInbox:   dataPlaneInbox,
   			inputInbox:       inputInbox,
   			outputChannel:    outChan,
   			logger:           logger,
   		}, nil
   	}

   	func (n *ZlusterNode) WithDefaultContext() *ZlusterNode {
   		ctx := context.Background()
   		ctx, can := context.WithCancel(ctx)
   		n.ctx = ctx
   		n.cancel = can
   		return n
   	}

   	func (n *ZlusterNode) SetContextDefaultCancel(ctx context.Context) {
   		ctx, can := context.WithCancel(ctx)
   		n.ctx = ctx
   		n.cancel = can
   	}

   	func (n *ZlusterNode) SetContextCancelFunc(ctx context.Context, cancel context.CancelFunc) {
   		n.ctx = ctx
   		n.cancel = cancel
   	}

   	func (n *ZlusterNode) EnableLogging() {
   		n.logger.EnableLogging()
   	}

   	func (n *ZlusterNode) DisableLogging() {
   		n.logger.DisableLogging()
   	}

   // Logs the given string. Wrap around logger.Printf

   	func (n *ZlusterNode) logf(filename, format string, a ...any) {
   		n.logger.Logf(filename, fmt.Sprintf("{%d}. %s", n.logicalClock.Snapshot(), format), a...)
   	}

   // Increments this node's logical clock and returns its value

   	func (n *ZlusterNode) IncrementClock() uint64 {
   		return n.logicalClock.IncrementClock()
   	}

   // Updates this node's logical clock based on the received one

   	func (n *ZlusterNode) UpdateClock(received uint64) {
   		n.logicalClock.UpdateClock(received)
   	}

   // Dispatches incoming messages based on message type.
   // This is supposed to be run as a goroutine.
   // It waits for incoming messages and dispatches them to the correct input channel.
   // If no channel meets the criteria, the message is dropped.
   func (n *ZlusterNode) RunInputDispatcher() {

   		n.logf("main", "Started input dispatcher. Awaiting messages...")
   		defer n.logf("main", "FATAL: Input dispatcher has failed...")

   		for {
   			select {
   			case <-n.ctx.Done():
   				n.logf("main", "Input dispatcher: Stop signal received")
   				return
   			default:
   			}

   			if err := n.poll(500 * time.Millisecond); err != nil {
   				if isRecvNotReadyError(err) {
   					continue
   				}
   				n.logf("main", "Polling error %v:", err)
   				time.Sleep(100 * time.Millisecond)
   				continue
   			}

   			id, msg, err := n.recv()
   			if err != nil {
   				if isRecvNotReadyError(err) {
   					n.logf("main", "Recv not ready: %v", err)
   					continue
   				}
   				n.logf("main", "Message received on dispatcher with an error: %v", err)
   				continue
   			}

   			header := msg.GetHeader()
   			n.logf("main", "Message received on dispatcher: %d, %v", id, header.String())

   			n.markAlive(id)
   			n.logf("heartbeat", "Message used as keepalive for %d", id)

   			switch header.Type {
   			case protocol.Topology:
   				if m, ok := msg.(*protocol.TopologyMessage); ok {
   					n.topologyInbox <- m
   				}

   			case protocol.Election:
   				if m, ok := msg.(*election_definitions.ElectionMessage); ok {
   					n.electionInbox <- m
   				}

   			case protocol.Tree:
   				if m, ok := msg.(*protocol.TreeMessage); ok {
   					n.treeInbox <- m
   				}

   			case protocol.Heartbeat:
   				if !n.IsNeighborsWith(id) {
   					n.logf("heartbeat", "I don't know %d, but he keeps sending heartbeats", id)
   					heartbeats, _ := n.topologyMan.IncreaseRandomHeartbeat(id)
   					if heartbeats >= 5 {
   						n.logf("heartbeat", "%d is persistent... Maybe it's an old neighbor, trying to REJOIN", id)
   						n.topologyMan.ResetRandomHeartbeats(id)
   						n.topologyMan.SetReAckJoinPending(id)
   						n.SendMessageTo(id, n.newReJoinMessage(id))
   					}
   				}
   			}
   		}
   	}

   	func isRecvNotReadyError(err error) bool {
   		return topology.IsRecvNotReadyError(err)
   	}

   // This function is supposed to be run as a goroutine.
   // It waits on an output channel for messages, and each time one is received, it sends it to the destination neighbor.
   func (n *ZlusterNode) RunOutputDispatcher() {

   		n.logf("main", "Started output dispatcher. Awaiting messages to send...")
   		defer n.logf("main", "FATAL: Output dispatcher has failed...")

   		for {
   			select {
   			case <-n.ctx.Done():
   				n.logf("main", "Output dispatcher: Stop signal received")
   				return
   			case out := <-n.outputChannel:
   				err := n.sendToNeighbor(out.DestId, out.Message)

   				n.logf("main", "Message sent to %d: %v", out.DestId, out.Message.String())

   				if err != nil {
   					n.logf("main", "An error occurred after sendToNeighbor(): %v", err)
   				}
   			}
   		}
   	}

   // It sends an election message to the given node (based on ID). It sends the message onto the output channel.

   	func (n *ZlusterNode) SendMessageTo(neighbor node.NodeId, msg protocol.Message) {
   		n.outputChannel <- protocol.OutMessage{DestId: neighbor, Message: msg}
   	}

   	func (n *ZlusterNode) CopyMessage(newDest node.NodeId, msg protocol.Message) protocol.Message {
   		oldHeader := msg.GetHeader()
   		newHeader := n.NewMessageHeader(newDest, oldHeader.Type)

   		newMessage := msg.Clone()
   		newMessage.SetHeader(newHeader)
   		return newMessage
   	}

   	func (n *ZlusterNode) newTopologyMessage(neighbor node.NodeId, flags protocol.Jflags) *protocol.TopologyMessage {
   		addr := node.NewAddress(getOutboundIP(), n.getControlPort())
   		return protocol.NewTopologyMessage(
   			n.NewMessageHeader(neighbor, protocol.Topology),
   			addr,
   			flags,
   		)
   	}

   	func (n *ZlusterNode) newJoinMessage(neighbor node.NodeId) *protocol.TopologyMessage {
   		return n.newTopologyMessage(neighbor, protocol.Jflags_JOIN)
   	}

   	func (n *ZlusterNode) newReJoinMessage(neighbor node.NodeId) *protocol.TopologyMessage {
   		return n.newTopologyMessage(neighbor, protocol.Jflags_JOIN|protocol.Jflags_REJOIN)
   	}

   	func (n *ZlusterNode) newJoinAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
   		return n.newTopologyMessage(neighbor, protocol.Jflags_JOIN|protocol.Jflags_ACK)
   	}

   	func (n *ZlusterNode) newReJoinAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
   		return n.newTopologyMessage(neighbor, protocol.Jflags_JOIN|protocol.Jflags_ACK|protocol.Jflags_REJOIN)
   	}

   	func (n *ZlusterNode) newAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
   		return n.newTopologyMessage(neighbor, protocol.Jflags_ACK)
   	}

   	func (n *ZlusterNode) newReAckMessage(neighbor node.NodeId) *protocol.TopologyMessage {
   		return n.newTopologyMessage(neighbor, protocol.Jflags_ACK|protocol.Jflags_REJOIN)
   	}

   	func (n *ZlusterNode) newHeartbeatMessage(neighbor node.NodeId) *topology.HeartbeatMessage {
   		return topology.NewHeartbeatMessage(n.NewMessageHeader(neighbor, protocol.Heartbeat))
   	}

   	func (n *ZlusterNode) HeartbeatHandle() {
   		n.logf("heartbeat", "Heartbeat handle has started...")
   		defer n.logf("heartbeat", "FATAL: Heartbeat handle has failed...")

   		time.Sleep(100 * time.Millisecond)

   		ticker := time.NewTicker(2 * time.Second)
   		defer ticker.Stop()

   		for {
   			select {
   			case <-n.ctx.Done():
   				n.logf("main", "Heartbeat: Stop signal received")
   				n.logf("heartbeat", "Stop signal received")
   				return
   			case <-ticker.C:
   				for _, neighbor := range n.neighborList() {
   					n.SendMessageTo(neighbor, n.newHeartbeatMessage(neighbor))
   				}
   			}
   		}
   	}

   	func (n *ZlusterNode) JoinHandle() {
   		n.logf("join", "Started join handle")
   		defer n.logf("join", "FATAL: Join handle has failed...")

   		time.Sleep(100 * time.Millisecond)

   		for {
   			n.logf("join", "Awaiting join message...")
   			select {
   			case <-n.ctx.Done():
   				n.logf("main", "Join: Stop signal received")
   				n.logf("join", "Stop signal received")
   				return
   			case message := <-n.topologyInbox:

   				n.logf("join", "Join message received: %s", message.String())

   				rejoin := message.Flags&protocol.Jflags_REJOIN > 0

   				if rejoin {
   					n.handleRejoin(message)
   				} else {
   					n.handleFirstTimeJoin(message)
   				}
   			}
   		}
   	}

   // Sender -> JOIN -> Destination
   // Sender <- ACK, JOIN <- Destination
   // Sender -> ACK -> Destination

   	func (n *ZlusterNode) handleFirstTimeJoin(msg *protocol.TopologyMessage) error {
   		sourceId, err := extractIdentifier(msg.GetHeader().Sender)
   		if err != nil {
   			// Mal-formatted, skiphandleEnteringN
   			return err
   		}

   		if msg.Flags&protocol.Jflags_JOIN > 0 {

   			if msg.Flags&protocol.Jflags_ACK > 0 {
   				// This is a JOIN-ACK message
   				// send ack

   				n.logf("join", "%d sent a JOIN-ACK message: %v", sourceId, msg)

   				n.topologyMan.MarkAckJoin(sourceId)
   				n.markAlive(sourceId)
   				n.logf("join", "%d is ON from JOIN-ACK. A{%v} AJ{%v}", sourceId, n.topologyMan.AckPending, n.topologyMan.AckJoinPending)
   				n.SendMessageTo(sourceId, n.newAckMessage(sourceId))

   				return nil
   			}

   			// This is a JOIN message
   			// mark node as possible neighbor, send ack-join, wait for ack

   			n.logf("join", "%d sent a JOIN message: %v", sourceId, msg)
   			n.topologyMan.SetAckPending(sourceId, msg.Address)

   			n.SendMessageTo(sourceId, n.newJoinAckMessage(sourceId))
   			return nil
   		}

   		if msg.Flags&protocol.Jflags_ACK > 0 {
   			// This is an ACK message

   			n.logf("join", "%d sent a ACK message: %v", sourceId, msg)

   			n.topologyMan.MarkAck(sourceId)
   			n.markAlive(sourceId)
   			n.logf("join", "%d is ON from ACK. A{%v} AJ{%v}", sourceId, n.topologyMan.AckPending, n.topologyMan.AckJoinPending)

   			postCtx := n.postElectionCtx.Load()
   			// If we are not dealing with an election, we send the current leader
   			if n.getElectionState() == election_definitions.Idle && postCtx != nil {
   				n.logf("election", "Sending %d information about last election: %v", sourceId, postCtx)

   				n.logf("tree", "Since I have a new neighbor (%d), I send a Q and go back to being active to wait for its approval", sourceId)
   				n.treeMan.SwitchToState(topology.TreeActive)
   				n.SendCurrentLeader(sourceId)
   				return nil
   			}

   			// Otherwise, send start
   			n.startElection()
   			return nil
   		}
   		return nil
   	}

   	func (n *ZlusterNode) handleRejoin(msg *protocol.TopologyMessage) error {
   		sourceId, err := extractIdentifier(msg.GetHeader().Sender)
   		if err != nil {
   			// Mal-formatted, skiphandleEnteringN
   			return err
   		}

   		if msg.Flags&protocol.Jflags_JOIN > 0 {

   			if msg.Flags&protocol.Jflags_ACK > 0 {
   				// This is a REJOIN-ACK message
   				// send ack

   				n.logf("join", "%d sent a JOIN-ACK + REJOIN message: %v", sourceId, msg)

   				n.topologyMan.MarkReAckJoin(sourceId, msg.Address)
   				n.logf("join", "%d is ON from RE-JOIN-ACK. A{%v} AJ{%v}", sourceId, n.topologyMan.AckPending, n.topologyMan.AckJoinPending)
   				n.SendMessageTo(sourceId, n.newReAckMessage(sourceId))
   				return nil
   			}

   			// This is a REJOIN message
   			// mark node as possible neighbor, send ack-join, wait for ack

   			n.logf("join", "%d sent a JOIN + REJOIN message: %v", sourceId, msg)
   			// The neighbor might be turning on again => send ack + rejoin
   			n.topologyMan.SetReAckPending(sourceId)
   			n.sendToNeighbor(sourceId, n.newReJoinAckMessage(sourceId))
   			return nil
   		}

   		if msg.Flags&protocol.Jflags_ACK > 0 {
   			// This is an RE-ACK message

   			n.logf("join", "%d sent a ACK + REJOIN message: %v", sourceId, msg)
   			n.topologyMan.MarkReAck(sourceId)
   			n.logf("join", "%d is ON from RE-ACK. A{%v} AJ{%v}", sourceId, n.topologyMan.AckPending, n.topologyMan.AckJoinPending)

   			postCtx := n.postElectionCtx.Load()
   			// If we are not dealing with an election, we send the current leader
   			if n.getElectionState() == election_definitions.Idle && postCtx != nil {
   				n.logf("election", "Sending %d information about last election: %v", sourceId, postCtx)

   				n.logf("tree", "Since I have a new neighbor, i go back to being active to wait for its approval")
   				n.treeMan.SwitchToState(topology.TreeActive)
   				n.SendCurrentLeader(sourceId)
   				return nil
   			}
   			return nil
   		}
   		return nil
   	}

   	func (n *ZlusterNode) Start() {
   		n.logf("main", "Node booting up... I have %d buffered messages to send", len(n.outputChannel))

   		go n.RunInputDispatcher()
   		go n.RunOutputDispatcher()
   		go n.JoinHandle()
   		go n.HeartbeatHandle()
   		go n.ElectionHandle()
   		go n.TreeHandle()
   		go n.InputLogHandle()
   		go n.DataPlaneLogHandle()

   		defer n.logf("main", "Node's goroutine started correctly")
   	}

   	func (n *ZlusterNode) BootstrapDiscovery(bootstrapAddr string) error {
   		conn, err := grpc.NewClient(bootstrapAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
   		if err != nil {
   			return err
   		}
   		defer conn.Close()

   		client := bootstrap_protocol.NewBootstrapServiceClient(conn)

   		ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
   		defer cancel()

   		res, err := client.Register(ctx, &bootstrap_protocol.RegisterRequest{
   			Id:      uint64(n.getId()),
   			Address: getOutboundIP(),
   			Port:    uint32(n.getControlPort()),
   		})
   		if err != nil {
   			return err
   		}
   		if res.Success {
   			n.treeMan.Reset()
   			n.treeMan.SwitchToState(topology.TreeIdle)

   			addresses := make(map[node.NodeId]node.Address, 0)
   			for k, v := range res.Neighbors {
   				addresses[node.NodeId(k)] = node.Address{Host: v.Host, Port: uint16(v.Port)}
   			}

   			n.logf("main", "Neighbors recovered: %v", res.Neighbors)
   			for id, nodeInfo := range addresses {
   				n.AddNeighbor(node.NodeId(id), node.NewAddress(nodeInfo.Host, uint16(nodeInfo.Port)))
   			}
   		}
   		return nil
   	}

   	func (n *ZlusterNode) startElection() {
   		n.logf("election", "Preparing election context...")
   		electionID := n.ElectionSetup()
   		n.logf("election", "Setup the election context")

   		for _, neighbor := range n.neighborList() {
   			if n.isAlive(neighbor) {
   				n.SendMessageTo(neighbor, n.NewStartMessage(neighbor, electionID))
   				n.logf("election", "Sent START message to %d", neighbor)
   			} else {
   				n.logf("election", "NOT Sent START message to %d, Skipping", neighbor)
   			}
   		}

   		n.logf("election", "Sent START to all my ON neighbors, waiting confirm or earlier election proposal")
   		n.setElectionStartReceived()
   		n.enterYoDown()
   	}

   // FSM that handles the election process.
   // This function is supposed to be run as a goroutine, after the node's creation.
   // It starts in Idle state and waits for a message, then processes it based on the state.
   func (n *ZlusterNode) ElectionHandle() {

   		n.logf("election", "Started election handle")
   		defer n.logf("election", "FATAL: Election handle has failed...")

   		n.switchToElectionState(election_definitions.Idle)
   		time.Sleep(3 * time.Second)

   		timer := time.NewTimer(15 * time.Second)
   		defer timer.Stop()

   		for {
   			timer.Reset(15 * time.Second)
   			n.logf("election", "Awaiting election message...")
   			state := n.getElectionState()

   			select {
   			case <-n.ctx.Done():
   				n.logf("main", "Election: Stop signal received")
   				n.logf("election", "Stop signal received")
   				return

   			case message := <-n.electionInbox:

   				senderId, _ := extractIdentifier(message.GetHeader().Sender)
   				destId, _ := extractIdentifier(message.GetHeader().Destination)
   				if n.getId() != destId {
   					n.logf("election", "I Received someone else's message? (%d): %v", destId, message.String())
   					continue
   				}
   				if !n.IsNeighborsWith(senderId) {
   					n.logf("election", "Election message received by a STRANGER (%d): %s", senderId, message.String())
   					continue
   				}

   				n.logf("election", "Election message received: %s. Current state{%v}", message.String(), state.String())

   				switch state {

   				case election_definitions.Idle:
   					n.handleIdleState(message)

   				case election_definitions.WaitingYoDown:
   					n.handleWaitingYoDown(message)

   				case election_definitions.WaitingYoUp:
   					n.handleWaitingYoUp(message)
   				}

   			case <-timer.C:
   				n.logf("election", "Timeout occurred... Current state is %v", state.String())

   				switch state {
   				case election_definitions.WaitingYoDown:
   					n.handleYoDownTimeout()
   				case election_definitions.WaitingYoUp:
   					n.handleYoUpTimeout()
   				case election_definitions.Idle:
   					n.handleIdleTimeout()
   				}
   			}
   		}
   	}

   func (n *ZlusterNode) handleYoDownTimeout() {

   		for _, node := range n.electionCtx.InNodes() {
   			_, err := n.electionCtx.RetrieveProposal(node)
   			if err != nil { // Not proposed, i could add a map [node]counter. Counting (or not counting ..ADFBHS) the timeouts, if you dont vote but you already game me 50 heartbeats... dude wake up
   				if !n.isAlive(node) {
   					n.logf("election", "InNode %d is OFF. Restarting", node)
   					n.startElection()
   					return
   				}
   				n.electionCtx.IncreaseTimeout(node)
   				faulty, _ := n.electionCtx.IsFaulty(node)
   				if faulty {
   					n.logf("election", "%d is ON, but won't give election messages... sending JOIN to wake him up", node)
   					n.startElection()
   					return
   				}
   			}
   		}
   	}

   func (n *ZlusterNode) handleYoUpTimeout() {

   		for _, node := range n.electionCtx.OutNodes() {
   			_, err := n.electionCtx.RetrieveVote(node)
   			if err != nil {
   				if !n.isAlive(node) {
   					n.logf("election", "InNode %d is OFF. Restarting", node)
   					n.startElection()
   					return
   				}
   				n.electionCtx.IncreaseTimeout(node)
   				faulty, _ := n.electionCtx.IsFaulty(node)
   				if faulty {
   					n.logf("election", "%d is ON, but won't give election messages... sending JOIN to wake him up", node)
   					n.startElection()
   					return
   				}
   			}
   		}
   	}

   func (n *ZlusterNode) handleIdleTimeout() {

   		postCtx := n.postElectionCtx.Load()
   		if postCtx != nil {
   			leaderId := postCtx.GetLeader()
   			if n.getId() != leaderId { // Check leader status only if not leader
   				n.logf("heartbeat", "Am i neighbors with %d? %v", leaderId, n.IsNeighborsWith(leaderId))
   				if n.IsNeighborsWith(leaderId) && !n.isAlive(leaderId) {

   					postCtx := n.postElectionCtx.Load().Clone()
   					leaderTimeouts := postCtx.IncreaseLeaderTimeouts()
   					switch {
   					case leaderTimeouts < election.RejoinLeaderTimeout:
   						n.logf("election", "Leader has not responded for %d time now. %d more and i try to ReJoin", leaderTimeouts, election.RejoinLeaderTimeout-leaderTimeouts)
   					case leaderTimeouts == election.RejoinLeaderTimeout:
   						n.logf("election", "Leader shutoff? Probably forgot about me. Try probing with REJOIN")
   						n.SendMessageTo(leaderId, n.newReJoinMessage(leaderId))
   						n.topologyMan.SetReAckJoinPending(leaderId)
   					case leaderTimeouts > election.RejoinLeaderTimeout && leaderTimeouts < election.StartoverLeaderTimeout:
   						n.logf("election", "Rejoin didn't work. In %d we start over...", election.StartoverLeaderTimeout-leaderTimeouts)
   					default:
   						n.logf("election", "Leader is considered OFF. New election MUST start")
   						postCtx.ResetLeaderTimeouts()
   						n.postElectionCtx.Store(postCtx)
   						n.startElection()
   					}
   				}
   			}

   			switch n.treeMan.GetState() {
   			case topology.TreeDone: // Tree was all set, monitoring neighbors
   				parent, children, hasParent := n.GetTreeNeighbors()

   				// What about children?
   				for child := range children {
   					if !n.isAlive(child) {
   						childTimeouts := n.treeMan.IncreaseTimeout(child)
   						switch {
   						case childTimeouts < topology.TolerableTreeNeighborTimeouts:
   							n.logf("tree", "Child(%d) has been OFF for some time (%d timeouts)", child, childTimeouts)
   						case childTimeouts == topology.TolerableTreeNeighborTimeouts:
   							n.logf("tree", "Child(%d) has been OFF for some time (%d timeouts). Removing", child, childTimeouts)
   							n.RemoveTreeNeighbor(child) // Or better child death handling
   						}
   					}
   				}

   				if hasParent { // Non root, the only one to handle parent's deaths
   					if !n.isAlive(parent) && n.treeMan.HasParent() {
   						parentTimeouts := n.treeMan.IncreaseTimeout(parent)
   						switch {
   						case parentTimeouts <= topology.TolerableTreeNeighborTimeouts:
   							n.logf("tree", "Parent(%d) has been OFF for some time (%d timeouts)", parent, parentTimeouts)
   						case parentTimeouts > topology.TolerableTreeNeighborTimeouts:
   							n.logf("tree", "Parent(%d) has been OFF for some time (%d timeouts). Removing parent", parent, parentTimeouts)
   							n.RemoveTreeNeighbor(parent)
   							n.RemoveTreeParent()

   							neighbors := n.neighborList()
   							for _, neighbor := range neighbors {
   								if _, ok := children[neighbor]; ok || !n.isAlive(neighbor) {
   									continue
   								}
   								n.logf("tree", "Asking %d if he is still attached to the Tree", neighbor)
   								n.SendMessageTo(neighbor, n.newTreeHelpReq(neighbor))
   							}
   						}
   					}
   				}

   			case topology.TreeActive:
   				neighbors := n.neighborList()
   				for _, neighbor := range neighbors {
   					if !n.treeMan.HasAnswered(neighbor) && !n.isAlive(neighbor) {
   						childTimeouts := n.treeMan.IncreaseTimeout(neighbor)
   						n.logf("tree", "Neighbor(%d) did not answer the SHOUT for (%d) timeouts and is off...", neighbor, childTimeouts)
   						if childTimeouts == 5 {
   							n.logf("tree", "Neighbor(%d) did not answer the SHOUT and has been off for too much time. Considering NO", neighbor)
   							n.treeMan.AcknowledgeNo(neighbor)
   							if n.treeMan.GetCounter() == len(neighbors) {
   								n.becomeTreeDone()
   							}
   						}
   					}
   				}
   			}
   		} else { // postCtx is nil, we are idle, so no election has been performed yet
   			onNeighbors := 0
   			for _, node := range n.topologyMan.NeighborList() {
   				if n.isAlive(node) {
   					onNeighbors++
   				}
   			}
   			if onNeighbors > 0 {
   				return
   			}
   			n.electionCtx.IncreaseTimeout(n.getId()) // My own timeouts, to understand how many times i had a timeout by myself
   			if t, _ := n.electionCtx.GetTimeout(n.getId()); t == 5 {
   				n.electionCtx.ResetTimeouts(n.getId())
   				n.startElection()
   			}
   		}
   	}

   	func (n *ZlusterNode) newTreeMessage(destId node.NodeId, epoch uint64, flags protocol.TreeFlags, body []string) *protocol.TreeMessage {
   		return protocol.NewTreeMessage(
   			n.NewMessageHeader(destId, protocol.Tree),
   			epoch,
   			flags,
   			body,
   		)
   	}

   	func (n *ZlusterNode) newTreeInputNextHopMessage(tParent node.NodeId, inputNode node.NodeId) *protocol.TreeMessage {
   		return n.newTreeMessage(
   			tParent,
   			n.treeMan.GetEpoch(),
   			protocol.TFLags_INPUTNEXTHOP,
   			[]string{
   				strconv.FormatUint(uint64(inputNode), 10), // The ID of the input node
   			},
   		)
   	}

   	func (n *ZlusterNode) newTreeParentPortMessage(tChild node.NodeId, dataPort uint16) *protocol.TreeMessage {
   		return n.newTreeMessage(
   			tChild,
   			n.treeMan.GetEpoch(),
   			protocol.TFLags_PARENTPORT,
   			[]string{
   				strconv.FormatUint(uint64(dataPort), 10), // Port the parent (n) uses on the data plane
   			},
   		)
   	}

   	func (n *ZlusterNode) newTreeEpochMessage(tChild node.NodeId, epoch uint64) *protocol.TreeMessage {
   		return n.newTreeMessage(tChild, epoch, protocol.TFlags_EPOCH, []string{})
   	}

   	func (n *ZlusterNode) newTreeHelpReq(nonTreeNeighbor node.NodeId) *protocol.TreeMessage {
   		return n.newTreeMessage(nonTreeNeighbor, n.treeMan.GetEpoch(), protocol.TFlags_NOPARENTREQ, []string{})
   	}

   	func (n *ZlusterNode) newTreeHelpRep(nonTreeNeighbor node.NodeId, flags protocol.TreeFlags) *protocol.TreeMessage {
   		return n.newTreeMessage(
   			nonTreeNeighbor,
   			n.treeMan.GetEpoch(),
   			flags,
   			[]string{strconv.FormatUint(n.treeMan.GetRootHopCount(), 10)},
   		)
   	}

   	func (n *ZlusterNode) newTreeHelpRepPositive(nonTreeNeighbor node.NodeId) *protocol.TreeMessage {
   		return n.newTreeHelpRep(nonTreeNeighbor, protocol.TFlags_NOPARENTREP|protocol.TFlags_Q)
   	}

   	func (n *ZlusterNode) newTreeHelpRepNegative(nonTreeNeighbor node.NodeId) *protocol.TreeMessage {
   		return n.newTreeHelpRep(nonTreeNeighbor, protocol.TFlags_NOPARENTREP)
   	}

   	func (n *ZlusterNode) TreeHandle() {
   		n.logf("tree", "Started tree handle")
   		defer n.logf("tree", "FATAL: Tree handle has failed...")
   		time.Sleep(3 * time.Second)

   		for {
   			n.logf("tree", "Awaiting tree message...")
   			select {
   			case <-n.ctx.Done():
   				n.logf("main", "Tree: Stop signal received")
   				n.logf("tree", "Stop signal received")
   				return
   			case message := <-n.treeInbox:

   				sender, _ := extractIdentifier(message.Header.Sender)
   				n.logf("tree", "Message from %d received: %s", sender, message.String())

   				// Propagate epoch amongst children
   				flags := message.Flags

   				parent, children, hasParent := n.treeMan.GetTreeNeighbors()

   				switch flags {
   				case protocol.TFlags_EPOCH:
   					if hasParent && parent == sender {
   						n.treeMan.SetEpoch(message.Epoch)
   						for child := range children {
   							n.logf("tree", "Forwarded message to (%d)", child)
   							n.SendMessageTo(child, n.CopyMessage(child, message))
   						}
   					}

   				case protocol.TFlags_NOPARENTREQ: // Someone asked me if I'm still attached to the tree
   					if n.treeMan.HasParent() { // I could help
   						if message.Epoch >= n.treeMan.GetEpoch() { // I am the one behind, can't help
   							n.SendMessageTo(sender, n.newTreeHelpRepNegative(sender))
   						} else {
   							n.treeMan.RemoveTreeNeighbor(sender)
   							n.treeMan.SwitchToState(topology.TreeActive)
   							n.SendMessageTo(sender, n.newTreeHelpRepPositive(sender)) // True also sends Q
   						}
   					} else { // I can't help
   						n.SendMessageTo(sender, n.newTreeHelpRepNegative(sender))
   					}
   				case protocol.TFlags_NOPARENTREP | protocol.TFlags_Q: // Asked me to be his child, accept instantly
   					if !n.treeMan.HasParent() { // I still don't have a parent
   						n.logf("tree", "%d said he can help! Setting him as my parent. Sending YES", sender)
   						rootHops, _ := strconv.ParseUint(message.Body[0], 10, 64)
   						n.treeMan.SetParent(sender, rootHops+1)
   						n.treeMan.SetEpoch(message.Epoch)
   						n.SendNoTreeParentQAnswer(sender, true)
   						_, children, _ := n.treeMan.GetTreeNeighbors()
   						for child := range children {
   							n.logf("tree", "Forwarding new epoch (%d) to %d", message.Epoch, child)
   							n.SendMessageTo(child, n.newTreeEpochMessage(child, message.Epoch))
   						}
   					} else { // I already have a parent now
   						n.logf("tree", "%d said he can help! However he is late, I already have a new parent. Sending NO", sender)
   						n.SendNoTreeParentQAnswer(sender, false)
   					}
   				case protocol.TFlags_NOPARENTREP: // Denied
   					n.logf("tree", "%d said he can't help... ", sender)
   				case protocol.TFlags_A:
   					if n.treeMan.GetState() == topology.TreeActive {
   						answer, _ := strconv.ParseBool(message.Body[0])
   						if answer {
   							n.treeMan.AddTreeNeighbor(sender)
   							n.logf("tree", "%d Responded with YES, he is my new neighbor", sender)
   						} else {
   							n.treeMan.AcknowledgeNo(sender)
   							n.logf("tree", "%d Responded with No, he is not my neighbor", sender)
   						}
   						if n.treeMan.GetCounter() == len(n.topologyMan.NeighborList()) {
   							n.becomeTreeDone()
   						}
   					} else {
   						n.logf("tree", "Shouldn't have gotten this")
   					}
   				case protocol.TFLags_INPUTNEXTHOP:

   					inputNode, err := strconv.ParseUint(message.Body[0], 10, 64)
   					if err != nil {
   						continue
   					}

   					postCtx := n.postElectionCtx.Load().Clone()
   					postCtx.SetInputNextHop(node.NodeId(inputNode), sender)
   					n.logf("tree", "(%d) told me that (%d) is reachable through itself", sender, inputNode)

   					n.logf("tree", "My current map of input nodes{%v}", postCtx.GetInputMap())
   					if hasParent {
   						n.logf("tree", "Forwarding (%d) next input hvop to parent (%d)", inputNode, parent)
   						n.SendMessageTo(parent, n.newTreeInputNextHopMessage(parent, node.NodeId(inputNode)))
   					}
   					n.postElectionCtx.Store(postCtx)
   				case protocol.TFLags_PARENTPORT:
   					parentPort, err := strconv.ParseUint(message.Body[0], 10, 64)
   					if err != nil {
   						continue
   					}

   					n.logf("tree", "Parent (%d) said he listens on port (%d) for data plane", sender, parentPort)
   					postCtx := n.postElectionCtx.Load().Clone()
   					postCtx.SetUpstreamPort(uint16(parentPort))
   					n.DataPlaneManager.SetRouteFinder(postCtx)
   					n.postElectionCtx.Store(postCtx)

   					dataPort := n.getDataPort()
   					for child := range children {
   						n.logf("tree", "Sending my data plane port (%d) to (%d)", dataPort, child)
   						n.SendMessageTo(child, n.newTreeParentPortMessage(child, dataPort))
   					}

   				case protocol.TFlags_Q:
   					n.logf("tree", "I lost hope")
   				default:
   					n.logf("tree", "I lost hope")
   				}
   			}
   		}
   	}

   	func (n *ZlusterNode) SendNoTreeParentQAnswer(neighbor node.NodeId, answer bool) {
   		h := n.NewMessageHeader(neighbor, protocol.Tree)
   		msg := protocol.NewTreeMessage(
   			h,
   			n.treeMan.GetEpoch(),
   			protocol.TFlags_A,
   			[]string{
   				strconv.FormatBool(answer),
   			},
   		)
   		n.outputChannel <- protocol.OutMessage{DestId: neighbor, Message: msg}
   	}

   func (n *ZlusterNode) handleIdleState(message *election_definitions.ElectionMessage) {

   		switch message.MessageType {

   		case election_definitions.Start: // A node sent START because a neighbor received JOIN or START

   			postCtx := n.postElectionCtx.Load()
   			if postCtx != nil {

   				electionId := postCtx.GetElectionId()
   				receivedId := election_definitions.ElectionId(message.Body[0]) // A start message has the electionId proposal in the body

   				cmp := electionId.Compare(receivedId)

   				if cmp == 0 {
   					senderId, _ := extractIdentifier(message.Header.Sender)
   					n.treeMan.SwitchToState(topology.TreeActive) // forse
   					n.SendCurrentLeader(senderId)
   					return
   				}
   				if cmp > 0 {
   					n.logf("election", "The start message is for an older, or this, election, ignoring")
   					return
   				}

   			}
   			// Either the first election or a newer and stronger one, follow it
   			n.handleStartMessage(message)

   		case election_definitions.Leader:
   			postCtx := n.postElectionCtx.Load()
   			if postCtx != nil {
   				electionId := postCtx.GetElectionId()
   				receivedId := message.ElectionId

   				cmp := electionId.Compare(receivedId)
   				if cmp > 0 {
   					n.logf("election", "The leader message is for an older, or this, election, ignoring")
   					return
   				}
   			}
   			// Either the first election of a newer one, forwarding it
   			n.handleLeaderMessage(message)

   		case election_definitions.Proposal, election_definitions.Vote:
   			postCtx := n.postElectionCtx.Load()
   			if postCtx != nil {
   				cmp := postCtx.GetElectionId().Compare(message.ElectionId)
   				if cmp > 0 {
   					n.logf("election", "Received a proposal/vote for an older election, ignoring")
   					return
   				}
   				if cmp == 0 {
   					senderId, _ := extractIdentifier(message.Header.Sender)
   					n.treeMan.SwitchToState(topology.TreeActive) // forse
   					n.SendCurrentLeader(senderId)
   					return
   				}
   			}
   			// Either the first election or someone sent me a proposal for the current/newer election, try  to start another
   			cmp := message.ElectionId.Compare(n.electionCtx.GetId())
   			if cmp >= 0 {
   				if cmp > 0 && !n.hasReceivedElectionStart() {
   					n.ElectionSetupWithID(message.ElectionId)
   					n.handleWaitingYoDown(message)
   					n.enterYoDown()
   				}
   				if message.MessageType == election_definitions.Proposal {
   					n.handleWaitingYoDown(message)
   				} else {
   					n.handleWaitingYoUp(message)
   				}
   			}
   			//n.startElection()
   		}
   	}

   func (n *ZlusterNode) handleWaitingYoDown(message *election_definitions.ElectionMessage) {

   		h := message.GetHeader()
   		switch message.MessageType {
   		case election_definitions.Proposal:
   			sender, _ := extractIdentifier(h.Sender)
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
   					n.SendMessageTo(neighbor, n.NewStartMessage(neighbor, receivedElectionId))
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
   					n.SendMessageTo(outNode, n.newProposalMessage(outNode, smallestId))
   					n.logf("election", "Forwarding proposal to %d", outNode)
   				}

   			case election_definitions.Sink:
   				n.electionCtx.StoreProposal(sender, node.NodeId(proposed))
   				n.logf("election", "Stored the proposal of %d by %d, waiting for %d more votes...", proposed, sender, n.electionCtx.GetAwaitedProposals())

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

   	func (n *ZlusterNode) handleWaitingYoUp(message *election_definitions.ElectionMessage) {
   		h := message.GetHeader()

   		switch message.MessageType {

   		case election_definitions.Vote:
   			sender, _ := extractIdentifier(h.Sender)
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
   					n.SendMessageTo(neighbor, n.NewStartMessage(neighbor, receivedElectionId))
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
   				n.logf("election", "Stored the vote of %v by %d, waiting for %d more vote...  Pruning asked: %v", vote, sender, n.electionCtx.GetAwaitedVotes(), pruneChild)
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
   				n.SendMessageTo(inNodes[0], n.newVoteMessage(inNodes[0], vote && voteValidator, false))
   				n.logf("election", "Sent vote message to %d", inNodes[0])
   				for _, inNode := range inNodes[1:] {
   					voteValidator, _ = n.electionCtx.DetermineVote(inNode) // Did this parent propose the winning ID?
   					n.SendMessageTo(inNode, n.newVoteMessage(inNode, vote && voteValidator, pruneMe))
   					n.logf("election", "Sent vote message to %d", inNode)
   					if pruneMe {
   						n.electionCtx.PruneThisRound(inNode)
   					}
   				}
   				n.nextElectionRound()
   				n.enterYoDown()

   			case election_definitions.Source:
   				n.logf("election", "Stored the vote of %v by %d, waiting for %d more votes...  Pruning asked: %v", vote, sender, n.electionCtx.GetAwaitedVotes(), pruneChild)
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

   		case election_definitions.Leader:
   			n.handleLeaderMessage(message)
   		default:
   			n.logf("election", "Received %v type during WaitingYoUp State, ignoring?", message.MessageType)
   		}
   	}

   func (n *ZlusterNode) enterYoDown() {

   		n.logf("election", "Yo down started for round %d", n.currentElectionRound())
   		switch n.getElectionStatus() {

   		case election_definitions.Winner:
   			n.logf("election", "I won the election, sending my ID to others")
   			n.startShout()
   			n.switchToElectionState(election_definitions.Idle)
   		case election_definitions.Loser:
   			n.logf("election", "I lost the election")
   			n.switchToElectionState(election_definitions.Idle)

   		case election_definitions.Source:
   			n.logf("election", "Source: Sending my id to out nodes...")
   			for _, outNode := range n.electionCtx.OutNodes() {
   				n.SendMessageTo(outNode, n.newProposalMessage(outNode, n.getId()))
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

   func (n *ZlusterNode) enterYoUp() {

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
   				n.SendMessageTo(inNode, n.newVoteMessage(inNode, vote, pruneMe))
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
   				n.SendMessageTo(inNodes[0], n.newVoteMessage(inNodes[0], vote, false))
   				n.logf("election", "Sent vote %v to %d", vote, inNodes[0])

   				for _, inNode := range inNodes[1:] {
   					vote, _ := n.electionCtx.DetermineVote(inNode)
   					n.SendMessageTo(inNode, n.newVoteMessage(inNode, vote, pruneMe))
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

   	func (n *ZlusterNode) ElectionSetup() election_definitions.ElectionId {
   		id := n.generateElectionId()
   		n.ElectionSetupWithID(id)
   		return id
   	}

   	func (n *ZlusterNode) ElectionSetupWithID(id election_definitions.ElectionId) {
   		n.logf("election", "Resetting the election context...")
   		n.electionCtx.Reset(n.IncrementClock())
   		n.setElectionId(id)

   		n.treeMan.Reset() // After this election we need to build a new spanning tree

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

   	func (n *ZlusterNode) SendCurrentLeader(neighbor node.NodeId) {
   		postCtx := n.postElectionCtx.Load()
   		electionMsg := n.newLeaderAnnouncement(neighbor, postCtx.GetLeader(), postCtx.GetElectionId(), n.treeMan.GetRootHopCount()).(*election_definitions.ElectionMessage)
   		if electionMsg.ElectionId == election_definitions.InvalidId {
   			log.Fatal("The electionId in post election context is invalid")
   		}
   		n.SendMessageTo(neighbor, electionMsg)
   	}

   	func (n *ZlusterNode) handleStartMessage(message *election_definitions.ElectionMessage) error {
   		h := message.GetHeader()
   		sender, _ := extractIdentifier(h.Sender)
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
   			n.SendMessageTo(neighbor, n.NewStartMessage(neighbor, electionId))
   			n.logf("election", "Sent START message to %d", neighbor)
   		}
   		n.enterYoDown()
   		return nil
   	}

   func (n *ZlusterNode) handleLeaderMessage(message *election_definitions.ElectionMessage) error {

   		postCtx := n.postElectionCtx.Load()

   		if postCtx != nil {
   			if message.ElectionId.Compare(postCtx.GetElectionId()) < 0 {
   				n.logf("election", "Someone, %s, sent me a LEADER message for an older election: %s", message.Header.Sender, message.ElectionId)
   				return nil
   			}
   		}

   		currentId := n.electionCtx.GetId()
   		receivedId := message.ElectionId
   		sender, err := extractIdentifier(message.GetHeader().Sender)
   		if err != nil {
   			return err
   		}

   		cmp := currentId.Compare(receivedId)
   		switch {
   		case cmp > 0:
   			n.logf("election", "Received a leader message, by %d, for an older election (mine: %s, received: %s) Ignoring.", sender, currentId, receivedId)
   			return nil
   		case cmp == 0:
   			n.logf("tree", "%d My state is %v.", sender, n.treeMan.GetState().String())

   			switch n.treeMan.GetState() {
   			case topology.TreeActive:
   				flag, err := strconv.ParseUint(message.Body[1], 10, 64)
   				if err != nil {
   					return err
   				}

   				if flag == uint64(LFlags_TREEQUESTION) { // Q
   					n.logf("tree", "Received Q leader message from %d. Sending NO", sender)
   					leaderId, err := strconv.ParseUint(message.Body[0], 10, 64)
   					if err != nil {
   						return err
   					}
   					n.SendMessageTo(sender, n.newLeaderResponse(sender, node.NodeId(leaderId), message.ElectionId, false))
   				} else if flag == uint64(LFlags_TREEANSWER) {
   					yes, err := strconv.ParseBool(message.Body[2])
   					if err != nil {
   						return err
   					}
   					neighborLen := n.topologyMan.Length()
   					if yes {
   						n.treeMan.AddTreeNeighbor(sender)
   						n.logf("tree", "Received YES from %d, adding to tree neighbors. Current counter{%d}", sender, n.treeMan.GetCounter())
   					} else {
   						n.treeMan.AcknowledgeNo(sender)
   						n.logf("tree", "Received NO from %d, just increasing counter {%d}", sender, n.treeMan.GetCounter())
   					}
   					n.logf("tree", "Awaiting %d responses... %v", (n.treeMan.GetCounter())-neighborLen, n.neighborList())
   					if n.treeMan.GetCounter() == neighborLen {
   						n.becomeTreeDone()
   					}
   				} else { // Y/N
   					return fmt.Errorf("Unkwown flag set in %v", message)
   				}
   			case topology.TreeDone:
   				flag, err := strconv.ParseUint(message.Body[1], 10, 64)
   				if err != nil {
   					return err
   				}
   				if flag == uint64(LFlags_TREEQUESTION) { // Q
   					n.logf("tree", "Received Q leader message while DONE from %d. My tree is already set, sending NO", sender)
   					leaderId, err := strconv.ParseUint(message.Body[0], 10, 64)
   					if err != nil {
   						return err
   					}
   					n.SendMessageTo(sender, n.newLeaderResponse(sender, node.NodeId(leaderId), message.ElectionId, false))
   				}
   			}
   			if n.electionCtx.HasReceivedLeader() {
   				n.logf("election", "Received a leader message, by %d, for the current election (mine: %s, received: %s) Confirming.", sender, currentId, receivedId)
   				return nil
   			}
   		case cmp < 0:
   			n.logf("election", "Received a leader message, by %d, for a newer election (mine: %s, received: %s) Accepting this one.", sender, currentId, receivedId)
   		}

   		leaderId, err := strconv.ParseUint(message.Body[0], 10, 64)
   		if err != nil {
   			return err
   		}

   		n.endElection(node.NodeId(leaderId), message.ElectionId)
   		rootHopCount, _ := strconv.ParseUint(message.Body[2], 10, 64)

   		n.logf("election", "%d said it was leader. closing my election context... with %v", leaderId, n.postElectionCtx.Load())

   		n.logf("tree", "Setting up spanning tree process, im IDLE and received a message")
   		n.treeMan.SetRoot(false)
   		n.treeMan.SetParent(sender, rootHopCount+1)
   		n.treeMan.AddTreeNeighbor(sender)
   		parent, children, hasParent := n.treeMan.GetTreeNeighbors()
   		n.logf("tree", "Root{%v}, Parent{sender:%v}, TreeChildren{%v}", !hasParent, parent, children)

   		n.SendMessageTo(sender, n.newLeaderResponse(sender, node.NodeId(leaderId), message.ElectionId, true))
   		counter := n.treeMan.GetCounter()

   		n.logf("tree", "Sent yes to %d. Counter = %d", sender, counter)
   		if counter == n.topologyMan.Length() { // if 1 neighbor
   			n.becomeTreeDone()
   		} else {
   			n.logf("tree", "Other neighbors, sending Q's")
   			for _, neighbor := range n.neighborList() {
   				if neighbor == sender { // || neighbor == node.NodeId(leaderId) {
   					continue
   				}

   				if n.isAlive(neighbor) {

   					msg := n.newLeaderAnnouncement(neighbor, node.NodeId(leaderId), message.ElectionId, n.treeMan.GetRootHopCount())
   					n.SendMessageTo(neighbor, msg)

   					n.logf("tree", "Sent Q to %d", neighbor)
   					n.logf("election", "Sent new Leader (%d) message to %d. %v", leaderId, neighbor, msg)
   				} else {

   					n.logf("tree", "NOT Sent Q to %d. Skipping offline", neighbor)
   					n.logf("election", "NOT Sent new Leader (%d) message to %d. Skipping offline", leaderId, neighbor)
   				}
   			}
   			n.logf("tree", "Awaiting responses... Becoming ACTIVE")
   			n.treeMan.SwitchToState(topology.TreeActive)
   		}

   		return nil
   	}

   	func (n *ZlusterNode) becomeTreeDone() {
   		n.treeMan.SwitchToState(topology.TreeDone)
   		roleFlags := n.updareTreeRole()

   		n.logf("tree", "No more neighbors to await, becoming DONE. My roles: %v", roleFlags.String())

   		postCtx := n.postElectionCtx.Load().Clone()
   		postCtx.SetRoles(roleFlags)
   		n.postElectionCtx.Store(postCtx)

   		n.setupRolesContext()
   	}

   	func (n *ZlusterNode) setupRolesContext() error {
   		postCtx := n.postElectionCtx.Load()
   		if postCtx == nil {
   			return fmt.Errorf("No election has been hold yet")
   		}

   		roleFlags := postCtx.GetRoles()

   		if makeBind := !n.treeMan.IsLeaf(); makeBind {
   			n.DataPlaneManager.BindPort(n.getDataPort())
   		}

   		n.DataPlaneManager.SetRouteFinder(postCtx)

   		//canWrite := false
   		if roleFlags&election.RoleFlags_LEADER > 0 {
   			//canWrite = true
   			go n.startTreeEpochCounter()
   			_, children, _ := n.GetTreeNeighbors()

   			n.logf("tree", "Sending all of my children my port for the data plane...")
   			for child := range children {
   				n.logf("tree", "Sending (%d) the port (%d)", child, n.getDataPort())
   				n.SendMessageTo(child, n.newTreeParentPortMessage(child, n.getDataPort()))
   			}
   		}

   		if roleFlags&election.RoleFlags_PERSISTENCE > 0 { // Start persistence
   		}

   		if roleFlags&election.RoleFlags_INPUT > 0 { // Start input
   			if parent, hasParent := n.GetTreeParent(); hasParent {
   				n.logf("tree", "Telling my parent (%d) I am input node", parent)
   				n.SendMessageTo(parent, n.newTreeInputNextHopMessage(parent, n.getId()))
   			}
   			n.logf("input", "Starting input service...")
   			n.startNewInputService()
   		}

   		return nil
   	}

   	func (n *ZlusterNode) startNewInputService() {
   		n.inputMan = input.NewInputManager(n.ctx, n.inputInbox)
   		//n.inputMan.SetAuthService(service.NewAuthService(nil, &SUPER{}, n.inputInbox))
   		go n.inputMan.Start()
   	}

   	func (n *ZlusterNode) InputLogHandle() {
   		for {
   			select {
   			case <-n.ctx.Done():
   				n.logf("main", "INPUT Stop signal received...")
   				n.logf("input", "Stop signal received...")
   			case message := <-n.inputInbox:
   				n.logf("input", "%s", message)
   			}
   		}
   	}

   	func (n *ZlusterNode) DataPlaneLogHandle() {
   		for {
   			select {
   			case <-n.ctx.Done():
   				n.logf("main", "DATA Stop signal received...")
   				n.logf("data", "Stop signal received...")
   			case message := <-n.dataPlaneInbox:
   				n.logf("data", "%s", message)
   			}
   		}
   	}

   func (n *ZlusterNode) startTreeEpochCounter() {

   		n.logf("tree", "Started tree epoch counter. Initial value {0}")
   		var counter uint64 = 0

   		ticker := time.NewTicker(2 * time.Second)

   		_, children, _ := n.treeMan.GetTreeNeighbors()
   		for {
   			select {
   			case <-n.ctx.Done():
   				n.logf("tree", "Stop signal received, last value {%d}", counter)
   			case <-ticker.C:
   				for child := range children {
   					n.SendMessageTo(child, n.newTreeEpochMessage(child, counter))
   					n.logf("tree", "Sending current counter{%d} to %d", counter, child)
   				}
   				counter++
   			}
   		}
   	}

   	func (n *ZlusterNode) updareTreeRole() election.NodeRoleFlags {
   		var roleFlags election.NodeRoleFlags = election.RoleFlags_NONE

   		treeParent, treeChildren, hasTreeParent := n.GetTreeNeighbors()
   		if !hasTreeParent { // I'm root
   			roleFlags |= election.RoleFlags_LEADER | election.RoleFlags_PERSISTENCE
   			if len(treeChildren) == 0 {
   				roleFlags |= election.RoleFlags_INPUT
   			}
   		} else {
   			if len(treeChildren) == 0 { // Only 1 tree neighbor (sender) => Leaf => Input
   				roleFlags |= election.RoleFlags_INPUT
   			}
   			if treeParent == n.postElectionCtx.Load().GetLeader() { // MaxHops from leader: 1 => Storage
   				roleFlags |= election.RoleFlags_PERSISTENCE
   			}
   		}
   		return roleFlags
   	}

   	func (n *ZlusterNode) resettreeMan() {
   		n.treeMan.Reset()
   	}

   func (n *ZlusterNode) startShout() {

   		n.logf("tree", "Setting up spanning tree process, im INITIATOR")
   		n.treeMan.SetRoot(true)
   		n.logf("tree", "Root{true}, Parent{none}, TreeNeighbors{<empty>}")

   		electionId := n.electionCtx.GetId()
   		onNeighbors := 0
   		for _, neighbor := range n.neighborList() {

   			if n.isAlive(neighbor) {
   				onNeighbors++
   				n.SendMessageTo(neighbor, n.newLeaderAnnouncement(neighbor, n.getId(), electionId, 0))
   				n.logf("election", "Sent leader message to %d", neighbor)
   				n.logf("tree", "Sent Q message to %d", neighbor)
   			} else {
   				n.logf("election", "NOT sent leader message to %d", neighbor)
   				n.logf("tree", "NOT sent Q message to %d", neighbor)
   			}
   		}
   		n.endElection(n.getId(), electionId)

   		if onNeighbors > 0 {
   			n.logf("tree", "Awaiting responses... becoming ACTIVE")
   			n.treeMan.SwitchToState(topology.TreeActive)
   		} else {
   			n.logf("tree", "I have no ON neighbors... I am alone")
   			n.becomeTreeDone()
   		}
   	}

   	func (n *ZlusterNode) NewMessageHeader(neighbor node.NodeId, mType protocol.MessageType) *protocol.MessageHeader {
   		return protocol.NewMessageHeader(
   			n.connectionIdentifier(),
   			neighbor.Identifier(),
   			mType,
   		)
   	}

   	func (n *ZlusterNode) NewStartMessage(neighbor node.NodeId, proposedElectionId election_definitions.ElectionId) protocol.Message {
   		return election_definitions.NewElectionMessage(
   			n.NewMessageHeader(neighbor, protocol.Election),
   			election_definitions.Start,
   			election_definitions.InvalidId, // Not yet set,
   			[]string{string(proposedElectionId)},
   			0,
   		)
   	}

   	func (n *ZlusterNode) newProposalMessage(neighbor, proposal node.NodeId) protocol.Message {
   		return election_definitions.NewElectionMessage(
   			n.NewMessageHeader(neighbor, protocol.Election),
   			election_definitions.Proposal,
   			n.electionCtx.GetId(),
   			[]string{fmt.Sprintf("%d", proposal)},
   			n.electionCtx.CurrentRound(),
   		)
   	}

   	func (n *ZlusterNode) newVoteMessage(neighbor node.NodeId, vote, prune bool) protocol.Message {
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

   const (

   	LFlags_TREEQUESTION uint8 = 0b00000001
   	LFlags_TREEANSWER   uint8 = 0b00000010

   )

   	func (n *ZlusterNode) newLeaderAnnouncement(neighbor node.NodeId, leaderId node.NodeId, electionId election_definitions.ElectionId, hopCount uint64) protocol.Message {
   		return election_definitions.NewElectionMessage(
   			n.NewMessageHeader(neighbor, protocol.Election),
   			election_definitions.Leader,
   			electionId,
   			[]string{
   				strconv.FormatUint(uint64(leaderId), 10),
   				strconv.FormatUint(uint64(LFlags_TREEQUESTION), 10),
   				strconv.FormatUint(uint64(hopCount), 10),
   			},
   			n.electionCtx.CurrentRound(),
   		)
   	}

   	func (n *ZlusterNode) newLeaderResponse(neighbor node.NodeId, leaderId node.NodeId, electionId election_definitions.ElectionId, answer bool) protocol.Message {
   		return election_definitions.NewElectionMessage(
   			n.NewMessageHeader(neighbor, protocol.Election),
   			election_definitions.Leader,
   			electionId,
   			[]string{
   				strconv.FormatUint(uint64(leaderId), 10),
   				strconv.FormatUint(uint64(LFlags_TREEANSWER), 10),
   				strconv.FormatBool(answer),
   			},
   			n.electionCtx.CurrentRound(),
   		)
   	}

   	func (n *ZlusterNode) endElection(leaderId node.NodeId, electionId election_definitions.ElectionId) {
   		newContext := election.NewPostElectionContext(leaderId, electionId)
   		n.electionCtx.SetId(electionId)

   		n.electionCtx.Clear()
   		n.electionCtx.SetLeaderReceived()
   		n.postElectionCtx.Store(newContext)
   	}

   	func (n *ZlusterNode) timestampMessage(message protocol.Message) {
   		h := message.GetHeader()
   		h.MarkTimestamp(n.IncrementClock())
   	}

   // Function used to destroy the node.
   // Deallocating used resources (mainly sockets).

   	func (n *ZlusterNode) destroy() {
   		n.topologyMan.Destroy()
   		if i := n.inputMan; i != nil {
   			i.Stop()
   		}
   		n.cancel()
   	}

   //============================================================================//
   //  Wrappers for NodeConfig component                                         //
   //============================================================================//

   // Returns the ID of the node.

   	func (n *ZlusterNode) getId() node.NodeId {
   		return n.config.GetId()
   	}

   // Returns the port of the node used for the control plane.

   	func (n *ZlusterNode) getControlPort() uint16 {
   		return n.config.GetControlPort()
   	}

   // Returns the port of the node used for the data plane.

   	func (n *ZlusterNode) getDataPort() uint16 {
   		return n.config.GetDataPort()
   	}

   //============================================================================//
   //  Wrappers for TopologyManager component                                    //
   //============================================================================//

   // Adds the node with given ID as a neighbor in the topology. The address must be in formatted as `<ip-address>:<port>`.

   	func (n *ZlusterNode) AddNeighbor(possibleNeighbor node.NodeId, address node.Address) error {
   		if n.getId() == possibleNeighbor {
   			return fmt.Errorf("Cannot set the node as its own neighbor")
   		}
   		err := n.topologyMan.Add(possibleNeighbor, address)
   		go n.ReliableJoin(possibleNeighbor)
   		return err
   	}

   	func (n *ZlusterNode) ReliableJoin(possibleNeighbor node.NodeId) {
   		backoff := 500 * time.Millisecond
   		maxBackoff := 8 * time.Second

   		for {
   			if n.isAlive(possibleNeighbor) {
   				n.logf("join", "%d is ON, I can stop with JOINs", possibleNeighbor)
   				return
   			}

   			n.SendMessageTo(possibleNeighbor, n.newJoinMessage(possibleNeighbor))
   			n.logf("join", "Sending JOIN to %d...", possibleNeighbor)

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

   	func (n *ZlusterNode) acknowledgeNeighborExistence(id node.NodeId, address node.Address) {
   		n.topologyMan.LogicalAdd(id, address)
   	}

   // Removes the neighbor with the given ID from the topology.

   	func (n *ZlusterNode) removeNeighbor(id node.NodeId) error {
   		return n.topologyMan.Remove(id)
   	}

   // Retrieves the IP address of the neighbor with given ID.

   	func (n *ZlusterNode) getNeighborAddress(id node.NodeId) (node.Address, error) {
   		return n.topologyMan.Get(id)
   	}

   // Returns true if this node is neighbors with node that has id ID.
   // This is true after some time in two situations:
   //   - This node previously called AddNeighbor() passing the ID of the other node;
   //   - The other node called AddNeighbor() passing this node's ID (if the IP was correct).

   	func (n *ZlusterNode) IsNeighborsWith(id node.NodeId) bool {
   		return n.topologyMan.Exists(id)
   	}

   // Returns true when the node has at least one neighbor.

   	func (n *ZlusterNode) hasNeighbors() bool {
   		return n.topologyMan.HasNeighbors()
   	}

   // Returns a slice containing the IDs of all the neighboring nodes.

   	func (n *ZlusterNode) neighborList() []node.NodeId {
   		return n.topologyMan.NeighborList()
   	}

   	func (n *ZlusterNode) markAlive(neighbor node.NodeId) {
   		n.topologyMan.UpdateLastSeen(neighbor, time.Now())
   	}

   	func (n *ZlusterNode) isAlive(neighbor node.NodeId) bool {
   		return n.topologyMan.IsAlive(neighbor)
   	}

   // Sends a message to the neighbor node with given ID.
   // Returns an error if the message is mal-formatted.
   func (n *ZlusterNode) sendToNeighbor(id node.NodeId, message protocol.Message) error {

   		n.timestampMessage(message)

   		payload, err := json.Marshal(message)
   		if err != nil {
   			return err
   		}
   		return n.topologyMan.SendTo(id, payload)
   	}

   	func (n *ZlusterNode) poll(timeout time.Duration) error {
   		return n.topologyMan.Poll(timeout)
   	}

   // Retrieves a message from the topology.
   // Returns an error if the message was mal-formatted or if there was a network error.
   // Otherwise it returns the ID of the sender node and a pointer to the message

   	func (n *ZlusterNode) recv() (id node.NodeId, msg protocol.Message, err error) {
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
   		n.UpdateClock(header.TimeStamp)

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

   // Returns the identifier of this node.
   // It's equivalent to call connectionIdentifier(n.getId())

   	func (n *ZlusterNode) connectionIdentifier() string {
   		return n.getId().Identifier()
   	}

   	func extractIdentifier(identifier string) (node.NodeId, error) {
   		return node.ExtractIdentifier([]byte(identifier))
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

   	func (n *ZlusterNode) AddTreeNeighbor(neighborId node.NodeId) error {
   		if n.getId() == neighborId {
   			return fmt.Errorf("Cannot set the node as its own tree neighbor")
   		}
   		if !n.IsNeighborsWith(neighborId) {
   			return fmt.Errorf("Cannot have a non neighboring node as tree neighbor")
   		}
   		return n.treeMan.AddTreeNeighbor(neighborId)
   	}

   // Removes the node with given ID from the children in the SPT.
   // Returns an error if the child was not present.

   	func (n *ZlusterNode) RemoveTreeNeighbor(neighborId node.NodeId) {
   		n.treeMan.RemoveTreeNeighbor(neighborId)
   	}

   	func (n *ZlusterNode) RemoveTreeParent() error {
   		return n.treeMan.RemoveParent()
   	}

   // Returns the number of children in the SPT.

   	func (n *ZlusterNode) GetTreeNeighbors() (parent node.NodeId, children map[node.NodeId]struct{}, hasParent bool) {
   		return n.treeMan.GetTreeNeighbors()
   	}

   // Sets the node with given ID as the parent in the SPT.
   // Returns an error if the ID corresponds to this node or a non neighbor.

   	func (n *ZlusterNode) SetTreeParent(parentId node.NodeId, rootHopCount uint64) error {
   		if n.getId() == parentId {
   			return fmt.Errorf("Cannot set the node as its own tree parent")
   		}
   		if !n.IsNeighborsWith(parentId) {
   			return fmt.Errorf("Cannot have a non neighboring node as tree parent")
   		}
   		n.treeMan.SetParent(parentId, rootHopCount)
   		return nil
   	}

   // Returns the ID of the parent node and a bool stating wheter it has a parent or not.

   	func (n *ZlusterNode) GetTreeParent() (parentId node.NodeId, hasParent bool) {
   		return n.treeMan.GetParent()
   	}

   // Returns true when this node is root in the SPT.

   	func (n *ZlusterNode) isTreeRoot() bool {
   		return n.treeMan.IsRoot()
   	}

   // Returns true when this node is a leaf in the SPT.

   	func (n *ZlusterNode) isTreeLeaf() bool {
   		return n.treeMan.IsLeaf()
   	}

   //============================================================================//
   //  Wrappers for ElectionContext (and PostElectionContext) component(s)       //
   //============================================================================//

   // Returns the state of this node during the election (Idle, Waiting for YoDown or Waiting for YoUp).

   	func (n *ZlusterNode) getElectionState() election_definitions.ElectionState {
   		return n.electionCtx.GetState()
   	}

   // Returns the status of this node during the current round of the election (Source, Internal Node, Sink, Leader or Lost).

   	func (n *ZlusterNode) getElectionStatus() election_definitions.ElectionStatus {
   		return n.electionCtx.GetStatus()
   	}

   // Switches to the given state.

   	func (n *ZlusterNode) switchToElectionState(state election_definitions.ElectionState) {
   		n.electionCtx.SwitchToState(state)
   	}

   // Marks the "START" message as received

   	func (n *ZlusterNode) setElectionStartReceived() {
   		n.electionCtx.SetStartReceived()
   	}

   	func (n *ZlusterNode) hasReceivedElectionStart() bool {
   		return n.electionCtx.HasReceivedStart()
   	}

   // Returns the current round number for this election

   	func (n *ZlusterNode) currentElectionRound() uint {
   		return n.electionCtx.CurrentRound()
   	}

   	func (n *ZlusterNode) stashFutureElectionProposal(sender node.NodeId, proposed node.NodeId, roundEpoch uint) {
   		n.electionCtx.StashFutureProposal(sender, proposed, roundEpoch)
   	}

   	func (n *ZlusterNode) generateElectionId() election_definitions.ElectionId {
   		return election.GenerateId(n.IncrementClock(), n.getId())
   	}

   	func (n *ZlusterNode) setElectionId(id election_definitions.ElectionId) {
   		n.electionCtx.SetId(id)
   	}

   	func (n *ZlusterNode) nextElectionRound() {
   		n.logf("election", "Preparing for next round...")
   		n.electionCtx.NextRound()
   		n.logf("election", "Context prepared, current round is %v and I am %s", n.currentElectionRound(), n.getElectionStatus().String())
   	}
*/
package cluster
