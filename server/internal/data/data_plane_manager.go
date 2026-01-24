package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"server/cluster/control"
	"server/cluster/network"
	"server/cluster/nlog"
	"server/cluster/node"
	"server/cluster/node/protocol"
	"sync"
	"sync/atomic"
	"time"
)

// RequestHandler handles Remote requests (obvy), these are just functions that, given a DataRequest,
// know how to handle it, performing, if possible, the operation requested, and returning a DataResponse
type RequestHandler func(msg *protocol.DataMessage) (*protocol.DataMessage, error)

// SyncHandler are for Nodes that are ReadOnly. Since they cannot perform C_UD operations, jusr _R__.
// However the node needs to have a consistent replica.
// This is achiveved with SyncHandlers, that receive a DataResponse from the Writer, and perform the same opearation
type SyncHandler func(msg *protocol.DataMessage) error

// EpochCache is an interface used to Get, and Save, the epochs that are retrieved from the DB, into cache, for faster consulting of such
type EpochCache interface {
	GetCachedEpoch() uint64
	UpdateEpochCache(uint64)
}

// DataPlaneManager is responsible for handling the Data Plane of the node.
// It is agnostic towards the Control Plane, and is only interested in 'Data' Scope messages.
// Before starting, it needs to be configured appropriately with permissions, especially for the persistence (R/RW).
// This manager implements the Forwarder interface and is responsible for forwarding the request to the appropriare Reader, or Writer.
type DataPlaneManager struct {
	canWrite bool // Write permissions
	canRead  bool // Read permissions

	connMan         node.Connection                         // Component that handles the network communication with other data plane nodes
	pendingRequests map[string](chan *protocol.DataMessage) // Maps known node identities to a channel of messages (to wait for responses)
	requestMutex    sync.RWMutex                            // Mutex for the pending request map

	logger       nlog.Logger        // Logs format strings
	logicalClock *node.LogicalClock // Clock used for partial ordering and synchronization with other data plane nodes.

	hostFinder node.HostFinder                         //Component that helps in retrieving the IP of known data nodes
	runtime    *atomic.Pointer[control.RuntimeContext] // Component that hold: Last election leader (the single Writer), he Roles and a routing table (knows upstreams towards writer and downstreams towards input nodes)

	epochCache EpochCache // Cached epoch

	inbox                   chan *protocol.DataMessage // Channel that receives DataMessages from other nodes and handles them based on content
	outputChannel           chan protocol.OutMessage   // Channel that buffers messages that are to be sent to other nodes
	enstablishedConnections map[node.NodeId]struct{}   // Map of enstablished connections. Since connections are OFF until needed, this map tells wheter a connections is enstablished, and can be used, or needs to be enstabilshed.

	running          atomic.Bool   // Is this manager running?
	internalStopChan chan struct{} // Channel used to manager within itself

	snapshotCallback RequestHandler // Callback that handles Snapshot Requests. Usually only set by the Writer to handle Reader nodes asking for a snapshot.
	warmupCallback   SyncHandler    // Callback that handles Snapshot Responses. Usually only set by the Readers to handle the Writer node's snapshot.

	requestHandlers map[protocol.DataAction]RequestHandler // Maps each Data Action to a Request Handler
	syncHandlers    map[protocol.DataAction]SyncHandler    // Maps each Data Action to a Sync Handler (usually only Write operations)
}

//=========================================================================//
// These functions are helpers used to set the required components of the  //
// DataPlaneManager. They are all required to start the manager correctly  //
//=========================================================================//

// NewDataPlaneManager creates and returns a new, empty, DataPlaneManager
func NewDataPlaneManager(c node.Connection) *DataPlaneManager {

	c.(*network.ConnectionManager).StartMonitoring(func(s string) {
		fmt.Printf("%s is ON", s)
	})

	d := &DataPlaneManager{
		connMan:                 c,
		pendingRequests:         make(map[string](chan *protocol.DataMessage)),
		enstablishedConnections: make(map[node.NodeId]struct{}),
		inbox:                   make(chan *protocol.DataMessage, 500),
		outputChannel:           make(chan protocol.OutMessage, 500),
		requestHandlers:         make(map[protocol.DataAction]RequestHandler),
		syncHandlers:            make(map[protocol.DataAction]SyncHandler),
		running:                 atomic.Bool{},
		internalStopChan:        make(chan struct{}, 1),
	}
	return d
}

// IsReady tells whether the manager is ready, that is, all the components are set
func (d *DataPlaneManager) IsReady() bool {
	return d.logger != nil && d.logicalClock != nil && d.hostFinder != nil && d.runtime != nil && d.epochCache != nil
}

// SetLogger injects a logger component
func (d *DataPlaneManager) SetLogger(l nlog.Logger) {
	d.logger = l
}

// Logf logs a format string using the logger component
func (d *DataPlaneManager) Logf(format string, v ...any) {
	d.logger.Logf(format, v...)
}

// SetEpochCacher injects a Epoch Cacher component
func (d *DataPlaneManager) SetEpochCacher(e EpochCache) {
	d.epochCache = e
}

// SetClock injects a logical clock
func (d *DataPlaneManager) SetClock(clock *node.LogicalClock) {
	d.logicalClock = clock
}

// SetRWPermissions sets the Read and Write permissions
func (d *DataPlaneManager) SetRWPermissions(canRead, canWrite bool) {
	d.canRead = canRead
	d.canWrite = canWrite
}

// SetHostFinder injects a Host Finder component
func (d *DataPlaneManager) SetHostFinder(h node.HostFinder) {
	d.hostFinder = h
}

// SetRuntimenjects a Runtime Context component
func (d *DataPlaneManager) SetRuntime(r *atomic.Pointer[control.RuntimeContext]) {
	d.runtime = r
}

// SetSnapshotCallback njects a Snapshot Callback
func (d *DataPlaneManager) SetSnapshotCallback(r RequestHandler) {
	d.snapshotCallback = r
}

// SetWarmupCallback injects a WarmupCallback
func (d *DataPlaneManager) SetWarmupCallback(s SyncHandler) {
	d.warmupCallback = s
}

// BindPort binds the socket with the given port
func (d *DataPlaneManager) BindPort(port uint16) error {
	if d.connMan == nil {
		return fmt.Errorf("Connection manager is not set on data manager...")
	}
	return d.connMan.Bind(port)
}

// RegisterRequestHandler injects a request handler for the given action
func (d *DataPlaneManager) RegisterRequestHandler(action protocol.DataAction, handler RequestHandler) {
	d.requestHandlers[action] = handler
}

// RegisterSyncHandler injects a sync handler for the given action
func (d *DataPlaneManager) RegisterSyncHandler(action protocol.DataAction, handler SyncHandler) {
	d.syncHandlers[action] = handler
}

// IsRecvNotReadyError tells wheter the error err is ErrRecvNotRead
func IsRecvNotReadyError(err error) bool {
	return errors.Is(err, network.ErrRecvNotReady)
}

// Run starts the data plane manager, in particular:
//   - Starts the async receiver
//   - Starts the async sender
//   - Start handling data messages
func (d *DataPlaneManager) Run(ctx context.Context) {
	d.Logf("Started data plane manager")

	go d.RunAsyncReceiver(ctx)
	go d.RunAsyncSender(ctx)
	go d.HandleIncomingMessage(ctx)
	d.running.Store(true)
}

// Stop stops the data plane manager
func (d *DataPlaneManager) Stop() {
	if d.running.Load() {
		d.internalStopChan <- struct{}{}
		d.running.Store(false)
	}
}

//============================================================================//
//                                                                            //
//	        Handlers for various events (election, topology, tree).           //
//                                                                            //
//        THESE SHOULD BE RUN AS GOROUTINES, THEY ARE MOSTLY BLOCKING         //
//                                                                            //
//============================================================================//

// RunAsyncReceiver receives incoming data messages.
// N.B. This is blocking, run as goroutine preferrably.
func (d *DataPlaneManager) RunAsyncReceiver(ctx context.Context) {
	d.Logf("Started async receiver")
	for {
		select {
		case <-ctx.Done():
			d.Logf("Receiver: Stop signal received")
			return
		case <-d.internalStopChan:
			d.Logf("Sender: Internal stop signal received")
			return
		default:
		}

		if err := d.connMan.Poll(500 * time.Millisecond); err != nil {
			if IsRecvNotReadyError(err) {
				continue
			}
			d.Logf("Polling error %v:", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		id, msg, err := d.Recv()
		if err != nil {
			if IsRecvNotReadyError(err) {
				d.Logf("Recv not ready: %v", err)
				continue
			}
			d.Logf("Message received on dispatcher with an error: %v", err)
			continue
		}

		head := msg.GetHeader()
		d.Logf("Message received on receiver: From %d, %v", id, head.String())

		if head.Scope == protocol.Data {
			if m, ok := msg.(*protocol.DataMessage); ok {
				d.inbox <- m
			}
		}
	}
}

// RunAsyncSender waits on an output channel for messages, and each time one is received, it sends it to the destination neighbor.
// N.B. This is blocking, run as goroutine preferrably.
func (d *DataPlaneManager) RunAsyncSender(ctx context.Context) {
	d.Logf("Started async sender")
	for {

		select {
		case <-ctx.Done():
			d.Logf("Sender: Stop signal received")
			return
		case <-d.internalStopChan:
			d.Logf("Sender: Internal stop signal received")
			return
		case m := <-d.outputChannel:
			d.SendTo(m.DestId, m.Message)
		}
	}
}

// HandleIncomingMessage waits on an input, internal, channel for data messages and handles it based on their type.
//
// Request: it checks if it's destination, if so, and has RW permissions, he can handle it, and then sending it down the path it came up.
// If the node is not the destination, but has R permissions and the operation is READ, and he is correctly synched in time with the writer, he can respond using its replica.
// Otherwise it is sent up (towards the root, Writer)
//
// Response: if it's destinsation, it retrieves a channel corresponding that pending request, and sends the response there.
// Otherwise, it sends it downstrem, towards the correct destination.
//
// Sync 1: message used by the writer to notify the reader of a change to be performed on the replica. If the receving node has R permissions, then
// it performs the operation, and sends it down (there could be other R nodes, not in this current architecture but there could), otherwise its ignored
//
// Snapshot: Request that a Writer can receive from a Reader asking for a DB snapshot. After receiving the snapshot, it sends back a SYNC ALL to the reader
//
// Sync all: Response of Snapshot. The reader sent a snapshot of the DB, so this Reader has to apply it
//
// Rule of thumb:
//   - Requests start from the Input nodes and are forwarded upstream towards the Leader (Writer), but can be interceptec by a persistence node.
//   - Responses start from the Writer, o readers, and go downstream towards the original sender. Thanks to the Next Hops given by the children in the tree, such path is doable.
//
// N.B. This is blocking, run as goroutine preferrably.
func (d *DataPlaneManager) HandleIncomingMessage(ctx context.Context) {

	myId, _ := node.ExtractIdentifier([]byte(d.connMan.GetIdentity()))

	d.Logf("Started message handle")
	for {
		select {
		case <-ctx.Done():
			d.Logf("Message handle: Stop signal received")
			return
		case <-d.internalStopChan:
			d.Logf("Sender: Internal stop signal received")
			return
		case message := <-d.inbox:
			switch message.Type {
			case protocol.REQUEST: // Handle it or forward it up?
				d.Logf("I received a request from %s: %v", message.Header.Sender, message.String())
				if myId == message.DestinationNode && (d.canRead && d.canWrite) { // The request is for me
					d.Logf("This message is for me!")

					// Handle
					response, err := d.HandleRequest(message)
					if err != nil {
						d.Logf("Error occurred in request: %v", err)
						continue
					}

					// Send response back
					d.Logf("Response created: %v", response)
					d.SendDown(response.DestinationNode, response)

					if protocol.ActionRWFlags[response.Action] == protocol.WRITE {
						d.Logf("Sending sync to nexthops: %v", response)
						d.BroadcastSync(response) // Ask replicas to Sync the update as wel;
					}
					continue
				}

				// Not the destination, but it's a read operation and I'm a R-enabled ndoe
				if protocol.ActionRWFlags[message.Action] == protocol.READ && d.canRead { // Not for me, but I could handle it with my replica
					epoch := d.epochCache.GetCachedEpoch()
					if message.Epoch > 0 && message.Epoch <= epoch {
						d.Logf("This message is not for me but i can handle it (read request and up to date)!")
						// Handle
						response, err := d.HandleRequest(message)
						if err != nil {
							d.Logf("Error occurred in request: %v", err)
							continue
						}
						d.Logf("Response created: %v", response)
						d.SendDown(response.DestinationNode, response)
						continue
					}
					d.Logf("This message is not for me, i could handle it  but I'm not up to date")
				}

				// Can't, send it up
				if err := d.SendUp(message); err != nil {
					d.Logf("Error occurred: %v", err)
					continue
				}
				d.Logf("Message forwarded UP")
			case protocol.RESPONSE: // Mine or forward it down?
				d.Logf("I received a response from %s: %v", message.Header.Sender, message.String())

				if myId == message.DestinationNode { // Response is for me, handle it
					d.Logf("This message is for me, send it to its channel")
					d.requestMutex.Lock()
					cha, ok := d.pendingRequests[message.MessageID]
					d.requestMutex.Unlock()

					if ok {
						select {
						case cha <- message:
						default:
						}
					}
					continue
				}
				// Forward it down
				if err := d.SendDown(message.DestinationNode, message); err != nil {
					d.Logf("Error occurred: %v", err)
					continue
				}
				d.Logf("Message forwarder DOWN")
			case protocol.SYNC_ONE:
				if d.canRead { // Having read pemissions means that I should sync the updates given by the writer
					d.Logf("I received a Sync1 message from %s: %v", message.Header.Sender, message.String())
					if err := d.HandleSyncOne(message); err != nil {
						d.Logf("ERROR SYNC: %v", err)
						continue
					}
					d.SendDownToAll(message)
				}
			case protocol.SNAPSHOT:
				if d.canWrite {
					d.Logf("I received a snapshot request from %s. He needs a database copy", message.Header.Sender)
					megaMsg, err := d.HandleSnapshot(message)
					if err != nil {
						d.Logf("ERROR SNAPSHOT %v", err)
						continue
					}
					megaMsg.Type = protocol.SYNC_ALL
					d.Logf("Sending to %s: %v", megaMsg.Header.Destination, megaMsg)
					d.Logf("Outcome: %v", d.SendDownTo(megaMsg.DestinationNode, megaMsg))
				}
			case protocol.SYNC_ALL:
				if d.canRead && message.DestinationNode == myId { // Having read pemissions means that I should sync the updates given by the writer
					d.Logf("I received a snapshot respose from %s. Syncing", message.Header.Sender)
					if err := d.HandleSyncAll(message); err != nil {
						d.Logf("ERROR SYNCALL %v", err)
						continue
					}
					d.Logf("No error after SYNCALL")
				}
			}
		}
	}
}

// SendUp sends a deep copy of the message original upstream.
// An upstream next hop is searched in the routing table, if found, the message is sent to the buffered out channel.
// When successful, error is nil
func (d *DataPlaneManager) SendUp(original protocol.Message) error {
	nextHop, ok := d.runtime.Load().GetRouting().GetUpstreamNextHop()
	if !ok {
		return fmt.Errorf("No upstream next hop is set")
	}
	new := d.CopyMessageWithNewHeader(original, nextHop)
	d.Logf("Next hop for upstream is %d", nextHop)
	d.BufferOut(nextHop, new)
	return nil
}

// SendDown sends a deep copy of the message original downstream, towards a specific destination.
// A downstream next hop for the destination is searched in the routing table, if found, the message is sent to the buffered out channel.
// When successful, error is nil
func (d *DataPlaneManager) SendDown(destination node.NodeId, original protocol.Message) error {
	nextHop, ok := d.runtime.Load().GetRouting().GetDownstreamNextHop(destination)
	if !ok {
		return fmt.Errorf("The downstream node %d has no next hop", destination)
	}

	new := d.CopyMessageWithNewHeader(original, nextHop)
	d.Logf("Next hop for downstream %d is %d", destination, nextHop)
	d.BufferOut(nextHop, new)
	return nil
}

// SendDownTo sends the original message to the actual destination, if that is a nexthop, that is, reachable with 1 hop.
// When successful, error is nil
func (d *DataPlaneManager) SendDownTo(destination node.NodeId, original protocol.Message) error {
	nextHops := d.runtime.Load().GetRouting().GetDownstreamHops()
	if _, ok := nextHops[destination]; !ok {
		return fmt.Errorf("%d is not a next hop %v", destination, nextHops)
	}

	d.Logf("Sending to nextHop %d", destination)
	d.BufferOut(destination, original)
	return nil
}

// SendDownToAll sends a deep copy of original to each next hop, that is, reachable with 1 hop.
// When successful, error is nil
func (d *DataPlaneManager) SendDownToAll(original protocol.Message) {
	nextHops := d.runtime.Load().GetRouting().GetDownstreamHops()
	for nextHop := range nextHops {
		new := d.CopyMessageWithNewHeader(original, nextHop)
		d.Logf("Sending to nextHop %d", nextHop)
		d.BufferOut(nextHop, new)
	}
}

// SendWarmupRequest creates a warmup (SNAPSHOT) request message and sends it to the Writer
func (d *DataPlaneManager) SendWarmupRequest() {
	nodeId, _ := node.ExtractIdentifier([]byte(d.connMan.GetIdentity()))
	leader := d.runtime.Load().GetLastElection().GetLeaderID()

	snapshotReq := &protocol.DataMessage{
		Header:          *protocol.NewMessageHeader(nodeId.Identifier(), leader.Identifier(), protocol.Data),
		MessageID:       "",
		OriginNode:      nodeId,
		DestinationNode: leader,
		Type:            protocol.SNAPSHOT,
		Action:          protocol.ActionNone,
		Status:          protocol.UNDEFINED,
		Payload:         []byte{},
		Epoch:           0,
		ErrorMessage:    "",
	}

	d.BufferOut(leader, snapshotReq)
}

// HandleRequest calls a request handler for the given REQUEST message, and executes it, returning the response.
// When successful, err = nil
func (d *DataPlaneManager) HandleRequest(req *protocol.DataMessage) (rep *protocol.DataMessage, err error) {
	handler, ok := d.requestHandlers[req.Action]
	if !ok {
		return nil, fmt.Errorf("No handler registered for opertation {%v}.", req.Action)
	}
	return handler(req)
}

// BroadcastSync takes a response, copies it and makes it a SYNC_ONE message, sending it to all the children (possible replicas)
func (d *DataPlaneManager) BroadcastSync(rep *protocol.DataMessage) {
	syncMsg := rep.Clone().(*protocol.DataMessage)
	syncMsg.Type = protocol.SYNC_ONE
	d.SendDownToAll(syncMsg)
}

// HandleSnapshot calls, if set, the snapshot callback, returning the response.
// When successful, err = nil
func (d *DataPlaneManager) HandleSnapshot(req *protocol.DataMessage) (rep *protocol.DataMessage, err error) {
	if d.snapshotCallback == nil {
		return nil, fmt.Errorf("Snapshot callback is not set")
	}
	return d.snapshotCallback(req)
}

// HandleSyncOne calls a sync handler for the given SYNC_ONE message, and executes it.
// When successful, err = nil
func (d *DataPlaneManager) HandleSyncOne(syn *protocol.DataMessage) error {
	handler, ok := d.syncHandlers[syn.Action]
	if !ok {
		return fmt.Errorf("No handler registered for opertation {%v}.", syn.Action)
	}
	if err := handler(syn); err != nil {
		return err
	}
	old := d.epochCache.GetCachedEpoch()
	d.epochCache.UpdateEpochCache(syn.Epoch)
	new := d.epochCache.GetCachedEpoch()
	d.Logf("CALLING UPDATE CACHE, OLD VAL: %d, NEW VAL: %d", old, new)
	return nil
}

// HandleSyncAll calls, if set, the warmup callback
// When successful, err = nil
func (d *DataPlaneManager) HandleSyncAll(syn *protocol.DataMessage) error {
	if d.warmupCallback == nil {
		return fmt.Errorf("Warmup callback is not set")
	}

	if err := d.warmupCallback(syn); err != nil {
		return err
	}
	old := d.epochCache.GetCachedEpoch()
	d.epochCache.UpdateEpochCache(syn.Epoch)
	new := d.epochCache.GetCachedEpoch()
	d.Logf("CALLING UPDATE CACHE, OLD VAL: %d, NEW VAL: %d", old, new)
	return nil
}

func (d *DataPlaneManager) ExecuteRemote(id string, action protocol.DataAction, epoch uint64, payload []byte) (*protocol.DataMessage, error) {
	runtime := d.runtime.Load()

	d.Logf("Need to forward the request{Id{%s}, payload{%s}}", id, string(payload))

	parent, ok := runtime.GetRouting().GetUpstreamNextHop()
	if !ok {
		return nil, fmt.Errorf("Request could not be forwarded")
	}

	nodeId, _ := node.ExtractIdentifier([]byte(d.connMan.GetIdentity()))

	request := protocol.NewRequestMessage(
		protocol.NewMessageHeader(
			nodeId.Identifier(),
			parent.Identifier(),
			protocol.Data,
		),
		id,
		nodeId,
		runtime.GetLastElection().GetLeaderID(),
		action,

		payload,
		epoch,
	)

	d.Logf("Waiting response for Request{%s}", id)

	responseChannel := make(chan *protocol.DataMessage, 1)
	d.requestMutex.Lock()
	d.pendingRequests[id] = responseChannel
	d.requestMutex.Unlock()

	defer func() {
		d.requestMutex.Lock()
		delete(d.pendingRequests, id)
		d.requestMutex.Unlock()
	}()

	d.BufferOut(parent, request)

	select {
	case response := <-responseChannel:
		d.Logf("Received response for Request{%s}: %s", id, response.String())
		return response, nil
	case <-time.After(15 * time.Second):
		d.Logf("Timeout of 15 seconds occurred for Request{%d}", id)
		return nil, fmt.Errorf("Timeout: could not satisfy request in time")
	}
}

//============================================================================//
//  Utility wrappers                                                          //
//============================================================================//

// CopyMessageWithNewHeader creates and returns a message that has the same body as the original one, but has the header changed in the following manner:
//
//	The source is 'this' node
//	The destination is nextHop
//
// This should be used when we want to forward a message without modifying anything inside, just the header (for correct routing)
func (d *DataPlaneManager) CopyMessageWithNewHeader(message protocol.Message, nextHop node.NodeId) protocol.Message {
	newMsg := message.Clone()
	newMsg.SetHeader(protocol.NewMessageHeader(
		d.connMan.GetIdentity(),
		nextHop.Identifier(),
		protocol.Data,
	))
	return newMsg
}

// BufferOut sends a pair {destId, message} to a buffered channel
func (d *DataPlaneManager) BufferOut(destId node.NodeId, message protocol.Message) {
	d.outputChannel <- protocol.OutMessage{DestId: destId, Message: message}
}

// SendTo sends message to the destId.
// When successful error is nil
func (d *DataPlaneManager) SendTo(destId node.NodeId, message protocol.Message) error {

	h := message.GetHeader()
	h.MarkTimestamp(d.logicalClock.IncrementClock())
	message.SetHeader(h)

	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}

	d.Logf("About to send the message to %d... %v", destId, d.enstablishedConnections)

	// The connections are lazy. The sockets are created instantly and also binded, however as for the connection,
	// one is not setup (child -> parent) until a message has to be sent. After that, however, the connection remains open.
	if _, ok := d.enstablishedConnections[destId]; !ok {
		host, err := d.hostFinder.GetHost(destId)
		if err != nil {
			return err
		}

		// Retrieving upstream port, building tcp://<host>:<port>
		port := d.runtime.Load().GetRouting().GetUpstreamPort()
		s := node.FullAddr(host, port)

		d.connMan.ConnectTo(s)
		time.Sleep(2 * time.Second)
		d.enstablishedConnections[destId] = struct{}{}
		d.Logf("Connection did not exist... Creating one with (%d => %s:%d)...[%s]", destId, host, port, s)
	}

	d.Logf("Sending to (%s)...", destId.Identifier())
	return d.connMan.SendTo(destId.Identifier(), payload)
}

// Recv retrieves a message from the connection.
// Returns an error if the message was mal-formatted or if there was a network error.
// Otherwise it returns the ID of the sender node and, pointer to the message and err = nil
func (d *DataPlaneManager) Recv() (sourceId node.NodeId, msg protocol.Message, err error) {
	d.Logf("Waiting to recv a message...")
	payload, err := d.connMan.Recv()
	if err != nil {
		return 0, nil, err
	}

	sourceId, _ = node.ExtractIdentifier(payload[0])
	payload = payload[2:]

	d.Logf("Recv'd a message from %d", sourceId)

	var headerWrapper struct {
		Header protocol.MessageHeader `json:"header"`
	}
	if err := json.Unmarshal(payload[0], &headerWrapper); err != nil {
		return 0, nil, err
	}

	header := headerWrapper.Header
	d.logicalClock.UpdateClock(header.TimeStamp)

	switch header.Scope {
	case protocol.Data:
		msg = &protocol.DataMessage{}
	default:

		return 0, nil, fmt.Errorf("Unknown message type: %v", header.Scope)
	}

	if err := json.Unmarshal(payload[0], msg); err != nil {
		return 0, nil, err
	}
	return sourceId, msg, nil
}
